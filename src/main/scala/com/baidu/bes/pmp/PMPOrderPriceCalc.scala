package com.baidu.bes.pmp

import java.io._
import java.util.Date

import org.slf4j.LoggerFactory

import scala.io.Source

/**
 * PMP order price calculator
 *
 * @author zhangxu
 */
object PMPOrderPriceCalc {

  private[this] val logger = LoggerFactory.getLogger(getClass.getName)

  /** 截取百分之多少的展现来计算Cpm */
  val impressionPercentage = 0.25

  /** 订单Cpm单价的溢价系数 */
  val premiumCoefficient = 1.5

  /** 单价异常的范围，指溢价后的单价和最大CPM单价渠道的比值 */
  val auditThreshold = (3.0, 1.0)

  /** 文件的编码 */
  val fileEncoding = "GBK"

  /**
   * 计算订单价格
   * @param tuStatFile tu数据文件
   * @param ordersFile 订单文件
   * @param output 输出，可以是sql或者csv文件，或者是日志，利用桥接模式的Output+writer组合
   * @param msgOutput 信息的输出
   * @return (成功订单，失败订单，待审订单)
   */
  def execute(tuStatFile: String, ordersFile: String, output: Output, msgOutput: Output = null):
  (List[CalcResult], List[CalcResult], List[CalcResult]) = {
    // 文件格式：7       9223372032562353936     caoliu1.com     0       247     12      194     1       300*250
    val tuStats = FileReader(tuStatFile).fromFile((s: String) => {
      val fields = s.split("\t")
      val domain = fields(2)
      val dspId = fields(3).toInt
      val imp = fields(4).toLong
      val clk = fields(5).toLong
      val cost = fields(6).toLong
      val resType = fields(7).toInt
      val sizeId = fields(8).getSizeBitMask
      TuStat(domain, dspId, resType, sizeId, imp, clk, cost)
    }, (c: TuStat) => c.impression > 0 && c.impression >= c.clk)

    // 文件格式：pre_order_id    order_id        ssp_name        ssp_url creative_styles adsize  dsp_id  dsp_name
    val orders = FileReader(ordersFile).fromFile((s: String) => {
      val fields = s.split("\t")
      val orderKeyIndex = fields(0).toLong
      val orderId = fields(1)
      val domainName = fields(2)
      val domain = fields(3)
      val creativeStyles = fields(4).toInt
      val sizeId = fields(5).toLong
      val dspId = fields(6).toInt
      val dspName = fields(7)
      PDOrder(orderKeyIndex, orderId, domainName, domain, creativeStyles, sizeId, dspId, dspName)
    })

    val res = calcAllOrderPrice(tuStats, orders)
    output.output(res._1, "Output success results, num=" + res._1.size)
    output.output(res._2, "Output fail results, num=" + res._2.size)
    output.output(res._3, "Output audit results, num=" + res._3.size)

    if (msgOutput != null) {
      msgOutput.output(res._1, "Successful PD order num is " + res._1.size)
      msgOutput.output(res._3, "Auditable PD order num is " + res._3.size)
      msgOutput.output(res._2, "Failed to cacludate PD order num is " + res._2.size)
    }

    res
  }

  def calcAllOrderPrice(tuStats: List[TuStat], orders: List[PDOrder]): (List[CalcResult], List[CalcResult], List[CalcResult]) = {
    // 1）计算全网cpm，仅仅是为了打印使用
    val allStat = tuStats.par.foldLeft(newTuStat)(_.merge(_))
    val allAvgCpm = allStat.cpm
    logger.info("All tu stats num=" + tuStats.size)
    logger.info("All tu stats=" + allStat)
    logger.info("All avg cpm=" + allAvgCpm)

    // 2）domain或者domain+size粒度聚合为Map，因为同一个尺寸下面会存在多个TU广告位，一个TU也可以投在多个domain下面
    // 例如("58.com_300*200" -> List[TuStat]))
    // 尺寸会转换为int
    // 下面两个作为缓存常驻内存
    val domain2TuStatCache = tuStats.groupBy(DomainKey)
    val domainSize2TuStatCache = tuStats.groupBy(DomainSizeKey)

    // 3）遍历所有PD订单，计算溢价后的CPM订单价格
    logger.info("Start to calculate " + orders.size + " orders")
    var num = 0;
    val results = orders.map(o => {
      try {
        calcOrderPrice(domain2TuStatCache, domainSize2TuStatCache, o)
      } catch {
        case e: Throwable => {
          val msg = o + "\t" + e.getMessage
          logger.error(msg)
          newCalcResult(o.orderKeyIndex, o.orderId, o.domainName, o.domain, TrafficTypeDefine.WEB, o.sizeId).isFail(true).msg(msg)
        }
      } finally {
        num = num + 1
        logger.info("Finish " + num);
      }
    })

    val partitionedResults = results.filter(_.isFail == false).partition(isNeedAudit(_))
    val failResults = results.filter(_.isFail == true)
    val successResults = partitionedResults._2
    val auditResults = partitionedResults._1

    logger.info("Results: " + results.size)
    logger.info("Success results: " + successResults.size)
    logger.info("Normal results: " + successResults.filter(!_.isCpmLessThan100).size)
    logger.info("CpmLessThan100 results: " + successResults.filter(_.isCpmLessThan100).size)
    logger.info("Fail results:" + failResults.size)
    logger.info("Audit results:" + auditResults.size)

    (successResults, failResults, auditResults)
  }

  /**
   * 计算单价
   * @param domain2TuStatCache domain对应的所有tu stats
   * @param domainSize2TuStatCache domain+size对应的所有tu stats
   * @param order PD订单
   * @return 单价结果
   */
  def calcOrderPrice(domain2TuStatCache: Map[String, List[TuStat]], domainSize2TuStatCache: Map[String, List[TuStat]], order: PDOrder): CalcResult = {
    import ConcatStringable._

    val tuStats = if (order.sizeId == 0L) domain2TuStatCache else domainSize2TuStatCache
    val key = if (order.sizeId == 0) toConcatString(order.domain) else toConcatString(order.domain, order.sizeId)
    logger.info("Start to calculate " + key + ", size=" + order.sizeId.getLiteralSize)
    if (!tuStats.contains(key)) {
      throw new IllegalArgumentException("No size found for " + order.domain + " " + order.sizeId.getLiteralSize)
    }

    // 根据domain+size获取本次要计算的TuStat list，按照cpm降序排列
    val tuStatList = tuStats(key).sortBy(-_.cpm)
    if (logger.isDebugEnabled) {
      logger.info("Filter tu stats by " + key + " and sort by CPM desc")
      for (t <- tuStatList) {
        logger.debug(t.toString)
      }
    }
    logger.info("Filter out tu stats num=" + tuStatList.size)

    // 合并List所有的TuStat，在展现、点击、消费上做聚合，新生成一个对象mergedTuStat
    val mergedTuStat = tuStatList.foldLeft(newTuStat)(_.merge(_))
    logger.info("Merged tu stats=" + mergedTuStat)

    // 打印domain+size 总体的cpm
    val originalTuStatCpm = mergedTuStat.cpm
    logger.info("all tu stat cpm=" + originalTuStatCpm)

    // 根据上一步的聚合结果mergedTuStat计算出展现的impressionPercentage(25%)分位值
    var top25PercImp = mergedTuStat.impression * impressionPercentage
    logger.info("25% impression=" + top25PercImp.toLong)

    // 分渠道最高CPM的tu stat，这里使用主域，不加入尺寸
    val byDspIdTuStatList = domain2TuStatCache(toConcatString(order.domain)).groupBy(_.dspId).mapValues(t => t.foldLeft(newTuStat)(_.merge(_)))
    byDspIdTuStatList.foreach(t => t._2.dspId = t._1)
    val byDspIdMaxCpmTuStat = byDspIdTuStatList.maxBy(_._2.cpm)
    logger.info("byDspId tu stat=" + byDspIdTuStatList)
    logger.info("byDspId max cpm tu stat=" + byDspIdMaxCpmTuStat + ", dsp=" + DspIdDefine.getLiteral(byDspIdMaxCpmTuStat._1))

    // 将TuStat list中的元素（ 已经降序排列 )，按照25%分位值做截断，被截断的tail的展现和消费等比例缩减
    var isNextRoundQuit = false
    def takeCondition(tuStat: TuStat): Boolean = {
      if (isNextRoundQuit) {
        false
      } else {
        if (top25PercImp - tuStat.impression < 0) {
          isNextRoundQuit = true
        } else {
          top25PercImp = top25PercImp - tuStat.impression
        }
        true
      }
    }
    val topTuStatList = tuStatList.takeWhile(takeCondition)
    logger.debug("original top 25% imp tu stat num=" + topTuStatList.size)
    logger.debug("original top 25% imp tu stat=" + topTuStatList)
    val perc = top25PercImp / topTuStatList.last.impression //计算截断的百分比
    logger.debug("cut percentage=" + perc)
    //Note：这里修改了引用的元素值，违背的不可变性，因此暂时保留了topTuStatList.last中的值，后续还原
    val cachedTopTuLast = (topTuStatList.last.impression, topTuStatList.last.cost)
    topTuStatList.last.impression = top25PercImp.toLong //默认展现截断
    topTuStatList.last.cost = (topTuStatList.last.cost * perc).toLong //按照比例缩减cost
    logger.debug("cut top 25% imp tu stat=" + topTuStatList)

    // 打印domain+size cpm排序后top25%展现的cpm
    val top25TuStatCpm = topTuStatList.foldLeft(newTuStat)(_.merge(_)).cpm
    logger.info("top25 imp tu stat cpm=" + top25TuStatCpm)
    topTuStatList.last.impression = cachedTopTuLast._1
    topTuStatList.last.cost = cachedTopTuLast._2

    // 返回计算结果
    var premiumCpm = (top25TuStatCpm * premiumCoefficient).toLong
    if (premiumCpm < 100) premiumCpm = 100
    CalcResult(order.orderKeyIndex, order.orderId, order.domainName, order.domain, TrafficTypeDefine.WEB, order.sizeId,
      premiumCpm, originalTuStatCpm, byDspIdMaxCpmTuStat._2.cpm, byDspIdMaxCpmTuStat._1,
      DspIdDefine.getLiteral(byDspIdMaxCpmTuStat._1),
      byDspIdMaxCpmTuStat._2.impression, isCpmLessThan100 = premiumCpm < 100)
  }

  def isNeedAudit(r: CalcResult): Boolean = {
    val c = r.premiumCpm.toDouble / r.byDspIdMaxCpm.toDouble
    logger.debug("premiumCpm/byDspIdMaxCpm=" + c)
    !r.isFail && (c > auditThreshold._1 || c < auditThreshold._2)
  }

  def newTuStat(): TuStat = {
    TuStat("-", 0, 0, 0L, 0L, 0L, 0L)
  }

  def newCalcResult(preOrderId: Long, orderId: String, domainName: String, domain: String, trafficType: Int, sizeId: Long): CalcResult = {
    CalcResult(preOrderId, orderId, domainName, domain, trafficType, sizeId, 0L, 0L, 0L, 0, "-", 0L)
  }

  implicit class getSizeId(s: String) {
    def getSizeBitMask = {
      s match {
        case "0" => 0L
        case _ => try {
          val widthAndHeight = s.split("\\*")
          widthAndHeight(1).toLong << 32 | widthAndHeight(0).toLong
        } catch {
          case e: Exception => {
            logger.warn(e.getMessage)
            0L
          }
        }
      }
    }
  }

  implicit class getSizeLiteral(l: Long) {
    def getLiteralSize = {
      if (l == 0L) {
        ""
      } else {
        val width = l & 0xFFFFFFFFL
        val height = l >> 32
        width + "*" + height
      }
    }
  }

  /**
   * 计算结果
   * @param preOrderId 所谓的整型的自增主键
   * @param orderId 订单id
   * @param domainName 主域名称
   * @param domain 主域
   * @param trafficType 流量类型
   * @param sizeId 广告位尺寸，格式：300*200经过转换得到的整型
   * @param premiumCpm 经过溢价后的CPM
   * @param originalAllCpm 主域+广告位尺寸下的所有流量的平均CPM
   * @param byDspIdMaxCpm 主域+广告位尺寸某个DSP渠道的最大CPM
   * @param maxCpmDspId 主域+广告位尺寸下最大CPM的DSP渠道
   * @param maxCpmDspIdName 主域+广告位尺寸下最大CPM的DSP渠道名称
   * @param maxCpmDspImpression 主域+广告位尺寸下最大CPM的DSP渠道的展现
   * @param isCpmLessThan100 是否CPM小于100，即1块钱
   * @param isFail 是否计算失败
   * @param msg 计算失败的原因
   */
  case class CalcResult(preOrderId: Long,
                        orderId: String,
                        domainName: String,
                        domain: String,
                        trafficType: Int,
                        sizeId: Long,
                        premiumCpm: Long,
                        originalAllCpm: Long,
                        byDspIdMaxCpm: Long,
                        maxCpmDspId: Int,
                        maxCpmDspIdName: String,
                        maxCpmDspImpression: Long,
                        var isCpmLessThan100: Boolean = false,
                        var isFail: Boolean = false,
                        var msg: String = "") {

    def isFail(isFail: Boolean): CalcResult = {
      this.isFail = isFail
      this
    }

    def isCpmLessThan100(isCpmLessThan100: Boolean): CalcResult = {
      this.isCpmLessThan100 = isCpmLessThan100
      this
    }

    def msg(msg: String): CalcResult = {
      this.msg = msg
      this
    }

    override def toString: String = {
      "CalcResult" + List("orderId=" + orderId,
        "domainName=" + domainName,
        "domain=" + domain,
        "sizeId=" + sizeId,
        "premiumCpm=" + premiumCpm,
        "originalAllCpm=" + originalAllCpm,
        "byDspIdMaxCpm=" + byDspIdMaxCpm,
        "maxCpmDspId=" + maxCpmDspId,
        "maxCpmDspIdName=" + maxCpmDspIdName,
        "maxCpmDspImp=" + maxCpmDspImpression,
        "isFail=" + isFail,
        "msg=" + msg)
        .mkString("(", ",", ")")
    }
  }

  /**
   * BFP & SSP原始数据，后续会使用ODS
   * @param domain 主域，例如58.com
   * @param dspId DSP ID，也就是分DSP渠道
   * @param resType 资源类型
   * @param sizeId 广告位尺寸，格式：300*200经过转换得到的整型
   * @param impression 近30日分渠道的展现
   * @param clk 近30日分渠道的点击
   * @param cost 近30日分渠道的消费
   *
   * @see DspIdDefine
   * @see ResTypeDefine
   */
  case class TuStat(domain: String,
                    var dspId: Int,
                    resType: Int,
                    sizeId: Long,
                    var impression: Long,
                    var clk: Long,
                    var cost: Long) {

    def merge(t: TuStat): TuStat = {
      this.impression += t.impression
      this.clk += t.clk
      this.cost += t.cost
      this
    }

    /** 计算cpm */
    def cpm: Long = {
      if (impression == 0) {
        0L
      } else {
        (cost / (impression / 1000.0)).toLong
      }
    }

    override def toString: String = {
      "TuStat" + List(domain, dspId, resType, sizeId, impression, clk, cost, cpm).mkString("(", ",", ")")
    }
  }

  /**
   * PD订单
   * @param orderKeyIndex 主键ID
   * @param orderId 订单id，字符串，例如BesPreOrder-1446006566-8204或者PDBS-1446121836-8703
   * @param domainName 主域，例如：百度
   * @param domain 主域，例如：baidu.com
   * @param creativeStyles: 创意类型  //暂时无用
   * @param sizeId 尺寸，经过计算后转为整型的
   * @param dspId dsp id
   * @param dspName dsp名称
   */
  case class PDOrder(orderKeyIndex: Long,
                     orderId: String,
                     domainName: String,
                     domain: String,
                     creativeStyles: Int,
                     sizeId: Long,
                     dspId: Int,
                     dspName: String)

  object ConcatStringable {
    def toConcatString(a: Any*): String = {
      a.mkString("_")
    }
  }

  object DomainKey extends (TuStat => String) {
    def apply(s: TuStat) = {
      ConcatStringable.toConcatString(s.domain)
    }
  }

  object DomainSizeKey extends (TuStat => String) {
    def apply(s: TuStat) = {
      ConcatStringable.toConcatString(s.domain, s.sizeId)
    }
  }

  case class FileReader(filePath: String) {
    def fromFile[V](f: String => V, c: V => Boolean): List[V] = {
      Source.fromFile(filePath, fileEncoding).getLines().map(f).filter(c).toList
    }

    def fromFile[V](f: String => V): List[V] = {
      fromFile(f, (v: V) => true)
    }
  }

  trait Output {
    def output(s: Seq[CalcResult], msgInfo: String)
  }

  val currDate = new java.text.SimpleDateFormat("yyyy-MM-dd").format(new Date())

  case class SqlOutput(writer: Writer) extends Output {
    def output(s: Seq[CalcResult], msgInfo: String) = {
      logger.info("Sql output begin...")
      val f = (c: CalcResult) => {
        if (!c.isFail && !isNeedAudit(c)) {
          "update one_main.pmp_pre_order set order_price = %8.2f, pre_state=%d, mod_time='%s' where order_id = '%s';"
            .format(c.premiumCpm / 100.0, OrderStateDefine.DSP_NEED_CONFIRM, currDate, c.orderId)
        } else if (!c.isFail && isNeedAudit(c)) {
          // 注意insert ignore 如果存在了审核订单则不更新，不能使用replace into，因为audit status可能不为0
          ("insert ignore into one_main.pmp_pre_order_audit(pre_order_id,order_id,ssp_name,ssp_url,traffic_type," +
            "ad_size,advice_price,adjusted_price,average_price,channel_highest_price,highest_price_channel,highest_price_pv," +
            "audit_status,add_time,mod_time) values(%d, '%s','%s','%s',%d,%s,%8.2f,0.0,%8.2f,%8.2f,%d,%s,0,'%s','%s');")
            .format(c.preOrderId, c.orderId, c.domainName, c.domain, c.trafficType, c.sizeId, c.premiumCpm / 100.0,
              c.originalAllCpm / 100.0, c.byDspIdMaxCpm / 100.0, c.maxCpmDspId, c.maxCpmDspImpression, currDate, currDate)
        } else if (c.isFail) {
          "update one_main.pmp_pre_order set pre_state=%d, mod_time='%s' where order_id = '%s';".format(OrderStateDefine.NOT_FOR_BUYING, currDate, c.orderId)
        } else {
          ""
        }
      }
      writer.write(s, f)
      logger.info("Sql output end")
    }
  }

  case class CsvOutput(writer: Writer) extends Output {
    def output(s: Seq[CalcResult], msgInfo: String) = {
      logger.info("Csv output begin...")
      val f = (c: CalcResult) => {
        if (c.isFail) {
          Array(c.preOrderId, c.orderId, c.domain, c.sizeId, c.msg).mkString("\t")
        } else {
          Array(c.preOrderId, c.orderId, c.domain, c.sizeId, c.premiumCpm, c.originalAllCpm, c.byDspIdMaxCpm,
            c.maxCpmDspId, c.maxCpmDspIdName, c.maxCpmDspImpression).mkString("\t")
        }
      }
      writer.write(s, f, msgInfo)
      logger.info("Csv output end")
    }
  }

  trait Writer {
    def write[V](s: Seq[V], f: V => String, header: String = "")
  }

  case class LocalFileWriter(filePath: String) extends Writer {
    override def write[V](s: Seq[V], f: V => String, header: String = "") = {
      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath, true), fileEncoding));
      if (!"".equals(header)) {
        writer.write(header + "\n")
      }
      for (i <- s) {
        writer.write(f(i) + "\n")
      }
      writer.close()
    }
  }

  object LoggerWriter extends Writer {
    override def write[V](s: Seq[V], f: V => String, header: String = "") = {
      logger.info(header)
      for (i <- s) {
        logger.info(f(i))
      }
    }
  }

  object SysOutConsoleWriter extends Writer {
    override def write[V](s: Seq[V], f: V => String, header: String = "") = {
      println(header)
      for (i <- s) {
        println(f(i))
      }
    }
  }

  /** 分DSP渠道 */
  object DspIdDefine {
    val innerMap = Map((1, "NOVA"),
      (4, "LU"),
      (5, "PC-DSP"),
      (6, "MDSP"),
      (0, "3rd-DSP"))

    def getLiteral(dspId: Int): String = {
      innerMap.getOrElse(dspId, "-")
    }
  }

  /** 资源类型 */
  object ResTypeDefine {
    val innerMap = Map((1, "PC"),
      (2, "WAP"),
      (3, "APP"),
      (4, "PC贴片"),
      (5, "APP贴片"),
      (0, "未知"))

    def getLiteral(resType: Int): String = {
      innerMap.getOrElse(resType, "-")
    }
  }

  /** 流量类型 */
  object TrafficTypeDefine {
    val WEB = 1;
    val APP = 2;

    val innerMap = Map((WEB, "WEB"),
      (APP, "APP"))

    def getLiteral(trafficType: Int): String = {
      innerMap.getOrElse(trafficType, "-")
    }
  }

  /** 订单状态 */
  object OrderStateDefine {
    //新建订单状态
    val NEW = 0;
    //待DSP确认，一般是本程序计算完OK的状态
    val DSP_NEED_CONFIRM = 6;
    //无法预订
    val NOT_FOR_BUYING = 7;
  }

}