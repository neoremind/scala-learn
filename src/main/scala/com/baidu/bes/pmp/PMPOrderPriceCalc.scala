package com.baidu.bes.pmp


import java.io.{File, PrintWriter}
import org.slf4j.LoggerFactory

import scala.collection.parallel.ParSeq
import scala.io.Source
import scala.sys.process._

/**
 * @author zhangxu
 */
object PMPOrderPriceCalc {

  private[this] val logger = LoggerFactory.getLogger(getClass.getName)

  /** 截取百分之多少的展现来计算Cpm */
  val impressionPercentage = 0.25

  /** 订单Cpm单价的溢价系数 */
  val premiumCoefficient = 1.5

  def main(args: Array[String]) {
    val tuStats = FileReader.fromFile("/Users/baidu/work/tu_stat_month", (s: String) => {
      val fields = s.split("\t")
      val domain = fields(2)
      val dspId = fields(3).toInt
      val imp = fields(4).toLong
      val clk = fields(5).toLong
      val cost = fields(6).toLong
      val resType = fields(7).toInt
      val size = bitMaskSizeTransform(fields(7))
      TuStat(domain, dspId, resType, size, imp, clk, cost)
    }, (c: TuStat) => c.impression > 0 && c.impression > c.clk)

    // domain或者domain+size粒度聚合为Map
    // 例如("58.com_300*200" -> Map[Key, TuStat]))
    // 尺寸会转换为int
    // 下面两个作为缓存常驻内存
    val domain2TuStat = tuStats.groupBy(DomainKey)
    val domainSize2TuStat = tuStats.groupBy(DomainSizeKey)

    val domain = "baixing.com"
    val size = "300*300"
    //val sizeId = bitMaskSizeTransform(size)
    val sizeId = 0
    //runCmd2("ls -l")
    //    runCmd2("mysql -h10.100.75.52 -P8746 -ubeidoudb -pbeidou -Done_report --default-character-set=utf8 -e \"select * " +
    //      "from tu_stat_month;\" > /Users/baidu/output")
    //runCmd2("sh /Users/baidu/work/export.sh ")
    //    val tuStats = List(TuStat("58.com", 1, 1, bitMaskSizeTransform("200*200"), 100, 10, 500),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("300*300"), 100, 10, 400),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("400*400"), 200, 20, 6000),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("500*500"), 100, 10, 500),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("200*200"), 300, 30, 2000),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("300*300"), 100, 10, 500),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("400*400"), 500, 50, 3000),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("500*500"), 100, 10, 500),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("300*300"), 300, 40, 3000),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("300*300"), 100, 10, 500),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("300*300"), 100, 10, 500),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("300*300"), 100, 10, 200),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("300*300"), 100, 10, 100),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("300*300"), 100, 10, 50),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("300*300"), 100, 10, 80),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("300*300"), 100, 10, 700),
    //      TuStat("58.com", 1, 1, bitMaskSizeTransform("300*300"), 100, 10, 300),
    //      TuStat("58.com", 2, 1, bitMaskSizeTransform("300*300"), 100, 10, 250),
    //      TuStat("58.com", 2, 1, bitMaskSizeTransform("300*300"), 50, 10, 120),
    //      TuStat("ifeng.com", 1, 1, bitMaskSizeTransform("500*500"), 100, 10, 500),
    //      TuStat("ifeng.com", 1, 1, bitMaskSizeTransform("300*300"), 400, 40, 1000),
    //      TuStat("ifeng.com", 1, 1, bitMaskSizeTransform("300*300"), 100, 10, 500)
    //    )

    logger.info("All tu stats num=" + tuStats.size)
    // 计算全网cpm
    val allStat = tuStats.par.foldLeft(newTuStat)(_.merge(_))
    val allAvgCpm = allStat.cpm
    logger.info("All tu stats=" + allStat)
    logger.info("All avg cpm=" + allAvgCpm)

    logger.info(calcOrderPrice(if (sizeId == 0) domain2TuStat else domainSize2TuStat, domain, sizeId).toString)

    val resList = tuStats.map(_.domain).distinct.par.map(calcOrderPrice(domain2TuStat, _, 0))
    FileWriter.toFile("/Users/baidu/work/result", resList, (c: CalcResult) => {
      c.toString
    })

  }

  def calcOrderPrice(tuStats: Map[String, List[TuStat]], domain: String, sizeId: Long): CalcResult = {
    import ConcatStringable._

    val key = if (sizeId == 0) toConcatString(domain) else toConcatString(domain, sizeId)
    logger.info("Start to calculate " + key)

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
    logger.info("25% impression=" + top25PercImp)

    // 分渠道最高CPM的tu stat
    val byDspIdTuStatList = tuStatList.groupBy(_.dspId).mapValues(t => t.foldLeft(newTuStat)(_.merge(_)))
    byDspIdTuStatList.foreach(t => t._2.dspId = t._1)
    val byDspIdMaxCpmTuStat = byDspIdTuStatList.maxBy(_._2.cpm)
    logger.info("byDspId tu stat=" + byDspIdTuStatList)
    logger.info("byDspId max cpm tu stat=" + byDspIdMaxCpmTuStat + ", dsp=" + DspIdDefine.getLiteral(byDspIdMaxCpmTuStat._1))

    // 将TuStat list中的元素（ 已经降序排列 )，按照25 % 分位值做截断 ， 被截断的tail的展现和消费等比例缩减
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
    logger.debug("original top 25% imp tu stat=" + topTuStatList)
    val perc = top25PercImp / topTuStatList.last.impression //计算截断的百分比
    logger.debug("cut percentage=" + perc)
    //TODO 这里修改了引用的元素值，违背的不可变性
    topTuStatList.last.impression = top25PercImp.toLong //默认展现截断
    topTuStatList.last.cost = (topTuStatList.last.cost * perc).toLong //按照比例缩减cost
    logger.debug("cut top 25% imp tu stat=" + topTuStatList)

    // 打印domain+size cpm排序后top25%展现的cpm
    val top25TuStatCpm = topTuStatList.foldLeft(newTuStat)(_.merge(_)).cpm
    logger.info("top25 imp tu stat cpm=" + top25TuStatCpm)

    // 返回计算结果
    CalcResult(domain, sizeId, (top25TuStatCpm * premiumCoefficient).toLong, originalTuStatCpm, byDspIdMaxCpmTuStat
      ._2.cpm, byDspIdMaxCpmTuStat._1, byDspIdMaxCpmTuStat._2.impression)
  }


  def newTuStat(): TuStat = {
    TuStat("-", 0, 0, 0L, 0L, 0L, 0L)
  }

  def bitMaskSizeTransform(size: String): Long = {
    try {
      val widthAndHeight = size.split("\\*")
      widthAndHeight(1).toLong << 32 | widthAndHeight(0).toLong
    } catch {
      case e: Exception => 0L
    }
  }

  case class CalcResult(domain: String,
                        sizeId: Long,
                        premiumCpm: Long,
                        allCpm: Long,
                        byDspIdMaxCpm: Long,
                        maxCpmDspId: Int,
                        maxCpmDspImpression: Long) {
    override def toString: String = {
      "CalcResult" + List("domain=" + domain,
        "sizeId=" + sizeId,
        "premiumCpm=" + premiumCpm,
        "allCpm=" + allCpm,
        "byDspIdMaxCpm=" + byDspIdMaxCpm,
        "maxCpmDspId=" + maxCpmDspId,
        "maxCpmDspImpression=" + maxCpmDspImpression).mkString("(", ",", ")")
    }
  }

  /**
   * BFP & SSP原始数据，后续会使用ODS
   *
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

  object DomainDspIdKey extends (TuStat => String) {
    def apply(s: TuStat) = {
      ConcatStringable.toConcatString(s.domain, s.dspId)
    }
  }

  object DomainSizeDspIdKey extends (TuStat => String) {
    def apply(s: TuStat) = {
      ConcatStringable.toConcatString(s.domain, s.sizeId, s.dspId)
    }
  }

  /**
   * http://stackoverflow.com/questions/12772605/scala-shell-commands-with-pipe
   */
  def runCmd(cmd: String, dir: String) {
    "cd " + dir + " ; " + cmd !
  }

  def runCmd2(cmd: String) {
    println(cmd)
    cmd !
  }

  object FileReader {
    def fromFile[V](d: String, f: String => V, c: V => Boolean): List[V] = {
      Source.fromFile(d, "UTF-8").getLines().map(f).filter(c).toList
    }
  }

  object FileWriter {
    def toFile[V](d: String, v: ParSeq[V], f: V => String) = {
      val writer = new PrintWriter(new File(d))
      for (i <- v) {
        writer.println(i)
      }
      writer.close()
    }
  }


  /** 分DSP渠道 */
  object DspIdDefine {
    private val innerMap = Map((1, "NOVA"),
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
    private val innerMap = Map((1, "PC"),
      (2, "WAP"),
      (3, "APP"),
      (4, "PC贴片"),
      (5, "APP贴片"),
      (0, "未知"))

    def getLiteral(resType: Int): String = {
      innerMap.getOrElse(resType, "-")
    }
  }

}