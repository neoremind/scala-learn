package com.baidu.bes.pmp

import com.baidu.bes.pmp.PMPOrderPriceCalc._
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

import scala.sys.process._

/**
 * @author zhangxu
 */
class PMPOrderPriceCalcAll extends FlatSpec with MockFactory {

  "PMPOrderPriceCalc" should "calculate all tu stats" in {
    val tuStatFile = "/Users/baidu/work/bes_pd_auto_cpm/tu_stat_month"
    val mockOrderFile = "/Users/baidu/work/bes_pd_auto_cpm/mock_orders"
    val csvFile = "/Users/baidu/work/bes_pd_auto_cpm/mock_orders_results.csv"

    ("rm " + mockOrderFile) !

    val orders = PMPOrderPriceCalc.FileReader(tuStatFile).fromFile((s: String) => {
      val fields = s.split("\t")
      val domain = fields(2)
      val sizeId = fields(8).getSizeBitMask
      //PDOrder(1, "PD", "xxx.com", domain, 0, sizeId, 0, "DSP") //计算domain+size
      PDOrder(1, "PD", "xxx.com", domain, 0, 0, 0, "DSP") //计算domain
    }).distinct

    println("Start to calculate " + orders.size + " orders")

    PMPOrderPriceCalc.LocalFileWriter(mockOrderFile).write(orders, (o: PDOrder) => {
      o.orderKeyIndex + "\t" +
        o.orderId + "\t" +
        o.domainName + "\t" +
        o.domain + "\t" +
        o.creativeStyles + "\t" +
        o.sizeId + "\t" +
        o.dspId + "\t" +
        o.dspName
    })

    PMPOrderPriceCalc.execute(tuStatFile, mockOrderFile, CsvOutput(LocalFileWriter(csvFile)))
  }

  "PMPOrderPriceCalc" should "calculate online orders" in {
    val tuStatFile = "/Users/baidu/work/bes_pd_auto_cpm/tu_stat_month"
    val onlineOrderFile = "/Users/baidu/work/bes_pd_auto_cpm/online_orders"
    val csvFile = "/Users/baidu/work/bes_pd_auto_cpm/online_orders_results.csv"

    PMPOrderPriceCalc.execute(tuStatFile, onlineOrderFile, CsvOutput(LocalFileWriter(csvFile)))
  }

}
