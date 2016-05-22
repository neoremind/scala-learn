package com.baidu.bes.pmp

import com.baidu.bes.pmp.PMPOrderPriceCalc.{SqlOutput, SysOutConsoleWriter, CsvOutput}
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

/**
 * @author zhangxu
 */
class PMPOrderPriceCalcTest extends FlatSpec {

  "PMPOrderPriceCalc" should "should calculate simple data successfully" in {
    val results = PMPOrderPriceCalc.execute("src/test/scala/com/baidu/bes/pmp/data/tu_stat_month_simple.txt",
      "src/test/scala/com/baidu/bes/pmp/data/orders_simple.txt", SqlOutput(SysOutConsoleWriter))
    val succRes = results._1
    succRes.foreach(println _)
    succRes should have size 1
    succRes(0).domain should equal("abc.com")
    succRes(0).sizeId should equal(257698038720L)
    succRes(0).premiumCpm should equal(265)
    succRes(0).originalAllCpm should equal(110)
    succRes(0).byDspIdMaxCpm should equal(204)
    succRes(0).maxCpmDspIdName should equal("NOVA")
    succRes(0).maxCpmDspImpression should equal(81650)
    succRes(0).isFail should equal(false)
    results._2 should have size 0
    results._3 should have size 0
  }

  "PMPOrderPriceCalc" should "should audit successfully" in {
    val results = PMPOrderPriceCalc.execute("src/test/scala/com/baidu/bes/pmp/data/tu_stat_month_audit.txt",
      "src/test/scala/com/baidu/bes/pmp/data/orders_audit.txt", SqlOutput(SysOutConsoleWriter))
    val auditRes = results._3
    auditRes.foreach(println _)
    auditRes should have size 1
    auditRes(0).domain should equal("abc.com")
    auditRes(0).sizeId should equal(1073741824300L)
    auditRes(0).premiumCpm should equal(4500)
    auditRes(0).originalAllCpm should equal(3000)
    auditRes(0).byDspIdMaxCpm should equal(1458)
    auditRes(0).maxCpmDspIdName should equal("NOVA")
    auditRes(0).maxCpmDspImpression should equal(2081650)
    auditRes(0).isFail should equal(false)
    results._1 should have size 0
    results._2 should have size 0
  }

}
