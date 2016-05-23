package com.baidu.bes.pmp


import com.baidu.bes.pmp.PMPOrderPriceCalc.{SqlOutput, LocalFileWriter, CsvOutput, SysOutConsoleWriter}

/**
 * 程序主入口
 */
object Main extends App {

  val tuStatFilePath = "/Users/baidu/work/bes_pd_auto_cpm/tu_stat_month"
  val ordersFilePath = "/Users/baidu/work/bes_pd_auto_cpm/offline_orders"
  //val output = CsvOutput(SysOutConsoleWriter)
  //val output = CsvOutput(LoggerWriter)
  //val output = CsvOutput(LocalFileWriter("/Users/baidu/work/results.csv"))
  val output = SqlOutput(LocalFileWriter("/Users/baidu/work/bes_pd_auto_cpm/offline_orders.sql"))
  PMPOrderPriceCalc.execute(tuStatFilePath, ordersFilePath, output)

}

//object Main extends App {
//
//  val tuStatFilePath = args(0)
//  val ordersFilePath = args(1)
//  val output = SqlOutput(LocalFileWriter(args(2)))
//  val msgOutput = CsvOutput(LocalFileWriter(args(3)))
//  PMPOrderPriceCalc.execute(tuStatFilePath, ordersFilePath, output, msgOutput)
//
//}
