package com.baidu.bes.pmp

import com.baidu.bes.pmp.PMPOrderPriceCalc.{TuStat, FileReader}

/**
 * @author zhangxu
 */
object SizeTranslator extends App {

  val size = FileReader("src/main/scala/com/baidu/bes/pmp/size.txt").fromFile((s: String) => {
    (s, s.getSizeBitMask)
  })

  size.foreach(println(_))

  implicit class get(s: String) {
    def getSizeBitMask = {
      try {
        val widthAndHeight = s.split("\\*")
        widthAndHeight(1).toLong << 32 | widthAndHeight(0).toLong
      } catch {
        case e: Exception => {
          e.printStackTrace()
          0L
        }
      }
    }
  }

}
