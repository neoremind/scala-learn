package etl


import scala.io.Source

/**
 * @author zhangxu
 */
object SSPETL {

  def main(args: Array[String]) {
    //    Source.fromFile("ssp.txt", "UTF-8").foreach {
    //      print
    //    }

    val res = Source.fromFile("ssp.txt", "UTF-8").getLines().map { line =>
      val fields = line.split("\\s+")
      if (fields.length != 8) {
        invalidTuDomain
      } else {
        val tu = fields(0).toLong
        val domain = fields(3)
        val spv = fields(5).toInt
        val clk = fields(6).toInt
        val cost = (fields(7).toDouble * 100).toInt
        TuDomain(tu, domain, spv, clk, cost)
      }
    }.filter(_.tu == invalidTuDomain).toList.groupBy(_.key).par.mapValues(t => t.reduce(_.merge(_)))

    res.foreach(println)
  }

  case class TuDomain(tu: Long, domain: String, var spv: Int, var clk: Int, var cost: Int) {

    def merge(t: TuDomain): TuDomain = {
      this.spv += t.spv
      this.clk += t.clk
      this.cost += t.cost
      Thread.sleep(1000)
      this
    }

    def key: String = tu + "_" + domain

  }

  val invalidTuDomain = new TuDomain(0, "", 0, 0, 0)

}
