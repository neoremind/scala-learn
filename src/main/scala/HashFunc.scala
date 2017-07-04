
import org.slf4j.LoggerFactory

case class Partner(ns: String, hashSalt: String) {
  def getNs: String = ns
}

/**
  * Created by xu.zhang on 7/1/17.
  */
trait PartnerConfig {

  val name = ""
  val nameSpace = ""
  lazy val partnerMap: Map[String, Partner] = dmpClient.map(p => {
    println("call here")
    p.getNs -> p
  }).toMap

  def salt: String = dmpConfig.hashSalt

  def dmpConfig: Partner = {
    println("###")
    partnerMap(nameSpace)
  }

  def dmpClient = List(Partner("AC", "sdfsdfd"), Partner("CMP", "53423434"))

  def hiveTableName: String = s"Ingest_Hash_Table_$name"

  def hashFunction(salt: String): Int => String

  def hash(userId: Int): String = hashFunction(salt)(userId)

  def calcHash(userId: Int, partnerType: String, salt: String) = hashFunction(salt)(userId)

}

object Acxiom extends PartnerConfig {
  override val name = "Acxiom"
  override val nameSpace = "AC"

  def hashFunction(salt: String) = (userId: Int) => userId.toString + "ll"
}

object Bluekai extends PartnerConfig {
  override val name = "Bluekai"
  override val nameSpace = "CMP"

  def hashFunction(salt: String) = (userId: Int) => (salt + userId.toString) + "kk"
}

object Dummy extends PartnerConfig {
  override val name = "Dummy"

  def hashFunction(salt: String) = (userId: Int) => (userId.toString + "_" + salt) + "jj"
}

object TestHash extends App {
  val logger = LoggerFactory.getLogger(TestHash.getClass)
  val a = Acxiom
  val b = Bluekai
  println(a.salt)
  println(a.hash(1))
  println(a.hash(12))
  println(a.hash(13))
  println(a.calcHash(1, "Acxiom", "nnnnn"))
  println(b.calcHash(2, "Bluekai", "mmmmm"))
}