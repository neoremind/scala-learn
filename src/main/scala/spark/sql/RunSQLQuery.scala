package spark.sql

import org.apache.spark.sql.SparkSession

/**
  * cp conf/hive-site.xml conf/hive-env.sh ../spark-2.2.1-bin-without-hadoop/conf/
  *
  */
object RunSQLQuery {

  def main(args: Array[String]): Unit = {
    // warehouseLocation points to the default location for managed databases and tables
    //val warehouseLocation = new File("/user/hive/warehouse").getAbsolutePath
    val warehouseLocation = "/user/hive/warehouse"

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.sql.hive.metastore.version", "2.1.0")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("SELECT * FROM salary").show();
  }
}
