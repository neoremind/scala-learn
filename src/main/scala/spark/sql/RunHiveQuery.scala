package spark.sql

import org.apache.spark.sql.SparkSession

/**
  * cp conf/hive-site.xml ../spark-2.2.1-bin-without-hadoop/conf/
  *
  * add conf/hive-site.xml to resources
  *
  * bin/spark-submit --class spark.sql.RunHiveQuery \
  * --master yarn \
  * --deploy-mode cluster \
  * --driver-memory 1g  \
  * --executor-memory 1g  \
  * --executor-cores 1  \
  * /Users/xu.zhang/IdeaProjects/scala-learn/target/scala-2.11/scala-learn_2.11-1.0.jar
  */
object RunHiveQuery {

  val sql1 =
    """
      |SELECT * FROM salary
    """.stripMargin

  /**
    * TPC-H query
    */
  val sql2 =
    """
      |select
      |	l_returnflag,
      |	l_linestatus,
      |	sum(l_quantity) as sum_qty,
      |	sum(l_extendedprice) as sum_base_price,
      |	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
      |	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
      |	avg(l_quantity) as avg_qty,
      |	avg(l_extendedprice) as avg_price,
      |	avg(l_discount) as avg_disc,
      |	count(*) as count_order
      |from
      |	lineitem
      |where
      |	l_shipdate <= '1998-09-16'
      |group by
      |	l_returnflag,
      |	l_linestatus
      |order by
      |	l_returnflag,
      |	l_linestatus
    """.stripMargin

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.metastore.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql(sql2).show();
  }
}
