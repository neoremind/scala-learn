package spark.sql

import org.apache.spark.sql.SparkSession

/**
  * CREATE TABLE IF NOT EXISTS default.salary (
  * team STRING COMMENT 'team name',
  * name STRING COMMENT 'play name',
  * salary INT COMMENT 'salary',
  * position STRING COMMENT 'position')
  * COMMENT 'NLBB Salaries 2005'
  * ROW FORMAT DELIMITED
  * FIELDS TERMINATED BY '\t'
  * LINES TERMINATED BY '\n'
  * STORED AS TEXTFILE;
  *
  *
  * bin/spark-submit --class spark.sql.RunDatasetQuery \
  * --master yarn \
  * --deploy-mode cluster \
  * --driver-memory 1g  \
  * --executor-memory 1g  \
  * --executor-cores 1  \
  * /Users/xu.zhang/IdeaProjects/scala-learn/target/scala-2.11/scala-learn_2.11-1.0.jar
  *
  * +--------------------+------------------+
  * |                team|       avg(salary)|
  * +--------------------+------------------+
  * |     Cincinnati Reds|         2063086.1|
  * |       New York Mets|3752067.4444444445|
  * |    San Diego Padres|2260386.8928571427|
  * |San Francisco Giants|3469211.5384615385|
  * |   Milwaukee Brewers|        1597393.32|
  * |  Pittsburgh Pirates| 1361892.857142857|
  * |        Chicago Cubs| 3108319.035714286|
  * |Arizona Diamondbacks|2308487.6296296297|
  * |      Atlanta Braves|        3458292.08|
  * |    Colorado Rockies|1605166.6666666667|
  * | Los Angeles Dodgers|2767966.6666666665|
  * |Washington Nationals|1619383.3333333333|
  * | St. Louis Cardinals|         3542570.5|
  * |     Florida Marlins| 2237364.222222222|
  * |      Houston Astros|2953038.4615384615|
  * |Philadelphia Phil...| 3673923.076923077|
  * +--------------------+------------------+
  *
  * 下面的逻辑等同于select team, AVG(salary) from salary group by team;
  */
object RunDatasetQuery {

  case class Salary(team: String, name: String, salary: Int, position: String)

  val sql1 =
    """
      |SELECT team,name,salary,position FROM salary
    """.stripMargin

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.metastore.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    spark.sql(sql1).as[Salary].groupBy("team").avg("salary").show()
  }
}
