name := "scala-learn"

version := "1.0"

scalaVersion := "2.11.8"

//resolvers +=
//  "Sonatype OSS Snapshots" at "http://maven.scm.baidu.com:8081/nexus/content/groups/public"

//libraryDependencies += "com.typesafe.akka" %% "akka-remote" % "2.3.6"
//libraryDependencies += "io.spray" % "spray-can" % "1.1.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-remote" % "2.3.6",
  "net.databinder.dispatch" %% "dispatch-core" % "0.11.2",
  "io.spray" % "spray-can" % "1.1.3",
  "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test",
  "org.slf4j" % "slf4j-log4j12" % "1.6.0",
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-streaming" % "2.2.1",
  "org.apache.spark" %% "spark-hive" % "2.2.1",
  "org.apache.spark" %% "spark-mllib" % "2.2.1",
  "com.jsuereth" %% "scala-arm" % "2.0"
  //  "net.databinder" %% "dispatch-http" % "0.8.5",
  //  "net.databinder" %% "dispatch-lift-json" % "0.8.5",
  //  "com.baidu.ub.msoa.stub" % "101001.mediaService" % "2016032300-SNAPSHOT",
  //  "com.baidu.ub.msoa.stub" % "101006.siteIndustryService" % "2015122400-SNAPSHOT",
  //  "com.baidu.ub.msoa" % "service-container" % "20160316-SNAPSHOT",
)
