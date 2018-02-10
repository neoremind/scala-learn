package kraps

import kraps.spark.MyRdd

/**
  * Copy from `org.apache.spark.util.Utils`
  *
  * 展示了SparkUI上面看到的每个stage的代码片段是如何获取的
  *
  * short form和long form打印如下：
  * ```
  * flatMap at CallSiteTest.scala:15
  * ----------
  *kraps.spark.MyRdd.flatMap(Utils.scala:10)
  *kraps.CallSiteTest$.main(CallSiteTest.scala:15)
  *kraps.CallSiteTest.main(CallSiteTest.scala)
  *sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
  *sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
  *sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
  *java.lang.reflect.Method.invoke(Method.java:606)
  *com.intellij.rt.execution.application.AppMain.main(AppMain.java:147)
  * ----------
  * ```
  *
  * Created by xu.zhang on 7/22/17.
  */
object CallSiteTest {

  def main(args: Array[String]): Unit = {
    val rdd = new MyRdd("myrdd")
    rdd.flatMap()
    println(rdd.getCallSite().shortForm)
    println("----------")
    println(rdd.getCallSite().longForm)
    println("----------")
    println(rdd.getCallSite())
  }

}

