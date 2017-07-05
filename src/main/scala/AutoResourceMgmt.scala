import java.io.{BufferedReader, FileInputStream, FileReader}

/**
  * [[https://github.com/jsuereth/scala-arm/wiki]]
  */
object AutoResourceMgmt extends App {

  // run FileTest first to gen test.txt

  /**
    * Imperative Style
    */

  import resource._
  // Copy input into output.
  import resource._

  for (input <- managed(new FileInputStream("test.txt"))) {
    // Code that uses the input as a FileInputStream
  }
  for {
    input <- managed(new java.io.FileInputStream("test.txt"))
    output <- managed(new java.io.FileOutputStream("test2.txt"))
  } {
    val buffer = new Array[Byte](512)

    // 尾递归
    def read(): Unit = input.read(buffer) match {
      case -1 => ()
      case n =>
        output.write(buffer, 0, n)
        read()
    }

    read()
  }

  /**
    * Monadic style
    */
  val first_ten_bytes = managed(new FileInputStream("test.txt")) map {
    input =>
      val buffer = new Array[Byte](10)
      input.read(buffer)
      buffer
  }
  first_ten_bytes.opt.get.foreach(println)

  ///////////////////////////

  def using[T <: {def close()}](resource: T)(block: T => Unit) {
    try {
      block(resource)
    } finally {
      if (resource != null) resource.close()
    }
  }

  using(new BufferedReader(new FileReader("test.txt"))) { r =>
    var count = 0
    while (r.readLine != null) count += 1
    println(count)
  }

  /////////////////////
  // 有返回值的

  def using2[T <: {def close()}, X](resource: T)(block: T => X) = {
    try {
      block(resource)
    } finally {
      if (resource != null) resource.close()
    }
  }

  val count = using2(new BufferedReader(new FileReader("test.txt"))) { r =>
    var count = 0
    while (r.readLine != null) count += 1
    //    println(count)
    count
  }

  println(count)

}
