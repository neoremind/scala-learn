package programmingpattern.p21

import scala.io.Source
import java.lang.Process
import scala.collection.JavaConversions._
import scala.language.postfixOps

/**
  * 外部DSL：SQL, ANTLR
  * 内部DSL：构建于某些通用语言之上，并且其存在形式也受限于宿主语言的语法约束
  *
  * 通过
  * 1、后缀和中缀位置使用方法，定义操作符
  * 2、隐式转换
  * 3、伴生对象做类的工厂
  */
object Example extends App {

  case class CommandResult(status: Int, output: String, error: String)

  class Command(commandParts: List[String]) {
    def run() = {
      val processBuilder = new ProcessBuilder(commandParts)
      val process = processBuilder.start()
      val status = process.waitFor()
      val outputAsString =
        Source.fromInputStream(process.getInputStream()).mkString("")
      val errorAsString =
        Source.fromInputStream(process.getErrorStream()).mkString("")
      CommandResult(status, outputAsString, errorAsString)
    }
  }

  object Command {
    def apply(commandString: String) = new Command(commandString.split("\\s").toList)
  }

  implicit class CommandString(commandString: String) {
    def run() = Command(commandString).run
  }

  println("ls -al" run)

}

object ExtendedExample extends App {

  case class CommandResult(status: Int, output: String, error: String)

  class Command(commandParts: List[String]) {
    def run = {
      val processBuilder = new ProcessBuilder(commandParts)
      val process = processBuilder.start()
      val status = process.waitFor()
      val outputAsString = Source.fromInputStream(process.getInputStream()).mkString("");
      val errorAsString = Source.fromInputStream(process.getErrorStream()).mkString("");
      CommandResult(status, outputAsString, errorAsString)
    }

  }

  implicit class CommandVector(existingCommands: Vector[String]) {
    def run = {
      val pipedCommands = existingCommands.mkString(" | ")
      Command("/bin/sh", "-c", pipedCommands).run
    }

    def pipe(nextCommand: String): Vector[String] = {
      existingCommands :+ nextCommand
    }
  }

  object Command {
    def apply(commandString: String) = new Command(commandString.split("\\s").toList)

    def apply(commandParts: String*) = new Command(commandParts.toList)
  }

  implicit class CommandString(firstCommandString: String) {
    def run = Command(firstCommandString).run

    def pipe(secondCommandString: String) =
      Vector(firstCommandString, secondCommandString)
  }

  println(("ls -al" pipe "wc -l" run).output)

}
