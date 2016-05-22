import scala.sys.process._

/**
 * @author zhangxu
 */
object RunCmd {

  def main(args: Array[String]) {
    "ls -l" !
  }

  /**
   * http://stackoverflow.com/questions/12772605/scala-shell-commands-with-pipe
   */
  def runCmd(cmd: String, dir: String) {
    "cd " + dir + " ; " + cmd !
  }

  def runCmd2(cmd: String) {
    println(cmd)
    cmd !
  }

}
