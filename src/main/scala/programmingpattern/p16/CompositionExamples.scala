package programmingpattern.p16


/**
  * 函数生成器，组成
  *
  * [[https://twitter.github.io/scala_school/pattern-matching-and-functional-composition.html]]
  */
object CompositionExamples extends App {

  val appendA = (s: String) => s + "a"
  val appendB = (s: String) => s + "b"
  val appendC = (s: String) => s + "c"

  val appendCBA = appendA compose appendB compose appendC
  println(appendCBA("1"))

  val appendABC = appendA andThen appendB andThen appendC
  println(appendABC("1"))

  ///////////////////

  case class HttpRequest(
                          headers: Map[String, String],
                          payload: String,
                          principal: Option[String] = None)

  def checkAuthorization(request: HttpRequest) = {
    val authHeader = request.headers.get("Authorization")
    val mockPrincipal = authHeader match {
      case Some(headerValue) => Some("AUser")
      case _ => None
    }
    request.copy(principal = mockPrincipal)
  }

  def logFingerprint(request: HttpRequest) = {
    val fingerprint = request.headers.getOrElse("X-RequestFingerprint", "")
    println("FINGERPRINT=" + fingerprint)
    request
  }

  def composeFilters(filters: Seq[Function1[HttpRequest, HttpRequest]]) =
    filters.reduce {
      (allFilters, currentFilter) => allFilters compose currentFilter
    }

  val filterChain = composeFilters(Seq(checkAuthorization, logFingerprint))
  val requestHeaders = Map("Authorization" -> "Auth", "X-RequestFingerprint" -> "fingerprint")
  val request = HttpRequest(requestHeaders, "body")
  println(filterChain(request))

  ///////////////////


  /////////////////////

  case class Email(
                    subject: String,
                    text: String,
                    sender: String,
                    recipient: String)

  val addMissingSubject = (email: Email) =>
    if (email.subject.isEmpty) email.copy(subject = "No subject")
    else email
  val checkSpelling = (email: Email) =>
    email.copy(text = email.text.replaceAll("your", "you're"))
  val removeInappropriateLanguage = (email: Email) =>
    email.copy(text = email.text.replaceAll("dynamic typing", "**CENSORED**"))
  val addAdvertismentToFooter = (email: Email) =>
    email.copy(text = email.text + "\nThis mail sent via Super Awesome Free Mail")

  val pipeline = Function.chain(Seq(
    addMissingSubject,
    checkSpelling,
    removeInappropriateLanguage,
    addAdvertismentToFooter))

  println(pipeline(Email("", "hello, my friend, your, dynamic typing", "ap@gmail.com", "haha@gmail.com")))


}
