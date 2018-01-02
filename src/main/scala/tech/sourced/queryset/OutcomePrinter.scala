package tech.sourced.queryset

trait OutcomePrinter {

  val HeadMessage: String
  val Colour: String

  def printMessage(message: String): Unit = {
    println(Colour + s"$HeadMessage $message" + Console.RESET)
  }

}
