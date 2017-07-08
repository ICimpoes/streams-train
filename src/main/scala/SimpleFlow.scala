import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * @see http://doc.akka.io/docs/akka/2.5.3/scala/stream/index.html
  */
object SimpleFlow extends App {

  implicit val system = ActorSystem("MyAkkaSystem")
  implicit val materializer = ActorMaterializer()


  val input = List("a", "bb", "c", "d", "e", "f", "g", "hb", "a", "b", "c", "d", "e", "f", "gb", "h", "a", "b", "cd", "d", "e", "f", "gb", "h", "a", "asdb", "cx", "d", "esa", "f", "g", "h")

  Source.fromIterator(() => input.toIterator)
    .zipWithIndex
    .runForeach(println)

  Source.fromIterator(() => input.toIterator)
    .zipWithIndex
    .runFold(0 -> 0) { case ((one, more), (str, ind)) =>
      if (str.length > 1) one -> (more + 1)
      else (one + 1) -> more
    }.onComplete(println)


}
