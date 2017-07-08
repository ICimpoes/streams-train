import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.{ActorAttributes, ActorMaterializer, Supervision}

object DeciderExample extends App {

  implicit val system = ActorSystem("MyAkkaSystem")
  implicit val materializer = ActorMaterializer()

  /**
    * @see http://doc.akka.io/docs/akka/2.5.3/scala/stream/stream-error.html
    */
  val decider: Supervision.Decider = {
    case _: ValidationException => Supervision.Resume
    case _: NullPointerException => Supervision.Restart
    case _ => Supervision.Stop
  }


  def input() = List("valid1", "invalid1", "valid2", "valid3", "invalid2", "valid4", "null", "valid5", "stop", "valid6").toIterator

  val flow = Flow[String]
    .scan("") { (acc, str) =>
      if (str.contains("invalid")) throw ValidationException(s"invalid: $str")
      else if (str == "null") throw new NullPointerException
      else if (str == "stop") throw new RuntimeException
      else acc + str
    }
    .withAttributes(ActorAttributes.supervisionStrategy(decider))

  Source.fromIterator(input).via(flow).runForeach(println)

}
