import java.nio.file.{Path, Paths}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, Framing, GraphDSL, Merge, Partition, RunnableGraph, Sink, Source, UnzipWith, ZipWith}
import akka.util.ByteString

import scala.collection.immutable.Seq
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/**
  * @see http://doc.akka.io/docs/akka/2.5.3/scala/stream/stream-graphs.html
  */
object GraphTrain extends App {

  implicit val system = ActorSystem("MyAkkaSystem")
  implicit val materializer = ActorMaterializer()

  import system.dispatcher

  val input = List("a", "bb", "c", "d", "e", "f", "g", "hb", "a", "b", "c", "d", "e", "f", "gb", "h", "a", "b", "cd", "d", "e", "f", "gb", "h", "a", "asdb", "cx", "d", "esa", "f", "g", "h")

  val src = Source.fromIterator(() => input.toIterator)
    .zipWithIndex.map((Record.apply _).tupled)

  val validateFlow = Flow[Record].map { r => if (r.line.length > 1) Left(s"Invalid record: $r") else Right(r) }

  val prt = Partition[Record](2, _.lineNr.toInt % 2)

  val flow1 = Flow[Record]
    .mapAsyncUnordered(parallelism = 3)(r => Future {
      Thread.sleep(1000)
      r.copy(line = r.line.toUpperCase)
    })

  val flow2 = Flow[Record]
    .mapAsyncUnordered(parallelism = 3) { r =>
      Future {
        Thread.sleep(1000)
        r
      }
    }

  val collectErrors = Flow[Either[String, Record]].collect { case Left(err) => err }

  val closedShapeGraph = RunnableGraph.fromGraph(GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._

      // Source
      val source: Outlet[Record] = builder.add(src).out

      val P = builder.add(prt)
      val F1 = builder.add(flow1)
      val F2 = builder.add(flow2)
      val V = builder.add(validateFlow)

      val M = builder.add(Merge[Record](2))

      val B = builder.add(Broadcast[Either[String, Record]](2))
      val CE = builder.add(collectErrors)
      val S = builder.add(Flow[Either[String, Record]].fold(0 -> 0) { case ((s, f), res) => if (res.isRight) (s + 1, f) else (s, f + 1) })

      val I = builder.add(Sink.foreach(println))
      val I1 = builder.add(Sink.foreach[(Int, Int)] { case (s, f) => println(s"success: $s, fail: $f") })
      // Flows

      // Graph
      // source -> partition(by line nr) -> flow1 (uppercase) -> merge result -> validate -> broadcast results -> collect errors(left)
      //                                 -> flow2             -> merge result                                  -> aggregate result (nr of lefts and number of rights)

      /*                       */ source ~> P
      /*            */ B <~ V <~ M <~ F1 <~ P
      /*                      */ M <~ F2 <~ P
      /* */ I <~ CE <~ B
      /* */ I1 <~ S <~ B

      ClosedShape
  })
  closedShapeGraph.run()

  //--------------------------------------------------------------------------------------------------------------------
  val sinkShapeGraph = Sink.fromGraph(GraphDSL.create() {
    implicit builder =>
      import GraphDSL.Implicits._

      //Unzip string to first char and the remained chars
      val U = builder.add(UnzipWith { s: String => s.head -> s.tail })

      val toUper = builder.add(Flow[String].map(_.toUpperCase))
      val toLower = builder.add(Flow[Char].map(_.toLower))

      val C = builder.add(Sink.foreach[Any](c => println(s"Char: $c"))).in
      val S = builder.add(Sink.foreach[Any](c => println(s"String: $c"))).in

      U.out0 ~> toLower ~> C
      U.out1 ~> toUper ~> S

      SinkShape(U.in)
  })

  val itrSrc: Source[String, NotUsed] = Source.fromIterator(() => Iterator("Abb", "Cbsf"))

  val filePath: Path = Paths.get(getClass.getResource("/test.dat").getPath)
  val fileSrc = FileIO.fromPath(filePath)
    .via(Framing.delimiter(ByteString("\n"), Int.MaxValue, true).map(_.utf8String))

  sinkShapeGraph.runWith(itrSrc)
  sinkShapeGraph.runWith(fileSrc)

  //--------------------------------------------------------------------------------------------------------------------
  val flowShapeGraph: Graph[FlowShape[(String, Long), String], NotUsed] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._

    val toRecord = builder.add(Flow[(String, Long)].map((Record.apply _).tupled))
    val validate = builder.add(validateFlow)
    val collectE = builder.add(collectErrors)


    toRecord ~> validate ~> collectE

    FlowShape.of(toRecord.in, collectE.out)
  }

  val source = Source.fromIterator(() => input.toIterator)
    .zipWithIndex

  val sink = Sink.seq[String]

  val result: Future[Seq[String]] = source.via(flowShapeGraph).runWith(sink)

  println(Await.result(result, 2 seconds))
}
