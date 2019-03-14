package com.akka.parse.aggregate

import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{FileIO, Flow, Framing}
import akka.util.ByteString

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.util.Try

object CsvAggregatorApp extends App {

  implicit val system: ActorSystem = ActorSystem("csv-converter")
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  val CsvSeparator = ','
  val Quote = "\""
  val StringSeparator = "\n"
  val InputFilePath = Paths.get(args(0))
  val OutputFilePath = Paths.get(args(1))

  val DateFormatString = "yyyy-MM-dd HH:mm:ss"
  val Period = Try {
    args(2).toInt seconds
  }.toOption.getOrElse({
    println("Period is not supplied or can not be parsed, using default value.")
    60 minutes
  })

  if (InputFilePath.toFile.canRead && OutputFilePath.toFile.createNewFile) {
    println(s"Input file: $InputFilePath, output file: $OutputFilePath, period: $Period")
    println("Started computation, please wait.")
    FileIO.fromPath(InputFilePath)
      .via(Framing.delimiter(ByteString(StringSeparator), 1000, false))
      .map(_.utf8String
        .replace(Quote, "")
        .split(CsvSeparator))
      .filter(_.length == 3)
      //here events come in sorted by timestamp order, if not the logic would be violated
      .map(a => UserAuthEvent(a(0), a(1), LocalDateTime.parse(a(2), DateTimeFormatter.ofPattern(DateFormatString))))
      .via(Flow.fromGraph(new EventPeriodFlow(Period)))
      .mapConcat(s => s
        .groupBy(_.ip)
        //different filters can be applied
        .filter(ip => SeqUserAuthEvent(ip._2).eventsByFilter)
      ).map(UserAuthEventMethod.toCsvString)
      .log("MultiAuth")
      .map(ByteString(_))
      .runWith(FileIO.toPath(OutputFilePath))
      .onComplete {
        _ =>
          system.terminate()
          println(s"Operation successfully performed, you can view results in file: $OutputFilePath")
      }
  } else {
    system.terminate()
    println("One of supplied file paths is wrong, can not perform read or write. Please restart program with other parameters.")
  }
}

