package com.akka.parse.aggregate

import java.time.LocalDateTime


case class UserAuthEvent(username: String, ip: String, date: LocalDateTime) {
  override def toString: String =
    s"""{
       |"Username" : "$username",
       |"Ip" : "$ip",
       |"Date" : "$date"
       |}""".stripMargin
}

case class SeqUserAuthEvent(ipEvents: Seq[UserAuthEvent]) {

  override def toString: String = {
    s"""{ "SeqUserAuthEvent" : [ ${ipEvents.mkString(", ").map(x => x)} ]
       |}""".stripMargin
  }
  def eventsByFilter : Boolean = ipEvents.size > 1
  def multiUserString: String = {
    //events are ordered by timestamp, otherwise they should be sorted before processing
    val sb = StringBuilder.newBuilder
    for (i <- ipEvents.indices) {
      val e = ipEvents(i)
      sb.append(s"${e.username}:${e.date}")
      if (i != ipEvents.size - 1) {
        sb.append(",")
      }
    }
    sb.toString
  }
}

object UserAuthEventMethod {

  val CsvSeparator = ','
  val Quote = "\""
  val StringSeparator = "\n"

  def toCsvString(tuple: (String, Seq[UserAuthEvent])): String = {
    s"$Quote${tuple._1}$Quote$CsvSeparator" +
      s"$Quote${tuple._2.head.date}$Quote$CsvSeparator" +
      s"$Quote${tuple._2.last.date}$Quote$CsvSeparator" +
      s"$Quote${SeqUserAuthEvent(tuple._2).multiUserString}$Quote" +
      StringSeparator
  }
}