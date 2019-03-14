package com.akka.parse.aggregate

import java.time.{LocalDateTime, ZoneOffset}

import scala.concurrent.duration.Duration

trait DateUtils {
  val DefaultZone: ZoneOffset = ZoneOffset.UTC

  def startOfPeriod(date: LocalDateTime,
                    period: Duration,
                    countdownPoint: LocalDateTime,
                    zoneOffset: ZoneOffset = DefaultZone
                   ): LocalDateTime = {

    val startSecond = countdownPoint.toEpochSecond(zoneOffset)
    val periodSeconds = period.toSeconds
    val dateSecond = date.toEpochSecond(zoneOffset)

    val startPeriodSecond = if (date.isAfter(countdownPoint)) {
      val startDiffSeconds = ((dateSecond - startSecond) / periodSeconds) * periodSeconds
      startSecond + startDiffSeconds
    } else {
      val diff = startSecond - dateSecond
      val diffPeriods = if (diff % periodSeconds == 0) {
        diff / periodSeconds
      } else {
        diff / periodSeconds + 1
      }
      val startDiffSeconds = diffPeriods * periodSeconds
      startSecond - startDiffSeconds
    }
    LocalDateTime.ofEpochSecond(startPeriodSecond, 0, zoneOffset)
  }
}
