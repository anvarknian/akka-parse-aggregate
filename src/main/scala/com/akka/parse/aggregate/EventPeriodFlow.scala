package com.akka.parse.aggregate

import java.time.LocalDateTime

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

/**
  * Flow aggregates events by time periods and sends downstream batch of events that correspond to single period.
  * <p>
  * !!!Attention!!!
  * Here made an assumption that incoming events flow is `ordered by timestamp`, if this is violated this stage won't work properly.
  *
  * @param period         period of aggregation
  * @param countdownStart start point for period countdown (default value is time of first incoming event)
  */


class EventPeriodFlow(period: FiniteDuration, countdownStart: Option[LocalDateTime] = None)
  extends GraphStage[FlowShape[UserAuthEvent, immutable.Seq[UserAuthEvent]]] with DateUtils {

  private val in = Inlet[UserAuthEvent]("EventPeriodFlow.in")
  private val out = Outlet[immutable.Seq[UserAuthEvent]]("EventPeriodFlow.out")

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

    private var prevPeriod: Option[LocalDateTime] = None
    private var eventsInPeriod :Option[immutable.Seq[UserAuthEvent]]= Some(immutable.Seq.empty[UserAuthEvent])
    private var firstIncomingEventTime: Option[LocalDateTime] = _

    override def onPush(): Unit = {
      val event = grab(in)
      if (firstIncomingEventTime.isEmpty) firstIncomingEventTime = Some(event.date)
      val eventPeriod = startOfPeriod(event.date, period, countdownStart.getOrElse(firstIncomingEventTime.get))
      val isNewPeriod = prevPeriod.exists(!_.isEqual(eventPeriod))
      if (isNewPeriod) {
        prevPeriod = Some(eventPeriod)
        push(out, eventsInPeriod)
        eventsInPeriod = Some(immutable.Seq(event))
      } else {
        if (prevPeriod.isEmpty) prevPeriod = Some(eventPeriod)
        eventsInPeriod.get :+= event
        pull(in)
      }
    }

    override def onPull(): Unit = {
      pull(in)
    }

    override def onUpstreamFinish(): Unit = {
      if (isAvailable(out)) push(out, eventsInPeriod)
      eventsInPeriod = None
      completeStage()
    }

    setHandler(in, this)
    setHandler(out, this)
  }

  override def shape: FlowShape[UserAuthEvent, immutable.Seq[UserAuthEvent]] = FlowShape(in, out)
}
