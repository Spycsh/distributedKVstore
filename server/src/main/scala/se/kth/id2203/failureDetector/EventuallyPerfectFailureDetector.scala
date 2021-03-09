package se.kth.id2203.failureDetector;

import se.kth.id2203.consensus.SequenceConsensus
import se.kth.id2203.networking._
import se.sics.kompics.KompicsEvent
import se.sics.kompics.network._
import se.sics.kompics.sl._
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}

case class Suspect(process: NetAddress) extends KompicsEvent
case class Restore(process: NetAddress) extends KompicsEvent

case class EPFD_Start(pi: Set[NetAddress]) extends KompicsEvent;

class EventuallyPerfectFailureDetector extends Port {
  indication[Suspect]
  indication[Restore]

  request[EPFD_Start]
}

case class CheckTimeoutFD(timeout: ScheduleTimeout) extends Timeout(timeout)
case class HeartbeatReply(seq: Int) extends KompicsEvent
case class HeartbeatRequest(seq: Int) extends KompicsEvent

case class FullTopology(nodes: Set[NetAddress]) extends KompicsEvent;

class EPFD() extends ComponentDefinition {
  val timer = requires[Timer]
  val net = requires[Network]
  val epfd = provides[EventuallyPerfectFailureDetector]
//  val topo = requires[Topology]
  val sc = requires[SequenceConsensus];

  //configuration parameters
  val self = cfg.getValue[NetAddress]("id2203.project.address")
  var topology: Set[NetAddress] = Set(self)
  val delta = cfg.getValue[Long]("id2203.project.epfd.delay")

  //mutable state
  var period = cfg.getValue[Long]("id2203.project.epfd.delay")
  var alive: Set[NetAddress] = Set(self)
  var suspected = Set[NetAddress]()
  var seqnum = 0

  def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(delay)
    scheduledTimeout.setTimeoutEvent(CheckTimeoutFD(scheduledTimeout))
    trigger(scheduledTimeout -> timer)
  }

  timer uponEvent {
    case CheckTimeoutFD(_) => {
      // !!!
      // when timeout
      // update the topology of SC component with the alive ones
//      trigger(SC_InitializeTopology(alive) -> sc);
//      log.info("Check Timeout FD......");
      if (alive.intersect(suspected).nonEmpty) {
//        log.info("Some nodes fail......");
//        log.info("Suspected:"+suspected.size+"......");
//        log.info("Alive:"+alive.size+"......");
        period += delta
      }
      seqnum = seqnum + 1
      for (p <- topology) {
        if (!alive.contains(p) && !suspected.contains(p)) {
          suspected += p
          trigger(Suspect(p) -> epfd)
        } else if (alive.contains(p) && suspected.contains(p)) {
          suspected = suspected - p
          trigger(Restore(p) -> epfd)
        }

        trigger(NetMessage(self, p, HeartbeatRequest(seqnum)) -> net)
      }
      alive = Set[NetAddress]()
      startTimer(period)
    }
  }

  net uponEvent {
    case NetMessage(header, HeartbeatRequest(seq)) =>  {
      trigger(NetMessage(self, header.src, HeartbeatReply(seq)) -> net)
    }
    case NetMessage(header, HeartbeatReply(seq)) =>  {
      if(seq == seqnum || suspected.contains(header.src)) {
        alive += header.src
      }
    }
  }

  epfd uponEvent {
    //! indication !! should be written in overlaymanager
//    case Suspect(p) =>  {
//      println(s"Node $p is suspected to have failed");
//      log.info(s"Node $p is suspected to have failed")
//      suspected += p
//    }
//    case Restore(p) =>  {
//      println(s"Node $p is not suspected to have failed anymore");
//      log.info(s"Node $p is not suspected to have failed anymore")
//      suspected -= p
//    }

      // initialize the full topology of epfd
    case EPFD_Start(nodes) => {
      topology = nodes
      alive = nodes
      startTimer(period)
    }
  }
}

