package se.kth.id2203.leaderElection;

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network.Network
import se.sics.kompics.sl._
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}
import se.sics.kompics.{KompicsEvent, Start}

import scala.collection.mutable

class BallotLeaderElection extends Port {
  indication[BLE_Leader];
  // init the topology and majority
  request[BLE_Start];
}

case class BLE_Start(pi: Set[NetAddress]) extends KompicsEvent;

case class BLE_Leader(address: NetAddress, l: Long) extends KompicsEvent;

case class CheckTimeout(timeout: ScheduleTimeout) extends Timeout(timeout);

case class HeartbeatReq(round: Long, highestBallot: Long) extends KompicsEvent;

case class HeartbeatResp(round: Long, ballot: Long) extends KompicsEvent;

class GossipLeaderElection extends ComponentDefinition {

  val ble = provides[BallotLeaderElection];
  val pl = requires[Network];
  val timer = requires[Timer];

  var topology: Set[NetAddress] = Set();
  val self = cfg.getValue[NetAddress]("id2203.project.address");

  val delta = cfg.getValue[Long]("id2203.project.keepAlivePeriod");
  var majority = 0;

  private var period = delta;
  private val ballots = mutable.Map.empty[NetAddress, Long];

  private var round = 0l;
  private var ballot = ballotFromNAddress(0, self);

  private var leader: Option[(Long, NetAddress)] = None;
  private var highestBallot: Long = ballot;

  private val ballotOne = 0x0100000000l;

  def ballotFromNAddress(n: Int, adr: NetAddress): Long = {
    val nBytes = com.google.common.primitives.Ints.toByteArray(n);
    val addrBytes = com.google.common.primitives.Ints.toByteArray(adr.hashCode());
    val bytes = nBytes ++ addrBytes;
    val r = com.google.common.primitives.Longs.fromByteArray(bytes);
    assert(r > 0); // should not produce negative numbers!
    r
  }

  def incrementBallotBy(ballot: Long, inc: Int): Long = {
    ballot + inc.toLong * ballotOne
  }

  private def incrementBallot(ballot: Long): Long = {
    ballot + ballotOne
  }

  private def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(period);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  private def makeLeader(topProcess: (Long, NetAddress)) {
    leader = Some(topProcess);
  }

  private def checkLeader() {

    var topProcess = self;
    var topBallot = ballot;

    ballots += (self -> ballot);
    // get top ballot from the ballots
    // compare by ballot
    for (b <- ballots) {
      if (b._2 > topBallot) {
        topBallot = b._2;
        topProcess = b._1;
      }
    }
    var top = (topBallot, topProcess);
    if (topBallot < highestBallot) {
      while (ballot <= highestBallot) {
        ballot = incrementBallotBy(ballot, 1);
      }
      leader = None;
    } else {
      if (Some(top) != leader) {
        highestBallot = topBallot;
        makeLeader((topBallot, topProcess));
        log.info("Ballot leader election end");
        trigger(BLE_Leader(topProcess, topBallot) -> ble);
      }
    }

  }

  ble uponEvent {
    //to initialize the topology
    // after that, every period of time a leader will be elected out
    // then in that round, only the leader will be responsible to propose values and decide
    // without leader election, everyone should propose and it cost a lot msgs

    case BLE_Start(pi) => {
      topology = pi;
      majority = (topology.size / 2) + 1;
      startTimer(period);
      log.info("Ballot leader election initialize topology");
      log.info("Ballot leader election start");
    }
  }

  timer uponEvent {
    case CheckTimeout(_) => {

      if (ballots.size + 1 >= majority) {
        checkLeader();
      }
      ballots.clear;
      round = round + 1;
      for (p <- topology) {
        if (p != self) {
          trigger(NetMessage(self, p, HeartbeatReq(round, highestBallot)) -> pl);
        }
      }
      startTimer(period);
    }
  }

  pl uponEvent {
    case NetMessage(header, HeartbeatReq(r, hb)) => {

      if (hb > highestBallot) {
        highestBallot = hb;
      }
      trigger(NetMessage(self, header.src, HeartbeatResp(r, ballot)) -> pl);
    }
    case NetMessage(header, HeartbeatResp(r, b)) => {

      if (r == round) {
        ballots += (header.src -> b);
      } else {
        period = period + delta;
      }
    }
  }
}