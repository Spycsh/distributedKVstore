package se.kth.id2203.consensus

;

import se.kth.id2203.leaderElection.{BLE_Leader, BLE_Start, BallotLeaderElection, GossipLeaderElection}
import se.kth.id2203.kvstore.Operation
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.network.Network
import se.sics.kompics.KompicsEvent
import se.sics.kompics.sl._

import scala.collection.mutable;

case class Prepare(nL: Long, ld: Int, na: Long) extends KompicsEvent;

case class Promise(nL: Long, na: Long, suffix: List[Operation], ld: Int) extends KompicsEvent;

case class AcceptSync(nL: Long, suffix: List[Operation], ld: Int) extends KompicsEvent;

case class Accept(nL: Long, c: Operation) extends KompicsEvent;

case class Accepted(nL: Long, m: Int) extends KompicsEvent;

case class Decide(ld: Int, nL: Long) extends KompicsEvent;

object State extends Enumeration {
  type State = Value;
  val PREPARE, ACCEPT, UNKOWN = Value;
}

object Role extends Enumeration {
  type Role = Value;
  val LEADER, FOLLOWER = Value;
}

import Role._
import State._

//class SequencePaxos(init: Init[SequencePaxos]) extends ComponentDefinition {
class SequencePaxos extends ComponentDefinition {
  def suffix(s: List[Operation], l: Int): List[Operation] = {
    s.drop(l)
  }

  def prefix(s: List[Operation], l: Int): List[Operation] = {
    s.take(l)
  }

  val sc = provides[SequenceConsensus];
  val ble = requires[BallotLeaderElection];
  val pl = requires[Network];

  var pi: Set[NetAddress] = Set();
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  var others: Set[NetAddress] = Set();
  var majority = 0;
  //  val (self, pi, others) = init match {
  //    case Init(addr: NetAddress, pi: Set[NetAddress] @unchecked) => (addr, pi, pi - addr)
  //  }
  //  val majority = (pi.size / 2) + 1;

  var state = (FOLLOWER, UNKOWN);
  var nL = 0l;
  var nProm = 0l;
  var leader: Option[NetAddress] = None;
  var na = 0l;
  var va = List.empty[Operation];
  var ld = 0;

  // leader state
  var propCmds = List.empty[Operation];
  val las = mutable.Map.empty[NetAddress, Int];
  val lds = mutable.Map.empty[NetAddress, Int];
  var lc = 0;
  val acks = mutable.Map.empty[NetAddress, (Long, List[Operation])];


  ble uponEvent {
    case BLE_Leader(l, n) => {
      if (n > nL) {
        leader = Some(l);
        nL = n;
        if ((self == l) && (nL > nProm)) {
          state = (LEADER, PREPARE);
          propCmds = List.empty[Operation];
          las.clear;
          lds.clear;
          acks.clear;
          lc = 0;
          for (p <- others) {
            trigger(NetMessage(self, p, Prepare(nL, ld, na)) -> pl);
          }
          acks += (l -> (na, suffix(va, ld)));
          lds += (self -> ld);
          nProm = nL;
        } else {
          state = (FOLLOWER, state._2);
        }
      }
    }
  }

  pl uponEvent {
    case NetMessage(header, Prepare(np, ldp, n)) => {
      val p = header.src;

      if (nProm < np) {
        nProm = np;
        state = (FOLLOWER, PREPARE);

        var sfx = List.empty[Operation];
        if (na >= n) {
          sfx = suffix(va, ld);
        }
        //ldp or ld?
        trigger(NetMessage(self, p, Promise(np, na, sfx, ldp)) -> pl);
      }
    }


    case NetMessage(header, Promise(n, na, sfxa, lda)) => {
      val a = header.src;
      if ((n == nL) && (state == (LEADER, PREPARE))) {

        acks += (a -> (na, sfxa));
        lds += (a -> lda);

        if (acks.keySet.size == majority) {
          //max acks[p]
          var k = 0l;
          var sfx = List.empty[Operation];
          for (ack <- acks) {
            // round number is bigger
            // or round number is the same, the seq size is bigger
            if ((ack._2._1 > k) || ((ack._2._1 == k) && (ack._2._2.size > sfx.size))) {
              sfx = ack._2._2;
              k = ack._2._1;
            }
          }
          va = prefix(va, ld) ::: sfx ::: propCmds;

          las += (self -> va.size);
          propCmds = List.empty[Operation];
          state = (LEADER, ACCEPT);
          for (p <- pi) {
            if (lds.contains(p) && (p != self)) {
              var sfxTemp = suffix(va, lds(p));
              trigger(NetMessage(self, p, AcceptSync(nL, sfxTemp, lds(p))) -> pl);
            }
          }
        }

      } else if ((n == nL) && (state == (LEADER, ACCEPT))) {

        lds += (a -> lda);
        val sfx = suffix(va, lds(a));
        trigger(NetMessage(self, a, AcceptSync(nL, sfx, lds(a))) -> pl);
        if (lc != 0) {
          trigger(NetMessage(self, a, Decide(ld, nL)) -> pl);
        }
      }
    }


    case NetMessage(header, AcceptSync(nL, sfx, ldp)) => {
      val p = header.src;
      if ((nProm == nL) && (state == (FOLLOWER, PREPARE))) {

        na = nL;

        va = prefix(va, ldp) ::: sfx;
        trigger(NetMessage(self, p, Accepted(nL, va.size)) -> pl);
        state = (FOLLOWER, ACCEPT);
      }
    }


    case NetMessage(header, Accept(nL, c)) => {
      val p = header.src;
      if ((nProm == nL) && (state == (FOLLOWER, ACCEPT))) {

        va = va ::: List(c);
        // println("accepting");
        trigger(NetMessage(self, p, Accepted(nL, va.size)) -> pl);
      }
    }


    case NetMessage(_, Decide(l, nL)) => {

      if (nProm == nL) {
        while (ld < l) {
          // println("sequence consensus decide: " + va(ld));
          trigger(SC_Decide(va(ld)) -> sc);
          ld += 1;
        }
      }
    }


    case NetMessage(header, Accepted(n, m)) => {
      val a = header.src;
      if ((n == nL) && (state == (LEADER, ACCEPT))) {

        las += (a -> m);

        var counter = 0;
        for (p <- pi) {
          if (las.contains(p)) {
            if (las(p) >= m) {
              counter += 1;
            }
          }
        }

        if (counter >= majority && lc < m) {
          lc = m;
          for (p <- pi) {
            if (lds.contains(p)) {
              trigger(NetMessage(self, p, Decide(lc, nL)) -> pl);
            }
          }
        }

      }
    }
  }


  sc uponEvent {
    case SC_Propose(c) => {

      if (state == (LEADER, PREPARE)) {
        propCmds = propCmds ::: List(c);
      }
      else if (state == (LEADER, ACCEPT)) {

        va = va ::: List(c);
        var temp = las(self);
        temp += 1;
        las += (self -> temp);
        for (p <- pi) {
          if (lds.contains(p) && (p != self)) {
            trigger(NetMessage(self, p, Accept(nL, c)) -> pl);
          }
        }
      }
      else {
        //here is different from the lab
        // pass the operation to the leader if self is not leader
        if (leader.isDefined) { //there exists a leader
          trigger(NetMessage(self, leader.get, c) -> pl); //forward the propose operation to the leader
        } else {
          // println("No leader yet");
        }
      }
    }

    //to set the topology initially in the overlay manager
    case SC_InitializeTopology(topology) => {
      pi = topology;
      others = pi - self;
      majority = (pi.size / 2) + 1;
      log.info("Sequence consensus initializes topology");
      //to initialize the topology of the ballot leader election
      trigger(BLE_Start(pi) -> ble);
    }

  }
}