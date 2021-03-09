package se.kth.id2203.consensus;

import se.kth.id2203.kvstore.Operation
import se.kth.id2203.networking.NetAddress
import se.sics.kompics.sl._
import se.sics.kompics.KompicsEvent


class SequenceConsensus extends Port {
  request[SC_Propose];
  indication[SC_Decide];
  //for initializing the topology
  request[SC_InitializeTopology];

}

case class SC_Propose(value: Operation) extends KompicsEvent;
case class SC_Decide(value: Operation) extends KompicsEvent;
case class SC_InitializeTopology(topology: Set[NetAddress]) extends KompicsEvent;




