/*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.kvstore

;

import java.util.UUID;
import se.kth.id2203.networking._;

import se.kth.id2203.overlay._;
import se.sics.kompics.sl._;
import se.sics.kompics.{Kompics, KompicsEvent, Start};
import se.sics.kompics.network.Network;
import se.sics.kompics.timer._;
import collection.mutable;
import concurrent.{Future, Promise};

case class ConnectTimeout(spt: ScheduleTimeout) extends Timeout(spt);

//edited
//OpResponse=>OperationResponse
case class OpWithPromise(op: Operation, promise: Promise[OperationResponse] = Promise()) extends KompicsEvent;

class ClientService extends ComponentDefinition {

  //******* Ports ******
  val timer = requires[Timer];
  val net = requires[Network];
  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  val server = cfg.getValue[NetAddress]("id2203.project.bootstrap-address");
  private var connected: Option[ConnectAck] = None;
  private var timeoutId: Option[UUID] = None;
  //for pending all the operations with id
  //if the operation response is received, then it might be removed from the pending list
  private val pending = mutable.SortedMap.empty[UUID, Promise[OperationResponse]];

  //******* Handlers ******
  ctrl uponEvent {
    case _: Start => {
      log.debug(s"Starting client on $self. Waiting to connect...");
      val timeout: Long = (cfg.getValue[Long]("id2203.project.keepAlivePeriod") * 2L);
      val st = new ScheduleTimeout(timeout);
      st.setTimeoutEvent(ConnectTimeout(st));
      trigger(st -> timer);
      timeoutId = Some(st.getTimeoutEvent().getTimeoutId());
      trigger(NetMessage(self, server, Connect(timeoutId.get)) -> net);
      trigger(st -> timer);
    }
  }

  net uponEvent {
    //receive the connect ack message from server
    case NetMessage(header, ack@ConnectAck(id, clusterSize)) => {
      log.info(s"Client connected to $server, cluster size is $clusterSize");
      if (id != timeoutId.get) {
        log.error("Received wrong response id! System may be inconsistent. Shutting down...");
        System.exit(1);
      }
      connected = Some(ack);
      val c = new ClientConsole(ClientService.this);
      val tc = new Thread(c);
      tc.start();
    }
    //    case NetMessage(header, or @ OpResponse(id, status)) => {
    //      log.debug(s"Got OpResponse: $or");
    //      pending.remove(id) match {
    //        case Some(promise) => promise.success(or);
    //        case None          => log.warn(s"ID $id was not pending! Ignoring response.");
    //      }
    //    }

    //receive get operation response
    case NetMessage(header, or@GetResponse(id, status, value)) => {
      log.debug(s"[ClientService] GET Response: $or");
      println("The operation status: " + status);
      println("The value for your key: " + value);
      //remove the operation from the pending list
      pending.remove(id) match {
        case Some(promise) => promise.success(or);
        case None => log.warn(s"ID $id was not pending! Ignoring response.");
      }
    }

    //receive put operation response
    case NetMessage(header, or@PutResponse(id, status, value)) => {
      log.debug(s"[ClientService] PUT Response: $or");
      println("The operation status: " + status);
      println("The value for your key: " + value);
      pending.remove(id) match {
        case Some(promise) => promise.success(or);
        case None => log.warn(s"ID $id was not pending! Ignoring response.");
      }
    }

    //receive cas operation response
    case NetMessage(header, or@CasResponse(id, status, refValue, newValue)) => {
      log.debug(s"CAS Response: $or");
      println("The operation status: " + status);
      println("The value for your key: " + newValue);
      pending.remove(id) match {
        case Some(promise) => promise.success(or);
        case None => log.warn(s"ID $id was not pending! Ignoring response.");
      }
    }

  }

  timer uponEvent {
    case ConnectTimeout(_) => {
      connected match {
        case Some(ack) => // already connected
        case None => {
          log.error(s"Connection to server $server did not succeed. Shutting down...");
          Kompics.asyncShutdown();
        }
      }
    }
  }

  loopbck uponEvent {
    //send the message with operation to the current server
    //there is only one partition in the system, so there is no need to route message
    case OpWithPromise(op, promise) => {
      //val rm = RouteMsg(op.key, op); // don't know which partition is responsible, so ask the bootstrap server to forward it
      //trigger(NetMessage(self, server, rm) -> net);
      trigger(NetMessage(self, server, op) -> net);
      //add the operation to the pending list
      pending += (op.id -> promise);
    }
  }


  def get(key: String): Future[OperationResponse] = {
    val op = Get(key);
    val owf = OpWithPromise(op);
    trigger(owf -> onSelf); // to loopbck port
    owf.promise.future
  }

  //added
  def put(key: String, value: String): Future[OperationResponse] = {
    val op = Put(key, value);
    val owf = OpWithPromise(op);
    trigger(owf -> onSelf);
    owf.promise.future
  }

  //added
  def cas(key: String, refValue: String, newValue: String): Future[OperationResponse] = {
    val op = Cas(key, refValue, newValue);
    val owf = OpWithPromise(op);
    trigger(owf -> onSelf);
    owf.promise.future
  }

}
