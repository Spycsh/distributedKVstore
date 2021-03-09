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

import se.kth.id2203.networking._
import se.kth.id2203.overlay.Routing
import se.kth.id2203.consensus.{SC_Decide, SC_Propose, SequenceConsensus}
import se.sics.kompics.sl._
import se.sics.kompics.network.Network

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.Promise;


class KVService extends ComponentDefinition {

  //******* Ports ******
  val net = requires[Network];
  val route = requires(Routing);
  val sc = requires[SequenceConsensus];



  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  //added
  //stores all key-value pairs
  //initially has three values
  var store = mutable.HashMap("A" -> "OnlyOneCharacterKeyIsAllowed");
  //to store all operations
  private val pendingList = mutable.SortedMap.empty[UUID, NetAddress];



  //******* Handlers ******
  //  net uponEvent {
  //    case NetMessage(header, op @ Get(key, _)) => {
  //      log.info("Got operation {}! Now implement me please :)", op);
  //      trigger(NetMessage(self, header.src, op.response(OpCode.NotImplemented)) -> net);
  //    }
  //    case NetMessage(header, op @ Put(key, value, _)) => {
  //      log.info("Got operation {}! Now implement me please :)", op);
  //      trigger(NetMessage(self, header.src, op.response(OpCode.NotImplemented)) -> net);
  //    }
  //  }

  //receive the operation
  net uponEvent {
    case NetMessage(header, operation: Operation) => {
      log.info(" Receive operation: {}", operation);
      pendingList += (operation.id -> header.src); //add to pending list
      //propose to the sequence consensus component
      // wait until SC_Decide, then send the result
      trigger(SC_Propose(operation) -> sc);
    }
  }

  sc uponEvent {
    //sequence consensus decide
    case SC_Decide(operation: Operation) => {
      log.info("Decide operation: {}", operation);
      var opSrc = self;
      if (pendingList.contains(operation.id)) {
        opSrc = pendingList.get(operation.id).get; //get the address of the operation sender
      }

      operation match {
        case Get(key, id) => {
          //get operation
          if (store.contains(key)) { //the key exists
            val getValue = store.get(key);
            log.info("GET operation: " + key + " - " + getValue);
            //send back the response
            trigger(NetMessage(self, opSrc, GetResponse(id, OpCode.Ok, getValue.get)) -> net);

            pendingList.remove(id);
          } else { //key not found
            log.info("GET operation error: key " + key + " not found");
            trigger(NetMessage(self, opSrc, GetResponse(id, OpCode.NotFound, "null")) -> net);
          }
        }
        //put operation
        case Put(key, value, id) => {
          log.info("PUT operation: " + key + " - " + value);
          store += (key -> value.toString); //update data
          //send response
          trigger(NetMessage(self, opSrc, PutResponse(id, OpCode.Ok, value)) -> net);
          pendingList.remove(id);
        }

        //cas operation
        case Cas(key, refValue, value, id) => {
          if (store.contains(key)) { //if the key exists
            if (store.get(key).get != refValue) { //not match the ref Value
              println("CAS operation error: " + key + " - " + refValue + " not match");
              trigger(NetMessage(self, opSrc, CasResponse(id, OpCode.NotFound, refValue, refValue)) -> net);

            } else { //success
              println("CAS operation: " + key + " - " + value);
              store += (key -> value);
              trigger(NetMessage(self, opSrc, CasResponse(id, OpCode.Ok, refValue, value)) -> net);
              pendingList.remove(id);
            }
          } else { //key not exist
            println("CAS operation error: " + key + " not found");
            trigger(NetMessage(self, opSrc, CasResponse(id, OpCode.NotFound, refValue, refValue)) -> net);
          }
        }
      }
    }

  }


}
