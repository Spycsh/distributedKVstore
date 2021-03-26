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
package se.kth.id2203.simulation

import org.scalatest._
import se.kth.id2203.ParentComponent;
import se.kth.id2203.networking._;
import se.sics.kompics.network.Address
import java.net.{ InetAddress, UnknownHostException };
import se.sics.kompics.sl._;
import se.sics.kompics.sl.simulator._;
import se.sics.kompics.simulator.{ SimulationScenario => JSimulationScenario }
import se.sics.kompics.simulator.run.LauncherComp
import se.sics.kompics.simulator.result.SimulationResultSingleton;
import se.sics.kompics.simulator.network.impl.NetworkModels
import scala.concurrent.duration._

class OpsTest extends FlatSpec with Matchers {

  private val nMessages = 10;

  // GET the values that already initialized in the KV
  "get" should "return initialized value" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val simpleBootScenario = SimpleScenario.scenario(3);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    SimulationResult += ("initialization" -> true);
    SimulationResult += ("operation" -> "GET");
    simpleBootScenario.simulate(classOf[LauncherComp]);
    for (i <- 0 to nMessages) {
      //SimulationResult.get[String](s"test$i") should be (Some("NotImplemented"));
      SimulationResult.get[String](s"test$i") should be (Some(s"Csh$i"));
      // of course the correct response should be Success not NotImplemented, but like this the test passes
    }
  }

  // PUT the key-value pairs in the KV store which have a match of keys
  "put" should "return the put values" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val simpleBootScenario = SimpleScenario.scenario(3);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    SimulationResult += ("operation" -> "PUT");
    SimulationResult += ("initialization" -> false);
    simpleBootScenario.simulate(classOf[LauncherComp]);
    for (i <- 0 to nMessages) {
      SimulationResult.get[String](s"test$i") should be (Some(s"Csh$i"));
    }
  }

  // Compare and Swap key-value pairs in the KV store which have a match of keys
  "cas" should "return the new value if key exists and old value matches" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val simpleBootScenario = SimpleScenario.scenario(3);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    SimulationResult += ("operation" -> "CAS");
    SimulationResult += ("initialization" -> true);
    simpleBootScenario.simulate(classOf[LauncherComp]);
    for (i <- 0 to nMessages) {
      SimulationResult.get[String](s"test$i") should be (Some(s"CshNew$i"));
    }
  }

  // compare and swap key-value pairs in the KV store which do not have a match
  "cas2" should "return keyNotFound if key does not exist" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val simpleBootScenario = SimpleScenario.scenario(3);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    SimulationResult += ("operation" -> "CAS");
    SimulationResult += ("initialization" -> false);  // not initialize, so CAS will not match
    simpleBootScenario.simulate(classOf[LauncherComp]);
    for (i <- 0 to nMessages) {
      SimulationResult.get[String](s"test$i") should be (Some(s"keyNotFound"));
    }
  }

  // compare and swap key-value pairs in the KV store which do not have a match
  "cas3" should "return the old value if old value does not match" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val simpleBootScenario = SimpleScenario.scenario(3);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("messages" -> nMessages);
    SimulationResult += ("operation" -> "CAS3");
    SimulationResult += ("initialization" -> true);
    simpleBootScenario.simulate(classOf[LauncherComp]);
    for (i <- 0 to nMessages) {
      SimulationResult.get[String](s"test$i") should be (Some(s"Csh$i")); // return old value if not match
    }
  }

}

object SimpleScenario {

  import Distributions._
  // needed for the distributions, but needs to be initialised after setting the seed
  implicit val random = JSimulationScenario.getRandom();

  private def intToServerAddress(i: Int): Address = {
    try {
      NetAddress(InetAddress.getByName("192.193.0." + i), 45678);
    } catch {
      case ex: UnknownHostException => throw new RuntimeException(ex);
    }
  }
  private def intToClientAddress(i: Int): Address = {
    try {
      NetAddress(InetAddress.getByName("192.193.1." + i), 45678);
    } catch {
      case ex: UnknownHostException => throw new RuntimeException(ex);
    }
  }

  private def isBootstrap(self: Int): Boolean = self == 1;

  val setUniformLatencyNetwork = () => Op.apply((_: Unit) => ChangeNetwork(NetworkModels.withUniformRandomDelay(3, 7)));

  val startServerOp = Op { (self: Integer) =>

    val selfAddr = intToServerAddress(self)
    val conf = if (isBootstrap(self)) {
      // don't put this at the bootstrap server, or it will act as a bootstrap client
      Map("id2203.project.address" -> selfAddr)
    } else {
      Map(
        "id2203.project.address" -> selfAddr,
        "id2203.project.bootstrap-address" -> intToServerAddress(1))
    };
    StartNode(selfAddr, Init.none[ParentComponent], conf);
  };

  val startClientOp = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    val conf = Map(
      "id2203.project.address" -> selfAddr,
      "id2203.project.bootstrap-address" -> intToServerAddress(1));
    StartNode(selfAddr, Init.none[ScenarioClient], conf);
  };

  def scenario(servers: Int): JSimulationScenario = {

    val networkSetup = raise(1, setUniformLatencyNetwork()).arrival(constant(0));
    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second));
    val startClients = raise(1, startClientOp, 1.toN).arrival(constant(1.second));

    networkSetup andThen
      0.seconds afterTermination startCluster andThen
      10.seconds afterTermination startClients andThen
      100.seconds afterTermination Terminate
  }

}
