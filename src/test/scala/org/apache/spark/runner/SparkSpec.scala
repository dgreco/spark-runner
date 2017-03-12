/*
 * Copyright 2017 David Greco
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.runner

import java.net.{ InetAddress, Socket }

import org.apache.spark.runner.kafka.{ EmbeddedKafka, EmbeddedZookeeper, getAvailablePort }
import org.apache.spark.sql.SparkSession
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.zookeeper.{ WatchedEvent, Watcher, ZooKeeper }
import org.scalatest.{ BeforeAndAfterAll, MustMatchers, WordSpec }

@SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
class SparkSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  var sparkSession: SparkSession = _

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def beforeAll(): Unit = {
    val conf = new SparkConf().
      setAppName("spark-runner-test").
      setMaster("local[16]")
    sparkSession = SparkSession.builder().config(conf).getOrCreate()
    ()
  }

  "Spark" must {
    "run a function correctly" in {
      implicit val sparkContext: SparkContext = sparkSession.sparkContext

      val nodes = getNodes

      executeOnNodes[(String, String)](GetAddress).map(_._1).toSet must be(getNodes.toSet)
    }
  }

  "The EmbeddedZookeeper" must {
    "be created and run correctly" in {
      val const = 1000
      val port = getAvailablePort
      val zk = new EmbeddedZookeeper(port, const)
      zk.startup()
      import java.util.concurrent.CountDownLatch
      val connSignal = new CountDownLatch(1)
      val zkCli = new ZooKeeper(zk.getConnection, const, new Watcher {
        override def process(event: WatchedEvent): Unit = {
          import org.apache.zookeeper.Watcher.Event.KeeperState
          if (event.getState eq KeeperState.SyncConnected) connSignal.countDown
        }
      })
      connSignal.await
      import org.apache.zookeeper.CreateMode
      import org.apache.zookeeper.ZooDefs.Ids
      val _ = zkCli.create("/test", Array[Byte](), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      zkCli.exists("/test", false).getAversion must be(0)
      zkCli.delete("/test", 0)
      zkCli.close()
      zk.shutdown()
    }
  }

  "The EmbeddedKafka" must {
    "be created and run correctly" in {
      val const = 1000
      val zkPort = getAvailablePort
      val zk = new EmbeddedZookeeper(zkPort, const)
      zk.startup()
      val kafkaPort = getAvailablePort
      val kafkaServer = new EmbeddedKafka(0, zk.getConnection, kafkaPort)
      kafkaServer.startup()
      var connected = true
      connected = try {
        new Socket(InetAddress.getLocalHost.getHostName, kafkaPort)
        true
      } catch {
        case e: Exception => false
      }
      connected must be(true)
      kafkaServer.shutdown()
      zk.shutdown()
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def afterAll(): Unit = {
    sparkSession.stop()
  }

}
