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
import java.util.concurrent.CountDownLatch
import java.util.function
import java.util.function.Consumer

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.runner.functions.GetAddress
import org.apache.spark.runner.kafka.{ EmbeddedKafka, EmbeddedQuorumZookeeper, SingleEmbeddedZookeeper, QuorumConfigBuilder }
import org.apache.spark.runner.utils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.{ Milliseconds, StreamingContext }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.zookeeper.{ WatchedEvent, Watcher, ZooKeeper }
import org.scalatest.{ BeforeAndAfterAll, MustMatchers, WordSpec }

import scala.language.postfixOps

@SuppressWarnings(Array(
  "org.wartremover.warts.TryPartial",
  "org.wartremover.warts.JavaSerializable",
  "org.wartremover.warts.Equals",
  "org.wartremover.warts.Var",
  "org.wartremover.warts.Null",
  "org.wartremover.warts.NonUnitStatements"))
class SparkSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  var sparkSession: SparkSession = _

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def beforeAll(): Unit = {
    val conf = new SparkConf().
      setAppName("spark-runner-test").
      setMaster("local")
    sparkSession = SparkSession.builder().config(conf).getOrCreate()
    ()
  }

  "The EmbeddedZookeeper Quorum" must {
    "be created and run correctly" in {

      val numZkServers = 3

      val hosts = (0 until numZkServers) map { _ => ("localhost", getAvailablePort) } toArray

      val quorumConfigBuilder = QuorumConfigBuilder(hosts)

      val configs = (0 until numZkServers) map quorumConfigBuilder.buildConfig

      val zks = configs map {
        config =>
          val zkServer = new EmbeddedQuorumZookeeper(config)
          val result = zkServer.startup()
          if (result.isSuccess)
            Some(zkServer)
          else
            None
      }

      zks.filter(_.isDefined).size must be(numZkServers)

      val zkConnection = zks.map(_.fold("")(_.getConnection)).mkString(",")

      val const = 1000
      val connSignal = new CountDownLatch(1)
      val zkCli = new ZooKeeper(zkConnection, const, new Watcher {
        override def process(event: WatchedEvent): Unit = {
          import org.apache.zookeeper.Watcher.Event.KeeperState
          if (event.getState eq KeeperState.SyncConnected) connSignal.countDown()
        }
      })
      connSignal.await()
      import org.apache.zookeeper.CreateMode
      import org.apache.zookeeper.ZooDefs.Ids
      val _ = zkCli.create("/test", Array[Byte](), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      zkCli.exists("/test", false).getAversion must be(0)
      zkCli.delete("/test", 0)
      zkCli.close()

      zks.foreach(_.foreach(zk => zk.stop().isSuccess must be(true)))

    }
  }

  "The EmbeddedKafka" must {
    "be created and run correctly" in {

      val numZkServers = 1

      val hosts = (0 until numZkServers) map { _ => ("localhost", getAvailablePort) } toArray

      val quorumConfigBuilder = QuorumConfigBuilder(hosts)

      val configs = (0 until numZkServers) map quorumConfigBuilder.buildConfig

      val zks = configs map {
        config =>
          val zkServer = new SingleEmbeddedZookeeper(config)
          val result = zkServer.startup()
          if (result.isSuccess)
            Some(zkServer)
          else
            None
      }

      zks.filter(_.isDefined).size must be(numZkServers)

      val zkConnection = zks.map(_.fold("")(_.getConnection)).mkString(",")

      val kafkaPort = getAvailablePort
      val kafkaServer = new EmbeddedKafka(0, zkConnection, kafkaPort)
      kafkaServer.startup().isSuccess must be(true)
      var connected = true
      connected = try {
        new Socket(InetAddress.getLocalHost.getHostName, kafkaPort)
        true
      } catch {
        case _: Exception => false
      }
      connected must be(true)
      kafkaServer.shutdown().isFailure must be(false)
      zks.foreach(_.foreach(zk => zk.stop().isSuccess must be(true)))
    }
  }

  "Spark" must {
    "run a function correctly" in {
      implicit val sparkContext: SparkContext = sparkSession.sparkContext

      val nodes = getNodes

      executeOnNodes(GetAddress).map(_._1).toSet must be(nodes.toSet)
    }
  }

  "Spark" must {
    "run a function correctly using the Java APIs" in {
      implicit val sparkContext: SparkContext = sparkSession.sparkContext

      val nodes = getNodes

      val func = new function.Function[ExecutionContext, (String, String)] with java.io.Serializable {
        override def apply(t: ExecutionContext): (String, String) = GetAddress(t)
      }

      JRunner.executeOnNodes(func, new JavaSparkContext(sparkContext)).map(_._1).toSet must be(nodes.toSet)
    }
  }

  "Spark" must {
    "run a function and streaming the result correctly" in {
      val batchIntervalInMillis = 100L

      val numItems = 100

      implicit val sparkContext: SparkContext = sparkSession.sparkContext

      implicit val streamingContext: StreamingContext = new StreamingContext(sparkContext, Milliseconds(batchIntervalInMillis))

      val latch = new CountDownLatch(numItems)

      val func: (StreamingExecutionContext) => Unit = (ec: StreamingExecutionContext) => {
        for (i <- 1 to numItems) {
          ec.send(i.toString.getBytes)
        }
      }

      streamingExecuteOnNodes(func).foreachRDD(rdd => rdd.collect().foreach(_ => latch.countDown()))

      streamingContext.start()

      latch.await()

      streamingContext.stop(false)

    }
  }

  "Spark" must {
    "run a function and streaming the result correctly using the Java APIs" in {
      val batchIntervalInMillis = 100L

      val numItems = 100

      val sparkContext: SparkContext = sparkSession.sparkContext

      val streamingContext: StreamingContext = new StreamingContext(sparkContext, Milliseconds(batchIntervalInMillis))

      val latch = new CountDownLatch(numItems)

      val func: Consumer[StreamingExecutionContext] = new Consumer[StreamingExecutionContext] with java.io.Serializable {
        override def accept(ec: StreamingExecutionContext): Unit = for (i <- 1 to numItems) {
          ec.send(i.toString.getBytes)
        }
      }

      JRunner.streamingExecuteOnNodes(func, new JavaStreamingContext(streamingContext)).dstream.
        foreachRDD(rdd => rdd.collect().foreach(_ => latch.countDown()))

      streamingContext.start()

      latch.await()

      streamingContext.stop(false)

    }
  }

  "Spark" must {
    "return the list of the executor nodes correctly using the Java APIs" in {
      val sparkContext: SparkContext = sparkSession.sparkContext

      JRunner.getNodes(new JavaSparkContext(sparkContext)).head must be(InetAddress.getLocalHost.getHostAddress)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def afterAll(): Unit = {
    sparkSession.stop()
  }

}
