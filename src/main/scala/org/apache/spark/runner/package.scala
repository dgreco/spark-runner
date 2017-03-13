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

package org.apache.spark

import java.io.File
import java.net.{ InetAddress, URL, URLClassLoader }
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.runner.kafka.{ EmbeddedKafka, EmbeddedZookeeper, makeProducer }
import org.apache.spark.runner.utils._
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.reflect.ClassTag
import scala.util.Random

package object runner {

  val TICK_TIME = 1000
  val TIMEOUT = 1000
  val SLEEP: Long = 1000

  //Simple function for adding a directory to the system classpath
  def addPath(dir: String): Unit = {
    val method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    method.setAccessible(true)
    val _ = method.invoke(ClassLoader.getSystemClassLoader, new File(dir).toURI.toURL)
  }

  //given a class it returns the jar (in the classpath) containing that class
  def getJar(klass: Class[_]): String = {
    val codeSource = klass.getProtectionDomain.getCodeSource
    codeSource.getLocation.getPath
  }

  def numOfSparkExecutors(implicit sparkContext: SparkContext): Int = if (sparkContext.isLocal) 1 else {
    val sb = sparkContext.schedulerBackend
    sb match {
      case _: LocalSchedulerBackend => 1
      case b: CoarseGrainedSchedulerBackend => b.getExecutorIds.length
      case _ => sparkContext.getExecutorStorageStatus.length - 1
    }
  }

  def getNodes(implicit sparkContext: SparkContext): Array[String] = {
    val numNodes = numOfSparkExecutors(sparkContext)

    val rdd: RDD[Int] = sparkContext.parallelize[Int](1 to numNodes, numNodes)

    rdd.mapPartitions[String](_ => new Iterator[String] {
      @SuppressWarnings(Array("org.wartremover.warts.Var"))
      var firstTime = true

      @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
      override def hasNext: Boolean =
        if (firstTime) {
          firstTime = false
          true
        } else
          firstTime

      @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
      override def next(): String = {
        val address: InetAddress = InetAddress.getLocalHost
        address.getHostAddress
      }
    }, preservesPartitioning = true).collect().distinct
  }

  def executeOnNodes[T](func: ExecutionContext => T)(implicit sparkContext: SparkContext, ev: ClassTag[T]): Array[T] = {
    val numNodes = numOfSparkExecutors(sparkContext)

    val rdd: RDD[Int] = sparkContext.parallelize[Int](1 to numNodes, numNodes)

    rdd.mapPartitionsWithIndex[T]((id, _) => new Iterator[T] {
      @SuppressWarnings(Array("org.wartremover.warts.Var"))
      var firstTime = true

      @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
      override def hasNext: Boolean =
        if (firstTime) {
          firstTime = false
          true
        } else
          firstTime

      @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
      override def next(): T = {
        func(SimpleExecutionContext(id, InetAddress.getLocalHost.getHostAddress))
      }
    }, preservesPartitioning = true).collect()
  }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf", "org.wartremover.warts.ToString", "org.wartremover.warts.While"))
  def streamingExecuteOnNodes(func: StreamingExecutionContext => Unit)(implicit streamingContext: StreamingContext): DStream[(String, String)] = {
    val TOPIC_LENGTH = 10
    val TOPIC = Random.alphanumeric.take(TOPIC_LENGTH).mkString
    val CLIENT_ID_LENGTH = 10
    val CLIENT_ID = Random.alphanumeric.take(TOPIC_LENGTH).mkString

    val zkPort = getAvailablePort
    implicit val sparkContext: SparkContext = streamingContext.sparkContext
    val nodes = getNodes
    val numNodes = nodes.length
    val embeddedZookeeper = new EmbeddedZookeeper(zkPort, TICK_TIME)
    val _ = embeddedZookeeper.startup()
    val zkConnection = embeddedZookeeper.getConnection
    val brokers = executeOnNodes(ec => {
      val kafkaPort = getAvailablePort
      val kafkaServer = new EmbeddedKafka(ec.id, zkConnection, kafkaPort)
      val _ = kafkaServer.startup()
      kafkaServer.getConnection
    }).mkString(",")

    val zkClient = new ZkClient(zkConnection, Integer.MAX_VALUE, TIMEOUT, new ZkSerializer {
      def serialize(data: Object): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

      def deserialize(bytes: Array[Byte]): Object = new String(bytes, "UTF-8")
    })
    val zkUtils = ZkUtils.apply(zkClient, isZkSecurityEnabled = false)
    AdminUtils.createTopic(zkUtils, TOPIC, numNodes, 1, new Properties())
    Thread.sleep(SLEEP)
    while (!AdminUtils.topicExists(zkUtils, TOPIC))
      Thread.sleep(SLEEP)
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers
    )
    val topics = Set(TOPIC)
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topics)
    val _2 = executeOnNodes(ec => {
      val tryProducer = makeProducer[String, String, StringSerializer, StringSerializer](CLIENT_ID, brokers)
      new Thread(new Runnable {
        override def run(): Unit = tryProducer.foreach(producer => {
          func(StreamingExecutionContext(ec.id, ec.address, TOPIC, producer))
        })
      }).start()
    })
    stream
  }

  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  object GetAddress extends (ExecutionContext => (String, String)) with Serializable {
    override def apply(ec: ExecutionContext): (String, String) = {
      val address: InetAddress = InetAddress.getLocalHost
      assert(ec.address == address.getHostAddress)
      (address.getHostAddress, address.getHostName)
    }
  }

}
