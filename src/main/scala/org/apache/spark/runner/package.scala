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
import java.lang
import java.net.{ InetAddress, URL, URLClassLoader }
import java.security.InvalidParameterException
import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, ByteArraySerializer, StringSerializer }
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.runner.functions.GetAddressAvailablePort
import org.apache.spark.runner.kafka._
import org.apache.spark.runner.utils._
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{ JavaDStream, JavaStreamingContext }
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.reflect.ClassTag
import scala.util.Random

package object runner extends Logging {

  private val TICK_TIME = 1000
  private val TIMEOUT = 10000
  private val SLEEP: Long = 1000

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

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.ImplicitParameter"))
  def numOfSparkExecutors(implicit sparkContext: SparkContext): Int = {
    val sb = sparkContext.schedulerBackend
    sb match {
      case b: LocalSchedulerBackend => b.totalCores
      case b: CoarseGrainedSchedulerBackend => b.getExecutorIds.length
    }
  }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.ImplicitParameter"))
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

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.ImplicitParameter"))
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

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def jexecuteOnNodes(func: java.util.function.Function[ExecutionContext, _], sparkContext: JavaSparkContext): Array[_] = {
    val sfunc = (ec: ExecutionContext) => func.apply(ec)
    executeOnNodes(sfunc)(sparkContext.sc, JavaSparkContext.fakeClassTag)
  }

  @SuppressWarnings(Array(
    "org.wartremover.warts.ImplicitParameter",
    "org.wartremover.warts.DefaultArguments"))
  def streamingExecuteOnNodes(
    func: StreamingExecutionContext => Unit,
    numOfKafkaBrokers: Option[Int] = None)(implicit streamingContext: StreamingContext): DStream[(String, Array[Byte])] = {
    val TOPIC_LENGTH = 10
    val TOPIC = Random.alphanumeric.take(TOPIC_LENGTH).mkString
    val CLIENT_ID_LENGTH = 10
    val CLIENT_ID = Random.alphanumeric.take(CLIENT_ID_LENGTH).mkString

    implicit val sparkContext: SparkContext = streamingContext.sparkContext

    val nodes = executeOnNodes(GetAddressAvailablePort)

    val zkConnection = startZookeeperQuorum(nodes)

    val (brokerIds: Set[Int], brokers: String) = startKafkaBrokers(numOfKafkaBrokers, nodes.length, zkConnection)

    createTopic(TOPIC, zkConnection, brokerIds)

    log.info(s"Creating the Kafka direct stream")
    val topics = Set(TOPIC)
    val kafkaParams = Map[String, AnyRef](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[ByteArrayDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean),
      "group.id" -> "spark-runner-groupId")
    val stream = KafkaUtils.createDirectStream(streamingContext, PreferConsistent, Subscribe[String, Array[Byte]](topics, kafkaParams))
    log.info(s"Created the Kafka direct stream")

    log.info(s"Starting the function execution on all the executors")
    val _ = executeOnNodes(ec => {
      val tryProducer = makeProducer[String, Array[Byte], StringSerializer, ByteArraySerializer](CLIENT_ID, brokers)
      new Thread {
        override def run(): Unit = tryProducer.foreach(
          producer => func(StreamingExecutionContext(ec.id, ec.address, zkConnection, TOPIC, producer)))
      }.start()
    })
    stream.map(cr => (cr.key(), cr.value()))
  }

  @SuppressWarnings(Array(
    "org.wartremover.warts.ImplicitParameter",
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.While"))
  private def createTopic(TOPIC: String, zkConnection: String, brokerIds: Set[Int]) = {
    log.info(s"Creating the topic $TOPIC")
    val zkClient = new ZkClient(zkConnection, Integer.MAX_VALUE, TIMEOUT, new ZkSerializer {
      def serialize(data: Object): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

      def deserialize(bytes: Array[Byte]): Object = new String(bytes, "UTF-8")
    })
    val zkUtils = ZkUtils.apply(zkClient, isZkSecurityEnabled = false)
    AdminUtils.createTopic(zkUtils, TOPIC, brokerIds.size, 1, new Properties())
    Thread.sleep(SLEEP)
    while (try {
      !AdminUtils.topicExists(zkUtils, TOPIC)
    } catch {
      case _: Throwable => false
    }) Thread.sleep(SLEEP)
    log.info(s"Created the topic $TOPIC")
  }

  @SuppressWarnings(Array(
    "org.wartremover.warts.Equals",
    "org.wartremover.warts.ImplicitParameter"))
  private def startZookeeperQuorum(nodes: Array[(String, Int)])(implicit sparkContext: SparkContext) = {
    log.info("Starting Zookeeper Quorum")

    val zkNodes = {
      if (nodes.length >= 3)
        Random.shuffle[(String, Int), Seq](nodes.toSeq).take(3)
      else
        Seq(nodes.apply(0))
    }.toMap

    val quorumConfigBuilder = QuorumConfigBuilder(zkNodes.map(node => (node._1, node._2)).toArray)
    val zkConnection = executeOnNodes(ec => {
      if (zkNodes.contains(ec.address)) {
        val embeddedZookeeper = if (zkNodes.size == 1)
          new SingleEmbeddedZookeeper(quorumConfigBuilder.buildConfig(quorumConfigBuilder.instanceSpecs.indexWhere(_.hostname == ec.address)))
        else
          new EmbeddedQuorumZookeeper(quorumConfigBuilder.buildConfig(quorumConfigBuilder.instanceSpecs.indexWhere(_.hostname == ec.address)))
        val resp = embeddedZookeeper.startup()
        if (resp.isSuccess)
          Some(embeddedZookeeper.getConnection)
        else
          None
      } else
        None
    }).filter(_.isDefined).map(_.getOrElse("")).mkString(",")
    log.info(s"Zookeeper Quorum started with connection = $zkConnection")
    zkConnection
  }

  @SuppressWarnings(Array(
    "org.wartremover.warts.Throw",
    "org.wartremover.warts.ImplicitParameter"))
  private def startKafkaBrokers(numOfKafkaBrokers: Option[Int], numNodes: Int, zkConnection: String)(implicit sparkContext: SparkContext) = {
    val brokerIds = {
      val brokerIds = (0 until numNodes).toSet
      numOfKafkaBrokers.fold(brokerIds)(nb => if (nb >= numNodes || nb < 1)
        throw new InvalidParameterException(s"numOfKafkaBrokers must be less than $numNodes and greater than 0")
      else
        Random.shuffle(brokerIds).take(nb))
    }

    log.info(s"Starting Kafka on the executors with ids = ${brokerIds.mkString(", ")}")
    val brokers = executeOnNodes(ec => {
      if (brokerIds.contains(ec.id)) {
        val kafkaPort = getAvailablePort
        val kafkaServer = new EmbeddedKafka(ec.id, zkConnection, kafkaPort)
        val resp = kafkaServer.startup()
        if (resp.isSuccess)
          Some(kafkaServer.getConnection)
        else
          None
      } else None
    }).filter(_.isDefined).map(_.getOrElse("")).mkString(",")
    log.info(s"Kafka started with brokers = $brokers")
    (brokerIds, brokers)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def jstreamingExecuteOnNodes(
    func: java.util.function.Consumer[StreamingExecutionContext],
    streamingContext: JavaStreamingContext): JavaDStream[(String, Array[Byte])] = {
    val sfunc: StreamingExecutionContext => Unit = (ec: StreamingExecutionContext) => func.accept(ec)
    streamingExecuteOnNodes(sfunc)(streamingContext.ssc)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Overloading"))
  def jstreamingExecuteOnNodes(
    func: java.util.function.Consumer[StreamingExecutionContext],
    numOfKafkaBrokers: Int,
    streamingContext: JavaStreamingContext): JavaDStream[(String, Array[Byte])] = {
    val sfunc: StreamingExecutionContext => Unit = (ec: StreamingExecutionContext) => func.accept(ec)
    streamingExecuteOnNodes(sfunc, Some(numOfKafkaBrokers))(streamingContext.ssc)
  }

}

