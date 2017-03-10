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

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.local.LocalSchedulerBackend
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.utils._

import scala.reflect.ClassTag

package object runner {
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

  def executeOnNodes[T](func: () => T)(implicit sparkContext: SparkContext, ev: ClassTag[T]): Array[T] = {
    val numNodes = numOfSparkExecutors(sparkContext)

    val rdd: RDD[Int] = sparkContext.parallelize[Int](1 to numNodes, numNodes)

    rdd.mapPartitions[T](_ => new Iterator[T] {
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
        func()
      }
    }, preservesPartitioning = true).collect()
  }

  def streamingExecuteOnNodes[T](func: () => Stream[T])(implicit streamingContext: StreamingContext, ev: ClassTag[T]): DStream[(String, T)] = {

    checkPrerequisites(streamingContext)

    val numNodes = numOfSparkExecutors(streamingContext.sparkContext)

    val dstreams = (1 to numNodes).map(_ => streamingContext.receiverStream[T](new OutputReceiver(func, getBatchDuration(streamingContext))))

    streamingContext.union(dstreams).transform(_.map(item => (InetAddress.getLocalHost.getHostName, item))) //.repartition(numNodes))
  }

  object GetAddress extends (() => (String, String)) with Serializable {
    override def apply: (String, String) = {
      val address: InetAddress = InetAddress.getLocalHost
      (address.getHostAddress, address.getHostName)
    }
  }

}
