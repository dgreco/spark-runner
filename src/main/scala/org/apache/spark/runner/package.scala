/*
 * Copyright 2018 David Greco
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

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.local.LocalSchedulerBackend

import scala.reflect.ClassTag

package object runner extends Logging {

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

    rdd.mapPartitionsWithIndex[T]((id: Int, _: Iterator[Int]) => new Iterator[T] {
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

}

