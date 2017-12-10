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

package me.davidgreco.examples.sparkrunner

import java.io.File

import org.apache.spark.runner._
import org.apache.spark.runner.utils._
import org.apache.spark.streaming.{ Milliseconds, StreamingContext }
import org.apache.spark.{ SparkConf, SparkContext }

import scala.language.postfixOps

object Main extends App {

  val yarn = true

  val conf: SparkConf = new SparkConf().setAppName("spark-runner-yarn")

  val master: Option[String] = conf.getOption("spark.master")

  val uberJarLocation: String = {
    val location = getJar(Main.getClass)
    if (new File(location).isDirectory) s"${System.getProperty("user.dir")}/assembly/target/scala-2.11/spark-runner-assembly-1.1.0.jar" else location
  }

  if (master.isEmpty) {
    //it means that we are NOT using spark-submit

    addPath(args(0))

    if (yarn) {
      val _ = conf.
        setMaster("yarn-client").
        setAppName("spark-runner-yarn").
        setJars(List(uberJarLocation)).
        set("spark.yarn.jars", "local:/opt/cloudera/parcels/SPARK2/lib/spark2/jars/*").
        set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
        set("spark.io.compression.codec", "lzf").
        set("spark.speculation", "true").
        set("spark.shuffle.manager", "sort").
        set("spark.shuffle.service.enabled", "true").
        set("spark.executor.instances", Integer.toString(4)).
        set("spark.executor.cores", Integer.toString(1)).
        set("spark.executor.memory", "1024m")
    } else {
      val _ = conf.
        setAppName("spark-runner-local").
        setMaster("local[4]")
    }
  }

  implicit val sparkContext: SparkContext = new SparkContext(conf)

  sparkContext.setLogLevel("ERROR")

  private val sparkBatchDuration = 1000L

  implicit val streamingContext: StreamingContext = new StreamingContext(sparkContext, Milliseconds(sparkBatchDuration))

  val func: (StreamingExecutionContext) => Unit = (ec: StreamingExecutionContext) => {
    val port = getAvailablePort
    Stream.continually((if (ec.id == 0) "master" else "slave", Thread.currentThread().getName, ec.zkQuorum, ec.id, ec.address, port)).foreach(item => {
      ec.send(s"$item".getBytes)
    })
  }

  streamingExecuteOnNodes(func).foreachRDD(rdd => rdd.collect().foreach(p => { println((p._1, new String(p._2))); println() }))

  streamingContext.start()

  streamingContext.awaitTermination()

  streamingContext.stop()

  sparkContext.stop()

}
