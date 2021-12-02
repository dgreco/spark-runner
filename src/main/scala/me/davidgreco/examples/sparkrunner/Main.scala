/*
 * Copyright 2021 David Greco
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

import org.apache.spark.runner._
import org.apache.spark.runner.functions.GetAddress
import org.apache.spark.{SparkConf, SparkContext}

import java.io.File

object Main extends App {

  val yarn = true

  val conf: SparkConf = new SparkConf().setAppName("spark-runner-yarn")

  val master: Option[String] = conf.getOption("spark.master")

  val uberJarLocation: String = {
    val location = getJar(Main.getClass)
    if (new File(location).isDirectory) s"${System.getProperty("user.dir")}/assembly/target/scala-2.13/spark-runner-assembly-2.0.0.jar" else location
  }

  if (master.isEmpty) {
    //it means that we are NOT using spark-submit
    val hadoop_conf_dir = Option(System.getenv("HADOOP_CONF_DIR"))
    hadoop_conf_dir.fold(Predef.assert(false, "please set the HADOOP_CONF_DIR env variable"))(addPath(_))

    if (yarn) {
      val _ = conf.
        setMaster("yarn").
        setAppName("spark-runner-yarn").
        setJars(List(uberJarLocation)).
        set("spark.yarn.jars", "local:/opt/spark-3.2.0/jars/*").
        set("spark.executor.instances", Integer.toString(4)).
        set("spark.executor.cores", Integer.toString(3)).
        set("spark.executor.memory", "512m")
    } else {
      val _ = conf.
        setAppName("spark-runner-local").
        setMaster("local[4]")
    }
  }

  implicit val sparkContext: SparkContext = new SparkContext(conf)

  sparkContext.setLogLevel("ERROR")

  private val res = executeOnNodes[(String, String)](GetAddress)

  println(res.toList)

  sparkContext.stop()

}
