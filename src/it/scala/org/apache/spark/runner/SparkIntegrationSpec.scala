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

package org.apache.spark.runner

import org.apache.spark.runner.functions.GetAddress
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SparkIntegrationSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  var sparkSession: SparkSession = _

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def beforeAll(): Unit = {

    val hadoop_conf_dir = Option(System.getenv("HADOOP_CONF_DIR"))

    hadoop_conf_dir.fold(Predef.assert(false, "please set the HADOOP_CONF_DIR env variable"))(addPath(_))

    val uberJarLocation = s"${System.getProperty("user.dir")}/assembly/target/scala-2.11/spark-runner-assembly-1.1.0.jar"

    val conf = new SparkConf().
      setMaster("yarn-client").
      set("spark.driver.bindAddress", "192.168.10.3").
      setAppName("spark-cdh5-template-yarn").
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
    sparkSession = SparkSession.builder().config(conf).getOrCreate()
  }

  "Spark" must {
    "run a function correctly" in {
      implicit val sparkContext: SparkContext = sparkSession.sparkContext

      val nodes = getNodes

      executeOnNodes[(String, String)](GetAddress).map(_._1).toSet must be(getNodes.toSet)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def afterAll(): Unit = {
    sparkSession.stop()
  }

}
