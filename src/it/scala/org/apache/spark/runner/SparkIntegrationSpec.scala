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

package org.apache.spark.runner

import org.apache.spark.runner.functions.GetAddress
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class SparkIntegrationSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.Null"))
  var sparkSession: SparkSession = _

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def beforeAll(): Unit = {

    val hadoop_conf_dir = Option(System.getenv("HADOOP_CONF_DIR"))

    hadoop_conf_dir.fold(Predef.assert(false, "please set the HADOOP_CONF_DIR env variable"))(addPath(_))

    val uberJarLocation = s"${System.getProperty("user.dir")}/assembly/target/scala-2.13/spark-runner-assembly-2.0.0.jar"

    val conf = new SparkConf().
      setMaster("yarn").
      setAppName("spark-runner-yarn").
      setJars(List(uberJarLocation)).
      set("spark.yarn.jars", "local:/opt/spark-3.2.0/jars/*").
      set("spark.executor.instances", Integer.toString(4)).
      set("spark.executor.cores", Integer.toString(1)).
      set("spark.executor.memory", "512m")
    sparkSession = SparkSession.builder().config(conf).getOrCreate()
  }

  "Spark" must {
    "run a function correctly" in {
      implicit val sparkContext: SparkContext = sparkSession.sparkContext

      executeOnNodes[(String, String)](GetAddress).map(_._1).toSet must be(getNodes.toSet)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def afterAll(): Unit = {
    sparkSession.stop()
  }

}
