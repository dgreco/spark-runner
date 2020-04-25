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

import java.net.InetAddress
import java.util.function

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.runner.functions.GetAddress
import org.apache.spark.sql.SparkSession
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

@SuppressWarnings(Array(
  "org.wartremover.warts.TryPartial",
  "org.wartremover.warts.JavaSerializable",
  "org.wartremover.warts.Equals",
  "org.wartremover.warts.Var",
  "org.wartremover.warts.Null",
  "org.wartremover.warts.NonUnitStatements"))
class SparkSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  var sparkSession: SparkSession = _

  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  override def beforeAll(): Unit = {
    val conf = new SparkConf().
      setAppName("spark-runner-test").
      set("spark.driver.bindAddress", "127.0.0.1").
      setMaster("local")
    sparkSession = SparkSession.builder().config(conf).getOrCreate()
    ()
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
