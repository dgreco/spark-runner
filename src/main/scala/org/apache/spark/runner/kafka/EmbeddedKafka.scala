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

package org.apache.spark.runner.kafka

import java.io.File
import java.net.InetAddress
import java.util.Properties

import kafka.server.{ KafkaConfig, KafkaServer }
import org.apache.spark.runner.utils._

import scala.util.{ Failure, Try }

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var", "org.wartremover.warts.NonUnitStatements"))
class EmbeddedKafka(id: Int, zkConnection: String, port: Int) {

  private var kafkaServer: Try[KafkaServer] = Failure[KafkaServer](new Exception())

  private var logDir: Try[File] = Failure[File](new Exception())

  def startup(): Try[Unit] = {
    logDir = constructTempDir("kafka-local")
    val properties = logDir.map(l => {
      val properties = new Properties()
      properties.setProperty("zookeeper.connect", zkConnection)
      properties.setProperty("broker.id", id.toString)
      properties.setProperty("host.name", InetAddress.getLocalHost.getHostAddress)
      properties.setProperty("port", Integer.toString(port))
      properties.setProperty("log.dir", l.getAbsolutePath)
      properties.setProperty("log.flush.interval.messages", String.valueOf(1))
      properties.setProperty("offsets.topic.replication.factor", String.valueOf(1))
      properties
    })
    kafkaServer = properties.map(p => new KafkaServer(new KafkaConfig(p, false)))
    Try {
      kafkaServer.foreach(_.startup())
    }
  }

  def shutdown(): Try[Unit] = Try {
    kafkaServer.foreach(_.shutdown())
    logDir.foreach(deleteFile(_))
  }

  def getConnection: String = s"${InetAddress.getLocalHost.getHostAddress}:$port"
}
