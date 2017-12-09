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

import java.nio.file.{ Files, Paths }
import java.util.Properties

import org.apache.spark.runner.utils.constructTempDir
import org.apache.zookeeper.server.quorum.QuorumPeerConfig

import scala.language.postfixOps

final case class InstanceSpec(
  port: Int,
  electionPort: Int,
  quorumPort: Int,
  deleteDataDirectoryOnClose: Boolean,
  serverId: Int,
  tickTime: Int,
  maxClientCnxns: Int,
  customProperties: Map[String, AnyRef],
  hostname: String)

@SuppressWarnings(Array(
  "org.wartremover.warts.Throw",
  "org.wartremover.warts.Overloading",
  "org.wartremover.warts.Equals",
  "org.wartremover.warts.ArrayEquals",
  "org.wartremover.warts.NonUnitStatements"))
final case class QuorumConfigBuilder(instanceSpecs: Array[InstanceSpec]) {

  def buildConfig(instanceIndex: Int): QuorumPeerConfig = {
    val spec = instanceSpecs(instanceIndex)
    val dir = constructTempDir("embedded-zk").getOrElse(throw new RuntimeException).getCanonicalPath

    Files.write(Paths.get(s"$dir/myid"), s"${spec.serverId}".getBytes)

    val properties = new Properties()
    properties.setProperty("initLimit", "10")
    properties.setProperty("syncLimit", "5")
    properties.setProperty("dataDir", dir)
    properties.setProperty("clientPort", s"${spec.port}")
    val tickTime = spec.tickTime
    if (tickTime >= 0)
      properties.setProperty("tickTime", s"$tickTime")
    val maxClientCnxns = spec.maxClientCnxns
    if (maxClientCnxns >= 0)
      properties.setProperty("maxClientCnxns", s"$maxClientCnxns")
    for (thisSpec <- instanceSpecs) {
      properties.setProperty(
        s"server.${thisSpec.serverId}",
        s"${thisSpec.hostname}:${thisSpec.quorumPort}:${thisSpec.electionPort}")
    }
    val customProperties = spec.customProperties
    if (customProperties != null) {
      for (property <- customProperties.seq) {
        properties.put(property._1, property._2)
      }
    }
    val config = new QuorumPeerConfig() {}
    config.parseProperties(properties)
    config
  }
}

@SuppressWarnings(Array(
  "org.wartremover.warts.Overloading"))
object QuorumConfigBuilder {
  def apply(hosts: Array[(String, Int)]): QuorumConfigBuilder = new QuorumConfigBuilder(
    hosts map {
      host =>
        {
          val port = host._2
          InstanceSpec(
            port = port,
            electionPort = port + 10,
            quorumPort = port + 20,
            deleteDataDirectoryOnClose = true,
            serverId = host._2,
            tickTime = 1000,
            maxClientCnxns = 100,
            customProperties = Map.empty[String, AnyRef],
            hostname = host._1)
        }
    })
}
