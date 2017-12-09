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
import java.net.{ InetAddress, InetSocketAddress }

import org.apache.spark.runner.utils._
import org.apache.zookeeper.server.quorum.{ QuorumPeer, QuorumPeerConfig }
import org.apache.zookeeper.server.{ NIOServerCnxnFactory, ServerCnxnFactory, ZKDatabase, ZooKeeperServer }

import scala.language.reflectiveCalls
import scala.util.{ Failure, Try }

trait EmbeddedZookeeper {
  def getConnection: String

  def startup(): Try[Unit]

  def stop(): Try[Unit]
}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Var"))
class EmbeddedQuorumZookeeper(config: QuorumPeerConfig) extends EmbeddedZookeeper {

  var quorumPeer: Option[QuorumPeer] = None

  private var thread: Option[Thread] = None

  def getConnection: String = quorumPeer.fold("")(quorumPeer => s"${quorumPeer.getQuorumAddress.getHostString}:${quorumPeer.getQuorumAddress.getPort}")

  def startup(): Try[Unit] = Try {
    val cnxnFactory = ServerCnxnFactory.createFactory
    cnxnFactory.configure(config.getClientPortAddress, config.getMaxClientCnxns)

    quorumPeer = Some(new QuorumPeer(
      config.getServers,
      new File(config.getDataDir),
      new File(config.getDataLogDir),
      config.getElectionAlg,
      config.getServerId,
      config.getTickTime,
      config.getInitLimit,
      config.getSyncLimit,
      config.getQuorumListenOnAllIPs,
      cnxnFactory,
      config.getQuorumVerifier))

    quorumPeer.foreach {
      quorumPeer =>
        quorumPeer.setClientPortAddress(config.getClientPortAddress)
        quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout)
        quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout)
        quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory))
        quorumPeer.setLearnerType(config.getPeerType)
        quorumPeer.setSyncEnabled(config.getSyncEnabled)
        quorumPeer.initialize()
        quorumPeer.start()
    }
  }

  def stop(): Try[Unit] = Try {
    quorumPeer.foreach(_.shutdown())
    val _ = deleteFile(new File(config.getDataDir))
  }

}

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Throw",
    "org.wartremover.warts.Var",
    "org.wartremover.warts.NonUnitStatements",
    "org.wartremover.warts.NoNeedForMonad"))
class SingleEmbeddedZookeeper(config: QuorumPeerConfig) extends EmbeddedZookeeper {

  private var factory: Try[ServerCnxnFactory] = Failure[ServerCnxnFactory](new Exception(""))
  private var snapshotDir: Try[File] = Failure[File](new Exception(""))
  private var logDir: Try[File] = Failure[File](new Exception(""))

  def startup(): Try[Unit] = {
    val maxNumClients = 1024
    factory = Try {
      new NIOServerCnxnFactory()
    }
    snapshotDir = constructTempDir("embedded-zk/snapshot")
    logDir = constructTempDir("embedded-zk/log")
    Try {
      for {
        f <- factory
        s <- snapshotDir
        l <- logDir
      } {
        f.configure(new InetSocketAddress(InetAddress.getLocalHost.getHostAddress, config.getClientPortAddress.getPort), maxNumClients)
        f.startup(new ZooKeeperServer(s, l, config.getTickTime))
      }
    }
  }

  def stop(): Try[Unit] = {
    Try {
      factory.foreach(_.shutdown())
      snapshotDir.foreach(deleteFile)
      logDir.foreach(deleteFile)
    }
  }

  def getConnection: String = s"${config.getClientPortAddress.getHostName}:${config.getClientPortAddress.getPort}"
}
