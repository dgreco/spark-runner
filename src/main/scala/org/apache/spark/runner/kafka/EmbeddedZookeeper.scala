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
import org.apache.zookeeper.server.{ NIOServerCnxnFactory, ServerCnxnFactory, ZooKeeperServer }

import scala.util.{ Failure, Try }

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Null",
    "org.wartremover.warts.Throw",
    "org.wartremover.warts.Var",
    "org.wartremover.warts.NonUnitStatements",
    "org.wartremover.warts.NoNeedForMonad"
  )
)
class EmbeddedZookeeper(port: Int, tickTime: Int) {

  private var factory: Try[ServerCnxnFactory] = Failure[ServerCnxnFactory]((new Exception("")))
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
        f.configure(new InetSocketAddress(InetAddress.getLocalHost.getHostAddress, port), maxNumClients)
        f.startup(new ZooKeeperServer(s, l, tickTime))
      }
    }
  }

  def shutdown(): Try[Unit] = {
    Try {
      factory.foreach(_.shutdown())
      snapshotDir.foreach(deleteFile(_))
      logDir.foreach(deleteFile(_))
    }
  }

  def getConnection: String = s"${InetAddress.getLocalHost.getHostAddress}:$port"
}
