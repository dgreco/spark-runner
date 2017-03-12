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

import java.io.{ File, FileNotFoundException, IOException }
import java.net.{ InetAddress, InetSocketAddress }

import org.apache.zookeeper.server.{ NIOServerCnxnFactory, ServerCnxnFactory, ZooKeeperServer }

@SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Throw", "org.wartremover.warts.Var", "org.wartremover.warts.NonUnitStatements"))
class EmbeddedZookeeper(port: Int, tickTime: Int) {

  private var factory: ServerCnxnFactory = _
  private var snapshotDir: File = _
  private var logDir: File = _

  def startup(): Unit = {
    val maxNumClients = 1024
    factory = new NIOServerCnxnFactory()
    factory.configure(new InetSocketAddress(InetAddress.getLocalHost.getHostAddress, port), maxNumClients)
    snapshotDir = constructTempDir("embedded-zk/snapshot")
    logDir = constructTempDir("embedded-zk/log")
    try factory.startup(new ZooKeeperServer(snapshotDir, logDir, tickTime))
    catch {
      case e: InterruptedException => throw new IOException(e)
    }
  }

  def shutdown(): Unit = {
    factory.shutdown()
    try deleteFile(snapshotDir)
    catch {
      case _: FileNotFoundException =>
      // ignore
    }
    try deleteFile(logDir)
    catch {
      case _: FileNotFoundException =>
    }
    ()
  }

  def getConnection: String = s"${InetAddress.getLocalHost.getHostAddress}:$port"
}
