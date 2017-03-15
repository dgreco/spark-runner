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

package org.apache.spark.runner.functions

import java.net.InetAddress

import org.apache.spark.runner.{ ExecutionContext, StreamingExecutionContext }

@SuppressWarnings(Array("org.wartremover.warts.Equals"))
object GetAddress extends (ExecutionContext => (String, String)) with Serializable {
  override def apply(ec: ExecutionContext): (String, String) = {
    val address: InetAddress = InetAddress.getLocalHost
    assert(ec.address == address.getHostAddress)
    (address.getHostAddress, address.getHostName)
  }
}

final case class SendNInts(numInts: Int) extends (StreamingExecutionContext => Unit) with Serializable {
  override def apply(ec: StreamingExecutionContext): Unit = {
    for (i <- 1 to numInts)
      ec.send(i.toString.getBytes)
  }
}
