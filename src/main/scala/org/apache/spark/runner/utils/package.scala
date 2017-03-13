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

package org.apache.spark.runner

import java.io.{ File, FileNotFoundException, IOException }
import java.net.ServerSocket

import scala.util.{ Random, Try }

package object utils {
  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  def getAvailablePort: Int = {
    try {
      val socket = new ServerSocket(0)
      try {
        socket.getLocalPort
      } finally {
        socket.close()
      }
    } catch {
      case e: IOException =>
        throw new IllegalStateException(s"Cannot find available port: ${e.getMessage}", e)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  def constructTempDir(dirPrefix: String): Try[File] = Try {
    val rndinterval = 10000000
    val file = new File(System.getProperty("java.io.tmpdir"), dirPrefix + Random.nextInt(rndinterval))
    if (!file.mkdirs)
      throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath)
    file.deleteOnExit()
    file
  }

  @SuppressWarnings(Array("org.wartremover.warts.Throw", "org.wartremover.warts.Var"))
  def deleteFile(path: File): Boolean = {
    if (!path.exists()) {
      throw new FileNotFoundException(path.getAbsolutePath)
    }
    var ret = true
    if (path.isDirectory)
      path.listFiles().foreach(f => ret = ret && deleteFile(f))
    ret
  }

}
