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

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord, RecordMetadata }

sealed trait ExecutionContext {
  def id: Int

  def address: String
}

final case class SimpleExecutionContext(id: Int, address: String) extends ExecutionContext

final case class StreamingExecutionContext(id: Int, address: String, topic: String, producer: KafkaProducer[String, Array[Byte]]) extends ExecutionContext {
  def send(value: Array[Byte]): Future[RecordMetadata] = producer.send(new ProducerRecord[String, Array[Byte]](topic, id, address, value))
}
