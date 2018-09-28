/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
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
package com.github.jcustenborder.kafka.connect.serialization;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Closeable;
import java.util.concurrent.Future;

public interface ConnectProducer extends Closeable   {
  /**
   * Method wraps the standard Producer interface so refer to the documentation there. The one
   * caveat is this method can return a null future in the case that the record was filtered by
   * a transformation.
   *
   * @param record
   * @return In the case that a record was filtered by a transformation, this could return null.
   * @see org.apache.kafka.clients.producer.Producer#send(ProducerRecord)
   */
  Future<RecordMetadata> send(SourceRecord record);

  /**
   * Method wraps the standard Producer interface so refer to the documentation there. The one
   * caveat is this method can return a null future in the case that the record was filtered by
   * a transformation.
   *
   * @param record
   * @param callback
   * @return In the case that a record was filtered by a transformation, this could return null.
   * @see org.apache.kafka.clients.producer.Producer#send(ProducerRecord, Callback)
   */
  Future<RecordMetadata> send(SourceRecord record, Callback callback);
}
