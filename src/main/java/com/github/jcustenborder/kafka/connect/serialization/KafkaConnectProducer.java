/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.serialization;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.connect.runtime.TransformationChain;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaConnectProducer implements ConnectProducer {
  final Producer<byte[], byte[]> producer;
  final Converter keyConverter;
  final Converter valueConverter;
  final HeaderConverter headerConverter;
  final TransformationChain<SourceRecord> transformationChain;

  public KafkaConnectProducer(Properties properties) {
    this(new ConnectProducerConfig(properties));
  }

  public KafkaConnectProducer(Map<String, Object> settings) {
    this(new ConnectProducerConfig(settings));
  }

  public KafkaConnectProducer(ConnectProducerConfig config) {
    this(new KafkaProducer<>(config.producerSettings), config.keyConverter(), config.valueConverter(), config.headerConverter(), null);
  }

  public KafkaConnectProducer(Producer<byte[], byte[]> producer, Converter keyConverter, Converter valueConverter, HeaderConverter headerConverter, TransformationChain<SourceRecord> transformationChain) {
    this.producer = producer;
    this.keyConverter = keyConverter;
    this.valueConverter = valueConverter;
    this.headerConverter = headerConverter;
    this.transformationChain = transformationChain;
  }

  ProducerRecord<byte[], byte[]> createRecord(final SourceRecord input) {
    if (null == input) {
      return null;
    }
    final SourceRecord record = this.transformationChain.apply(input);

    if (null == record) {
      // This code path happens when a SourceRecord is filtered by a transformation.
      return null;
    }

    final byte[] key = this.keyConverter.fromConnectData(
        record.topic(),
        record.keySchema(),
        record.key()
    );
    final byte[] value = this.valueConverter.fromConnectData(
        record.topic(),
        record.valueSchema(),
        record.value()
    );
    List<Header> headers = new ArrayList<>();
    return new ProducerRecord<>(record.topic(), record.kafkaPartition(), record.timestamp(), key, value, headers);
  }

  @Override
  public Future<RecordMetadata> send(SourceRecord record) {
    return send(record, null);
  }

  @Override
  public Future<RecordMetadata> send(SourceRecord record, Callback callback) {
    ProducerRecord producerRecord = createRecord(record);

    if (null == producerRecord) {
      // This code path happens when a SourceRecord is filtered by a transformation.
      return null;
    }

    return this.producer.send(producerRecord, callback);
  }

  @Override
  public void close() throws IOException {
    this.producer.close();
  }
}
