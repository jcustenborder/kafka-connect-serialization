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
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata;
import org.apache.kafka.clients.producer.internals.ProduceRequestResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.TransformationChain;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.Date;
import java.util.concurrent.Future;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaConnectProducerTest {
  Producer<byte[], byte[]> producer;
  Converter keyConverter;
  Converter valueConverter;
  HeaderConverter headerConverter;
  TransformationChain<SourceRecord> transformationChain;

  @Before
  public void before() {
    this.producer = mock(Producer.class);

    when(this.producer.send(any(ProducerRecord.class), any(Callback.class))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        ProducerRecord<?, ?> record = invocationOnMock.getArgumentAt(0, ProducerRecord.class);
        TopicPartition topicPartition = new TopicPartition(record.topic(), 0);
        ProduceRequestResult result = new ProduceRequestResult(topicPartition);
        return new FutureRecordMetadata(result, 123L, new Date().getTime(), null, 1234, 123);
      }
    });


    this.keyConverter = spy(new JsonConverter());
    this.keyConverter.configure(Collections.EMPTY_MAP, true);
    this.valueConverter = spy(new JsonConverter());
    this.valueConverter.configure(Collections.EMPTY_MAP, false);
    this.headerConverter = new SimpleHeaderConverter();

    Time time = Time.SYSTEM;
    RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(RetryWithToleranceOperator.RETRIES_DELAY_MIN_MS, RetryWithToleranceOperator.RETRIES_DELAY_MIN_MS, ToleranceType.NONE, time);
    this.transformationChain = new TransformationChain<>(Collections.EMPTY_LIST, retryWithToleranceOperator);
  }

  @Test
  public void send() {
    ConnectProducer connectProducer = new KafkaConnectProducer(producer, keyConverter, valueConverter, headerConverter, transformationChain);
    SourceRecord record = new SourceRecord(Collections.EMPTY_MAP, Collections.EMPTY_MAP, "test", Schema.STRING_SCHEMA, "foo");
    Future<RecordMetadata> metadataFuture = connectProducer.send(record);
    assertNotNull(metadataFuture);

    verify(keyConverter, times(1)).configure(anyMap(), eq(true));
    verify(keyConverter, times(1)).fromConnectData(eq("test"), eq(null), eq(null));
    verify(valueConverter, times(1)).configure(anyMap(), eq(false));
    verify(valueConverter, times(1)).fromConnectData(eq("test"), eq(Schema.STRING_SCHEMA), eq("foo"));

  }
}
