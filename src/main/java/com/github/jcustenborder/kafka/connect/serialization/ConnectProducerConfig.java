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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.TransformationChain;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.errors.ToleranceType;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class ConnectProducerConfig extends AbstractConfig {
  public static final String PRODUCER_PREFIX = "producer.";
  public final Map<String, Object> producerSettings;
  public static final String TRANSFORMS_CONFIG = "transforms";
  private static final String TRANSFORMS_DOC = "Aliases for the transformations to be applied to records.";

  public static final String KEY_CONVERTER_CLASS_CONFIG = WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
  public static final String KEY_CONVERTER_CLASS_DOC = WorkerConfig.KEY_CONVERTER_CLASS_DOC;

  public static final String VALUE_CONVERTER_CLASS_CONFIG = WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;
  public static final String VALUE_CONVERTER_CLASS_DOC = WorkerConfig.VALUE_CONVERTER_CLASS_DOC;

  public static final String HEADER_CONVERTER_CLASS_CONFIG = WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG;
  public static final String HEADER_CONVERTER_CLASS_DOC = WorkerConfig.HEADER_CONVERTER_CLASS_DOC;
  // The Connector config should not have a default for the header converter, since the absence of a config property means that
  // the worker config settings should be used. Thus, we set the default to null here.
  public static final String HEADER_CONVERTER_CLASS_DEFAULT = SimpleHeaderConverter.class.getName();

  public ConnectProducerConfig(Map<?, ?> originals) {
    super(config(), originals);
    Map<String, Object> producerSettings = originalsWithPrefix(PRODUCER_PREFIX, true);
    producerSettings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerSettings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    this.producerSettings = producerSettings;
  }

  public Converter keyConverter() {
    return null;
  }

  public Converter valueConverter() {
    return null;
  }

  public HeaderConverter headerConverter() {
    return null;
  }

  public static ConfigDef config() {
    return new ConfigDef()
        .define(KEY_CONVERTER_CLASS_CONFIG, Type.CLASS, null, Importance.LOW, KEY_CONVERTER_CLASS_DOC)
        .define(VALUE_CONVERTER_CLASS_CONFIG, Type.CLASS, null, Importance.LOW, VALUE_CONVERTER_CLASS_DOC)
        .define(HEADER_CONVERTER_CLASS_CONFIG, Type.CLASS, HEADER_CONVERTER_CLASS_DEFAULT, Importance.LOW, HEADER_CONVERTER_CLASS_DOC)
        .define(TRANSFORMS_CONFIG, Type.LIST, Collections.emptyList(), ConfigDef.CompositeValidator.of(new ConfigDef.NonNullValidator(), new ConfigDef.Validator() {
          @Override
          public void ensureValid(String name, Object value) {
            final List<String> transformAliases = (List<String>) value;
            if (transformAliases.size() > new HashSet<>(transformAliases).size()) {
              throw new ConfigException(name, value, "Duplicate alias provided.");
            }
          }

          @Override
          public String toString() {
            return "unique transformation aliases";
          }
        }), ConfigDef.Importance.LOW, TRANSFORMS_DOC);
  }

  public List<Transformation<SourceRecord>> transformations() {
    final List<String> transformAliases = getList(TRANSFORMS_CONFIG);

    final List<Transformation<SourceRecord>> transformations = new ArrayList<>(transformAliases.size());
    for (String alias : transformAliases) {
      final String prefix = TRANSFORMS_CONFIG + "." + alias + ".";
      final Transformation<SourceRecord> transformation;
      try {
        transformation = getClass(prefix + "type").asSubclass(Transformation.class).newInstance();
      } catch (Exception e) {
        throw new ConnectException(e);
      }
      transformation.configure(originalsWithPrefix(prefix));
      transformations.add(transformation);
    }

    return transformations;
  }

  public TransformationChain<SourceRecord> transformationChain() {
    RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(
        RetryWithToleranceOperator.RETRIES_DELAY_MIN_MS,
        RetryWithToleranceOperator.RETRIES_DELAY_MIN_MS,
        ToleranceType.NONE,
        Time.SYSTEM
    );
    return new TransformationChain<>(transformations(), retryWithToleranceOperator);
  }
}
