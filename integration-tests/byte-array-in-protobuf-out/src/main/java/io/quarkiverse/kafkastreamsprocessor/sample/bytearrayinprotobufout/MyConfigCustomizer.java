/*-
 * #%L
 * Quarkus Kafka Streams Processor
 * %%
 * Copyright (C) 2024 Amadeus s.a.s.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package io.quarkiverse.kafkastreamsprocessor.sample.bytearrayinprotobufout;

import java.nio.charset.StandardCharsets;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.Dependent;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.ConfigurationCustomizer;

@Dependent
@Priority(1)
public class MyConfigCustomizer implements ConfigurationCustomizer {
    @Override
    public void fillConfiguration(Configuration configuration) {
        configuration.setSourceValueSerde(new ByteArrayThrowingSerde());
    }

    public static class ByteArrayThrowingSerde implements Serde<byte[]> {
        @Override
        public Serializer<byte[]> serializer() {
            return new ByteArraySerializer();
        }

        @Override
        public Deserializer<byte[]> deserializer() {
            return new ByteArrayThrowingDeserializer();
        }
    }

    public static class ByteArrayThrowingDeserializer implements Deserializer<byte[]> {
        @Override
        public byte[] deserialize(String topic, byte[] data) {
            if ("error".equals(new String(data, StandardCharsets.UTF_8))) {
                throw new RuntimeException("Deserialization error");
            }
            return data;
        }
    }
}
