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

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Processor
public class PingProcessor extends ContextualProcessor<String, byte[], String, PingMessage.Ping> {
    @Override
    public void process(Record<String, byte[]> ping) {
        String value = new String(ping.value(), StandardCharsets.UTF_8);
        log.info("Process the value: {}", value);
        context().forward(ping.withValue(PingMessage.Ping.newBuilder().setMessage("" + value.length()).build()));
    }
}
