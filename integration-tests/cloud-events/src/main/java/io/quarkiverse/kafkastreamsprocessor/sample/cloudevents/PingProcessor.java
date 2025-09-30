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
package io.quarkiverse.kafkastreamsprocessor.sample.cloudevents;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.data.PojoCloudEventData;
import jakarta.inject.Inject;

import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import com.google.protobuf.InvalidProtocolBufferException;

import de.svenjacobs.loremipsum.LoremIpsum;
import io.quarkiverse.kafkastreamsprocessor.api.Processor;
import io.quarkiverse.kafkastreamsprocessor.api.exception.RetryableException;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Processor
public class PingProcessor extends ContextualProcessor<String, CloudEvent, String, CloudEvent> {
    @Override
    public void process(Record<String, CloudEvent> ping) {
        try {
            Ping unwrapped = Ping.parser().parseFrom(ping.value().getData().toBytes());
            CloudEvent wrappedPong = CloudEventBuilder.from(ping.value()).withData(
                PojoCloudEventData.wrap(
                    Ping.newBuilder().setMessage(Integer.toString(unwrapped.getMessage().length())).build(),
                    Ping::toByteArray))
                .withDataContentType(Ping.class.getTypeName())
                .build();
            context().forward(ping.withValue(wrappedPong));
        } catch (InvalidProtocolBufferException e) {

        }
    }
}
