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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.quarkiverse.kafkastreamsprocessor.api.exception.RetryableException;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;

@ExtendWith(MockitoExtension.class)
public class PingProcessorTest {

    @Mock
    ProcessorContext<String, CloudEvent> context;

    PingProcessor processor;

    @Captor
    ArgumentCaptor<Record<String, CloudEvent>> captor;

    @BeforeEach
    public void setup() {
        processor = new PingProcessor();
        processor.init(context);
    }

    @Test
    public void repliesWithPong() {
        PojoCloudEventData<Ping> pingPojoCloudEventData =
            PojoCloudEventData.wrap(Ping.newBuilder().setMessage("world").build(), Ping::toByteArray);
        CloudEvent ping = new CloudEventBuilder().withDataContentType(Ping.class.getTypeName())
            .withId("myID")
            .withSource(URI.create("https://example.com"))
            .withType("Blabla")
            .withData(pingPojoCloudEventData)
            .build();
        System.out.println(ping.toString());
        System.out.println(ping.getData().toString());

        processor.process(new Record<>("key", ping, 0L));

        verify(context).forward(captor.capture());
        CloudEvent pong = captor.getValue().value();

        System.out.println(pong.toString());
        System.out.println(pong.getData().toString());
        assertThat(pong.getId(), equalTo("myID"));
        assertThat(pong.getDataContentType(), equalTo(Ping.class.getTypeName()));
        assertThat(pong.getData().toBytes(), equalTo(Ping.newBuilder().setMessage("5").build().toByteArray()));
    }

}
