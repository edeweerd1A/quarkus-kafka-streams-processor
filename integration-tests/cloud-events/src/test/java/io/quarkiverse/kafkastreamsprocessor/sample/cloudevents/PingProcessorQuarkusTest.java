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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.data.PojoCloudEventData;
import io.cloudevents.core.v1.CloudEventBuilder;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage.Ping;
import io.quarkiverse.kafkastreamsprocessor.testframework.KafkaBootstrapServers;
import io.quarkiverse.kafkastreamsprocessor.testframework.QuarkusIntegrationCompatibleKafkaDevServicesResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Durations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * Blackbox test that can run both in JVM and native modes (@Inject and @ConfigProperty not allowed)
 */
@QuarkusTest
@QuarkusTestResource(QuarkusIntegrationCompatibleKafkaDevServicesResource.class)
class PingProcessorQuarkusTest {
  @KafkaBootstrapServers
  String kafkaBootstrapServers;

  String senderTopic = "ping-events";

  String consumerTopic = "pong-events";

  KafkaProducer<String, CloudEvent> producer;

  KafkaConsumer<String, CloudEvent> consumer;

  @BeforeEach
  void setup() {
    Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafkaBootstrapServers, "test", "true");
    consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), new CloudEventDeserializer());
    consumer.subscribe(List.of(consumerTopic));

    Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafkaBootstrapServers);
    producer = new KafkaProducer<>(producerProps, new StringSerializer(),
        new CloudEventSerializer());
  }

  @AfterEach
  void tearDown() {
    producer.close();
    consumer.close();
  }

  @Test
    void testCount() throws Exception {
        Ping world = Ping.newBuilder().setMessage("world").build();
        PojoCloudEventData eventData = PojoCloudEventData.wrap(world, Ping::toByteArray);
        CloudEvent event = new CloudEventBuilder().newBuilder().withData(eventData)
                .withSource(URI.create("blabla"))
                    .withId("blabla")
                        .withType(Ping.class.getTypeName()).build();
        producer.send(new ProducerRecord<>(senderTopic, event));
        producer.flush();
        ConsumerRecord<String, CloudEvent> record = KafkaTestUtils.getSingleRecord(consumer, consumerTopic, Durations.FIVE_SECONDS);
        assertEquals("5", Ping.parseFrom(record.value().getData().toBytes()).getMessage());
    }

}
