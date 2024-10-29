package io.quarkiverse.kafkastreamsprocessor.sample.bytearrayinprotobufout;

import static io.restassured.RestAssured.when;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Durations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.github.daniel.shuy.kafka.protobuf.serde.KafkaProtobufDeserializer;

import io.quarkiverse.kafkastreamsprocessor.sample.message.PingMessage;
import io.quarkiverse.kafkastreamsprocessor.testframework.KafkaBootstrapServers;
import io.quarkiverse.kafkastreamsprocessor.testframework.QuarkusIntegrationCompatibleKafkaDevServicesResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;

/**
 * Blackbox test that can run both in JVM and native modes (@Inject and @ConfigProperty not allowed)
 */
@QuarkusTest
@TestProfile(PingProcessorQuarkusTest.TestProfile.class)
@QuarkusTestResource(QuarkusIntegrationCompatibleKafkaDevServicesResource.class)
public class PingProcessorQuarkusTest {
    @KafkaBootstrapServers
    String kafkaBootstrapServers;

    String senderTopic = "ping-events";

    String consumerTopic = "pong-events";

    String dlqTopic = "dead-letter-queue";

    KafkaProducer<String, byte[]> producer;

    KafkaConsumer<String, PingMessage.Ping> consumer;

    KafkaConsumer<String, byte[]> dlqConsumer;

    @BeforeEach
    public void setup() throws Exception {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafkaBootstrapServers, "test", "true");
        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(),
                new KafkaProtobufDeserializer<>(PingMessage.Ping.parser()));
        consumer.subscribe(List.of(consumerTopic));

        Map<String, Object> dlqConsumerProps = KafkaTestUtils.consumerProps(kafkaBootstrapServers, "test", "true");
        dlqConsumer = new KafkaConsumer<>(dlqConsumerProps, new StringDeserializer(), new ByteArrayDeserializer());
        dlqConsumer.subscribe(List.of(dlqTopic));

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafkaBootstrapServers);
        producer = new KafkaProducer<>(producerProps, new StringSerializer(), new ByteArraySerializer());
    }

    @AfterEach
    public void tearDown() {
        producer.close();
        consumer.close();
        dlqConsumer.close();
    }

    @Test
    void countCharactersNotInDlq() throws Exception {
        Thread.sleep(5000);

        producer.send(new ProducerRecord<>(senderTopic, "1", "my_name".getBytes(StandardCharsets.UTF_8)));
        producer.flush();
        ConsumerRecord<String, PingMessage.Ping> record = KafkaTestUtils.getSingleRecord(consumer, consumerTopic,
                Durations.FIVE_SECONDS);
        assertThat(record.value().getMessage(), equalTo("7"));

        ConsumerRecords<String, byte[]> dlqRecords = KafkaTestUtils.getRecords(dlqConsumer, Durations.FIVE_SECONDS);
        assertThat(dlqRecords.count(), equalTo(0));
    }

    @Test
    void testProbes() throws Exception {
        Thread.sleep(5000);

        when().get("/q/health/ready").then().statusCode(200);

        when().get("/q/health/live").then().statusCode(200);
    }

    @Test
    void deserializationException() throws Exception {
        Thread.sleep(5000);

        producer.send(new ProducerRecord<>(senderTopic, "1", "error".getBytes(StandardCharsets.UTF_8)));
        producer.flush();
        ConsumerRecords<String, PingMessage.Ping> records = KafkaTestUtils.getRecords(consumer,
                Durations.FIVE_SECONDS);
        assertThat(records.count(), equalTo(0));

        ConsumerRecords<String, byte[]> dlqRecords = KafkaTestUtils.getRecords(dlqConsumer, Durations.TEN_SECONDS, 1);
        assertThat(new String(dlqRecords.iterator().next().value(), StandardCharsets.UTF_8), equalTo("error"));
    }

    public static class TestProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of(
                    "kafkastreamsprocessor.error-strategy", "dead-letter-queue",
                    "kafkastreamsprocessor.dlq.topic", "dead-letter-queue",
                    "quarkus.kafka-streams.topics", "ping-events,pong-events,dead-letter-queue");
        }
    }

}
