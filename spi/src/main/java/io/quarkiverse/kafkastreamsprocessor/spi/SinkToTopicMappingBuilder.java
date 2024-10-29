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
package io.quarkiverse.kafkastreamsprocessor.spi;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;

import lombok.extern.slf4j.Slf4j;

/**
 * Object to inject to get access to the resolved mapping between sink and topic for a multi output processor, using the
 * conventions set up by the framework based on config properties like:
 *
 * <pre>
 * quarkus.kafkastreamsprocessor.output.sinks.pong.topic=pong-events
 * quarkus.kafkastreamsprocessor.output.sinks.pang.topic=pang-events
 * </pre>
 * <p>
 * Where:
 * </p>
 * <ul>
 * <li>pong and pang are the sinks</li>
 * <li>pong-events and pang-events the Kafka topics</li>
 * </ul>
 * Multi-output topic configuration.
 * <p>
 * Inspired by <a href=
 * "https://github.com/smallrye/smallrye-reactive-messaging/blob/master/smallrye-reactive-messagingprovider/src/main/java/io/smallrye/reactive/messaging/impl/ConfiguredChannelFactory.java">smallrye-reactive-messaging
 * ConfiguredChannelFactory</a>
 * </p>
 * <p>
 * Example of usage in the multioutput integration test.
 * </p>
 */
@ApplicationScoped
@Slf4j
public class SinkToTopicMappingBuilder {
    private static final Pattern P_SINK = Pattern.compile("quarkus\\.kafkastreamsprocessor\\.output\\.sinks\\.(.*)\\.topic");
    /**
     * Default sink name created by KafkaStreams if no sink is configured manually
     */
    private static final String DEFAULT_SINK_NAME = "emitter-channel";

    private final Config config;

    /**
     * Constructor for CDI and usage in tests.
     *
     * @param config
     *        the configuration
     */
    @Inject
    public SinkToTopicMappingBuilder(Config config) {
        this.config = config;
    }

    /**
     * Looks at the configuration and extracts from it the mapping from the sink to the Kafka topic.
     * <p>
     * This method is exposed so you can do any kind of technical postprocessing based on the Kafka topic and the sink
     * names.
     * </p>
     *
     * @return a map with keys the sink names and values the corresponding Kafka topic name
     */
    public Map<String, String> sinkToTopicMapping() {
        // Extract topic name for each sink if any has been configured
        Map<String, String> sinkToTopicMapping = buildMapping(config);

        // Backward compatibility
        if (sinkToTopicMapping.isEmpty()) {
            Optional<String> singleOutputTopic = config.getOptionalValue("quarkus.kafkastreamsprocessor.output.topic",
                    String.class);
            if (singleOutputTopic.isPresent()) {
                return Map.of(DEFAULT_SINK_NAME, singleOutputTopic.get());
            }
        }

        return sinkToTopicMapping;
    }

    private Map<String, String> buildMapping(Config config) {
        Map<String, String> sinkToTopicMapping = new HashMap<>();
        for (String property : config.getPropertyNames()) {
            Matcher matcher = P_SINK.matcher(property);
            if (matcher.matches()) {
                String sinkName = matcher.group(1);
                String topic = config.getValue(property, String.class);
                if (sinkName.contains(".")) {
                    throw new IllegalStateException("Parsed sink name has a dot: " + sinkName);
                }
                sinkToTopicMapping.put(sinkName, topic);
            }
        }
        return sinkToTopicMapping;
    }

}
