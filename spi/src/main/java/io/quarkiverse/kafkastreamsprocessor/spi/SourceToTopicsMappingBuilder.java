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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;

import lombok.extern.slf4j.Slf4j;

/**
 * Object to inject to get access to the resolved mapping between input topics and sources for a multi input processor,
 * using the conventions set up by the framework based on config properties like:
 *
 * <pre>
 * quarkus.kafkastreamsprocessor.input.sources.pong.topics=pong-events
 * quarkus.kafkastreamsprocessor.input.sources.pang.topics=pang-events,ping-events
 * </pre>
 * <p>
 * Where:
 * </p>
 * <ul>
 * <li>pong and pang are the sources</li>
 * <li>ping-events, pong-events and pang-events the Kafka topics</li>
 * </ul>
 * Multi-input topic configuration Inspired by <a href=
 * "https://github.com/smallrye/smallrye-reactive-messaging/blob/main/smallrye-reactive-messaging-provider/src/main/java/io/smallrye/reactive/messaging/impl/ConfiguredChannelFactory.java">smallrye-reactive-messaging
 * ConfiguredChannelFactory</a>
 */
@ApplicationScoped
@Slf4j
public class SourceToTopicsMappingBuilder {

    private static final Pattern P_SOURCE = Pattern
            .compile("quarkus\\.kafkastreamsprocessor\\.input\\.sources\\.(.*)\\.topics");
    /**
     * Default source name created by KafkaStreams if no source is configured manually
     */
    private static final String DEFAULT_SOURCE_NAME = "receiver-channel";

    private final Config config;

    /**
     * Constructor for CDI and usage in tests.
     *
     * @param config
     *        the configuration
     */
    @Inject
    public SourceToTopicsMappingBuilder(Config config) {
        this.config = config;
    }

    /**
     * Looks at the configuration and extracts from it the mapping from the source to the Kafka topic(s).
     * <p>
     * This method is exposed so you can do any kind of technical postprocessing based on the Kafka topic and the source
     * names.
     * </p>
     *
     * @return a map with keys the sink names and values the corresponding list of Kafka topic names
     */
    public Map<String, String[]> sourceToTopicsMapping() {
        // Extract topic name for each channel
        Map<String, String[]> sourceToTopicMapping = buildMapping(config);

        // Backward compatibility
        if (sourceToTopicMapping.isEmpty()) {
            Optional<List<String>> inputTopicList = config.getOptionalValues("quarkus.kafkastreamsprocessor.input.topics",
                    String.class);
            if (inputTopicList.isPresent()) {
                return Map.of(DEFAULT_SOURCE_NAME, inputTopicList.get().toArray(new String[0]));
            }
            Optional<String> singleInputTopic = config.getOptionalValue("quarkus.kafkastreamsprocessor.input.topic",
                    String.class);
            if (singleInputTopic.isPresent()) {
                return Map.of(DEFAULT_SOURCE_NAME, new String[] { singleInputTopic.get() });
            }
        }

        return sourceToTopicMapping;
    }

    private Map<String, String[]> buildMapping(Config config) {
        Map<String, String[]> sourceToTopicMapping = new HashMap<>();
        for (String property : config.getPropertyNames()) {
            Matcher matcher = P_SOURCE.matcher(property);
            if (matcher.matches()) {
                String sourceName = matcher.group(1);
                String[] topics = config.getValue(property, String.class).split(",");
                if (sourceName.contains(".")) {
                    throw new IllegalStateException("Parsed source name has a dot: " + sourceName);
                }
                sourceToTopicMapping.put(sourceName, topics);
            }
        }
        return sourceToTopicMapping;
    }
}
