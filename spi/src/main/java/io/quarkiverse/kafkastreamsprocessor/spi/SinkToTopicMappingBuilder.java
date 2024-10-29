package io.quarkiverse.kafkastreamsprocessor.spi;

import java.util.Map;

public interface SinkToTopicMappingBuilder {
    /**
     * Looks at the configuration and extracts from it the mapping from the sink to the Kafka topic.
     * <p>
     * This method is exposed so you can do any kind of technical postprocessing based on the Kafka topic and the sink
     * names.
     * </p>
     *
     * @return a map with keys the sink names and values the corresponding Kafka topic name
     */
    Map<String, String> sinkToTopicMapping();
}
