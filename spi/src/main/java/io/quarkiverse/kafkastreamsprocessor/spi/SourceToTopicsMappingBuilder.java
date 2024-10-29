package io.quarkiverse.kafkastreamsprocessor.spi;

import java.util.Map;

public interface SourceToTopicsMappingBuilder {
    /**
     * Looks at the configuration and extracts from it the mapping from the source to the Kafka topic(s).
     * <p>
     * This method is exposed so you can do any kind of technical postprocessing based on the Kafka topic and the source
     * names.
     * </p>
     *
     * @return a map with keys the sink names and values the corresponding list of Kafka topic names
     */
    Map<String, String[]> sourceToTopicsMapping();
}
