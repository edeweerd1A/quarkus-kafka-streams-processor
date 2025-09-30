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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import io.cloudevents.CloudEvent;
import io.cloudevents.kafka.CloudEventDeserializer;
import io.cloudevents.kafka.CloudEventSerializer;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.Configuration;
import io.quarkiverse.kafkastreamsprocessor.api.configuration.ConfigurationCustomizer;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

@Dependent
@Priority(1)
public class CloudEventConfigCustomizer implements ConfigurationCustomizer {
    private final Serde<CloudEvent> serde;
    private final CloudEventSerializer serializer;

    @Inject
    public CloudEventConfigCustomizer() {
        this.serializer = new CloudEventSerializer();
        this.serde = new Serdes.WrapperSerde<>(serializer, new CloudEventDeserializer());
    }

    @Override
    public void fillConfiguration(Configuration configuration) {
        configuration.setSourceValueSerde(serde);
        configuration.setSinkValueSerializer(serializer);
    }
}
