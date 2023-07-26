
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.ConfluentSchemaRegistryDeserializerProvider;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.Duration;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.lang.System.exit;


public class KafkaBeamBeamPipeline  implements Serializable {


    /**
     * ConsumerFactoryFn will be call here
     */

    protected PCollection<GenericRecord> configurePipeline(KafkaBeamPipelineOptions options, Pipeline p, RunningSchemaVersions runningSchemaVersions) {
        List<String> srcTopics = Arrays.asList(options.getSourceTopics().get().replaceAll(" ", "").split(","));
        int windowDuration = options.getWindowDuration().get();
        return p.apply("Read from Kafka", KafkaIO.<byte[], GenericRecord>read()
                        .withBootstrapServers(options.getSourceBrokers().get())
                        .withTopics(srcTopics)
                        .withConsumerFactoryFn(new ConsumerFactoryFn())
                        .withKeyDeserializer(ByteArrayDeserializer.class)
                        .withConsumerConfigUpdates(recordConsumerExtraOptions(options))
                        .withOffsetConsumerConfigOverrides(offsetConsumerExtraOptions(options))
                        .withValueDeserializer(ConfluentSchemaRegistryDeserializerProvider.of(
                                options.getSourceSchemaRegistryUrl().get(),
                                options.getSourceSubject().get(),
                                runningSchemaVersions.getSourceSchemaVersion()))
                        .commitOffsetsInFinalize()
                        .withoutMetadata()).apply("Drop keys", Values.<GenericRecord>create())
                .apply("Windowing of " + windowDuration + " seconds", Window.<GenericRecord>into(FixedWindows.of(Duration.standardSeconds(windowDuration))));
    }


    /**
     * A Kafka consumer Factory that creates a new consumer that pauses reading from the topics when shutdown is initiated
     */
    private static class ConsumerFactoryFn
            implements SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> {
        ConsumerFactoryFn() {
        }

        @Override
        public Consumer<byte[], byte[]> apply(Map<String, Object> config) {

            // Create the consumer using config.
            final Consumer<byte[], byte[]> consumer =
                    new MyKafkaConsumer(config);

            return consumer;
        }
    }

}
