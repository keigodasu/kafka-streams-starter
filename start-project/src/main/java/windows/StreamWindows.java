package windows;

import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import utils.StreamUtil;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

public class StreamWindows {
    public static void main(String[] args) throws IOException {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put("schema.registry.url", "http://localhost:8081");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = "window-input-topic";
        final String outputTopic = "window-output-topic";
        final Map<String, Object> configMap = StreamUtil.propertiesToMap(config);

        final SpecificAvroSerde<ElectronicOrder> electronicSerde = StreamUtil.getSpecificAvroSerde(configMap);

        final KStream<String, ElectronicOrder> electronicStream = builder
                .stream(inputTopic, Consumed.with(Serdes.String(), electronicSerde))
                .peek((key, value) -> System.out.println("Incoming record - key " +key +" value " + value));

        electronicStream.groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .aggregate(() -> 0.0,
                        (key, order, total) -> total + order.getPrice(),
                        Materialized.with(Serdes.String(), Serdes.Double()))
                //Not emit calculated output until specified window closes
                .suppress(untilWindowCloses(BufferConfig.unbounded()))
                .toStream()
                //Windowed Record(KEY + WINDOW TIMESTAMP) to Normal Record
                .peek((key, value) -> System.out.println("Windowed record - key " +key +" value " + value))
                .map((windowedKey, value) -> KeyValue.pair(windowedKey.key(), value))
                .peek((key, value) -> System.out.println("Outgoing record - key " +key +" value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        TopicLoader.runProducer();
        kafkaStreams.start();
    }
}
