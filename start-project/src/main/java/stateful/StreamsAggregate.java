package stateful;

import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import utils.StreamUtil;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class StreamsAggregate {
    public static void main(String[] args) throws IOException {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateful-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put("schema.registry.url", "http://localhost:8081");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = "aggregate-input-topic";
        final String outputTopic = "aggregate-output-topic";
        final Map<String, Object> configMap = StreamUtil.propertiesToMap(config);

        final SpecificAvroSerde<ElectronicOrder> electronicSerde = StreamUtil.getSpecificAvroSerde(configMap);

        final KStream<String, ElectronicOrder> electronicStream = builder
                        .stream(inputTopic, Consumed.with(Serdes.String(), electronicSerde))
                        .peek((key, value) -> System.out.println("Incoming record -key " + key + " value " + value));

        electronicStream.groupByKey().aggregate(
                () -> 0.0,
                (key, order, total) -> total + order.getPrice(),
                Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .peek((key, value) -> System.out.println("Outgoing record - key " +key +" value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        TopicLoader.runProducer();
        kafkaStreams.start();
    }
}
