package join;

import io.confluent.developer.avro.ApplianceOrder;
import io.confluent.developer.avro.CombinedOrder;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.developer.avro.User;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class JoinWithSchema {
    static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Map<String, Object> serdeConfig) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(serdeConfig, false);
        return specificAvroSerde;
    }

    static Map<String, Object> propertiesToMap(final Properties properties) {
        final Map<String, Object> configs = new HashMap<>();
        properties.forEach((key, value) -> configs.put((String) key, value));
        return configs;
    }

    public static void main(String[] args) throws IOException {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "joining-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        config.put("schema.registry.url", "http://localhost:8081");

        StreamsBuilder builder = new StreamsBuilder();

        Map<String, Object> configMap = propertiesToMap(config);

        SpecificAvroSerde<ApplianceOrder> applianceSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<CombinedOrder> combinedSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<User> userSerde = getSpecificAvroSerde(configMap);

        ValueJoiner<ApplianceOrder, ElectronicOrder, CombinedOrder> orderJoiner =
                (applianceOrder, electronicOrder) -> CombinedOrder.newBuilder()
                        .setApplianceOrderId(applianceOrder.getOrderId())
                        .setApplianceId(applianceOrder.getApplianceId())
                        .setElectronicOrderId(electronicOrder.getOrderId())
                        .setTime(Instant.now().toEpochMilli())
                        .build();

        ValueJoiner<CombinedOrder, User, CombinedOrder> enrichmentJoiner = (combined, user) -> {
            if (user != null) {
                combined.setUserName(user.getName());
            }
            return combined;
        };

        KStream<String, ApplianceOrder> applianceStream = builder.stream("left-topic", Consumed.with(Serdes.String(), applianceSerde))
                .peek((key, value) -> System.out.println("Appliance stream incoming record key " + key + " value " + value));

        KStream<String, ElectronicOrder> electronicStream = builder.stream("right-topic", Consumed.with(Serdes.String(), electronicSerde))
                .peek((key, value) -> System.out.println("Appliance stream incoming record key " + key + " value " + value));

        KTable<String, User> userTable = builder.table("table-topic", Materialized.with(Serdes.String(), userSerde));

        KStream<String, CombinedOrder> combinedStream = applianceStream.join(
                electronicStream,
                orderJoiner,
                JoinWindows.of(Duration.ofMinutes(30)),
                StreamJoined.with(Serdes.String(), applianceSerde, electronicSerde))
                .peek((key, value) -> System.out.println("Stream-Stream Join record key " + key + " value " + value));

        combinedStream.leftJoin(
                userTable,
                enrichmentJoiner,
                Joined.with(Serdes.String(), combinedSerde, userSerde))
                .peek((key, value) -> System.out.println("Stream-Table Join record key " + key + " value " + value))
                .to("join-output", Produced.with(Serdes.String(), combinedSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        TopicLoader.runProducer();
        kafkaStreams.cleanUp();
        kafkaStreams.start();
    }
}