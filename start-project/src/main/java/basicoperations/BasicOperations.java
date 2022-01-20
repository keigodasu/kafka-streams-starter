package basicoperations;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class BasicOperations {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "basics-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()));

        stream
                .peek(((key, value) -> System.out.println("Incoming record - key " + key + " value " + value)))
                .filter((key, value) -> value.contains("order-"))
                .mapValues(value -> value.substring(value.indexOf("-") + 1))
                .filter((key, value) -> Long.parseLong(value) > 1000)
                .peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value))
                .to("output-topic", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
