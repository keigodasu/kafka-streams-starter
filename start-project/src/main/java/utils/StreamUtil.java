package utils;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Callback;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamUtil {
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Map<String, Object> serdeConfig) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(serdeConfig, false);
        return specificAvroSerde;
    }

    public static Map<String, Object> propertiesToMap(final Properties properties) {
        final Map<String, Object> configs = new HashMap<>();
        properties.forEach((key, value) -> configs.put((String) key, value));
        return configs;
    }

    public static Callback callback() {
        return ((metadata, exception) -> {
            if (exception != null) {
                System.out.printf("Producing records encountered error %s %n", exception);
            } else {
                System.out.printf("Record produced - offset - %d timestamp - %d %n", metadata.offset(), metadata.timestamp());
            }
        });
    }
}
