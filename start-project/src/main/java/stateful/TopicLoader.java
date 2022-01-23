package stateful;

import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.StreamUtil;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

public class TopicLoader {
    public static void main(String[] args) throws IOException {
        runProducer();
    }

    static void runProducer() throws IOException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        Callback callback = StreamUtil.callback();

        try(Admin adminClient = Admin.create(properties);
            Producer<String, ElectronicOrder> producer = new KafkaProducer<>(properties)) {

            Instant instant = Instant.now();

            ElectronicOrder electronicOrderOne = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("10261998")
                    .setPrice(2000.00)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(10L);

            ElectronicOrder electronicOrderTwo = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1033737373")
                    .setPrice(1999.23)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(10L);

            ElectronicOrder electronicOrderThree = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1026333")
                    .setPrice(4500.00)
                    .setTime(instant.toEpochMilli()).build();

            instant = instant.plusSeconds(12L);

            ElectronicOrder electronicOrderFour = ElectronicOrder.newBuilder()
                    .setElectronicId("HDTV-2333")
                    .setOrderId("instore-1")
                    .setUserId("1038884844")
                    .setPrice(1333.98)
                    .setTime(instant.toEpochMilli()).build();

            var electronicOrders = List.of(electronicOrderOne, electronicOrderTwo, electronicOrderThree,electronicOrderFour );

            electronicOrders.forEach((electronicOrder -> {
                ProducerRecord<String, ElectronicOrder> producerRecord = new ProducerRecord<>("aggregate-input-topic",
                        0,
                        electronicOrder.getTime(),
                        electronicOrder.getElectronicId(),
                        electronicOrder);
                producer.send(producerRecord, callback);
            }));
        }
    }
}
