package join;

import io.confluent.developer.avro.ApplianceOrder;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.developer.avro.User;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

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

        Callback callback = (metadata, exception) -> {
            if(exception != null) {
                System.out.printf("Producing records encountered error %s %n", exception);
            } else {
                System.out.printf("Record produced - offset - %d timestamp - %d %n", metadata.offset(), metadata.timestamp());
            }
        };

        try(Admin adminClient = Admin.create(properties);
            Producer<String, SpecificRecord> producer = new KafkaProducer<>(properties)) {

            final String leftSideTopic = "left-topic";
            final String rightSideTopic = "right-topic";
            final String tableTopic = "table-topic";
            final String outputTopic = "output-topic";

            ApplianceOrder applianceOrderOne = ApplianceOrder.newBuilder()
                    .setApplianceId("dishwasher-1333")
                    .setOrderId("remodel-1")
                    .setUserId("10261998")
                    .setTime(Instant.now().toEpochMilli()).build();

            ApplianceOrder applianceOrderTwo = ApplianceOrder.newBuilder()
                    .setApplianceId("stove-2333")
                    .setOrderId("remodel-2")
                    .setUserId("10261999")
                    .setTime(Instant.now().toEpochMilli()).build();
            var applianceOrders = List.of(applianceOrderOne, applianceOrderTwo);

            ElectronicOrder electronicOrderOne = ElectronicOrder.newBuilder()
                    .setElectronicId("television-2333")
                    .setOrderId("remodel-1")
                    .setUserId("10261998")
                    .setTime(Instant.now().toEpochMilli()).build();

            ElectronicOrder electronicOrderTwo = ElectronicOrder.newBuilder()
                    .setElectronicId("laptop-5333")
                    .setOrderId("remodel-2")
                    .setUserId("10261999")
                    .setTime(Instant.now().toEpochMilli()).build();

            var electronicOrders = List.of(electronicOrderOne, electronicOrderTwo);


            User userOne = User.newBuilder().setUserId("10261998").setAddress("5405 6th Avenue").setName("Elizabeth Jones").build();
            User userTwo = User.newBuilder().setUserId("10261999").setAddress("407 64th Street").setName("Art Vandelay").build();

            var users = List.of(userOne, userTwo);

            applianceOrders.forEach((ao -> {
                ProducerRecord<String, SpecificRecord> producerRecord = new ProducerRecord<>(leftSideTopic, ao.getUserId(), ao);
                producer.send(producerRecord, callback);
            }));

            electronicOrders.forEach((eo -> {
                ProducerRecord<String, SpecificRecord> producerRecord = new ProducerRecord<>(rightSideTopic, eo.getUserId(), eo);
                producer.send(producerRecord, callback);
            }));

            users.forEach(user -> {
                ProducerRecord<String, SpecificRecord> producerRecord = new ProducerRecord<>(tableTopic, user.getUserId(), user);
                producer.send(producerRecord, callback);
            });

        }
    }
}
