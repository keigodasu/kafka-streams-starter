package stateful;

import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;
import utils.StreamUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class StreamsAggregateTest {
    @Test
    public void shouldAggregateRecords() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-test");
        config.put("schema.registry.url", "mock://aggregation-test");

        final String inputTopicName = "input";
        final String outputTopicName = "output";
        final Map<String, Object> configMap = StreamUtil.propertiesToMap(config);

        final SpecificAvroSerde<ElectronicOrder> electronicSerde = StreamUtil.getSpecificAvroSerde(configMap);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, ElectronicOrder> electronicStream = builder.stream(inputTopicName, Consumed.with(Serdes.String(), electronicSerde));

        electronicStream.groupByKey().aggregate(() -> 0.0,
                (key, order, total) -> total + order.getPrice(),
                Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .to(outputTopicName, Produced.with(Serdes.String(), Serdes.Double()));

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), config)) {
            final TestInputTopic<String, ElectronicOrder> inputTopic =
                    testDriver.createInputTopic(inputTopicName, Serdes.String().serializer(), electronicSerde.serializer());
            final TestOutputTopic<String, Double> outputTopic =
                    testDriver.createOutputTopic(outputTopicName, Serdes.String().deserializer(), Serdes.Double().deserializer());

            final List<ElectronicOrder> orders = new ArrayList<>();
            orders.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("1").setUserId("vandeley").setTime(5L).setPrice(5.0).build());
            orders.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("2").setUserId("penny-packer").setTime(5L).setPrice(15.0).build());
            orders.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("3").setUserId("romanov").setTime(5L).setPrice(25.0).build());

            List<Double> expectedValues = List.of(5.0, 20.0, 45.0);
            //pipeInput sends a record with given key and value
            orders.forEach(order -> inputTopic.pipeInput(order.getElectronicId(), order));
            List<Double> actualValues = outputTopic.readValuesToList();
            assertEquals(expectedValues, actualValues);
        }
    }
}