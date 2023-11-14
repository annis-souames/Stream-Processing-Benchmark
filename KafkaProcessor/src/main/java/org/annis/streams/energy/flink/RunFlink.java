package org.annis.streams.energy.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import java.util.Properties;

public class RunFlink {
    /*
    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up the Kafka consumer properties
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "flink-consumer-group");

        // Create the Kafka consumer source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("orders-a")
                .setGroupId("flink-consumer-group")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        // Create serializer for the sink (producer)
        KafkaRecordSerializationSchema<> serializer =
        // Create the Kafka Sink
        KafkaSink<String> sink = KafkaSink.builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(serializer)
                .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .process(new LowercaseFunction()) // lowercase the input stream
                .sinkTo(sink); // send to the output topic

        // Execute the Flink job
        env.execute("Lowercase Stream");
    }

    *
     */
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set up the Kafka consumer properties
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "flink-consumer-group");

        // Create the Kafka consumer source
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("orders-a", new SimpleStringSchema(), kafkaProps);

        // Add the Kafka consumer source to the environment
        env.addSource(kafkaConsumer)
                .process(new LowercaseFunction()) // lowercase the input stream
                .addSink(new FlinkKafkaProducer<>("orders-output", new SimpleStringSchema(), kafkaProps)); // send to the output topic

        // Execute the Flink job
        env.execute("Lowercase Stream");
    }
    // Define the process function to lowercase the input stream
    public static class LowercaseFunction extends ProcessFunction<String, String> {
        @Override
        public void processElement(String input, Context context, Collector<String> collector) throws Exception {
            collector.collect(input.toLowerCase()); // lowercase the input and emit it to the collector
        }
    }
}