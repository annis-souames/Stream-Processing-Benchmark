package org.annis.streams.energy.kstream;

import com.google.gson.Gson;
import org.annis.streams.energy.utils.DataHelper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class RunKStream {
    public static void main(String[] args) {
        Properties props = setupConfig();


        StreamsBuilder builder = new StreamsBuilder();
        Gson json = new Gson();
        KStream<String, String> streamA = builder.stream("orders-a");
        KStream<String, String> streamB = builder.stream("orders-b");
        KStream<String, String> mergedOrders = streamA.merge(streamB);
        KTable<Windowed<String>, Double> productTotals = mergedOrders
                .map((key, value) -> KeyValue.pair(key, DataHelper.getPriceOfOrder(value)))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(3)))
                .aggregate(
                        () -> 0.0,
                        (key, value, aggregate) -> aggregate + value,
                        Materialized.with(Serdes.String(), Serdes.Double())
                );


        productTotals.toStream().to("orders-output");
        // Print the result to the console
        //productTotals.toStream().foreach((key, value) -> System.out.println(key + ": " + value));
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    public static Properties setupConfig(){
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "kstream-demo");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");
        config.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());
        config.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
        return config;
    }


}
