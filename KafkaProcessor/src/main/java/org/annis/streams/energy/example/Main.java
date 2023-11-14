package org.annis.streams.energy.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;


public class Main {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "wordcount-demo");
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

        WordCount wc = new WordCount(config);
        RandomTextProducer producer = new RandomTextProducer("wordcount-input");
        wc.run();
        // Start producer andsend 150 messages
        producer.start(50);
    }
}
