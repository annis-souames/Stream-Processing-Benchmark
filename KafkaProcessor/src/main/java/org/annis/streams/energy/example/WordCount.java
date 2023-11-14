package org.annis.streams.energy.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;

public class WordCount 
{
    private final Properties config;

    public WordCount(Properties config){
        this.config = config;
    }
    public void run()
    {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> wordCount = builder.stream("wordcount-input", Consumed.with(Serdes.String(), Serdes.String()));

        wordCount.flatMapValues(value-> Arrays.asList(value.toLowerCase().split(" ")))
                .selectKey((key, value) -> value) // Makes a new key = value
                .groupByKey()
                .count(Named.as("Counts")).toStream().to("wordcount-output");

        KafkaStreams streams = new KafkaStreams(builder.build(), this.config);
        streams.start();
    }

}
