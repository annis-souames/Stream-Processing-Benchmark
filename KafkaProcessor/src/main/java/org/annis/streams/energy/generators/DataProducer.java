package org.annis.streams.energy.generators;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;

public class DataProducer {


    private final Gson gson;
    private Properties props;
    public Producer<String, String> producer;
    public String topicName;

    public DataProducer(String topicName){
        this.setupProps();
        this.producer = new KafkaProducer<String,String>(this.props);
        this.topicName = topicName;
        this.gson = new Gson();
    }

    private void setupProps(){
        this.props = new Properties();
        this.props.put("bootstrap.servers","localhost:9092");
        //Set acknowledgements for producer requests.
        this.props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        this.props.put("retries", 0);
        //Specify buffer size in config
        this.props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        this.props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        this.props.put("buffer.memory", 33554432);
        this.props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        this.props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
    }

    public void send(String product, Map<String, Object> record){
        String recordJson = this.gson.toJson(record);
        this.producer.send(new ProducerRecord<String, String>(this.topicName,
                product, recordJson));
    }
}
