package org.annis.streams.energy.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class RandomTextProducer {

    public Producer<String, String> producer;
    public String topicName;
    public Properties props;
    public RandomTextProducer(String topicName){
        this.setupProps();
        this.producer = new KafkaProducer<String,String>(this.props);
        this.topicName = topicName;
    }

    public void start(int N){
        for(int i = 0; i < N; i++){
            String message = this.generateText();
            this.producer.send(new ProducerRecord<String, String>(this.topicName,
                    Integer.toString(i), message));
            System.out.println("Message sent successfully : " + message);
        }
        this.producer.close();
    }
    /**
     * Generate a random sentence of 5 to 8 words
     * @return
     */
    private String generateText() {
        // Defining the vocabulary
        String[] firstWord = {"man", "woman","kid"};
        String[] secondWord = {"eat", "throw", "grab", "put", "learn"};
        String[] thirdWord = {"red", "blue", "green", "small","big"};
        String[] fourthWord = {"tomato", "potato", "apple"};

        int rand1 = (int) (Math.random() * firstWord.length);
        int rand2 = (int) (Math.random() * secondWord.length);
        int rand3 = (int) (Math.random() * thirdWord.length);
        int rand4 = (int) (Math.random() * fourthWord.length);

        return new StringBuilder(firstWord[rand1]).append(" ")
                .append(secondWord[rand2]).append(" ")
                .append(thirdWord[rand3]).append(" ")
                .append(fourthWord[rand4]).toString();
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


}
