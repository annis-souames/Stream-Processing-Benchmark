package org.annis.streams.energy.generators;

import com.github.javafaker.Faker;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DataGenerator {
    private final String name;
    private final int rate;
    private final Faker faker;
    private final String[] products;
    private final String store;
    private final DataProducer producer;

    public DataGenerator(String topicName, int rate ){
       this.name = topicName;
       this.rate = rate;
       this.faker = new Faker();
       this.products = new String[]{"Product A","Product B", "Product C"};
       this.store = topicName;
       this.producer = new DataProducer(this.name);
    }
    public Map<String, Object> generateSingleOrder(){
        Random random = new Random();
        // Select a random product from products list
        String boughtProduct = this.products[random.nextInt(this.products.length)];
        // Create date time
        String formattedDateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        int cid = 1000 + random.nextInt(11);
        double price = 10.00 + (random.nextDouble() * (150.00 - 10.00));
        Map<String,Object> order = new HashMap<>();
        order.put("eventTime",System.currentTimeMillis());
        order.put("customer_id", cid);
        order.put("product", boughtProduct);
        order.put("store", this.store);
        order.put("price", price);
        order.put("timestamp", formattedDateTime);
        return order;
    }

    public void generateOrders() throws InterruptedException {
        long startTime = System.nanoTime();
        //float delay = (float)1000/(float)this.count;
        //long delayMS = (long) delay;
        //int delayNano = (int) ((delay - delayMS) * 1000000);
        for (int i = 0; i < this.rate; i++){
            Map<String, Object> order = this.generateSingleOrder();
            this.producer.send((String) order.get("product"), order);
        }
        // Calculate sleep delay
        long endTime = System.nanoTime(); // Get the end time
        long duration = endTime - startTime; // Calculate the duration in nanoseconds
        if(duration < 1000000000.0){
            long delay = (long)((1000000000.0 - duration)/1e6);
            System.out.println(delay);
            Thread.sleep(delay);
        }
        // Additional code for auditing reasons
        double delta = (double) (System.nanoTime() - startTime) / 1000000000.0;
        double rate = this.rate /delta;
        System.out.println("Generated "+this.rate + " messages in "+delta + " seconds \n with a rate of "+ rate);
    }

}
