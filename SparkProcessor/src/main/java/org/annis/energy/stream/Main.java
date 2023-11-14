package org.annis.energy.stream;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

import static org.apache.spark.sql.functions.col;

public class Main {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("StructuredStreamingExample")
                .getOrCreate();

        spark.conf().set("spark.sql.streaming.metricsEnabled", "true");

        // define the schema for the input JSON orders
        StructType schema = new StructType()
                .add("eventTime", "long")
                .add("product", "string")
                .add("price", "double");

        // read orders from a Kafka topic
        Dataset<Row> ordersA = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "orders-a")
                .option("startingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING) as value")
                .select(functions.from_json(col("value"), schema).as("data"))
                .select("data.*")
                .withColumn("eventTime", col("eventTime").cast("timestamp"));
                ;
        Dataset<Row> ordersB = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "orders-b")
                .option("startingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING) as value")
                .select(functions.from_json(col("value"), schema).as("data"))
                .select("data.*")
                .withColumn("eventTime", col("eventTime").cast("timestamp"));

        // Merge the two streams
        Dataset<Row> orders = ordersA.union(ordersB);

        // compute the sum per product field in a 3-minute time window
        Dataset<Row> productSum = orders
                .withWatermark("eventTime", "3 minutes")
                .groupBy(
                        functions.window(col("eventTime"), "3 minutes"),
                        col("product"))
                .agg(functions.sum("price").as("sum"));



        // Split the lines into words
        /*Dataset<String> words = df
                .map((MapFunction<String, String>) String::toUpperCase, Encoders.STRING());*/
  /*
        Dataset<Row> transformedDF = df
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .selectExpr("CAST(value AS STRING) AS order_data")
                .selectExpr("UPPER(order_data) AS order_data_uppercase");
*/


        // Write the processed data back to Kafka
        StreamingQuery query = productSum
                .selectExpr("to_json(struct(*)) AS value")
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "orders-output")
                .option("checkpointLocation", "/tmp/kafka-structured-streaming-example")
                .start();

        query.awaitTermination();

    }

}