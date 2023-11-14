/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;
import org.example.utils.DataHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {
	private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "demo-flink");

		FlinkKafkaConsumer<String> consumerA = new FlinkKafkaConsumer<>("orders-a", new SimpleStringSchema(), props);
		FlinkKafkaConsumer<String> consumerB = new FlinkKafkaConsumer<>("orders-b", new SimpleStringSchema(), props);

		DataStream<String> streamA = env.addSource(consumerA);
		DataStream<String> streamB = env.addSource(consumerB);

		DataStream<Tuple2<String,Double>> mergedOrders = streamA
				.union(streamB)
				.map(new MapFunction<String, Tuple2<String, Double>>() {
					@Override
					public Tuple2<String, Double> map(String s) throws Exception {
						return DataHelper.getTuple(s);
					}
				});

		DataStream<Tuple2<String, Double>> totals = mergedOrders
				.keyBy(value -> value.f0)
				.window(TumblingEventTimeWindows.of(Time.seconds(5)))
				.reduce(new ReduceFunction<Tuple2<String, Double>>() {
					public Tuple2<String, Double> reduce(Tuple2<String, Double> v1, Tuple2<String, Double> v2) {
						return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
					}
				});
		DataStream<String> result = totals.map(new MapFunction<Tuple2<String, Double>, String>() {
			@Override
			public String map(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
				String convertedResult = stringDoubleTuple2.toString();
				LOG.info(convertedResult);
				System.out.println(convertedResult);
				return convertedResult;
			}
		});

		result.print();
		result.addSink(new FlinkKafkaProducer<>("orders-output", new SimpleStringSchema(), props));

		env.execute("Product revenues per 3m");

	}
	private static class OrderFlatMapFunction implements FlatMapFunction<String, Order> {

		private final Gson gson = new Gson();

		@Override
		public void flatMap(String value, Collector<Order> out) {
			Order order = gson.fromJson(value, Order.class);
			out.collect(order);
		}
	}



	public static void tmpmain(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// Set up the Kafka consumer properties
		// Set up the Kafka consumer properties
		Properties consumerProps = new Properties();
		consumerProps.setProperty("bootstrap.servers", "localhost:9092");
		consumerProps.setProperty("group.id", "my-group");
		consumerProps.setProperty("auto.offset.reset", "latest");

		// Set up the Kafka producer properties
		Properties producerProps = new Properties();
		producerProps.setProperty("bootstrap.servers", "localhost:9092");

		// Create the Kafka consumer and add it as a source to the execution environment
		FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("orders-a", new SimpleStringSchema(), consumerProps);
		env.addSource(kafkaConsumer)

				// Transform the input by lowercasing it
				.map(String::toUpperCase)

				// Create the Kafka producer and add it as a sink to the execution environment
				.addSink(new FlinkKafkaProducer<>("orders-output", new SimpleStringSchema(), producerProps));

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.window()
		 * 	.process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}
}
