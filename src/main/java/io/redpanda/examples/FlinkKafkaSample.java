package io.redpanda.examples;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.mongoflink.config.MongoOptions;
import org.mongoflink.sink.MongoSink;

import java.util.Properties;

/**
 * This is a re-write of the Apache Flink WordCount example using Kafka connectors.
 * Find the original example at 
 * https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/wordcount/WordCount.java
 */
public class FlinkKafkaSample {

	final static String jobTitle = "KafkaSample";
	final static String bootstrapServers =  "my-cluster-kafka-bootstrap:9092";

	public static void main(String[] args) throws Exception {
		// Set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		KafkaSource<String> source = KafkaSource.<String>builder()
				.setBootstrapServers(bootstrapServers)
				.setTopics("my-topic")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		// non-transactional sink with a flush strategy of 1000 documents or 10 seconds
		Properties properties = new Properties();
		properties.setProperty(MongoOptions.SINK_TRANSACTION_ENABLED, "false");
		properties.setProperty(MongoOptions.SINK_FLUSH_ON_CHECKPOINT, "false");
		properties.setProperty(MongoOptions.SINK_FLUSH_SIZE, String.valueOf(1_000L));
		properties.setProperty(MongoOptions.SINK_FLUSH_INTERVAL, String.valueOf(10_000L));
		DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
		// print out values of stream
		text.sinkTo(new MongoSink<>("mongodb://mongodb", "local", "flinkcollection",
				new StringDocumentSerializer(), properties));
		// Execute program
		env.execute(jobTitle);
	}
}