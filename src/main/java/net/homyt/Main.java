package net.homyt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class Main {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {

        System.out.println("azmi");

        // Configuration properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "likes-aggregator-app");

        // Set to localhost:9092 since Kafka is running in Docker and exposed to localhost
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());

        // Build the Kafka Streams topology
        StreamsBuilder builder = new StreamsBuilder();

        // Stream from the 'likes' topic
        KStream<String, String> likesStream = builder.stream("mydbserver.public.likes");

        // Print each event (key-value pair)
        likesStream.foreach((key, value) -> {
            try {
                // Parse the value (JSON string) using Jackson
                JsonNode jsonNode = objectMapper.readTree(value);

                // Print 'after' field (assuming it contains the important data)
                JsonNode afterNode = jsonNode.get("after");
                if (afterNode != null) {
                    System.out.println("Parsed Data: " + afterNode.toString());
                } else {
                    System.out.println("No 'after' data for key: " + key);
                }
            } catch (Exception e) {
                System.err.println("Error parsing JSON: " + e.getMessage());
            }
        });

        // Build and start Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Uncaught exception handler to catch any runtime errors
//        streams.setUncaughtExceptionHandler((Throwable e) -> {
//            System.err.println("Stream application encountered an error: " + e.getMessage());
//            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
//        });

        streams.start();
        System.out.println("Kafka Streams application has started.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Kafka Streams...");
            streams.close();
            System.out.println("Kafka Streams shutdown complete.");
        }));
    }
}
