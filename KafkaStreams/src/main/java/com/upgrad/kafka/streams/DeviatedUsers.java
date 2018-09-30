package com.upgrad.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class DeviatedUsers {

	  public static void main(final String[] args) throws Exception {
		// Defining the Kafka Broker
	    final String bootstrapServers = args.length > 0 ? args[0] : "34.199.12.254:9092";
	    
	    // Defining the Stream Configuration
	    final Properties streamsConfiguration = new Properties();
	    
	    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
	    // against which the application is run.
	    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "upgrad-demo-anomaly-detection");
	    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "upfrad-demo-anomaly-detection");
	    
	    // Associating the Kafka Brokers.
	    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	    
	    // Specify default (de)serializers for record keys and for record values.
	    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
	    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
	    
	    // Set the commit interval to 500ms so that any changes are flushed frequently. The low latency
	    // would be important for anomaly detection.
	    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
	    
	    //  RocksDB (default) flushes the state store contents to the file system
	    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\temp");

	    // Define the Serde for String and Value 
	    final Serde<String> stringSerde = Serdes.String();
	    final Serde<Long> longSerde = Serdes.Long();
	    
	    // Creating a Stream Builder to be associated with the kafka topic 
	    final StreamsBuilder websiteClicksBuilder = new StreamsBuilder();

	    // Read the source kafka stream of a particular topic.
	    final KStream<String, String> views = websiteClicksBuilder.stream("WebSiteClicks");

	    // Read the source stream.  In this example, we ignore whatever is stored in the record key and
	    // assume the record value contains the username (and each record would represent a single
	    // click by the corresponding user).
	    final KTable<Windowed<String>, Long> deviatedUsers = views
	      // we ignore the key here and treat the incoming username as the key for further processing.
	      .map((ignoredKey, username) -> new KeyValue<>(username, username))
	      // count users, using one-minute tumbling windows;
	      // no need to specify explicit serdes because the resulting key and value types match our default serde settings
	      .groupByKey()
	      //setting a tumbling window of 2 minute 
	      .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(2)))
	      //counting the number of usernames
	      .count()
	      // get users whose one-minute count is >= 5
	      .filter((windowedUserId, count) -> count >= 5);

	    // Note: The below step will output the deviation detected above into a separate topic.
	    final KStream<String, Long> deviatedUsersForKafkaTopic = deviatedUsers
	      // Converting the KTable to KStream as it need to be pumped into a kafka topic
	      .toStream()
	      // Ignore records with user count as null
	      .filter((windowedUserId, count) -> count != null)
	      //Map the output to <String,Value>
	      .map((windowedUserId, count) -> new KeyValue<>(windowedUserId.toString(), count));

	    // write to the result topic
	    deviatedUsersForKafkaTopic.to("DeviatedUsers", Produced.with(stringSerde, longSerde));
	    
	    //Defining the topology
	    final KafkaStreams streams = new KafkaStreams(websiteClicksBuilder.build(), streamsConfiguration);
	    
	    // Cleaning Local State Data
	    streams.cleanUp();
	    
	    // Startign the stream.
	    streams.start();

	    // Add shutdown hook to respond to Ctrl+C and gracefully close Kafka Streams
	    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	  }

	}