package com.upgrad.kafkaspark;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.spark.streaming.Durations;

/**
 * Consumes messages from one or more topics in Kafka and perform insert into the database. Usage: KafkaSparkDemo <brokers> <groupId> <topics> <brokers>
 * is a list of one or more Kafka brokers <groupId> is a consumer group name to consume from topics <topics> is a list of one or more kafka
 * topics to consume from
 *
 * Example: $ spark-submit  --class com.upgrad.kafkaspark.KafkaSparkDemo /home/ec2-user/KafkaSparkDemo-0.0.3-SNAPSHOT-jar-with-dependencies.jar localhost:9092 1112 kafka_core
 */

public final class KafkaSparkDemo {

    public static void main(String[] args) throws Exception {
    	// Basic check for 3 input arguments
        if (args.length < 3) {
            System.err.println("Usage: KafkaSparkDemo <brokers> <groupId> <topics>\n"
                    + "  <brokers> is a list of one or more Kafka brokers\n" + "  <groupId> is a consumer group name to consume from topics\n"
                    + "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }
        
        //Creating Properties Object and reading from configurations file.
        //Properties configProps = new Properties();
        //try (InputStream resourceStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties")) {
        //    configProps.load(resourceStream);
        //} catch (IOException e) {
        //    e.printStackTrace();
        //}        
        
        // Setting up Database configuration parameters.
        //final String USER = configProps.getProperty("mysql_user");
        //final String PASS = configProps.getProperty("mysql_password");
        //final String DB_URL = "jdbc:mysql://" + configProps.getProperty("mysql_server") + "/" + configProps.getProperty("mysql_database");
        final String USER = "root";
        final String PASS = "Upgrad2018007#";
        final String DB_URL = "jdbc:mysql://34.199.12.254:3306/kafka_demo";

        
        // Persisting the input arguments into the variables.
        String brokers = args[0];
        String groupId = args[1];
        String topics = args[2];

        // Set a Unique name for the application.	
        // Create context with a 5 seconds batch interval. This is the crux of Spark Streaming i.e. defining a micro batch.
        SparkConf sparkConf = new SparkConf().setAppName("KafkaSparkDemo");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // Split the topics if multiple values are passed.
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        
        // Define a new HashMap for holding the kafka information. 
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        // Create direct kafka stream with brokers and topics/
        // LocationStrategy with prefer consistent allows partitions to be distributed consistently to the spark executors.
        // CosumerStrategy allows to subscribe to the kafka topics.
        // JavaInputDStream is a continuous input stream associated to the source.
        JavaInputDStream<ConsumerRecord<String, JsonNode>> messages = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        // JavaDStream is an internal stream object for processed data.
        // Mapping the incoming JSON from kafka stream to the object.
        JavaDStream<LoginDetails> lines = messages.map(new Function<ConsumerRecord<String, JsonNode>, LoginDetails>() {
            /**
             * 
             */
            private static final long serialVersionUID = 1L;
            // overriding the default call method
            @Override
            public LoginDetails call(ConsumerRecord<String, JsonNode> record) throws JsonProcessingException {
                ObjectMapper objectMapper = new ObjectMapper();

                return objectMapper.treeToValue(record.value(), LoginDetails.class);
            }
        });
        
        // Setting up the query to be executed against the database.
        String query = " insert into login_details (user_name, user_agent, country, os_name, os_version,browser_name,browser_version,device_type,ipaddress,time_zone,product,login_time)"
                + " values (?, ?, ?, ?, ?,?, ?, ?, ?,?, ?, ?)";

        // Iterating over the processed object and pushing it into the database.
        // RDD is created to store the records
        lines.foreachRDD(new VoidFunction<JavaRDD<LoginDetails>>() {
        	
        	// overriding the default call method.
            @Override
            public void call(JavaRDD<LoginDetails> logs) throws Exception {
                List<LoginDetails> logList = logs.collect();
                Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
                PreparedStatement preparedStmt = conn.prepareStatement(query);
                for (LoginDetails loginPojo : logList) {
                    preparedStmt.setString(1, loginPojo.getUsername());
                    preparedStmt.setString(2, loginPojo.getUserAgent());
                    preparedStmt.setString(3, loginPojo.getCountry());
                    preparedStmt.setString(4, loginPojo.getOs());
                    preparedStmt.setString(5, loginPojo.getOsVersion());
                    preparedStmt.setString(6, loginPojo.getBrowser());
                    preparedStmt.setString(7, loginPojo.getBrowserVersion());
                    preparedStmt.setString(8, loginPojo.getDeviceType());
                    preparedStmt.setString(9, loginPojo.getIpAddress());
                    preparedStmt.setString(10, loginPojo.getTz());
                    preparedStmt.setString(11, loginPojo.getProduct());
                    preparedStmt.setString(12, loginPojo.getLoginTime());

                    // execute the preparedstatement
                    preparedStmt.execute();
                }
                conn.close();
            }
        });

        // Start the streaming computation
        jssc.start();
        // / Add Await Termination to respond to Ctrl+C and gracefully close Spark Streams
        jssc.awaitTermination();
    }
}