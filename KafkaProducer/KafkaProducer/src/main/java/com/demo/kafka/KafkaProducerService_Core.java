package com.demo.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Properties;

import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


@Path("/logincore")
public class KafkaProducerService_Core {

    @Consumes(MediaType.APPLICATION_JSON)
    @POST
    public String logAccessDetails(@HeaderParam("User-Agent") String userAgent, @HeaderParam("X-Forwarded-For") String ipAddress,
            @HeaderParam("Accept-Language") String language, LoginDetails_Core details) throws URISyntaxException {
        
    	//Creating Properties Object and reading from configurations file.
        Properties configProps = new Properties();
        try(InputStream resourceStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties")) {
            configProps.load(resourceStream);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        // Assign topicName to string variable
        String topicName = configProps.getProperty("kafka_topic_core");

        // create instance for properties to access producer configs
        Properties props = new Properties();
     // Assign kafka broker server details.
        props.put("bootstrap.servers", configProps.getProperty("kafka_server"));
        // Assign acknowledgement as all for all the messages.
        props.put("acks", "all");
        // Assign retries as 0
        props.put("retries", 0);
        // Specify buffer size
        props.put("batch.size", 16384);
        // Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        // The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        // Serializers for pushing data to the kafka topic.
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
        
        //Setting the IP Address , Language and UserAgent recieved from the HTTP Header object.
        details.setIpAddress(ipAddress);
        details.setLanguage(language);
        details.setUserAgent(userAgent);
        
        // Creating Base Object for holding the incoming message.
        ObjectMapper objectMapper = new ObjectMapper();
        // Parsing the incoming message as JSON.
        JsonNode jsonNode = objectMapper.valueToTree(details);

        // Creating a <String,JSON> producer object using the properties set above.
        Producer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(props);
        // Associating the JSON parsed above to the topic for which the message needs to be pushed.
        ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(topicName, jsonNode);
        
        // Pushing message to the topic.
        producer.send(rec);

        // Closing the producer.	
        producer.close();
        
        // Returning Response to the Login API.
        return Response.Status.OK.toString();

    }
}