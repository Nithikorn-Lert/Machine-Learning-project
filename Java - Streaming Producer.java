package test.kafka.example1;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        // String bootstrap = "127.0.0.1:9092"

        Properties producerProps = new Properties();

        producerProps.put("bootstrap.servers", "127.0.0.1:9092");

        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(producerProps);



    }
}
