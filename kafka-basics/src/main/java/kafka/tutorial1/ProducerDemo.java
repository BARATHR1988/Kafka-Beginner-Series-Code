package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        String bootStrapServers = "127.0.0.1:9092";
        //create the producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

       //create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        //Create the Producer Record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", "Hello World!");

        //send data
        kafkaProducer.send(producerRecord);

        //flush data
        kafkaProducer.flush();
        //flush & close the Data
        kafkaProducer.close();
    }
}
