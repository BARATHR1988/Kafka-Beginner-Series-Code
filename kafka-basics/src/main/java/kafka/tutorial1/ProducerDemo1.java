package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo1 {

    public static void main(String[] args) {
        String bootStrapServers = "127.0.0.1:9092";
        //create a producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create a producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        //create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("second_topic","Hello Barath");

        //send data
        kafkaProducer.send(producerRecord);

        //flush the data
        kafkaProducer.flush();

        //flush and close the data
        kafkaProducer.close();
    }
}
