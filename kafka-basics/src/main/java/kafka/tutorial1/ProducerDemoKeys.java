package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootStrapServers = "127.0.0.1:9092";

        //create the producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

       //create the producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        for(int i=0; i<10; i++) {

            String topic ="my_first_topic";
            String value = "Welcome to Kafka Learning : " + Integer.toString(i);
            String key = "Key Id_" +Integer.toString(i);
            //Create the Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key_" +i);

            //send data
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Return Producer Record. \n +" +
                                "Topics:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "TimeStamp:" +recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing records", e);
                    }
                }
            }).get();  //Synchronous execution, not to use for Production
        }

        //flush data
        kafkaProducer.flush();
        //flush & close the Data
        kafkaProducer.close();
    }
}
