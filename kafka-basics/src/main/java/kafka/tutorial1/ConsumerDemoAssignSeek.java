package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        String bootStrapServers = "127.0.0.1:9092";
        String topic = "first_topic";
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        //Create a consumer Configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create a Kafka Consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Assign and Seek is used to fetch the data and read the specific messages
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offset = 15l;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //Seek
        consumer.seek(partitionToReadFrom, offset);

        int numberOfMessagesToRead = 5;
        int numberOfMessagesReadSoFar =0;
        boolean keepOnReading = true;

        //poll for new data
        while(keepOnReading){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){
                numberOfMessagesReadSoFar += 1;
                logger.info("Key :" +record.key() + ", Value :" +record.value());
                logger.info("Topic :" +record.topic() +  ", Partition :" +record.partition() + ", Offset :" +record.offset());
                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Exiting the Application!");
    }

}
