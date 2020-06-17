package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread {


    public static void main(String[] args) {
        new ConsumerThread().run();
    }

    private ConsumerThread(){

    }

    private void run(){
        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "my-eighth-application";
        String topic = "my-eighth-topic";
        Logger logger = LoggerFactory.getLogger(ConsumerThread.class);
        CountDownLatch latch = new CountDownLatch(1);

        //Create a Runnable Consumer
        Runnable myConsumerRunnable = new ConsumerRunnable(bootStrapServer, groupId, topic, latch);

        //Create a Thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add a Shut Down Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ((ConsumerRunnable) myConsumerRunnable).shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }finally {
                logger.info("Application has Exited!");
            }
        }));

    }



    public class ConsumerRunnable implements Runnable {
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        private KafkaConsumer<String, String> consumer;
        private CountDownLatch latch;

        public ConsumerRunnable(String bootStrapServer, String groupId, String topic, CountDownLatch latch){
            //Creating Properties and Configuration
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //Create a Consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

            //Consumer only read the data, using subscribe
            consumer.subscribe(Arrays.asList(topic));
        }


        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                    }
                }
            }catch (WakeupException e){
                logger.error("Application has Interrupted!", e);
            }finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutDown(){
            consumer.wakeup();
        }
    }

}
