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

public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads(){

    }

    private void run(){

        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-applications";
        String topic = "first-topic";
        CountDownLatch latch = new CountDownLatch(1);

        //Create a Runnable Consumer
        Runnable myConsumerRunnable = new ConsumerRunnable(bootStrapServers, groupId, topic, latch);

       //Start a Thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //add a Shut Down Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ((ConsumerRunnable) myConsumerRunnable).shutDown();
            try{
                latch.await();
            }catch (InterruptedException e){
                e.printStackTrace();
            }finally {
                logger.info("Application has existed");
            }
        }));

        try{
        latch.await();}
        catch (InterruptedException e){
            logger.error("Application got Interrupted", e);
        }finally {
            logger.info("Application has exited!");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private KafkaConsumer<String, String> consumer;
        private CountDownLatch latch;
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootStrapServers, String groupId, String topic, CountDownLatch latch){
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<String, String>(properties);

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
                logger.error("Received Shut-Down Signal!", e);
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
