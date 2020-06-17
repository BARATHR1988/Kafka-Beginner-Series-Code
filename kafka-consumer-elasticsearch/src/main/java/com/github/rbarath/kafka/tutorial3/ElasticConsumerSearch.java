package com.github.rbarath.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticConsumerSearch {

    public static RestHighLevelClient createClient(){
        //https://uu1unsxb7r:1ulefpcbva@kafka-course-4608698139.ap-southeast-2.bonsaisearch.net:443
       //replace with our original credentials
        String userName = "uu1unsxb7r";
        String password = "1ulefpcbva";
        String hostName = "kafka-course-4608698139.ap-southeast-2.bonsaisearch.net";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                         return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return  consumer;
    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromFeed(String tweetJson){
        //gson Library
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();

    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticConsumerSearch.class.getName());
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter-tweets");

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            Integer recordCount = records.count();
            logger.info("Received Records are :" + recordCount + " Records");
            BulkRequest request = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {
                //Can generate id in 2 ways == Idempotance
                //1) Kafka generic
                //String id = record.topic() + "_" + record.partitions() + "+" + record.offset();
                //2) using Feed Specific Id
                try {

                    String id = extractIdFromFeed(record.value());
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id /*to make use of the idempotence*/).source(record.value(), XContentType.JSON);
                    request.add(indexRequest);  //We add our bulk request[there is  no processing time]

                /*IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);  // Can be used when low records for processing.
                logger.info("Index Response Id:" +indexResponse.getId());
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/
                }catch (NullPointerException e){
                    logger.warn("Skipping Bad Data " +record.value());
                }
            }
            if (recordCount > 0) {
                BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
                logger.info("Committing Offsets");
                consumer.commitSync();
                logger.info("Offsets are committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //client.close();
    }
}
