import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;


public class ElasticSearchConsumerWithThreadTest {
    public static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerWithThreadTest.class.getName());

    public static String bootstrapServers = "172.31.5.128:9092";
    public static String groupId = "my-new-application";
    public static String topic = "twitter-streaming-test";

    public static void main(String[] args) {
        new ElasticSearchConsumerWithThreadTest().run();
    }

    private void run() {

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        // create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(
                bootstrapServers,
                groupId,
                topic,
                latch
        );

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private final CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;
            this.consumer = KafkaConsumerBuilder.getKafkaConsumer(Arrays.asList(topic), groupId, bootstrapServers);
        }

        @Override
        public void run() {
            RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost("localhost", 9200, "http"),
                            new HttpHost("localhost", 9201, "http")));

            // poll for new data
            try {
                Integer requestId = 1;
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("requestId: " + requestId.toString() + " Key: " + record.key() + ", Value: " + record.value());
//                        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                        Thread.sleep(1);

                        ObjectMapper mapper = new ObjectMapper();
                        Map<String, Object> map = mapper.readValue(record.value(), Map.class);

                        String tweetText = (String) map.get("text");
                        String created_at = (String) map.get("created_at");
                        String id_str = (String) map.get("id_str");

                        IndexRequest indexRequest = new IndexRequest("tweets")
                                .id(requestId.toString())
                                .source("tweetText", tweetText,
                                        "created_at", created_at,
                                        "id_str", id_str);
//
                        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                        requestId++;
                    }
                }
            } catch (WakeupException | InterruptedException | IOException | NullPointerException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();

                // tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
