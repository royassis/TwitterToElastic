import com.google.common.collect.Lists;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterKafkaProducerTest {
    static Logger logger = LoggerFactory.getLogger(TwitterKafkaProducerTest.class.getName());

    //TODO: change these when running on another host
    public static String kafkaHost = "172.31.5.128:9092";
    public static String credsFilePath = "C:\\Users\\Roy\\javaProjects\\HosebirdClient\\creds.json";
    public static String topic = "twitter-streaming-test";

    public static void main(String[] args) throws InterruptedException {
        new TwitterKafkaProducerTest().run();
    }

    public void run() throws InterruptedException {
        List<String> terms = Lists.newArrayList("twitter", "api");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        Client hosebirdClient = HousebirdClientBuilder.getHousebirdClient(credsFilePath, terms, msgQueue);

        // Attempts to establish a connection.
        hosebirdClient.connect();

        KafkaProducer producer = KafkaProducerBuilder.getProducer(kafkaHost);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            new TwitterKafkaProducerTest().shutdown(producer, hosebirdClient);
            logger.info("Application has exited");
        }));

        while (!hosebirdClient.isDone()) {
            String msg = msgQueue.take();
            logger.info("sending messgae:" + msg + " to kafka topid " + topic);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
            producer.send(record);
        }

        new TwitterKafkaProducerTest().shutdown(producer, hosebirdClient);
    }

    public void shutdown(KafkaProducer producer, Client hosebirdClient){
        producer.close();
        hosebirdClient.stop();
    }

}
