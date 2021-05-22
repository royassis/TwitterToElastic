import com.google.common.collect.Lists;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStreamerProducer {
    static Logger logger = LoggerFactory.getLogger(TwitterStreamerProducer.class.getName());

    public static void main(String[] args) throws InterruptedException {
        new TwitterStreamerProducer().run();
    }

    public TwitterStreamerProducer(){}

    public void run() throws InterruptedException {
        List<String> terms = Lists.newArrayList("twitter", "api");
        String credsFilePath = "C:\\Users\\Roy\\javaProjects\\HosebirdClient\\TwitterProducer\\src\\main\\resources\\creds.json";
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

        Client hosebirdClient = HousebirdClientBuilder.getHousebirdClient(credsFilePath, terms, msgQueue);

        // Attempts to establish a connection.
        hosebirdClient.connect();

        KafkaProducer producer = KafkaProducerBuilder.getProducer();
        String topic = "twitter-streaming-test";

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            new TwitterStreamerProducer().shutdown(producer, hosebirdClient);
            logger.info("Application has exited");
        }));

        while (!hosebirdClient.isDone()) {
            String msg = msgQueue.take();
            logger.info("sending messgae:" + msg + " to kafka topid " + topic);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
            producer.send(record);
        }

        new TwitterStreamerProducer().shutdown(producer, hosebirdClient);
    }

    public void shutdown(KafkaProducer producer, Client hosebirdClient){
        producer.close();
        hosebirdClient.stop();
    }

}
