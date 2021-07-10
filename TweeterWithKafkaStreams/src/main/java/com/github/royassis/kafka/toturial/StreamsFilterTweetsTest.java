package com.github.royassis.kafka.toturial;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


class TweetFilter<Key, Val> implements Predicate<Key, Val> {
    Logger logger = LoggerFactory.getLogger(StreamsFilterTweetsTest.class.getName());

    TweetFilter() {
    }

    @Override
    public boolean test(Key k, Val jsonTweet) {
        Integer cutoff = 1000;
        Boolean retval = false;
        Integer followers_count = 0;

        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson((String) jsonTweet, JsonObject.class);
        try {
            followers_count = jsonObject.get("user").getAsJsonObject()
                    .get("followers_count").getAsInt();
            logger.info("The followers count is: " + followers_count.toString());
        } catch (NullPointerException e) {
            logger.error("No followers_count key in tweet");
        }

        if (followers_count > cutoff) {
            retval = true;
        }

        return retval;
    }
}

public class StreamsFilterTweetsTest {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(StreamsFilterTweetsTest.class.getName());

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.68.46:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams-2");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputTopic = streamsBuilder.stream("twitter-streaming-test");
        KStream<String, String> filteredStream = inputTopic.filter(
                new TweetFilter<String, String>()
        );
        filteredStream.to("important_tweets");


        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
    }
}
