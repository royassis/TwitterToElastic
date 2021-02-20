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


class Foo<Key, Val> implements Predicate<Key, Val>{
    Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());
    Foo(){}
    @Override
    public boolean test(Key k, Val jsonTweet) {
        Integer cutoff = 10000;
        Boolean retval = false;
        Integer followers_count = 0;

        Gson gson =  new Gson();
        JsonObject jsonObject = gson.fromJson((String)jsonTweet, JsonObject.class);
        try {
            followers_count = jsonObject.get("payload").getAsJsonObject()
                    .get("User").getAsJsonObject()
                    .get("FollowersCount").getAsInt();
            logger.info("The followers count is: " + followers_count.toString());
        }catch (NullPointerException e){
            logger.error("No followers_count key in tweet");
        }

        if (followers_count >cutoff) {
            retval = true;
        }

        return retval;
    }
}

public class StreamsFilterTweets {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams-2");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_status_connect");
        KStream<String, String> filteredStream = inputTopic.filter(
                new Foo<String,String>()
        );
        filteredStream.to("important_tweets");


        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
    }
}
