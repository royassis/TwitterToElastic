package com.github.royassis.kafka.toturial;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsFilterTweets {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_status_connect");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k,jsonTweet) ->{
                    Integer cutoff = 10000;
                    Boolean retval = false;
                    Integer followers_count = 0;

                    Gson gson =  new Gson();
                    JsonObject jsonObject = gson.fromJson(jsonTweet, JsonObject.class);
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

        );
        filteredStream.to("important_tweets");


        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
    }
}
