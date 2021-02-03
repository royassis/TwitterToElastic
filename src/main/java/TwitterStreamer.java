import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStreamer {

    public static void main(String[] args) throws InterruptedException {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("twitter", "api");

        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        String credsFilePath = "C:\\Users\\Roy\\javaProjects\\HosebirdClient\\src\\main\\resources\\creds.json";
        JSONObject credsJson = getJsonFromFile(credsFilePath);

        String apiKey = (String) credsJson.get("apiKey");
        String apiSecretKey = (String) credsJson.get("apiSecretKey");
        String accessToken = (String) credsJson.get("accessToken");
        String accessTokenSecret = (String) credsJson.get("accessTokenSecret");

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(apiKey, apiSecretKey, accessToken, accessTokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        // Attempts to establish a connection.
        hosebirdClient.connect();

        while (!hosebirdClient.isDone()) {
            String msg = msgQueue.take();
            System.out.println(msg);
        }

        hosebirdClient.stop();
    }


    static private final JSONObject getJsonFromFile(String filePath) {
        JSONParser parser = new JSONParser();

        JSONObject jsonObject = null;
        try {
            Object obj = parser.parse(new FileReader(filePath));
            jsonObject = (JSONObject) obj;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return jsonObject;
    }
}
