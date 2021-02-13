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
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class HousebirdClientBuilder {

    static public Client getHousebirdClient(String credsFilePath, List terms, BlockingQueue<String> msgQueue) throws InterruptedException {

        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>(1000);

        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        JSONObject credsJson = JsonUtils.getJsonFromFile(credsFilePath);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(
                (String) credsJson.get("apiKey"),
                (String) credsJson.get("apiSecretKey"),
                (String) credsJson.get("accessToken"),
                (String) credsJson.get("accessTokenSecret")
        );

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        return hosebirdClient;

    }
}
