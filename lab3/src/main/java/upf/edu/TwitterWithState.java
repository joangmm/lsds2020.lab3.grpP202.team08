package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.apache.spark.api.java.Optional;
import java.util.List;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;
import scala.Tuple2;
import org.apache.spark.api.java.function.*;
import java.io.IOException;

public class TwitterWithState {
    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String language = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter With State");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // create a simpler stream of <user, count> for the given language
        final JavaPairDStream<String, Integer> tweetPerUser = stream                
                .filter(s -> s.getLang().equals(language))
                .mapToPair(s -> new Tuple2<>(s.getUser().getScreenName(), 1))
                .reduceByKey((a, b) -> a + b);
                
        
        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
        (values, state) -> {
            Integer newSum = 0;
            if (state.isPresent())  newSum = state.get();            
            for(int i=0; i< values.size(); i++)     newSum += values.get(i);
            return Optional.of(newSum);
        };
        
        final JavaPairDStream<String, Integer> tweetPerUser_updated = tweetPerUser       
                .updateStateByKey(updateFunction);
                
        // transform to a stream of <userTotal, userName> and get the first 20
        final JavaPairDStream<Integer, String> tweetsCountPerUser = tweetPerUser_updated
                .transformToPair(s -> s.sortByKey(false))
                .mapToPair(s -> s.swap());

        tweetsCountPerUser.print(30);

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }    

}
