package upf.edu;

import upf.edu.storage.DynamoHashTagRepository;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.api.java.JavaRDD;

import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;

public class TwitterHashtags{
    
    public static void main(String[] args) throws InterruptedException, IOException {
        String propertiesFile = args[0];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

                
        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Example");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // This is needed by spark to write down temporary data
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // <IMPLEMENT ME>        
        
        final DynamoHashTagRepository repository = new DynamoHashTagRepository();
        stream.foreachRDD(rdd -> rdd.foreach(t->repository.write(t)));
        
        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
