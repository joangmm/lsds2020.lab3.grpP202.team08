package upf.edu.storage;

import twitter4j.Status;
import upf.edu.model.HashTagCount;

import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.*;

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

public class DynamoHashTagRepository implements IHashtagRepository, Serializable {
    final static String endpoint = "dynamodb.us-east-1.amazonaws.com";
    final static String region = "us-east-1";
    final static String tableName = "TablaTest";//"LSDS2020-TwitterHashtags";
   

  @Override
  public void write(Status tweet) {
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/home/rjr/.aws/credentials), and is in valid format.",
                    e);
        }
        final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region)   )
                .withCredentials(credentialsProvider)
                .build();
        final DynamoDB dynamoDB = new DynamoDB(client);
        final Table dynamoDBTable = dynamoDB.getTable(tableName);
    

        try {        
            
            String lang = tweet.getLang();
            Long tweetId = tweet.getId();
            Long count = (long)1;
            List<String> hashtags = new ArrayList<String>();
            String[] text = tweet.getText().split("[ \n]"); 
            List<String> words = Arrays.asList(text);
            if(!words.isEmpty()){
                for(int i=0; i<words.size(); i++){                    
                    try{
                        if(words.get(i).charAt(0) == '#')
                            hashtags.add(words.get(i));
                    }catch (Exception e){                     
                    }
                }
            }
            
            
            Item item = new Item().withPrimaryKey("hashtag", hashtag).withKeyComponent("Language", lang).withKeyComponent("Counter", count);
           
            // Try update Item
            try {
                //System.out.println("Incrementing an atomic counter...");
                UpdateItemOutcome outcome = table.updateItem(item);
                //System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
            }
            // If item doesn't exist then create Item
            catch (Exception e) {            
                // Put Item in Table
                if(!hashtags.isEmpty()){
                    for(int i=0; i<hashtags.size(); i++){
                        String hashtag = hashtags.get(i);
                        System.out.println("Adding a new items...");
                        PutItemOutcome outcome = dynamoDBTable
                            .putItem(item);
                        System.out.println("PutItem succeeded:\n" + hashtag + outcome.getPutItemResult());
                    }
                }
            }
           
        }
        catch (Exception e) {
            System.err.println("Unable to add item: ");
            System.err.println(e.getMessage());
        }
  }
  
  

  @Override
  public List<HashTagCount> readTop10(String lang) {
    // IMPLEMENT ME
    return Collections.emptyList();
  }

}
