package upf.edu.storage;

import twitter4j.Status;
import upf.edu.model.HashTagCount;

import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.*;
import java.lang.*; 
import java.io.*; 

import java.util.Map;
import java.util.HashMap;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;

import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;

import com.amazonaws.services.dynamodbv2.model.ReturnValue;

public class DynamoHashTagRepository implements IHashtagRepository, Serializable {
    final static String endpoint = "dynamodb.us-east-1.amazonaws.com";
    final static String region = "us-east-1";
    final static String tableName = "LSDS2020-TwitterHashtags";   

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
    
        // Read Information from Tweet  
        String lang = tweet.getLang();
        Long tweetId = tweet.getId();
        Long count = (long)1;
        
        List<Long> id_list = new ArrayList<Long>();
        List<Long> empty_list = new ArrayList<Long>();
        id_list.add(tweetId);
        List<String> hashtags = new ArrayList<String>();
        String[] text = tweet.getText().split("[ \n]"); 
        List<String> words = Arrays.asList(text);
        if(!words.isEmpty()){
            for(int i=0; i<words.size(); i++){  
                if(words.get(i).length()>1){
                    if(words.get(i).charAt(0) == '#')
                        hashtags.add(words.get(i));
                }
            }
        }        
       
        // Put/Update Item in Table
        if(!hashtags.isEmpty()){
            for(int i=0; i<hashtags.size(); i++){ 
                String hashtag = hashtags.get(i);
                                
                UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("hashtag", hashtag)
                    .withUpdateExpression("set #lan = :lan, #c = :c + if_not_exists(#c,:empty), #id = list_append(if_not_exists(#id,:id_val),:id)")
                    .withNameMap(new NameMap().with("#lan", "Language").with("#c", "Counter").with("#id","List_IDs"))
                    .withValueMap(new ValueMap().withString(":lan",lang).withNumber(":c", count).withNumber(":empty",0).withList(":id",id_list).withList(":id_val", empty_list))
                    .withReturnValues(ReturnValue.UPDATED_NEW);
                               
                 try {
                    System.out.println("Updating the item...");
                    UpdateItemOutcome outcome = dynamoDBTable.updateItem(updateItemSpec);
                    System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

                }
                catch (Exception e) {
                    System.err.println("Unable to update item: " + hashtag);
                    System.err.println(e.getMessage());
                }
                
            }
        }
        
    }   
    


    @Override
    public List<HashTagCount> readTop10(String lang) {
        List<HashTagCount> allTweets = new ArrayList<HashTagCount>();
        
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


        ScanSpec scanSpec = new ScanSpec().withProjectionExpression("#lan, hashtag, #count, List_Ids")
            .withFilterExpression("#lan = :lang").withNameMap(new NameMap().with("#lan", "Language").with("#count", "Counter"))
            .withValueMap(new ValueMap().withString(":lang", lang));

        try {
            ItemCollection<ScanOutcome> items = dynamoDBTable.scan(scanSpec);

            Iterator<Item> iter = items.iterator();
            while (iter.hasNext()) {
                Item item = iter.next();
                HashTagCount htc = new HashTagCount(item.get("hashtag").toString(), item.get("Language").toString(), Long.parseLong(item.get("Counter").toString()));
                allTweets.add(htc);
            }

        }
        catch (Exception e) {
            System.err.println("Unable to scan the table:");
            System.err.println(e.getMessage());
        }

      
      
        Collections.sort(allTweets, new CustomComparator());
        List<HashTagCount> top10 = new ArrayList<HashTagCount>();
        for(int i=0;i<10;i++)
            top10.add(allTweets.get(i));
        
        return (top10);
    }

}


class CustomComparator implements Comparator<HashTagCount>{
    @Override
    public int compare(HashTagCount o1, HashTagCount o2) {
        return (int)(o2.getCount() - o1.getCount());
    }
}