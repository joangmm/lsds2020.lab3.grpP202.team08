package upf.edu;

import upf.edu.storage.DynamoHashTagRepository;
import upf.edu.model.HashTagCount;

import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.*;

public class TwitterHashtagsReader{
    
    public static void main(String[] args) throws InterruptedException, IOException {
        String lang = args[0];
        
        final DynamoHashTagRepository repository = new DynamoHashTagRepository();
        List<HashTagCount> topTen = repository.readTop10(lang);
        
        for(int i=0;i<topTen.size();i++){
            System.out.println(topTen.get(i).toString());
        }
        
        
    }
}
