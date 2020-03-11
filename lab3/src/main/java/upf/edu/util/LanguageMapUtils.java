package upf.edu.util;
import java.util.*;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class LanguageMapUtils {

    public static JavaPairRDD<String, String> buildLanguageMap(JavaRDD<String> lines) {
        String s = "\t";
        
        JavaPairRDD<String, String> languageMap = lines.mapToPair(l -> new Tuple2<>((l.split(s))[1],(l.split(s))[2]));
        
        return languageMap;
        //return null;// IMPLEMENT ME
    }
}
