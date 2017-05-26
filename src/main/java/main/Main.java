package main;

import javax.xml.datatype.Duration;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.util.parsing.combinator.testing.Str;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

import org.apache.log4j.lf5.util.StreamUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
public class Main {

	public static void main(String[] args) {

	    // Configuring Twitter credentials
	    String apiKey = "CZ38ANkOJ4PkNLXLuz93AFA8j";
	    String apiSecret = "7J2H8XsTh1djsHRX7cheByOHisaPEbnQT2qgpHd5bCKMunmzkU";
	    String accessToken = "867451954552819712-cnAdj8vesPG3zQKzPtTlvt1ozczcnTz";
	    String accessTokenSecret = "QuRDBWipnvrT23OVpjkV94oU7H4osM3PvokZFC8e5sK9O";
	    
	    System.setProperty("twitter4j.oauth.consumerKey",apiKey);
	    System.setProperty("twitter4j.oauth.consumerSecret",apiSecret);
	    System.setProperty("twitter4j.oauth.accessToken",accessToken);
	    System.setProperty("twitter4j.oauth.accessTokenSecret",accessTokenSecret);

       SparkConf conf = new SparkConf().setAppName("MyTwitterApp");
       conf.setMaster("local[*]");
       JavaSparkContext sc = new JavaSparkContext(conf);
       
//       JavaStreamingContext sc=new JavaStreamingContext(ctx);
       String[] filters={"#Android"};
       TwitterUtils.createStream(sc,twitterAuth,filters).flatMap(s -> Arrays.asList(s.getHashtagEntities())).map(h -> h.getText().toLowerCase()).filter(h -> !h.equals("android")).countByValue().print();
       sc.start();
       sc.awaitTermination();
		
	}
	//https://habrahabr.ru/company/jugru/blog/325070/
	//https://books.google.com.ua/books?id=xRSLDQAAQBAJ&pg=PA181&lpg=PA181&dq=duration+class+spark+%D0%BE%D0%BF%D0%B8%D1%81%D0%B0%D0%BD%D0%B8%D0%B5&source=bl&ots=VUAXxl5ka9&sig=4I1DmdrhCwdW0ba3RxcMpRY7rxQ&hl=ru&sa=X&ved=0ahUKEwjehv-LrYvUAhUFApoKHcqZDhUQ6AEIUTAG#v=onepage&q=duration%20class%20spark%20%D0%BE%D0%BF%D0%B8%D1%81%D0%B0%D0%BD%D0%B8%D0%B5&f=false
}
