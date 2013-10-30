package com.shopaholic.twitter.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterOAuthUtils {

    public static Twitter getTwitterInstance() {
    	Properties props = new Properties();
    	
    	 try {
			props.load(new FileInputStream("twitteroauth.properties"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	 
        ConfigurationBuilder cb = new ConfigurationBuilder().setDebugEnabled(true)
        .setOAuthConsumerKey(props.getProperty("oauthconsumerkey"))
        .setOAuthConsumerSecret(props.getProperty("oauthconsumersecret"))
        .setOAuthAccessToken(props.getProperty("oauthaccesstoken"))
        .setOAuthAccessTokenSecret(props.getProperty("oauthaccesstokensecret"));
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
        return twitter;
    }
}
