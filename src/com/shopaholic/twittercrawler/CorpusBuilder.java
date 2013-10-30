package com.shopaholic.twittercrawler;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.nio.charset.Charset;
import java.nio.file.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shopaholic.twitter.utils.TwitterOAuthUtils;

import twitter4j.HashtagEntity;
import twitter4j.Paging;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.URLEntity;
import twitter4j.UserMentionEntity;
import twitter4j.internal.http.HttpResponseCode;

public class CorpusBuilder {
    private static Logger LOG = LoggerFactory.getLogger(CorpusBuilder.class);
    private static final int PAGE_SIZE = 100;
    private static final Date INIT_DATE = getInitDate();
    private static final Date FINAL_DATE = getFinalDate();
    private static final long SLEEP_TIME_MILLIS = 5000;
    private static final int MAX_RETRIES = 5;
    private Twitter twitter = TwitterOAuthUtils.getTwitterInstance();
    private File corpusDir;
    private PrintWriter corpus;
    private List<String> usernames;
    private static int tweetCount = 0;
    private static boolean flag = false;

    public CorpusBuilder(File inputlist, File outputdir) throws IOException {
        usernames = new LinkedList<String>();
        String user;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(inputlist));
            while ((user = reader.readLine()) != null)
                usernames.add(user);
            corpusDir = outputdir;
        } finally {
            if (reader != null)
                reader.close();
        }
        if (LOG.isInfoEnabled())
            LOG.info("Read " + usernames.size() + " users from input file");
    }

    private static Date getInitDate() {
        Calendar c = Calendar.getInstance();
        c.set(2011, 4, 1, 0, 0, 0); // 1 May 2011 00:00:00
        return c.getTime();
    }

    private static Date getFinalDate() {
        Calendar c = Calendar.getInstance();
        c.set(2013, 4, 31, 23, 59, 59); // 31 May 2011 23:59:59
        return c.getTime();
    }

    public void run() throws InterruptedException, IOException {
        int cursor = 0, listSize = usernames.size();
        for (String username : usernames) {
            boolean success = false;
            File newFile = new File(corpusDir, username);
            try {
                cursor++;
                if (LOG.isInfoEnabled())
                    LOG.info("Building corpus of user (" + cursor + "/" + listSize + "): " + username);
                
                corpus = new PrintWriter(new BufferedWriter(new FileWriter(newFile)));
                int currentTrial = 0;
                corpus.println("[");
                while (!success && currentTrial++ < MAX_RETRIES) {
                    try {
                        if (LOG.isInfoEnabled())
                            LOG.info("Try (" + currentTrial + "/" + MAX_RETRIES + "): " + username);
                        
                        int startPage = findStartingPage(username);
                        success = crawl(username, startPage);
                        
                        
                    } catch (TwitterException e) {
                        e.printStackTrace();
                        if (e.isCausedByNetworkIssue())
                            Thread.sleep(SLEEP_TIME_MILLIS);
                        else if (e.exceededRateLimitation()) {
                            int millisToSleep = 1000 * (e.getRetryAfter() + 10); // 10s slack
                            if (LOG.isInfoEnabled())
                                LOG.info("Sleeping for " + millisToSleep / 1000 + " seconds");
                            long before = System.currentTimeMillis();
                            try {
                                Thread.sleep(millisToSleep);
                            } catch (InterruptedException ie) {
                                ie.printStackTrace();
                            }
                            long now = System.currentTimeMillis();
                            if (LOG.isInfoEnabled())
                                LOG.info("Woke up! Slept for " + (now - before) / 1000 + " seconds");
                        } else if (e.resourceNotFound() || e.getStatusCode() == HttpResponseCode.FORBIDDEN
                                || e.getStatusCode() == HttpResponseCode.UNAUTHORIZED)
                            break;
                    }
                }
            } finally {
                if (corpus != null) {
                	corpus.println("]");
                    corpus.close();
                    BufferedReader br = new BufferedReader(new FileReader(newFile));
                    if (new Scanner(newFile).useDelimiter("\\Z").next().length()==3)
                    	newFile.delete();

                    if (LOG.isInfoEnabled())
                        LOG.info("Closing corpus file: " + username);
                }
                if (!success) {
                    if (LOG.isWarnEnabled())
                        LOG.warn("Could not crawl user: " + username);
                } else if (LOG.isInfoEnabled())
                    LOG.info("Crawled corpus of user: " + username);
            }
        }
        if (LOG.isInfoEnabled())
            LOG.info("End!");
    }

    private int findStartingPage(String username) throws TwitterException {
        Status status = null;
        int currentPage = 1, lastPage = 0;
        boolean found = false, close = false;
        if (LOG.isInfoEnabled())
            LOG.info("Doubling Phase: Start!");
        while (!found) {
            Paging paging = new Paging(currentPage * PAGE_SIZE, 1); // get only the first status instead of the full page
            ResponseList<Status> list = twitter.getUserTimeline(username, paging);
            
            	
            if (LOG.isDebugEnabled())
                LOG.debug("Doubling Phase: got " + list.size() + " tweets");
            if (!list.isEmpty())
                status = list.iterator().next();
            found = list.isEmpty() || status.getCreatedAt().before(FINAL_DATE); // OR shortcut prevents NPE
            if (!found) {
                currentPage *= 2;
                if (LOG.isDebugEnabled())
                    LOG.debug("Doubling Phase: doubling, going to page " + currentPage);
            } else {
                if (LOG.isDebugEnabled())
                    LOG.debug("Doubling Phase: found end point! page " + currentPage);
                if (!list.isEmpty())
                    close = status.getCreatedAt().after(INIT_DATE);
                lastPage = currentPage / 2;
            }
        }

        if (LOG.isInfoEnabled())
            LOG.info("Closing Phase: Start!");
        found = currentPage <= 1;
        while (!found) {
            int currentGap = (currentPage - lastPage);
            close |= currentGap <= 1;
            if (LOG.isDebugEnabled())
                LOG.debug("CurrentGap=" + currentGap);
            if (close)
                currentPage--; // linear
            else
                currentPage -= currentGap / 2; // binary search
            if (LOG.isDebugEnabled())
                LOG.debug("Closing Phase: " + (close ? "linearly" : "binarily") + " reducing currentPage=" + currentPage);

            Paging paging = new Paging(currentPage * PAGE_SIZE, 1); // get only the first status instead of the full page
            ResponseList<Status> list = twitter.getUserTimeline(username, paging);
            if (LOG.isDebugEnabled())
                LOG.debug("Closing Phase: got " + list.size() + " tweets");
            if (!list.isEmpty()) {
                status = list.iterator().next();
                found = status.getCreatedAt().after(FINAL_DATE);
                close = status.getCreatedAt().after(INIT_DATE);
            }
            found |= currentPage <= 1;
        }
        if (found)
            if (LOG.isDebugEnabled())
                LOG.debug("Closing Phase: found end point! page " + currentPage);
        return Math.max(1, currentPage);
    }

    private boolean crawl(String username, int currentPage) throws TwitterException, InterruptedException {
        boolean done = false;
        if (LOG.isInfoEnabled())
            LOG.info("Crawling Phase: Start!");
        while (!done) {
            Paging paging = new Paging(currentPage++, PAGE_SIZE);
            ResponseList<Status> list = twitter.getUserTimeline(username, paging);

            if (LOG.isInfoEnabled())
                LOG.info("Crawling Phase: got " + list.size() + " tweets");
            //insert [, ] here.. 
            //corpus.println("[");
            int count = 0;
            for (Status s : list) {
                // System.out.println(formatFull(s));
            	count++;
            	if (count==list.size())
            		corpus.println(formatFull(s, ++tweetCount));
            	else 
            		corpus.println(formatFull(s, ++tweetCount)+",");
                done = s.getCreatedAt().before(INIT_DATE);
            }
            //corpus.println("]");
            done |= list.isEmpty();
            // list.getFeatureSpecificRateLimitStatus();
            Thread.sleep(SLEEP_TIME_MILLIS);
        }
        return true;
    }

    public static String formatFull(Status s, int tweetCount1) {
    	StringBuilder URLs = new StringBuilder();
    	StringBuilder userMentions = new StringBuilder();
    	StringBuilder hashTags = new StringBuilder();
        
        //return s.getId() + sep + s.getUser().getScreenName() + sep + s.getCreatedAt() + sep + s.getText();
    	URLEntity[] urlMention = s.getURLEntities();
        for(URLEntity url : urlMention){
        	  URLs.append(url.getExpandedURL()+",");
        }
        if (URLs.length()!=0)
        	URLs.deleteCharAt(URLs.length()-1);
        
        UserMentionEntity[] userMention = s.getUserMentionEntities();
        for(UserMentionEntity user : userMention){
        	userMentions.append(user.getName()+",");
        }
        if (userMentions.length()!=0)
        	userMentions.deleteCharAt(userMentions.length()-1);

        HashtagEntity[] hashTag = s.getHashtagEntities();
        for(HashtagEntity hashtag : hashTag){
        	hashTags.append(hashtag.getText()+",");
        }
        if (hashTags.length()!=0) {
        	hashTags.deleteCharAt(hashTags.length()-1);
        	System.out.println(hashTags);
        }

         
        StringBuilder statusToCorpus = new StringBuilder();
        
        statusToCorpus.append("{").append("\n").append("id:"+tweetCount1+",").append("\n").append("origin:").append(s.getText()+",").append("\n").append("id:").append(s.getId()+",");
        statusToCorpus.append("\ntime:").append(s.getCreatedAt()+",\n").append("screenname:").append(s.getUser().getScreenName()+",\n");
        statusToCorpus.append("retweetcount:").append(s.getRetweetCount()+",\n").append("favoritecount:").append(s.isFavorited()+",\n");
        statusToCorpus.append("mentionedentities:").append(userMentions.toString()+",\n").append("URL:").append(URLs.toString()+",\n").append("hashtags:");
        statusToCorpus.append(hashTags.toString()+",\n").append("geolocation:"+s.getGeoLocation()).append("\n}");
        String status = statusToCorpus.toString();
        
        return status;
    }

    public static void main(String[] args) throws TwitterException, IOException, InterruptedException {
    	System.out.println("it is in");
        if (args.length < 2) {
            System.err.println("Usage: " + CorpusBuilder.class.getName() + " <users_list_file> <output_directory>");
            System.exit(1);
        }
        new CorpusBuilder(new File(args[0]), new File(args[1])).run();
    }
}
