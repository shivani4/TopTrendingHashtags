
package shivani.org;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;


public class TwitterSpout extends BaseRichSpout {

    public static final String MESSAGE = "message";
    private final String _accessTokenSecret;
    private final String _accessToken;
    private final String _consumerSecret;
    private final String _consumerKey;
    private SpoutOutputCollector _collector;
    private TwitterStream _twitterStream;
    private LinkedBlockingQueue<Status> _msgs;
    private FilterQuery _tweetFilterQuery;
    private PrintWriter p ;

   
    public TwitterSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
        if (consumerKey == null ||
                consumerSecret == null ||
                accessToken == null ||
                accessTokenSecret == null) {
            throw new RuntimeException("Twitter4j OAuth field cannot be null");
        }
        _consumerKey = consumerKey;
        _consumerSecret = consumerSecret;
        _accessToken = accessToken;
        _accessTokenSecret = accessTokenSecret;

    }

    
    public TwitterSpout(String arg, String arg1, String arg2, String arg3, FilterQuery filterQuery) {
        this(arg,arg1,arg2,arg3);
        _tweetFilterQuery = filterQuery;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(MESSAGE));
    }

    /**
     * Creates a twitter stream listener which adds messages to a LinkedBlockingQueue. Starts to listen to streams
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    	try {
			p = new PrintWriter("/s/chopin/l/grad/shivani4/STORM_OUT/op.txt");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        _msgs = new LinkedBlockingQueue();
        _collector = spoutOutputCollector;
        ConfigurationBuilder _configurationBuilder = new ConfigurationBuilder();
        _configurationBuilder.setOAuthConsumerKey(_consumerKey)
                .setOAuthConsumerSecret(_consumerSecret)
                .setOAuthAccessToken(_accessToken)
                .setOAuthAccessTokenSecret(_accessTokenSecret);
        _twitterStream = new TwitterStreamFactory(_configurationBuilder.build()).getInstance();
        _twitterStream.addListener(new StatusListener() {
            @Override
            
            
            public void onStatus(Status status) {
                if (meetsConditions(status))
                    _msgs.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void onException(Exception ex) {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        });
        if (_tweetFilterQuery == null) {
            _twitterStream.sample();
        }
        else {
            _twitterStream.filter(_tweetFilterQuery);
        }


    }

    private boolean meetsConditions(Status status) {
        return true;
    }

    /**
     * When requested for next tuple, reads message from queue and emits the message.
     */
    @Override
    public void nextTuple() {
        // emit tweets
    	
        Status s = _msgs.poll();
        
        if (s == null) {
            Utils.sleep(1000);
        } else {
        	if(s.getHashtagEntities() != null && s.getHashtagEntities().length >0){
        		String log = "Time: "+String.valueOf(s.getCreatedAt().getTime()/1000)+"  HashTags: ";
        		for(HashtagEntity hash1:s.getHashtagEntities() ){
        			_collector.emit(new Values(hash1.getText()));
        			log+=hash1.getText()+"   ";
        	 	}
        		p.println(log);
        	 }
        }
        
    }
            

        
    

    @Override
    public void close() {
        _twitterStream.shutdown();
        super.close();
    }
}