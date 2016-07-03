package shivani.org;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HashtagCount extends BaseRichBolt{
	private OutputCollector collector;
	private HashMap<String, Long> counts = null;
	
	//declaring and initializing variables for lossy counting
	private Map<String, LossyObj> window = new ConcurrentHashMap<String,LossyObj>();
	private double e=0.02f;
	private int bucket_size=(int) Math.ceil(1/e);
	private int curr_bucket=1;
	private int number_of_elements=0;
	private double sminuse=0.01f;//s=0.03  0.03-0.01
	private int totalElements;

	
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.counts = new HashMap<String, Long>();
		totalElements=0;
	}

	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("message");
		totalElements++;
			lossy_counting(word);
	}

	
	public void lossy_counting(String hashtag)
	{
		if(number_of_elements<bucket_size) {
			if(!window.containsKey(hashtag)) {
				LossyObj d = new LossyObj();
				d.delta = curr_bucket-1;
				d.freq = 1;
				d.element = hashtag;
				window.put(hashtag, d);
			}
			else {
			 LossyObj d = window.get(hashtag);
				d.freq+=1;
				window.put(hashtag, d);
			}
			number_of_elements+=1;
		}
		if( number_of_elements == bucket_size) {
			Delete();
			for(String str_loss: window.keySet()) {
				LossyObj d = window.get(str_loss);
				//condition if required
				if(d.freq >= sminuse*totalElements)
				collector.emit(new Values(str_loss, d.freq, d.freq + d.delta));
			}
			number_of_elements=0;
			curr_bucket+=1;
		}
	}
	
	
	
	public void Delete()
	{
		for(String hashtag: window.keySet()) {
			LossyObj d = window.get(hashtag);
			double freqandDelta = d.freq + d.delta;
			if(freqandDelta <= curr_bucket) {
				window.remove(hashtag);
			}
		}
	}
	
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtag", "count","lossy_count"));
	}
}
