package shivani.org;

	import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Time;

public class ReportBolt extends BaseRichBolt {
		//private HashMap<String, String> counts = null;
		private HashMap<Double, List<LossyObj>> counts=null; 
		private double start_time;
		private double curr;
		public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
			this.counts = new HashMap<Double,List<LossyObj>>();
			start_time=new Date().getTime()/1000;
		}

	public void execute(Tuple tuple) {
		curr = new Date().getTime() / 1000;
		String word = tuple.getStringByField("hashtag");
		int count = tuple.getIntegerByField("count");
		int loss_count = tuple.getIntegerByField("lossy_count");
		LossyObj element = new LossyObj();
		element.element = word;
		element.freq = count;
		element.delta = loss_count;
		element.timestamp = curr;
		if (counts.containsKey(curr)) {
			List<LossyObj> list = counts.get(curr);
			list.add(element);
			counts.put(curr, list);
		} else {
			List<LossyObj> list = new ArrayList<LossyObj>();
			list.add(element);
			counts.put(curr, list);
		}
		// double curr = new Date().getTime()/1000;

		if (curr - start_time >= 10) {// will display output every 10 seconds {
			Output_Display();
			start_time = new Date().getTime() / 1000;
		}
	}
		
		
		public void Output_Display()
		{
			
			 System.out.println("Start time: "+String.format("%12.0f",start_time)+ "  End Time: "+String.format("%12.0f",curr));
			 System.out.println("DIFFERENCE: "+ (curr-start_time));
			 System.out.println("Below are the top hashtags");
			 HashMap<String, LossyObj> counts_mid= new HashMap<String,LossyObj>(); 
			 List<Double> keys = new ArrayList<Double>(counts.keySet());
			 
			 
			for (double key : keys) {
				if(key<=start_time+10)
				{
					List<LossyObj> objs = counts.get(key);
					 for(LossyObj obj : objs) {
						 if(counts_mid.get(obj.element)!=null)
						 {
							 if(counts_mid.get(obj.element).timestamp<obj.timestamp)
							 {
								 counts_mid.put(obj.element,obj);
							 }
						 }
						 else
						 {
							 counts_mid.put(obj.element,obj);
						 }
						 
					 }
					 counts.remove(key);
				}
			}
			
			Map<Integer, List<LossyObj>> entryCount = new TreeMap<Integer, List<LossyObj>>();
			 for(Map.Entry<String, LossyObj> entry : counts_mid.entrySet()) {
		            if(entryCount.get(entry.getValue().freq) != null) {
		                List<LossyObj> oList =  entryCount.get(entry.getValue().freq);
		                oList.add(entry.getValue());
		                entryCount.put(entry.getValue().freq, oList);
		            } else {
		                List<LossyObj> oList = new ArrayList<LossyObj>();
		                oList.add(entry.getValue());
		                entryCount.put(entry.getValue().freq, oList);
		            }
		            
		        }
		        
		        /*SORTED MAP IS SORTED BASED ON COUNT*/
		        List<Integer> keySet = new ArrayList<Integer>(entryCount.keySet());
		        Collections.sort(keySet, Collections.reverseOrder());
		        List<String> printedTags = new ArrayList<String>();
		        
		        
		        for (Integer entry : keySet) {
		            for(LossyObj o : entryCount.get(entry)) {
		                System.out.println(o.element+" : "+"Frequency : "+o.freq);
		            }
		        }
		        System.out.println();
		        System.out.println();
		        
		}
		
		public void declareOutputFields(OutputFieldsDeclarer declarer) { // this
																			// bolt
																			// does
																			// not
																			// emit
																			// anything
		}
		
	}

