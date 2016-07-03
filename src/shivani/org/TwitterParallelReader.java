package shivani.org;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TwitterParallelReader {
	public static final String TWITTER_SPOUT = "message1";
	private static final String COUNT_BOLT= "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String TOPOLOGY_NAME = "Twitter-topology-parallel";
	public static void main(String args[]) throws InterruptedException{
			TwitterSpout spout = new TwitterSpout("h4so6pE2Y8bU6zOD9CtwGs31t","OtLq1uD16HWG515jc7UOHcqqguSe3nIeBvW53gcnk0zCmm8emV","3883548734-fQnWcxbeNZztSorIlPnI4yYInS4mGBnRneEPbGu", "9fTfsAeyWA2iB1dMAwdchl8wpC04UQ60CKUqMuTSOIWgR");
			HashtagCount countBolt=new HashtagCount();
			ReportBolt reportBolt =new ReportBolt();
			Config config = new Config();
			//TwitterLogging logger = new TwitterLogging();
			
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout(TWITTER_SPOUT, spout);
			config.setNumWorkers(4);
			builder.setBolt(COUNT_BOLT, countBolt,4).fieldsGrouping(TWITTER_SPOUT,
					new Fields("message"));
			
			builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(
					COUNT_BOLT);
			
			try {
				StormSubmitter.submitTopology(TOPOLOGY_NAME,config,builder.createTopology()) ;
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//LocalCluster cluster = new LocalCluster();
			//cluster.submitTopology("TwitterTopology", config, builder.createTopology());
			/*Thread.sleep(1000000);
			cluster.killTopology("TwitterTopology");
			cluster.shutdown();*/
}
}	
	