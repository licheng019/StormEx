package StormEx;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class FileCountTopo {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("spout", new ReadFileSpout());
		builder.setBolt("b1", new FileBolt(),1).shuffleGrouping("spout");
		Config conf = new Config();
	    conf.setDebug(true);
	    
	    if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder.createTopology());
		}
		
	}
}
