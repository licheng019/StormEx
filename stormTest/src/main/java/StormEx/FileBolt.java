package StormEx;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FileBolt implements IRichBolt{
	OutputCollector collector = null;
	//destory, barely use
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	Integer num = 0;
	//handle business logic
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String log = input.getStringByField("log");
		System.out.println("bolt-print == "+log);
		num ++ ;
		System.out.println("num == "+num);
		this.collector.emit(new Values(num));
	}

	//initialize
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	//define output
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("num"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
