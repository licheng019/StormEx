package StormEx;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class ReadFileSpout implements IRichSpout{

		private static final long serialVersionUID = 1L;
		FileInputStream fis;
		InputStreamReader isr;
		BufferedReader br;			

		SpoutOutputCollector collector = null;
		
	//If open acker, tuple will call this method if run successfully, and tell storm that this tuple has already run successfully.
	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	//activate topo
	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	//stop topo
	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}
    //If open acker, tuple will call this method if run Fails, and tell storm that this tuple has already run fails.
	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	//accept data from outside
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		String str = null;
		try {
			while ((str = this.br.readLine()) != null) {
				collector.emit(new Values(str));
//				Thread.sleep(3000);
				//to do 
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	//initialize
	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		try {
			this.collector = collector;
			this.fis = new FileInputStream("track.log");
			this.isr = new InputStreamReader(fis, "UTF-8");
			this.br = new BufferedReader(isr);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//define output column name
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("log"));
	}

	//can set up property,barely use this method
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
