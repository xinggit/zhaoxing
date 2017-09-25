package com.zx.Storm4;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FristBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		System.out.println("调用FristBolt.prepare()");
		this.collector = collector;
	}

	
	@Override
	public void execute(Tuple input) {
		String line = input.getString(0);
		String[] words = line.split("\\s+");
		
		for (String word: words) {
			collector.emit(new Values(word,1));
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","num"));
	}

	@Override
	public void cleanup() {
		System.out.println("调用FristBolt.cleanup()");
	}


	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
