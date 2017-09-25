package com.zx.Storm4;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class SecondBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	private Map<String,Integer> map = new HashMap<>();

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		System.out.println("调用SecondBolt.prepare()");
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		
		String word = input.getString(0);
		int num = input.getInteger(1);
		
		if(map.containsKey(word)) {
			map.put(word, map.get(word) + num);
		} else {
			map.put(word, num);
		}
		
	}

	@Override
	public void cleanup() {
		System.out.println("调用SecondBolt.cleanup()");
		for (String key : map.keySet()) {
			System.out.println(key + " = " + map.get(key));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
