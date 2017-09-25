package com.zx.Storm;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class Bolt_count implements IRichBolt {

	private OutputCollector collector;
	private Map<String, Integer> count;
	private int index = 0;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		count = new HashMap<>();
		System.out.println("调用Bolt_count.prepare()方法");
	}

	@Override
	public void execute(Tuple input) {
		System.out.println("调用Bolt_count.execute()方法 = " + index++);
		String word = input.getString(0);

		if (!count.containsKey(word)) {
			count.put(word, 1);
		} else {
			int num = count.get(word) + 1;
			count.put(word, num);
			System.out.println("word = " + word + "num = " + num);
		}
		collector.ack(input);
	}

	@Override
	public void cleanup() {
		for (String key : count.keySet()) {
			System.out.println(key + " 出现次数 = " + count.get(key));
		}
		System.out.println("调用Bolt_count.cleanup()方法");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
