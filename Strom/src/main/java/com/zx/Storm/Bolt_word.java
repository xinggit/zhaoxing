package com.zx.Storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class Bolt_word implements IRichBolt {

	private OutputCollector collector;
	private int index = 0;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		System.out.println("调用Bolt_word.prepare()方法");
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		
		System.out.println("调用Bolt_word.execute()方法 = " + index++);
		
		String words = input.getString(0);

		String[] word = words.split("\\s+");

		for (String string : word) {

			collector.emit(new Values(string, 1));
		}

		collector.ack(input);

	}

	@Override
	public void cleanup() {
		System.out.println("调用Bolt_word.cleanup()方法");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "num"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
