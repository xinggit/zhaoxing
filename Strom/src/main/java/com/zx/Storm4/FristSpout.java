package com.zx.Storm4;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class FristSpout implements IRichSpout {

	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector collector;
	
	private int index = 0;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		
		System.out.println("调用FristSpout.open()");
		this.collector = collector;
		
	}

	@Override
	public void close() {
		System.out.println("调用FristSpout.close()");
	}

	@Override
	public void activate() {

	}

	@Override
	public void deactivate() {

	}

	@Override
	public void nextTuple() {
		
		System.out.println("调用FristSpout.nextTuple()");
		String[] words = { "hello world", "hello hadoop", "this is storm", "study storm" };
		for (;index < words.length; index++) {
			
			String string = words[index];
			collector.emit(new Values(string));

		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

	@Override
	public void ack(Object msgId) {

	}

	@Override
	public void fail(Object msgId) {

	}


	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
