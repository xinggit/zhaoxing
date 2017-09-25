package com.zx.Storm;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Spout_word implements IRichSpout {

	private static final long serialVersionUID = 6410685155164197322L;
	private int index = 0;
	private SpoutOutputCollector collector;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		System.out.println("调用Spout_open()方法");
	}

	@Override
	public void close() {

	}

	@Override
	public void activate() {
		System.out.println("调用Spout_activate()方法");
	}

	@Override
	public void deactivate() {

	}

	@Override
	public void nextTuple() {
		System.out.println("调用nextTuple()方法");
		String[] words = { "hello world", "hello hadoop", "this is storm", "study storm" };
		for (;index < words.length; index++) {
			
			String string = words[index];
			collector.emit(new Values(string));

		}
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void ack(Object msgId) {
		System.out.println("调用ack()方法 = " + msgId);
	}

	@Override
	public void fail(Object msgId) {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("words"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
