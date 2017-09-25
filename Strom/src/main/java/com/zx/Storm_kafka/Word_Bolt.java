package com.zx.Storm_kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class Word_Bolt implements IRichBolt {

	private static final long serialVersionUID = -5183053661112051085L;

	private OutputCollector collector;
	private List<String> words;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		words = new ArrayList<>();
	}

	@Override
	public void execute(Tuple input) {

		String word = input.getString(0);

		if (word == null || word.length() <= 0) {
			return;
		}
		
		if (word.endsWith(".")) {
			words.add(word);
			StringBuilder line = new StringBuilder();
			for (String string : words) {
				line.append(string);
			}
			String li = line.toString();
			// collector.emit(new Values(li));
			System.out.println("合成的句子 = " + li);
			// StreamUtil.Write("/home/hadoop/line.txt", li);
			line = null;
			words.clear();
		} else {
			words.add(word);
		}

	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
