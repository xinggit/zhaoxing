package com.zx.Storm3;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class BusinessSplitBolt implements IRichBolt {
	private static final long serialVersionUID = 6026849422508484487L;
	private Map<String, String> map;
	private OutputCollector collector;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("href", "money"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		if (map == null) {
			map = new HashMap<String, String>();
		}
		this.collector = collector;
		System.out.println("调用BusinessSplitBolt.prepare方法");
	}

	@Override
	public void execute(Tuple input) {
		String streamId = input.getSourceStreamId();
		System.out.println("调用BusinessSplitBolt.execute方法 = " + streamId);
		String user = input.getStringByField("user");
		switch (streamId) {
		case "browseId":
			String href = input.getStringByField("href");
			map.put(user, href);
			break;
		case "buyId":
			double money = input.getDoubleByField("money");
			if (map.containsKey(user)) {
				collector.emit(new Values(map.get(user), money));
			}
			break;
		}
	}

	@Override
	public void cleanup() {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
