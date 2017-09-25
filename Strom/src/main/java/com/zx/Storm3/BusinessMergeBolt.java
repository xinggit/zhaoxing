package com.zx.Storm3;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class BusinessMergeBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -4536364629904433619L;
	private Map<String, Double> map;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		if (map == null) {
			map = new HashMap<String, Double>();
		}
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String href = input.getStringByField("href");
		double money = input.getDoubleByField("money");
		map.put(href, map.containsKey(href) ? money + map.get(href) : money);
		System.out.println(String.format("来源自%s 的交易金额是：%f", href, map.get(href)));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
