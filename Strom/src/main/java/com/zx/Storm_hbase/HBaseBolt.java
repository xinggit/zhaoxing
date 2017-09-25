package com.zx.Storm_hbase;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class HBaseBolt implements IRichBolt {

	private static final long serialVersionUID = -124929306250307632L;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {

	}

	@Override
	public void execute(Tuple input) {

		String rowkey = input.getString(0);
		String name = input.getString(1);
		int age = input.getInteger(2);

		Map<String, Map<String, String>> fam = new HashMap<>();
		Map<String, String> text = new HashMap<>();
		text.put("name", name);
		text.put("age", age + "");
		fam.put("test", text);
		try {
			HBase.Put("cf1", rowkey, fam);
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void cleanup() {

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
