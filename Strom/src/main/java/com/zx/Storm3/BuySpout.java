package com.zx.Storm3;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class BuySpout extends BaseRichSpout {

	private static final long serialVersionUID = 6018305333836776809L;
	private SpoutOutputCollector collector;

	@Override
	public void open(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		this.collector = collector;
		System.out.println("BrowseSpout.open方法");
	}

	@Override
	public void nextTuple() {
		String[] users = { "user01", "user02", "user03", "user04", "user05", "user06", "user07", "user08", "user09",
				"user10" };

		for (int i = 0; i < 5; i++) {
			try {
				System.out.println("调用BuySpout.nextTuple方法");
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			collector.emit("buyId", new Values(System.currentTimeMillis(),
					users[(int) (Math.random() * users.length)], (int) (Math.random() * 1000000) / 100.0));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("buyId", new Fields("time", "user", "money"));

	}

}
