package com.zx.Storm3;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class BrowseSpout extends BaseRichSpout {

	private static final long serialVersionUID = 3732014316647908992L;
	private SpoutOutputCollector collector;

	@Override
	public void open(@SuppressWarnings("rawtypes") Map map, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		System.out.println("BrowseSpout.open方法");
	}

	@Override
	public void nextTuple() {
		String[] users = { "user01", "user02", "user03", "user04", "user05", "user06", "user07", "user08", "user09",
				"user10" };
		String[] hrefs = { "href01", "href02", "href03", "href04", "href05", "href06", "href07", "href08", "href09",
				"href10" };

		for (int i = 0; i < 5; i++) {
			try {
				Thread.sleep(500);
				System.out.println("输出一个值===========>");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			collector.emit("browseId",new Values(System.currentTimeMillis(), 
					users[(int) (Math.random() * users.length)],
					hrefs[(int) (Math.random() * hrefs.length)]));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declerar) {
		declerar.declareStream("browseId",new Fields("time", "user", "href"));
	}

}
