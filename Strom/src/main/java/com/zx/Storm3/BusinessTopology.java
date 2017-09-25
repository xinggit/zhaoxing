package com.zx.Storm3;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class BusinessTopology {
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		Config conf = new Config(); // storm配制

		builder.setSpout("browse", new BrowseSpout());
		builder.setSpout("buy", new BuySpout());
		BoltDeclarer bolt = builder.setBolt("split", new BusinessSplitBolt(), 2);
		bolt.shuffleGrouping("browse","browseId");
		bolt.shuffleGrouping("buy","buyId");
		builder.setBolt("merge", new BusinessMergeBolt(), 2).fieldsGrouping("split", new Fields("href"));

		conf.setDebug(true);

		conf.put(Config.NIMBUS_SEEDS, Arrays.asList("master"));
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("slave01:2181", "slave02:2181", "slave03:2181"));

		LocalCluster local = new LocalCluster();

		local.submitTopology("storm", conf, builder.createTopology());

		Thread.sleep(8000);
		local.shutdown();

		// System.setProperty("storm.jar" ,
		// "D:\\work\\bigdata\\hadoop42_011_storm\\target\\hadoop42_010_sqoop-0.0.1.jar");
		// StormSubmitter.submitTopology("business",
		// conf,builder.createTopology());
	}
}
