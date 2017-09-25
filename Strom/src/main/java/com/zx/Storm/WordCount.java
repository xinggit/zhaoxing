package com.zx.Storm;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCount {

	public static void main(String[] args) throws Exception {

		TopologyBuilder topo = new TopologyBuilder();
		Config conf = new Config();

		topo.setSpout("spout", new Spout_word());
		topo.setBolt("count_bolt", new Bolt_count()).fieldsGrouping("bolt", new Fields("word"));
		topo.setBolt("bolt", new Bolt_word()).shuffleGrouping("spout","id");

		conf.setDebug(true);
		conf.put(Config.NIMBUS_SEEDS, Arrays.asList("master"));
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("slave01:2181", "slave02:2181", "slave03:2181"));

		LocalCluster local = new LocalCluster();

		System.setProperty("storm.jar",
				"F:\\Users\\Administrator.USER-20160117RG\\workspace\\Storm\\target\\Storm-0.0.1-SNAPSHOT.jar");
		local.submitTopology("storm", conf, topo.createTopology());

		Thread.sleep(10000);
		local.shutdown();

	}

}
