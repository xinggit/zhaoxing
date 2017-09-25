package com.zx.Storm_hbase;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class App {

	public static void main(String[] args) throws Exception {

		Config conf = new Config();
		TopologyBuilder topy = new TopologyBuilder();

		conf.put(Config.NIMBUS_SEEDS, Arrays.asList("master"));
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("slave01:2181", "slave02:2181", "slave03:2181"));

		topy.setSpout("hbasespout", new HBaseSpout());
		topy.setBolt("hbasebolt", new HBaseBolt(),4).shuffleGrouping("hbasespout");

		LocalCluster local = new LocalCluster();
		local.submitTopology("hbase", conf, topy.createTopology());

		Thread.sleep(5000);
		local.shutdown();
	}

}
