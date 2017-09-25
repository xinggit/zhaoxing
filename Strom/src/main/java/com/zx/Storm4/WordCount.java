package com.zx.Storm4;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class WordCount {

	public static void main(String[] args) throws Exception {
		
		TopologyBuilder topo = new TopologyBuilder();
		Config conf = new Config();
		conf.setDebug(true);
		conf.put(Config.NIMBUS_SEEDS, Arrays.asList("master"));
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("slave01:2181", "slave02:2181", "slave03:2181"));
		
		topo.setSpout("spout", new FristSpout());
		topo.setBolt("FristBolt", new FristBolt(),2).shuffleGrouping("spout").setNumTasks(2);
		topo.setBolt("SecondBolt", new SecondBolt()).shuffleGrouping("FristBolt");
		
		LocalCluster local = new LocalCluster();
		local.submitTopology("wordcount", conf, topo.createTopology());
		Thread.sleep(6000);
		local.shutdown();
		
	}
	
}
