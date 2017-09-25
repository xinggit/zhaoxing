package com.zx.Storm_kafka;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

public class Nimbus {

	public static void main(String[] args) throws Exception {

		TopologyBuilder topology = new TopologyBuilder();
		Config conf = new Config();

		// kafka连接
		SpoutConfig sconf = new SpoutConfig(new ZkHosts("slave03:2181"), "test3", "", "id7");
		sconf.scheme = new SchemeAsMultiScheme(new StringScheme());
		topology.setSpout("kafka", new KafkaSpout(sconf));

		topology.setBolt("mybolt", new Word_Bolt()).globalGrouping("kafka");

		LocalCluster local = new LocalCluster();
		local.submitTopology("myNimbus", conf, topology.createTopology());

		Thread.sleep(60000);

		local.shutdown();

	}

}
