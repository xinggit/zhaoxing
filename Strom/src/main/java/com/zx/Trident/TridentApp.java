package com.zx.Trident;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TridentApp {

	public static void main(String[] args) throws Exception {

		// 1:首先 new一个trident
		TridentTopology trident = new TridentTopology();
		Config conf = new Config();
		conf.setDebug(true);
 
		
		// 2:创建一个数据源，此处数据源用来测试，参数是声明的字段字段
		FeederBatchSpout spout = new FeederBatchSpout(Arrays.asList("a", "b", "c", "d"));

		// 3:获取流
		Stream stream = trident.newStream("spout", spout);

		// 4:通过steam分发字段
		stream.shuffle().each(new Fields("a", "b"), new CheckEventSumFilter()).parallelismHint(2);

		LocalCluster local = new LocalCluster();

		local.submitTopology("storm", conf, trident.build());
		// 5:水龙头产生数据，此处是四个值
		spout.feed(Arrays.asList(new Values(1, 2, 3, 4)));

		Thread.sleep(10000);
		local.shutdown();

	}

}
