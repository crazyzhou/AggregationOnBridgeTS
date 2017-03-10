package cn.fudan.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import cn.fudan.tools.bolt.GetResult;
import cn.fudan.tools.bolt.ResultBolt;
import cn.fudan.tools.bolt.SliceCalcBolt;
import cn.fudan.tools.bolt.AvgBolt;
import cn.fudan.tools.bolt.CalData;
import cn.fudan.tools.bolt.FilterBolt;
import cn.fudan.tools.bolt.StaticFilter;
import cn.fudan.tools.spout.GetData;
import cn.fudan.tools.util.NewGenerate;


public class NewTopology
{
	public static final int WORKER_NUM = 6;
	public static void main(String[] args) throws Exception, InvalidTopologyException
	{
		NewGenerate.main(args);
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("getData", new GetData(), 1);
		builder.setBolt("calData", new CalData(), 1).fieldsGrouping("getData",
				new Fields("channelCode"));
		builder.setBolt("filterBolt", new FilterBolt(), 1).fieldsGrouping("calData",
				new Fields("channelCode"));
		builder.setBolt("sliceCalcBolt", new SliceCalcBolt(), 1).fieldsGrouping(
				"filterBolt", new Fields("channelCode"));
		builder.setBolt("resultBolt", new ResultBolt(), 1).fieldsGrouping(
				"sliceCalcBolt", new Fields("channelCode"));
		Config conf = new Config();
		//conf.setDebug(true);

		if (args != null && args.length > 0)
		{
			conf.setNumWorkers(WORKER_NUM);

			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());// readStormConfig(首先读取的是defaults.yaml下的配置，然后读取storm.conf.file这个变量（有可能读取不到任何东西，这个变量下存储的是一个文件的名字），如果为空就读取storm.yaml，否则读取这个文件下的配置，最后读取storm.options）
		} else
		{
			conf.setMaxTaskParallelism(3);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("BridgeStorm", conf,
					builder.createTopology());

			Thread.sleep(10800000);

			cluster.shutdown();
		}
	}
}
