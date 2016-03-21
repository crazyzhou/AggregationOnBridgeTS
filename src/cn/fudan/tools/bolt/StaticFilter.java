package cn.fudan.tools.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StaticFilter implements IRichBolt
{
	OutputCollector _collector;
	Map<String, List<String>> map; //channelCode=>functions

	@Override
	public void cleanup()
	{
	}

	@Override
	public void execute(Tuple tuple)
	{
		String ChannelCode = tuple.getStringByField("channelCode");
		long timeStamp = tuple.getLongByField("timeStamp");
		float value = tuple.getFloatByField("value");
		System.out.println(ChannelCode);
		if (map.containsKey(ChannelCode))
		{
			_collector.emit(new Values(ChannelCode, timeStamp, value));
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}

	public void put(String channelCode, String function)
	{
		if (map.containsKey(channelCode))
		{
			map.get(channelCode).add(function);
		} else
		{
			List<String> list = new ArrayList<String>();
			list.add(function);
			map.put(channelCode, list);
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector)
	{
		this._collector = collector;
		map = new HashMap<String, List<String>>();
		put("5AB001-DY", "avg");
		put("5AB003-DY", "min");
		put("5AB004-DY", "sum");
		put("5AB002-DY", "max");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("channelCode", "timeStamp", "value"));
	}
}