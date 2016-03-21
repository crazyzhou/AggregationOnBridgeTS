package cn.fudan.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class StaticFilter implements IRichBolt
{
	OutputCollector _collector;
	Map<String, List<String>> map;// ChannelCode

	@Override
	public void cleanup()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple tuple)// 固定代码
	{
		String ChannelCode = tuple.getStringByField("channelCode");
		long timeStamp = tuple.getLongByField("timeStamp");
		float value = tuple.getFloatByField("value");
		if (map.containsKey(ChannelCode))
		{
			List<String> streamId = map.get(ChannelCode);
			for (int i = 0; i < streamId.size(); i++)
			{
				_collector.emit(streamId.get(i), new Values(ChannelCode,
						timeStamp, value));
			}
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector)
	{
		this._collector = collector;
		map = new HashMap<String, List<String>>();
		// 这里要添加put语句
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
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		// declarer.declareStream(streamId, new Fields("channelCode",
		// "timeStamp", "value"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration()
	{
		// TODO Auto-generated method stub
		return null;
	}

}
