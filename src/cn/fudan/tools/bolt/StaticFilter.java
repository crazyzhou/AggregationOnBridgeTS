package cn.fudan.tools.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

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
		//System.out.println(ChannelCode + '\t' + timeStamp);
		if (map.containsKey(ChannelCode))
		{
			//System.out.println(ChannelCode + '\t' + timeStamp);
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