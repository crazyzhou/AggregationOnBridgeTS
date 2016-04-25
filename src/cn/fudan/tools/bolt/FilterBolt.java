package cn.fudan.tools.bolt;

import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.fudan.domain.GetQueryMap;
import cn.fudan.tools.util.NewGenerate;

public class FilterBolt implements IRichBolt{
	OutputCollector _collector;
	Set<String> channelSet;
	boolean isFirst;
	GetQueryMap getQueryMap;

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
		if (channelSet.contains(ChannelCode))
		{
			if (isFirst) {
				System.out.println("I'm in it");
				getQueryMap.setFirstTimestamp(timeStamp);
				System.out.println(ChannelCode + '\t' + timeStamp);
				isFirst = false;
			}
			_collector.emit(new Values(ChannelCode, timeStamp, value));
		}
	}

	@Override
	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector)
	{
		this._collector = collector;
		getQueryMap = NewGenerate.getQueryMap;
		isFirst = true;
		channelSet = getQueryMap.getWindowMap().keySet();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("channelCode", "timeStamp", "value"));
	}
}
