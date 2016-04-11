package cn.fudan.tools.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import bsh.Interpreter;
import cn.fudan.domain.AvgDataItem;
import cn.fudan.domain.ChannelWindow;
import cn.fudan.domain.GetQueryMap;
import cn.fudan.domain.PairedWindow;
import cn.fudan.domain.SliceManager;

public class SliceCalcBolt implements IRichBolt {
	Interpreter interpreter;
	OutputCollector _collector;
	Map<String, Set<ChannelWindow>> windowMap;
	Map<String, SliceManager> sliceManagers;
	
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this._collector = collector;
		windowMap = GetQueryMap.getWindowMap();
		for (String channelCode : windowMap.keySet())
		{
			List<PairedWindow> pairedWindows = new ArrayList<>();
			for (ChannelWindow window : windowMap.get(channelCode))
			{
				PairedWindow pairedWindow = new PairedWindow(window);
				pairedWindows.add(pairedWindow);
			}
			sliceManagers.put(channelCode, new SliceManager(pairedWindows, GetQueryMap.getFirstTimestamp()));
		}		
	}
	
	@Override
	public void execute(Tuple tuple) {
		String channelCode = tuple.getStringByField("channelCode");
		long timeStamp = tuple.getLongByField("timeStamp");
		float value = tuple.getFloatByField("value");
		long edgeTimeStamp = sliceManagers.get(channelCode).advanceWindowGetNextEdge();
		for (ChannelWindow channelWindow : windowMap.get(channelCode))
		{
			
		}
	}

	@Override
	public void cleanup()
	{
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("channelCode", "startTime", "sliceSize", "avg", "max", "min", "sum"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}
}
