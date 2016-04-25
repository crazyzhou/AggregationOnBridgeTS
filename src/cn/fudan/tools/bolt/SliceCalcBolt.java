package cn.fudan.tools.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.jCharts.axisChart.customRenderers.axisValue.AxisValueRenderEvent;

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
import cn.fudan.tools.util.NewGenerate;

public class SliceCalcBolt implements IRichBolt {
	Interpreter interpreter;
	OutputCollector _collector;
	Map<String, Set<ChannelWindow>> windowMap;
	Map<String, SliceManager> sliceManagers;
	Map<String, AvgDataItem> sliceDataMap;
	
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this._collector = collector;
		GetQueryMap getQueryMap = NewGenerate.getQueryMap;
		windowMap = getQueryMap.getWindowMap();
		sliceManagers = new HashMap<>();
		sliceDataMap = new HashMap<>();
		for (String channelCode : windowMap.keySet())
		{
			List<PairedWindow> pairedWindows = new ArrayList<>();
			for (ChannelWindow window : windowMap.get(channelCode))
			{
				PairedWindow pairedWindow = new PairedWindow(window);
				pairedWindows.add(pairedWindow);
			}
			System.out.println("shit" + getQueryMap.getFirstTimestamp());
			sliceManagers.put(channelCode, new SliceManager(pairedWindows, getQueryMap.getFirstTimestamp()));
		}	
	}
	
	@Override
	public void execute(Tuple tuple) {
		String channelCode = tuple.getStringByField("channelCode");
		long timeStamp = tuple.getLongByField("timeStamp");
		float value = tuple.getFloatByField("value");
		System.out.println(sliceManagers.get(channelCode));
		long edgeTimeStamp = sliceManagers.get(channelCode).advanceWindowGetNextEdge();
		System.out.println(timeStamp + ' ' + edgeTimeStamp);
		
		if (timeStamp < edgeTimeStamp) {
			if (!sliceDataMap.containsKey(channelCode)) {
				AvgDataItem avgDataItemtemp = new AvgDataItem(value, 1, value, value, timeStamp);
				sliceDataMap.put(channelCode, avgDataItemtemp);
			}
			else {
				AvgDataItem avgDataItemtemp = sliceDataMap.get(channelCode);
				avgDataItemtemp.setNum(avgDataItemtemp.getNum() + 1);
				avgDataItemtemp
						.setSum(avgDataItemtemp.getSum() + value);
				avgDataItemtemp
						.setMax(avgDataItemtemp.getMax() > value ? avgDataItemtemp
								.getMax() : value);
				avgDataItemtemp
						.setMin(avgDataItemtemp.getMin() < value ? avgDataItemtemp
								.getMin() : value);
				sliceDataMap.put(channelCode, avgDataItemtemp);
			}
		}
		else {
			System.out.println(channelCode);
			AvgDataItem avgDataItem = sliceDataMap.get(channelCode);
			System.out.println(avgDataItem);
			_collector.emit(new Values(channelCode, avgDataItem.getStartTime(),
					timeStamp,
					avgDataItem.getSum(), avgDataItem.getNum(),
					avgDataItem.getMax(), avgDataItem.getMin()));
			sliceDataMap.remove(channelCode);
		}
	}

	@Override
	public void cleanup()
	{
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("channelCode", "startTime", "endTime", "sum", "num", "max", "min"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}
}
