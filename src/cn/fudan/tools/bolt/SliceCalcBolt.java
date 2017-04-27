package cn.fudan.tools.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
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
	GetQueryMap getQueryMap;
	boolean isFirst;
	
	
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this._collector = collector;
		isFirst = true;
		getQueryMap = NewGenerate.getQueryMap;
		windowMap = getQueryMap.getWindowMap();
		sliceManagers = new HashMap<>();
		sliceDataMap = new HashMap<>();
	}
	
	@Override
	public void execute(Tuple tuple) {
		String channelCode = tuple.getStringByField("channelCode");
		long timeStamp = tuple.getLongByField("timeStamp");
		float value = tuple.getFloatByField("value");
		if (isFirst) {
			for (String channel : windowMap.keySet())
			{
				List<PairedWindow> pairedWindows = new ArrayList<>();
				for (ChannelWindow window : windowMap.get(channel))
				{
					PairedWindow pairedWindow = new PairedWindow(window);
					pairedWindows.add(pairedWindow);
				}
				if (getQueryMap.getFirstTimestampMap().get(channel) == null) {
					sliceManagers.put(channel, new SliceManager(pairedWindows, timeStamp));
					getQueryMap.getFirstTimestampMap().put(channel, timeStamp);
				}
				else 
					sliceManagers.put(channel, new SliceManager(pairedWindows, getQueryMap.getFirstTimestampMap().get(channel)));
			}
			isFirst = false;
		}
		long edgeTimeStamp = sliceManagers.get(channelCode).advanceWindowGetNextEdge(timeStamp);
		//System.out.println(timeStamp + "\t" + channelCode + "\t" + edgeTimeStamp);
		
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
			AvgDataItem avgDataItem = sliceDataMap.get(channelCode);
			//System.out.println(avgDataItem.getStartTime() + "\t" + timeStamp + "\t" + avgDataItem.getNum());
			_collector.emit(new Values(channelCode, avgDataItem.getStartTime(),
					timeStamp,
					avgDataItem.getSum(), avgDataItem.getNum(),
					avgDataItem.getMax(), avgDataItem.getMin()));
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
