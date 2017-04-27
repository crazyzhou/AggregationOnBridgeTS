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
import org.apache.storm.tuple.Tuple;
import bsh.EvalError;
import bsh.Interpreter;
import cn.fudan.domain.AvgDataItem;
import cn.fudan.domain.ChannelWindow;
import cn.fudan.domain.GetQueryMap;
import cn.fudan.domain.ResultDataItem;
import cn.fudan.tools.util.NewGenerate;

public class ResultBolt implements IRichBolt{
	boolean isFirst;
	OutputCollector _collector;
	Map<ResultDataItem, Map<Long, Float>> map;
	Map<String, Set<ChannelWindow>> windowMap;
	Map<ChannelWindow, Set<String>> functionMap;
	Map<ChannelWindow, Long> edgeTimeMap;
	Map<String, List<AvgDataItem>> dataMap;
	
	static final long deleteThreshold = 1000 * 60 * 3; //3 min
	long dealingSecond;
	Interpreter interpreter;
	
	@Override
	public void cleanup()
	{
	}
	
	private void cleanMemory(String dealingChannel, long curTimestamp) {
		for (ChannelWindow channelWindow : windowMap.get(dealingChannel)) {
			for (String func : functionMap.get(channelWindow)) {
				ResultDataItem resultDataItem = new ResultDataItem(func, channelWindow);
				Map<Long, Float> tmpMap = map.get(resultDataItem);
				for (Map.Entry<Long, Float> entry : tmpMap.entrySet()) {
					if (entry.getKey() + deleteThreshold < curTimestamp) {
						tmpMap.remove(entry.getKey());
					}
				}
			}
		}
		List<AvgDataItem> tmpList = dataMap.get(dealingChannel);
		for (AvgDataItem avgDataItem : tmpList) {
			if (avgDataItem.getStartTime() + deleteThreshold < curTimestamp) {
				tmpList.remove(avgDataItem);
			}
		}
	}
	
	private boolean canRun(String dealingChannel, long curTimestamp)
	{
		cleanMemory(dealingChannel, curTimestamp);
		Set<Long> dealSeconds = new HashSet<Long>();
		int count = 0;
		for (Map.Entry<ResultDataItem, Map<Long, Float>> entry : map.entrySet())
		{
			if (0 == count)
			{
				dealSeconds.addAll(entry.getValue().keySet());
				count++;
				continue;
			}
			if (dealSeconds.size() == 0)
			{
				break;
			}
			Map<Long, Float> value = entry.getValue();
			Set<Long> dealSecondsTemp = new HashSet(dealSeconds);
			for (Long l : dealSecondsTemp)
			{
				if (value.containsKey(l))
				{
					continue;
				} else
				{
					dealSeconds.remove(l);
				}
			}
		}
		if (0 == dealSeconds.size())
		{
			return false;
		}
		for (Long l : dealSeconds)
		{
			dealingSecond = l;
		}
		return true;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
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
		interpreter = new Interpreter();
		edgeTimeMap = new HashMap<>();
		dataMap = new HashMap<>();
		map = new HashMap<>();
		isFirst = true;
	}

	@Override
	public void execute(Tuple tuple)
	{
		if (isFirst) {
			GetQueryMap getQueryMap = NewGenerate.getQueryMap;
			windowMap = getQueryMap.getWindowMap();
			functionMap = getQueryMap.getFunctionMap();
			Map<String, Long> firstTimestampMap = getQueryMap.getFirstTimestampMap();
			for (ChannelWindow window : functionMap.keySet()) {
				edgeTimeMap.put(window, firstTimestampMap.get(window.getChannel()));
				if (!dataMap.containsKey(window.getChannel()))
					dataMap.put(window.getChannel(), new ArrayList<AvgDataItem>());
				for (String func : functionMap.get(window)) {
					map.put(new ResultDataItem(func, window), new HashMap<Long, Float>());
				}
			}
			isFirst = false;
		}
		String channelCode = tuple.getStringByField("channelCode");
		long startTime = tuple.getLongByField("startTime");
		long endTime = tuple.getLongByField("endTime");
		float sum = tuple.getFloatByField("sum");
		float num = tuple.getFloatByField("num");
		float max = tuple.getFloatByField("max");
		float min = tuple.getFloatByField("min");
		//System.out.println(channelCode + '\t' + startTime + '\t' + endTime + '\t' + num);
		for (ChannelWindow window : windowMap.get(channelCode)) {
			long lastEdge = edgeTimeMap.get(window);
			dataMap.get(channelCode).add(new AvgDataItem(sum, num, max, min, startTime));
			edgeTimeMap.put(window, lastEdge + window.getMoveSize());
			if (window.getWindowSize() + lastEdge <= endTime) {
				float allnum = 0;
				float allsum = 0;
				float allmax = Float.MIN_VALUE;
				float allmin = Float.MAX_VALUE;
				for (AvgDataItem avgDataItem : dataMap.get(channelCode)) {
					if (avgDataItem.getStartTime() > lastEdge) {
						allnum += avgDataItem.getNum();
						allsum += avgDataItem.getSum();
						allmin = allmin < avgDataItem.getMin() ? allmin : avgDataItem.getMin();
						allmax = allmax > avgDataItem.getMax() ? allmax : avgDataItem.getMax();
					}
				}
				for (String functionName : functionMap.get(window)) {
					ResultDataItem resultDataItem = new ResultDataItem(functionName, window);
					if (functionName.equals("avg"))
						map.get(resultDataItem).put(lastEdge, allsum / allnum);
					else if (functionName.equals("min"))
						map.get(resultDataItem).put(lastEdge, allmin);
					else if (functionName.equals("max"))
						map.get(resultDataItem).put(lastEdge, allmax);
					else if (functionName.equals("sum"))
						map.get(resultDataItem).put(lastEdge, allsum);
				}
				
			}
		}
		
		if (canRun(channelCode, endTime))
		{
			try
			{
				interpreter.set("getResultmap", map);
				interpreter.set("needDealSeconds", endTime);
				interpreter.set("temp", new TreeMap<Long, Float>());
				interpreter
						.eval("import java.util.HashMap;"
								+ "\n"
								+ "import cn.fudan.domain.ResultDataItem;"
								+ "\n"
								+ "import java.util.Map;"
								+ "\n"
								+ "public float avg(String channel, long windowSize, long moveSize){"
								+ "temp = getResultmap.get(new ResultDataItem(\"avg\", channel,windowSize, moveSize));"
								+ "float result = temp.get(needDealSeconds);"
								+ "return result;}"
								+ "public float max(String channel, long windowSize, long moveSize){"
								+ "temp = getResultmap.get(new ResultDataItem(\"max\", channel,windowSize, moveSize));"
								+ "float result = temp.get(needDealSeconds);"
								+ "return result;}"
								+ "public float sum(String channel, long windowSize, long moveSize){"
								+ "temp = getResultmap.get(new ResultDataItem(\"sum\", channel,windowSize, moveSize));"
								+ "float result = temp.get(needDealSeconds);"
								+ "return result;}"
								+ "public float min(String channel, long windowSize, long moveSize){"
								+ "temp = getResultmap.get(new ResultDataItem(\"min\", channel,windowSize, moveSize));"
								+ "float result = temp.get(needDealSeconds);"
								+ "return result;}"
								+ "out_MD471Z=avg("
								+ "\"5AB001-DY\""
								+ ",5000,1000);"
								+ "out_MD472Z=max("
								+ "\"5AB002-DY\""
								+ ",5000,1000);"
								+ "out_MD473Z=min("
								+ "\"5AB003-DY\""
								+ ",5000,1000);"
								+ "out_MD474Z=sum("
								+ "\"5AB004-DY\""
								+ ",5000,1000);"
								+ "double[] max={out_MD471Z,out_MD472Z,out_MD473Z,out_MD474Z};"
								+ "java.util.Arrays.sort(max);"
								+ "out_MD471474_MZ=max[max.length-1];"
								+ "out_MD471474_AZ=(out_MD471Z+out_MD472Z+out_MD473Z+out_MD474Z)/4;");
				for (Map.Entry<ResultDataItem, Map<Long, Float>> entry : map
						.entrySet())
				{
					entry.getValue().remove(dealingSecond);
				}
				for (String resultItem : NewGenerate.result)
				{
					System.out.println(resultItem + "在" + dealingSecond
							+ "秒钟结果为:" + interpreter.get(resultItem));
				}
			} catch (EvalError e)
			{
				e.printStackTrace();
			}
		}	
	}
}
