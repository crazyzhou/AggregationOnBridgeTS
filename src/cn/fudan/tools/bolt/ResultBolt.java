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
import backtype.storm.tuple.Tuple;
import bsh.EvalError;
import bsh.Interpreter;
import cn.fudan.domain.AvgDataItem;
import cn.fudan.domain.ChannelWindow;
import cn.fudan.domain.GetQueryMap;
import cn.fudan.domain.ResultDataItem;
import cn.fudan.tools.util.NewGenerate;

public class ResultBolt implements IRichBolt{
	OutputCollector _collector;
	Map<ResultDataItem, Map<Long, Float>> map;
	long firstTimestamp;
	Map<String, Set<ChannelWindow>> windowMap;
	Map<ChannelWindow, Set<String>> functionMap;
	Map<ChannelWindow, Long> edgeTimeMap;
	Map<ChannelWindow, List<AvgDataItem>> dataMap;
	
	long maxSeconds;
	long deleteStep;
	long needDealSeconds;
	Interpreter interpreter;
	
	@Override
	public void cleanup()
	{
	}
	
	public boolean canRun()
	{
		for (Map.Entry<ResultDataItem, Map<Long, Float>> entry : map.entrySet())
		{
			Map<Long, Float> value = entry.getValue();
			Long minSeconds = null;
			for (Map.Entry<Long, Float> entry1 : value.entrySet())
			{
				minSeconds = entry1.getKey();
				break;
			}
			if (minSeconds != null)
			{
				for (long i = minSeconds; i <= maxSeconds - deleteStep; i += deleteStep)
				{
					if (value.containsKey(i))
					{
						value.remove(i);
					}
				}
			}
		}
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
			needDealSeconds = l;
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
		GetQueryMap getQueryMap = NewGenerate.getQueryMap;
		interpreter = new Interpreter();
		edgeTimeMap = new HashMap<>();
		dataMap = new HashMap<>();
		firstTimestamp = getQueryMap.getFirstTimestamp();
		windowMap = getQueryMap.getWindowMap();
		functionMap = getQueryMap.getFunctionMap();
		for (ChannelWindow window : functionMap.keySet()) {
			edgeTimeMap.put(window, firstTimestamp);
			dataMap.put(window, new ArrayList<>());
		}
	}

	@Override
	public void execute(Tuple tuple)
	{
		String channelCode = tuple.getStringByField("channelCode");
		long startTime = tuple.getLongByField("startTime");
		long endTime = tuple.getLongByField("endTime");
		float sum = tuple.getFloatByField("sum");
		float num = tuple.getFloatByField("num");
		float max = tuple.getFloatByField("max");
		float min = tuple.getFloatByField("min");
		for (ChannelWindow window : windowMap.get(channelCode)) {
			long lastEdge = edgeTimeMap.get(window);
			dataMap.get(window).add(new AvgDataItem(sum, num, max, min, startTime));
			edgeTimeMap.put(window, lastEdge + window.getMoveSize());
			if (window.getWindowSize() + lastEdge <= endTime) {
				float allnum = 0;
				float allsum = 0;
				float allmax = Float.MIN_VALUE;
				float allmin = Float.MAX_VALUE;
				for (AvgDataItem avgDataItem : dataMap.get(window)) {
					if (avgDataItem.getStartTime() > lastEdge) {
						allnum += avgDataItem.getNum();
						allsum += avgDataItem.getSum();
						allmin = allmin < avgDataItem.getMin() ? allmin : avgDataItem.getMin();
						allmax = allmax > avgDataItem.getMax() ? allmax : avgDataItem.getMax();
					}
				}
				for (String functionName : functionMap.get(window)) {
					ResultDataItem resultDataItem = new ResultDataItem(functionName,
							channelCode, window.getWindowSize(), window.getMoveSize());
					if (functionName.equals("avg"))
						map.get(resultDataItem).put(startTime, allsum / allnum);
					else if (functionName.equals("min"))
						map.get(resultDataItem).put(startTime, allmin);
					else if (functionName.equals("max"))
						map.get(resultDataItem).put(startTime, allmax); 
					else if (functionName.equals("sum"))
						map.get(resultDataItem).put(startTime, allsum);
				}
				
			}
		}
		
		if (canRun())
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
					entry.getValue().remove(needDealSeconds);
				}
				for (String resultItem : NewGenerate.result)
				{
					System.out.println(resultItem + "在" + needDealSeconds
							+ "秒钟结果为:" + interpreter.get(resultItem));
				}
			} catch (EvalError e)
			{
				e.printStackTrace();
			}
		}	
	}
}
