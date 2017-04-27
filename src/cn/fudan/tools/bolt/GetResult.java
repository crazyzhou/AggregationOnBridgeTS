package cn.fudan.tools.bolt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.LinkedList;
import java.util.List;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import bsh.EvalError;
import bsh.Interpreter;
import cn.fudan.domain.ResultDataItem;

public class GetResult implements IRichBolt
{
	OutputCollector _collector;
	Map<ResultDataItem, Map<Long, Float>> map;
	long maxSeconds;
	long deleteStep;
	long needDealSeconds;
	Interpreter interpreter;
	Set<String> result;

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

	public void put(String functionName, String channelCode, long windowSize,
			long moveSize)
	{
		if (map == null)
		{
			map = new HashMap<ResultDataItem, Map<Long, Float>>();
		}
		map.put(new ResultDataItem(functionName, channelCode, windowSize,
				moveSize), new TreeMap<Long, Float>());
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector)
	{
		this._collector = collector;
		interpreter = new Interpreter();
		result = new HashSet<String>();
		result.add("out_MD471Z");
		result.add("out_MD472Z");
		result.add("out_MD473Z");
		result.add("out_MD474Z");
		result.add("out_MD471474_MZ");
		result.add("out_MD471474_AZ");
		put("min", "5AB003-DY", 5000, 1000);
		put("max", "5AB002-DY", 5000, 1000);
		put("sum", "5AB004-DY", 5000, 1000);
		put("avg", "5AB001-DY", 5000, 1000);
	}

	@Override
	public void execute(Tuple tuple)
	{
		String functionName = tuple.getStringByField("functionName");
		String channelCode = tuple.getStringByField("channelCode");
		long windowSize = tuple.getLongByField("windowSize");
		long moveSize = tuple.getLongByField("moveSize");
		long startTime = tuple.getLongByField("startTime");
		float average = tuple.getFloatByField("average");
		ResultDataItem resultDataItem = new ResultDataItem(functionName,
				channelCode, windowSize, moveSize);
		map.get(resultDataItem).put(startTime, average);
		deleteStep = moveSize * 10;
		if (startTime > maxSeconds)
		{
			maxSeconds = startTime;
		}
		if (canRun())
		{
			try
			{
				interpreter.set("getResultmap", map);
				interpreter.set("needDealSeconds", needDealSeconds);
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
				for (String resultItem : result)
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