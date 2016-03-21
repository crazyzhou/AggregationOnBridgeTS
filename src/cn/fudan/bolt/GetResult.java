package cn.fudan.bolt;

import java.util.HashMap;
import java.util.HashSet;
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
import cn.fudan.domain.ResultDataItem;

public class GetResult implements IRichBolt
{
	OutputCollector _collector;
	Map<ResultDataItem, Map<Long, Float>> map;
	long maxSeconds;// 当前最大秒数是多少，用于计算被清除的有哪些
	long deleteStep;// 超时步长，设置为移动窗口大小的10倍
	long needDealSeconds;// 表示当前所有的函数需要取出来的是哪一秒的数据
	Interpreter interpreter; // 构造一个解释器
	Set<String> result;// 这个东西记录了最终的结果的名字

	@Override
	public void cleanup()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple tuple)
	{
		// TODO Auto-generated method stub
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
				interpreter.set("getResultmap", map);// 需要处理的map
				interpreter.set("needDealSeconds", needDealSeconds);// 需要处理map中的每一项的哪一秒
				interpreter.set("temp", new TreeMap<Long, Float>());// beanshell里面不支持map泛型
				// interpreter.eval("expression");这里就是需要插入的静态代码块
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
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
				}// 删掉那些过时的Seconds
			}
		}
		Set<Long> dealSeconds = new HashSet<Long>();// 第一个Map项里面的临时结果
		int count = 0;// 标志位，第一个List用于放进去
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
	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector)
	{
		// TODO Auto-generated method stub
		this._collector = collector;
		result = new HashSet<String>();
		// String[] s = expression.split(";");
		// for(int i = 0;i=s.length;i++)
		// {
		// String[] s1 = s[i].split("=");
		// if(s1.length == 2)
		// {
		// result.add(s1[0]);
		// }
		// }
		interpreter = new Interpreter(); // 构造一个解释器
		// 在这里放put
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration()
	{
		// TODO Auto-generated method stub
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

	public float avg(String channel, long windowSize, long moveSize)
	{
		Map<Long, Float> temp = map.get(new ResultDataItem("avg", channel,
				windowSize, moveSize));
		float result = temp.get(needDealSeconds);
		temp.remove(result);
		return result;
	}
	
	public float sum(String channel, long windowSize, long moveSize)
	{
		Map<Long, Float> temp = map.get(new ResultDataItem("sum", channel,
				windowSize, moveSize));
		float result = temp.get(needDealSeconds);
		temp.remove(result);
		return result;
	}
	
	public float max(String channel, long windowSize, long moveSize)
	{
		Map<Long, Float> temp = map.get(new ResultDataItem("max", channel,
				windowSize, moveSize));
		float result = temp.get(needDealSeconds);
		temp.remove(result);
		return result;
	}
	
	public float min(String channel, long windowSize, long moveSize)
	{
		Map<Long, Float> temp = map.get(new ResultDataItem("min", channel,
				windowSize, moveSize));
		float result = temp.get(needDealSeconds);
		temp.remove(result);
		return result;
	}
}
