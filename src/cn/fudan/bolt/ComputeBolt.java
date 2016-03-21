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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import bsh.Interpreter;
import cn.fudan.domain.AvgDataItem;
import cn.fudan.domain.ChannelWindow;

public class ComputeBolt implements IRichBolt
{
	Interpreter interpreter; // 构造一个解释器
	OutputCollector _collector;
	Map<String, Map<Long, Map<Long, Map<Long, AvgDataItem>>>> map;// ChannelCode,窗口大小,移动窗口大小,开始秒,数据序列(当前总和，当前个数)
	// zzmadded finish
	Map<ChannelWindow, Set<String>> windowFunction;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector)
	{
		this.interpreter = new Interpreter();
		this._collector = collector;
		// 开始初始化map的地方
		map = new HashMap<String, Map<Long, Map<Long, Map<Long, AvgDataItem>>>>();
		// zzmadded,记得调用 finish
		windowFunction = new HashMap<ChannelWindow, Set<String>>();
	}

	// zzmadded finished
	public void put(String key, long windowSize, long moveSize, String function)
	{
		ChannelWindow cw = new ChannelWindow(key, windowSize, moveSize);
		if (windowFunction.containsKey(cw))
		{
			windowFunction.get(cw).add(function);
		} else
		{
			Set<String> set = new HashSet<String>();
			set.add(function);
			windowFunction.put(cw, set);
		}
	}

	public void put(String key, long windowSize, long moveSize)
	{
		if (map.containsKey(key))
		{
			Map<Long, Map<Long, Map<Long, AvgDataItem>>> windowSizeMap = map
					.get(key);
			if (windowSizeMap.containsKey(windowSize))
			{
				Map<Long, Map<Long, AvgDataItem>> moveSizeMap = windowSizeMap
						.get(windowSize);
				if (moveSizeMap.containsKey(moveSize))
				{
					return;
				} else
				{
					Map<Long, AvgDataItem> mapTemp = new TreeMap<Long, AvgDataItem>();
					moveSizeMap.put(moveSize, mapTemp);
				}
			} else
			{
				Map<Long, AvgDataItem> mapTemp = new TreeMap<Long, AvgDataItem>();
				Map<Long, Map<Long, AvgDataItem>> moveSizeMap = new HashMap<Long, Map<Long, AvgDataItem>>();
				moveSizeMap.put(moveSize, mapTemp);
				windowSizeMap.put(windowSize, moveSizeMap);
			}
		} else
		{
			Map<Long, AvgDataItem> mapTemp = new TreeMap<Long, AvgDataItem>();
			Map<Long, Map<Long, AvgDataItem>> moveSizeMap = new HashMap<Long, Map<Long, AvgDataItem>>();
			moveSizeMap.put(moveSize, mapTemp);
			Map<Long, Map<Long, Map<Long, AvgDataItem>>> windowSizeMap = new HashMap<Long, Map<Long, Map<Long, AvgDataItem>>>();
			windowSizeMap.put(windowSize, moveSizeMap);
			map.put(key, windowSizeMap);
		}
	}

	@Override
	public void execute(Tuple tuple)
	{
		String channelCode = tuple.getStringByField("channelCode");
		long timeStamp = tuple.getLongByField("timeStamp");
		float value = tuple.getFloatByField("value");
		Map<Long, Map<Long, Map<Long, AvgDataItem>>> channelMap = map
				.get(channelCode);
		if (channelMap != null)
		{
			for (Map.Entry<Long, Map<Long, Map<Long, AvgDataItem>>> entry : channelMap
					.entrySet())// 遍历获取每一个窗口大小
			{
				long size = entry.getKey();
				Map<Long, Map<Long, AvgDataItem>> channelSizeMap = entry
						.getValue();
				for (Map.Entry<Long, Map<Long, AvgDataItem>> entry1 : channelSizeMap
						.entrySet())// 准备获取每一个移动步大小
				{
					long moveSize = entry1.getKey();
					// long currentSeconds = timeStamp / 1000 / 3600 * 3600
					// + timeStamp / 1000 % 3600;// 计算这个元组是第几秒钟的
					long currentSeconds = timeStamp / 1000;
					Map<Long, AvgDataItem> channelMoveSizeMap = entry1
							.getValue();

					Long startSeconds = null;
					AvgDataItem avgDataItemtemp = channelMoveSizeMap
							.get(currentSeconds);
					if (avgDataItemtemp != null)
					{
						avgDataItemtemp.setNum(avgDataItemtemp.getNum() + 1);
						avgDataItemtemp
								.setSum(avgDataItemtemp.getSum() + value);
						// zzmadded
						avgDataItemtemp
								.setMax(avgDataItemtemp.getMax() > value ? avgDataItemtemp
										.getMax() : value);
						avgDataItemtemp
								.setMin(avgDataItemtemp.getMin() < value ? avgDataItemtemp
										.getMin() : value);
						channelMoveSizeMap.put(currentSeconds, avgDataItemtemp);
					} else
					{
						// zzmchanged
						avgDataItemtemp = new AvgDataItem(value, 1, value,
								value);
						for (Map.Entry<Long, AvgDataItem> entry2 : channelMoveSizeMap
								.entrySet())
						{
							startSeconds = entry2.getKey();// 获取下当前窗口的第一个值
							break;
						}
						if (null == startSeconds)
						{
							channelMoveSizeMap.put(currentSeconds,
									avgDataItemtemp);
							continue;
						}
						if (currentSeconds - startSeconds / (moveSize / 1000)
								* (moveSize / 1000) >= size / 1000)
						{
							float allnum = 0;
							float allsum = 0;
							// zzmadded
							float max = Float.MIN_VALUE;
							float min = Float.MAX_VALUE;
							for (Map.Entry<Long, AvgDataItem> entry2 : channelMoveSizeMap
									.entrySet())
							{
								AvgDataItem temp = entry2.getValue();
								allnum += temp.getNum();
								allsum += temp.getSum();
								// zzmadded
								min = min < temp.getMin() ? min : temp.getMin();
								max = max > temp.getMax() ? max : temp.getMax();
							}
							channelMoveSizeMap.put(currentSeconds,
									avgDataItemtemp);
							for (long i = startSeconds; i < startSeconds
									/ (moveSize / 1000) * (moveSize / 1000)
									+ moveSize / 1000; i++)
							{
								channelMoveSizeMap.remove(i);
							}
							// zzmadded
							ChannelWindow cw = new ChannelWindow(channelCode,
									size, moveSize);
							if (windowFunction.get(cw).contains("avg"))
							{
								_collector.emit(new Values("avg", channelCode,
										size, moveSize, startSeconds
												/ (moveSize / 1000)
												* (moveSize / 1000), allsum
												/ allnum));
							}
							if (windowFunction.get(cw).contains("max"))
							{
								_collector.emit(new Values("max", channelCode,
										size, moveSize, startSeconds
												/ (moveSize / 1000)
												* (moveSize / 1000), max));
							}
							if (windowFunction.get(cw).contains("min"))
							{
								_collector.emit(new Values("min", channelCode,
										size, moveSize, startSeconds
												/ (moveSize / 1000)
												* (moveSize / 1000), min));
							}
							if (windowFunction.get(cw).contains("sum"))
							{
								_collector.emit(new Values("sum", channelCode,
										size, moveSize, startSeconds
												/ (moveSize / 1000)
												* (moveSize / 1000), allsum));
							}
						}
					}
				}
			}

		}
	}

	@Override
	public void cleanup()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("functionName", "channelCode",
				"windowSize", "moveSize", "startTime", "average"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration()
	{
		// TODO Auto-generated method stub
		return null;
	}
}
