package cn.fudan.tools.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cn.fudan.domain.ChannelWindow;
import cn.fudan.tools.util.BoltFunctionItem;

public class AvgFunction
{
	public static void generate() throws Exception
	{
		String permanentCode = "import java.util.HashMap;"
				+ "\n"
				+ "import java.util.Map;"
				+ "\n"
				+ "import java.util.HashSet;"
				+ "\n"
				+ "import java.util.Set;"
				+ "\n"
				+ "import java.util.TreeMap;\n"
				+ "import backtype.storm.task.OutputCollector;"
				+ "\n"
				+ "import backtype.storm.task.TopologyContext;"
				+ "\n"
				+ "import backtype.storm.topology.IRichBolt;"
				+ "\n"
				+ "import backtype.storm.topology.OutputFieldsDeclarer;"
				+ "\n"
				+ "import backtype.storm.tuple.Fields;"
				+ "\n"
				+ "import backtype.storm.tuple.Tuple;"
				+ "\n"
				+ "import backtype.storm.tuple.Values;"
				+ "\n"
				+ "import bsh.Interpreter;"
				+ "\n"
				+ "import cn.fudan.domain.AvgDataItem;"
				+ "\n"
				+ "import cn.fudan.domain.ChannelWindow;"
				+ "\n"
				+ "import cn.fudan.tools.util.BoltFunctionItem;public class AvgBolt implements IRichBolt"
				+ "\n"

				+ "{"
				+ "\n"
				+ "Interpreter interpreter;"
				+ "\n"
				+ // 构造一个解释器
				"OutputCollector _collector;"
				+ "\n"
				+ "Map<String, Map<Long, Map<Long, Map<Long, AvgDataItem>>>> map;"
				+ "\n"
				+ "Map<ChannelWindow, Set<String>> windowFunction;"
				+ "\n"
				+ // ChannelCode,窗口大小,移动窗口大小,开始秒,数据序列(当前总和，当前个数)

				"@Override"
				+ "\n"
				+ "public void execute(Tuple tuple)"
				+ "\n"
				+ "{"
				+ "\n"
				+ "String channelCode = tuple.getStringByField(\"channelCode\");"
				+ "\n"
				+ "long timeStamp = tuple.getLongByField(\"timeStamp\");"
				+ "\n"
				+ "float value = tuple.getFloatByField(\"value\");"
				+ "\n"
				+ "Map<Long, Map<Long, Map<Long, AvgDataItem>>> channelMap = map.get(channelCode);"
				+ "\n"
				+ "if (channelMap != null)"
				+ "\n"
				+ "{"
				+ "\n"
				+ "for (Map.Entry<Long, Map<Long, Map<Long, AvgDataItem>>> entry : channelMap.entrySet())"
				+ "\n"
				+ "{"
				+ "\n"
				+ "long size = entry.getKey();"
				+ "\n"
				+ "Map<Long, Map<Long, AvgDataItem>> channelSizeMap = entry.getValue();"
				+ "\n"
				+ "for (Map.Entry<Long, Map<Long, AvgDataItem>> entry1 : channelSizeMap.entrySet())"
				+ "\n"
				+ "{"
				+ "\n"
				+ "long moveSize = entry1.getKey();"
				+ "\n"
				+ "long currentSeconds = timeStamp / 1000;"
				+ "\n"
				+ "Map<Long, AvgDataItem> channelMoveSizeMap = entry1.getValue();"
				+ "\n"
				+

				"Long startSeconds = null;"
				+ "\n"
				+ "AvgDataItem avgDataItemtemp = channelMoveSizeMap.get(currentSeconds);"
				+ "\n"
				+ "if (avgDataItemtemp != null)"
				+ "\n"
				+ "{"
				+ "\n"
				+ "avgDataItemtemp.setNum(avgDataItemtemp.getNum() + 1);"
				+ "\n"
				+ "avgDataItemtemp.setSum(avgDataItemtemp.getSum() + value);"
				+ "\n"
				+ "avgDataItemtemp.setMax(avgDataItemtemp.getMax() > value ? avgDataItemtemp.getMax() : value);"
				+ "\n"
				+ "avgDataItemtemp.setMin(avgDataItemtemp.getMin() < value ? avgDataItemtemp.getMin() : value);"
				+ "\n"
				+ "channelMoveSizeMap.put(currentSeconds, avgDataItemtemp);"
				+ "\n"
				+ "} else"
				+ "\n"
				+ "{"
				+ "\n"
				+ "avgDataItemtemp = new AvgDataItem(value,1,value,value);"
				+ "\n"
				+ "channelMoveSizeMap.put(currentSeconds,avgDataItemtemp);"
				+ "\n"
				+ "for (Map.Entry<Long, AvgDataItem> entry2 : channelMoveSizeMap.entrySet())"
				+ "\n"
				+ "{"
				+ "\n"
				+ "startSeconds = entry2.getKey();"
				+ "\n"
				+ "break;"
				+ "\n"
				+ "}"

				+ "if(null == startSeconds)"
				+ "\n"
				+ "{"
				+ "\n"
				+ "continue;}"

				+ "\n"
				+ "if (currentSeconds - startSeconds / (moveSize / 1000)* (moveSize / 1000) >= size / 1000)"
				+ "\n"
				+ "{"
				+ "\n"
				+ "float allnum = 0;"
				+ "\n"
				+ "float allsum = 0;"
				+ "\n"
				+ "float max = Float.MIN_VALUE;"
				+ "\n"
				+ "float min = Float.MAX_VALUE;"
				+ "\n"
				+ "for (Map.Entry<Long, AvgDataItem> entry2 : channelMoveSizeMap.entrySet())"
				+ "\n"
				+ "{"
				+ "\n"
				+ "AvgDataItem temp = entry2.getValue();"
				+ "\n"
				+ "allnum += temp.getNum();"
				+ "\n"
				+ "allsum += temp.getSum();"
				+ "\n"
				+ "min = min < temp.getMin() ? min : temp.getMin();"
				+ "\n"
				+ "max = max > temp.getMax() ? max : temp.getMax();"
				+ "\n"
				+ "}"
				+ "\n"
				+ "for (long i = startSeconds; i < startSeconds/ (moveSize/1000) * (moveSize/1000) + moveSize/1000; i++)"
				+ "\n"
				+ "{"
				+ "\n"
				+ "channelMoveSizeMap.remove(i);"
				+ "\n"
				+ "}"
				+ "\n"
				+ "ChannelWindow cw = new ChannelWindow(channelCode,size, moveSize);"
				+ "\n"
				+ "if (windowFunction.get(cw).contains(\"avg\")){_collector.emit(new Values(\"avg\", channelCode,size, moveSize, startSeconds/(moveSize/1000)*(moveSize/1000), allsum/ allnum));}"
				+ "\n"
				+ "if (windowFunction.get(cw).contains(\"max\")){_collector.emit(new Values(\"max\", channelCode,size, moveSize, startSeconds/(moveSize/1000)*(moveSize/1000), allsum/ allnum));}"
				+ "\n"
				+ "if (windowFunction.get(cw).contains(\"min\")){_collector.emit(new Values(\"min\", channelCode,size, moveSize, startSeconds/(moveSize/1000)*(moveSize/1000), allsum/ allnum));}"
				+ "\n"
				+ "if (windowFunction.get(cw).contains(\"sum\")){_collector.emit(new Values(\"sum\", channelCode,size, moveSize, startSeconds/(moveSize/1000)*(moveSize/1000), allsum/ allnum));}"
				+ "\n"
				+ "}}}}}}"
				+ "\n"
				+

				"@Override"
				+ "\n"
				+ "public void cleanup(){}"
				+ "\n"
				+

				"@Override"
				+ "\n"
				+ "public void declareOutputFields(OutputFieldsDeclarer declarer)"
				+ "\n"
				+ "{"
				+ "\n"
				+ "declarer.declare(new Fields(\"functionName\", \"channelCode\",\"windowSize\", \"moveSize\", \"startTime\", \"average\"));"
				+ "\n"
				+ "}"
				+ "\n"
				+ "public void put(String key, long windowSize, long moveSize, String function){"
				+ "\n"
				+ "ChannelWindow cw = new ChannelWindow(key, windowSize, moveSize);"
				+ "\n"
				+ "if (windowFunction.containsKey(cw)){windowFunction.get(cw).add(function);} "
				+ "\n"
				+ "else{Set<String> set = new HashSet<String>();set.add(function);windowFunction.put(cw, set);}}"
				+ "\n"
				+

				"public void put(String key,long windowSize,long moveSize){"
				+ "\n"
				+ "if (map.containsKey(key)){"
				+ "\n"
				+ "Map<Long, Map<Long, Map<Long, AvgDataItem>>> windowSizeMap = map.get(key);"
				+ "\n"
				+ "if (windowSizeMap.containsKey(windowSize)){"
				+ "\n"
				+ "Map<Long, Map<Long, AvgDataItem>> moveSizeMap = windowSizeMap.get(windowSize);"
				+ "\n"
				+ "if (moveSizeMap.containsKey(moveSize)){"
				+ "\n"
				+ "return;"
				+ "\n"
				+ "} else"
				+ "\n"
				+ "{Map<Long, AvgDataItem> mapTemp = new TreeMap<Long, AvgDataItem>();moveSizeMap.put(moveSize, mapTemp);}"
				+ "\n"
				+ "} else{"
				+ "\n"
				+ "Map<Long, AvgDataItem> mapTemp = new TreeMap<Long, AvgDataItem>();"
				+ "\n"
				+ "Map<Long, Map<Long, AvgDataItem>> moveSizeMap = new HashMap<Long, Map<Long, AvgDataItem>>();"
				+ "\n"
				+ "moveSizeMap.put(moveSize, mapTemp);"
				+ "\n"
				+ "windowSizeMap.put(windowSize, moveSizeMap);}} else{"
				+ "\n"
				+ "Map<Long, AvgDataItem> mapTemp = new TreeMap<Long, AvgDataItem>();"
				+ "\n"
				+ "Map<Long, Map<Long, AvgDataItem>> moveSizeMap = new HashMap<Long, Map<Long, AvgDataItem>>();"
				+ "\n"
				+ "moveSizeMap.put(moveSize, mapTemp);"
				+ "\n"
				+ "Map<Long, Map<Long, Map<Long, AvgDataItem>>> windowSizeMap = new HashMap<Long, Map<Long, Map<Long, AvgDataItem>>>();"
				+ "\n" + "windowSizeMap.put(windowSize, moveSizeMap);" + "\n"
				+ "map.put(key, windowSizeMap);}}" + "\n" +

				"@Override" + "\n"
				+ "public Map<String, Object> getComponentConfiguration()"
				+ "\n" + "{return null;}";
		String prepareCode = "@Override"
				+ "\n"
				+ "public void prepare(Map stormConf, TopologyContext context,OutputCollector collector)"
				+ "\n"
				+ "{this._collector = collector;"
				+ "\n"
				+
				// 开始初始化map的地方
				"map = new HashMap<String, Map<Long, Map<Long, Map<Long, AvgDataItem>>>>();"
				+ "\n"
				+ "windowFunction = new HashMap<ChannelWindow, Set<String>>();"
				+ "\n";
		if (BoltFunction.avgMap != null)
		{
			for (Map.Entry<String, Set<BoltFunctionItem>> entry : BoltFunction.avgMap
					.entrySet())
			{
				String key = entry.getKey();
				for (BoltFunctionItem item : entry.getValue())
				{
					prepareCode = prepareCode + "put(" + "\"" + key + "\","
							+ item.getWindowSize() + "," + item.getMoveSize()
							+ ");";
					// 为windowFunctionMap构造put语句
					prepareCode = prepareCode + "put(" + "\"" + key + "\","
							+ item.getWindowSize() + "," + item.getMoveSize()
							+ "," + "\"avg\"" + ");";
				}
			}
		}
		if (BoltFunction.sumMap != null)
		{
			for (Map.Entry<String, Set<BoltFunctionItem>> entry : BoltFunction.sumMap
					.entrySet())
			{
				String key = entry.getKey();
				for (BoltFunctionItem item : entry.getValue())
				{
					prepareCode = prepareCode + "put(" + "\"" + key + "\","
							+ item.getWindowSize() + "," + item.getMoveSize()
							+ ");";
					// 为windowFunctionMap构造put语句
					prepareCode = prepareCode + "put(" + "\"" + key + "\","
							+ item.getWindowSize() + "," + item.getMoveSize()
							+ "," + "\"sum\"" + ");";
				}
			}
		}
		if (BoltFunction.minMap != null)
		{
			for (Map.Entry<String, Set<BoltFunctionItem>> entry : BoltFunction.minMap
					.entrySet())
			{
				String key = entry.getKey();
				for (BoltFunctionItem item : entry.getValue())
				{
					prepareCode = prepareCode + "put(" + "\"" + key + "\","
							+ item.getWindowSize() + "," + item.getMoveSize()
							+ ");";
					// 为windowFunctionMap构造put语句
					prepareCode = prepareCode + "put(" + "\"" + key + "\","
							+ item.getWindowSize() + "," + item.getMoveSize()
							+ "," + "\"min\"" + ");";
				}
			}
		}
		if (BoltFunction.maxMap != null)
		{
			for (Map.Entry<String, Set<BoltFunctionItem>> entry : BoltFunction.maxMap
					.entrySet())
			{
				String key = entry.getKey();
				for (BoltFunctionItem item : entry.getValue())
				{
					prepareCode = prepareCode + "put(" + "\"" + key + "\","
							+ item.getWindowSize() + "," + item.getMoveSize()
							+ ");";
					// 为windowFunctionMap构造put语句
					prepareCode = prepareCode + "put(" + "\"" + key + "\","
							+ item.getWindowSize() + "," + item.getMoveSize()
							+ "," + "\"max\"" + ");";
				}
			}
		}
		prepareCode = prepareCode + "}";
		BufferedWriter bw = new BufferedWriter(new FileWriter("AvgBolt.java"));
		StringBuilder ss = new StringBuilder();
		bw.append(permanentCode + prepareCode);
		bw.append("}");
		bw.close();
	}
}
