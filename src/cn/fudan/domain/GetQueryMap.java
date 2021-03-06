package cn.fudan.domain;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GetQueryMap {
	private static Map<String, Set<ChannelWindow>> windowMap;
	private static Map<ChannelWindow, Set<String>> functionMap;
	private static Map<String, Long> firstTimestampMap;
	private static Map<String, Set<Integer>> groupingMap;

	public GetQueryMap() {
		windowMap = new HashMap<>();
		functionMap = new HashMap<>();
		firstTimestampMap = new HashMap<>();
		groupingMap = new HashMap<>();
	}

	public Map<String, Set<ChannelWindow>> getWindowMap() {
		return windowMap;
	}
	
	public Map<ChannelWindow, Set<String>> getFunctionMap() {
		return functionMap;
	}
	
	public Map<String, Long> getFirstTimestampMap() {
		return firstTimestampMap;
	}

	public Map<String, Set<Integer>> getGroupingMap() {
		return groupingMap;
	}

	public void AddOneQuery(String channelCode, long windowSize, long moveSize, String functionName)
	{
		ChannelWindow channelWindow = new ChannelWindow(channelCode, windowSize, moveSize);
		if (windowMap.containsKey(channelCode))
		{
			windowMap.get(channelCode).add(channelWindow);
		}
		else
		{
			Set<ChannelWindow> windowSet = new HashSet<>();
			windowSet.add(channelWindow);
			windowMap.put(channelCode, windowSet);
		}
		
		if (functionMap.containsKey(channelWindow))
		{
			functionMap.get(channelWindow).add(functionName);
		}
		else
		{
			Set<String> functionSet = new HashSet<>();
			functionSet.add(functionName);
			functionMap.put(channelWindow, functionSet);
		}
	}
}
