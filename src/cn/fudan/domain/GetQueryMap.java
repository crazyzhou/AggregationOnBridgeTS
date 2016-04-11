package cn.fudan.domain;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.inet.tds.f;

public class GetQueryMap {
	private static Map<String, Set<ChannelWindow>> windowMap;
	private static Map<ChannelWindow, Set<String>> functionMap;
	private static long firstTimestamp;
	
	public GetQueryMap() {
		windowMap = new HashMap<>();
		functionMap = new HashMap<>();
		firstTimestamp = 0;
	}
	
	public static long getFirstTimestamp() {
		return firstTimestamp;
	}
	
	public static void setFirstTimestamp(long firstTimestamp) {
		GetQueryMap.firstTimestamp = firstTimestamp;
	}
	
	public static Map<String, Set<ChannelWindow>> getWindowMap() {
		return windowMap;
	}
	
	public static Map<ChannelWindow, Set<String>> getFunctionMap() {
		return functionMap;
	}
	
	public static void AddOneQuery(String channelCode, long windowSize, long moveSize, String functionName)
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
