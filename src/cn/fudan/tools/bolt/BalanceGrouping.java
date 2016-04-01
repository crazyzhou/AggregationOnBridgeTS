/*package cn.fudan.tools.bolt;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;


public class BalanceGrouping implements CustomStreamGrouping, Serializable{
	
    private static final long serialVersionUID = 2L;
	
    Map<String, Integer> frequencyMap = new HashMap<>();
	Map<Integer, Long> loadMap = new TreeMap<>();	
	 
    private List<Integer> tasks;
    
    private void constructFrequencyMap() {
		
	}
 
    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
        List<Integer> targetTasks)
    {
    	this.tasks = targetTasks;
    	constructFrequencyMap();
    	
    	for (String componentId : context.getComponentIds())
    	{
    		loadMap.put(componentId, new HashMap<Integer, Long>());
    	}
        for (Integer taskId : tasks)
        {
        	String componentId = context.getComponentId(taskId);
        	Map<Integer, Long> tempMap = loadMap.get(componentId);
        	tempMap.put(taskId, 0L);
        	loadMap.put(componentId, tempMap);
        }
    }
    
    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
    	values.get("channelCode")
    	
    }

}
*/