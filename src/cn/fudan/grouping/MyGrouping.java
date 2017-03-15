package cn.fudan.grouping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import cn.fudan.domain.GetQueryMap;
import cn.fudan.tools.util.NewGenerate;
public class MyGrouping implements CustomStreamGrouping {

	GetQueryMap getQueryMap;
	Preprocess preprocess;
	Map<Integer, Integer> assignment;
	Map<String, Set<Integer>> groupingMap;
	private List<Integer> targetTasks;
	String channelCode;
	
	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> res = new ArrayList<>();
		for (int num : groupingMap.get(channelCode)) {
			res.add(targetTasks.get(assignment.get(num)));
		}
		return res;
	}

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		preprocess = new Preprocess("/directory/to/queryfiles");
		assignment = preprocess.getAssignment();
		groupingMap = NewGenerate.getQueryMap.getGroupingMap();
		
		//TODO : how to get tuple
		
		this.targetTasks = targetTasks;
	}
	
}
