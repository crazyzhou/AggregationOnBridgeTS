package cn.fudan.grouping;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import cn.fudan.domain.GetQueryMap;
import cn.fudan.tools.util.NewGenerate;
public class MyGrouping implements CustomStreamGrouping {

	GetQueryMap getQueryMap;
	Preprocess preprocess;
	ArrayList<Set<Integer>> assignment;
	private List<Integer> targetTasks;
	
	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		// TODO Auto-generated method stub
		
		return null;
	}

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		// TODO Auto-generated method stub
		preprocess = new Preprocess("/directory/to/queryfiles");
		assignment = preprocess.getAssignment();
		NewGenerate.getQueryMap.getGroupingMap();
		
		//
		
		this.targetTasks = targetTasks;
	}
	
}
