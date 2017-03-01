package cn.fudan.grouping;

import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class MyGrouping implements CustomStreamGrouping{

	@Override
	public List<Integer> chooseTasks(int arg0, List<Object> arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(WorkerTopologyContext arg0, GlobalStreamId arg1, List<Integer> arg2) {
		// TODO Auto-generated method stub
		
	}
	
}
