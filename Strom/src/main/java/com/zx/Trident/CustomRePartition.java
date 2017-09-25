package com.zx.Trident;

import java.io.Serializable;
import java.util.List;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

public class CustomRePartition implements CustomStreamGrouping, Serializable {

	private static final long serialVersionUID = 1L;

	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		
	}

	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		return null;
	}

}
