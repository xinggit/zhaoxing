package com.zx.Storm2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

public class CustomGroup implements CustomStreamGrouping, Serializable {

	private static final long serialVersionUID = 1L;
	private static Integer MAX_TASK = Integer.MIN_VALUE;

	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {

		for (Integer i : targetTasks) {
			MAX_TASK = MAX_TASK > i ? MAX_TASK : i;
			System.out.println("=========>taskid = " + i);
		}
		System.out.println("=========>MAX_TASK = " + MAX_TASK);

	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		return Arrays.asList(MAX_TASK);
	}

}
