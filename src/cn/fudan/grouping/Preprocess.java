package cn.fudan.grouping;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.fudan.tools.util.NewGenerate;
import cn.fudan.topology.NewTopology;

public class Preprocess {

	private ArrayList<String> sList;
	private ArrayList<Integer> worker = new ArrayList<>(NewTopology.WORKER_NUM);
	private Map<Integer, Integer> assignment = new HashMap<>();

	public Map<Integer, Integer> getAssignment() {
		return assignment;
	}

	private int findIndexOfMin(ArrayList<Integer> list) {
		if (list == null || list.size() <= 1) {
			return 0;
		}
		int max = list.get(0);
		int ret = 0;
		for (int i = 0; i < list.size(); i++) {
			if (list.get(i) > max) {
				max = list.get(i);
				ret = i;
			}
		}
		return ret;
	}

	public Preprocess(String dir) {
		File directory = new File(dir);
		Map<Integer, Integer> costMap = null;
		if (directory.isDirectory()) {
			File[] fileList = directory.listFiles();
			costMap = new HashMap<>();
			sList = new ArrayList<>();
			int count = 0;
			for (File file : fileList) {
				try {
					StringBuilder sb = new StringBuilder();
					BufferedReader br = new BufferedReader(new FileReader(file));
					String tmpString = null;
					while ((tmpString = br.readLine()) != null) {
						sb.append(tmpString);
					}
					br.close();
					sList.add(sb.toString());
					int calcCount = NewGenerate.generate(sb.toString(), count);
					// calculate each query logic's cost
					costMap.put(count, calcCount);
					count++;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		//distribute channelId to different tasks by descending cost
		List<Map.Entry<Integer, Integer>> list = new ArrayList<Map.Entry<Integer, Integer>>(costMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<Integer, Integer>>() {
			@Override
			public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
				return (o2.getValue() - o1.getValue());
			}
		});

		for (int i = 0; i < costMap.size(); i++) {
			int k = findIndexOfMin(worker);
			assignment.put(i, k); // Assign query i to worker k
			worker.set(k, worker.get(k) + costMap.get(i));
		}
	}

}
