package cn.fudan.domain;

import java.util.List;
import java.util.PriorityQueue;

public class SliceManager {
	private PriorityQueue<Edge> H;
	
	public SliceManager(List<PairedWindow> windowList, long startTime) {
		H = new PriorityQueue<>();
		for (PairedWindow pairedWindow : windowList) {
			addEdges(startTime, pairedWindow);
		}
	}
	
	private void addEdges(long startTime, PairedWindow pairedWindows) {
		if (pairedWindows.getLeftSize() != 0) 
			H.add(new Edge(startTime + pairedWindows.getLeftSize(), pairedWindows, false));
		H.add(new Edge(startTime + pairedWindows.getLeftSize() + pairedWindows.getRightSize(), pairedWindows, true));
	}
	
	public long advanceWindowGetNextEdge(long timeStamp) {
		Edge curEdge;
		while (timeStamp >= H.peek().getCurrentTime()) {
			curEdge = H.remove();
			if (curEdge.isLast()) {
				addEdges(curEdge.getCurrentTime(), curEdge.getPairedWindows());
			}
		}
		return H.peek().getCurrentTime();
	}
	
}
