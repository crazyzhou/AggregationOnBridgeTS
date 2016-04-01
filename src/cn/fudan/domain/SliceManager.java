package cn.fudan.domain;

import java.util.List;
import java.util.PriorityQueue;

public class SliceManager {
	private static PriorityQueue<Edge> H;
	
	public SliceManager(List<PairedWindow> windowList, long startTime) {
		H = new PriorityQueue<>();
		for (PairedWindow pairedWindow : windowList) {
			addEdges(startTime, pairedWindow);
		}
	}
	
	private void addEdges(long startTime, PairedWindow pairedWindows) {
		H.add(new Edge(startTime + pairedWindows.getLeftSize(), pairedWindows, false));
		H.add(new Edge(startTime + pairedWindows.getLeftSize() + pairedWindows.getRightSize(), pairedWindows, true));
	}
	
	public long advanceWindowGetNextEdge() {
		Edge curEdge;
		long curTime = H.peek().getCurrentTime();
		while (curTime == H.peek().getCurrentTime()) {
			curEdge = H.remove();
			
			//not sure
			for (Edge edge : H) {
				if (edge.getPairedWindows().equals(curEdge.getPairedWindows())) {
					H.remove(edge);
				}
			}
			
			if (curEdge.isLast()) {
				addEdges(curEdge.getCurrentTime(), curEdge.getPairedWindows());
			}
		}
		return curTime;
	}
	
}
