package cn.fudan.domain;

public class Edge implements Comparable<Edge>{
	private long currentTime;
	private PairedWindow pairedWindows;
	private boolean isLast;
	
	public Edge(long currentTime, PairedWindow pairedWindows, boolean isLast) {
		this.currentTime = currentTime;
		this.pairedWindows = pairedWindows;
		this.isLast = isLast;
	}

	public long getCurrentTime() {
		return currentTime;
	}

	public void setCurrentTime(long currentTime) {
		this.currentTime = currentTime;
	}

	public PairedWindow getPairedWindows() {
		return pairedWindows;
	}

	public void setPairedWindows(PairedWindow pairedWindows) {
		this.pairedWindows = pairedWindows;
	}

	public boolean isLast() {
		return isLast;
	}

	public void setLast(boolean isLast) {
		this.isLast = isLast;
	}
	
	@Override
	public int compareTo(Edge other) {
	    return Long.compare(this.currentTime, other.currentTime);
	}
}
