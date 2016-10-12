package cn.fudan.domain;

import java.util.ArrayList;

public class CalcTreeNode {
	String varName;
	String functionName;
	String channelCode;
	long windowSize;
	long moveSize;
	ArrayList<CalcTreeNode> children;
	CalcTreeNode parent;
	
	public CalcTreeNode(String varName) {
		this.varName = varName;
		this.children = new ArrayList<CalcTreeNode>();
	}
	
	public CalcTreeNode(String varName, String functionName, String channelCode,
			long windowSize, long moveSize)
	{
		this.varName = varName;
		this.functionName = functionName;
		this.channelCode = channelCode;
		this.windowSize = windowSize;
		this.moveSize = moveSize;
		this.children = new ArrayList<CalcTreeNode>();
	}
	
	public String getFunctionName() {
		return functionName;
	}

	public void setFunctionName(String functionName) {
		this.functionName = functionName;
	}

	public String getChannelCode() {
		return channelCode;
	}

	public void setChannelCode(String channelCode) {
		this.channelCode = channelCode;
	}

	public long getWindowSize() {
		return windowSize;
	}

	public void setWindowSize(long windowSize) {
		this.windowSize = windowSize;
	}

	public long getMoveSize() {
		return moveSize;
	}

	public CalcTreeNode getChildren(int i) {
		return children.get(i);
	}

	public void setMoveSize(long moveSize) {
		this.moveSize = moveSize;
	}

	public String getVarName() {
		return varName;
	}

	public void setVarName(String varName) {
		this.varName = varName;
	}

	public CalcTreeNode getParent() {
		return parent;
	}

	public void setParent(CalcTreeNode parent) {
		this.parent = parent;
	}
	
	public int getChildrenNum() {
		return this.children.size();
	}
	
	public void addChild(CalcTreeNode child) {
		this.children.add(child);
	}
}
