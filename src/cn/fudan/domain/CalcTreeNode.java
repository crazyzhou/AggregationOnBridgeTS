package cn.fudan.domain;

import java.util.ArrayList;

public class CalcTreeNode {
	String varName;
	ResultDataItem value;
	ArrayList<CalcTreeNode> children;
	CalcTreeNode parent;
	
	public CalcTreeNode(String varName, String functionName, String channelCode,
			long windowSize, long moveSize)
	{
		this.varName = varName;
		this.value = new ResultDataItem(functionName, channelCode, windowSize, moveSize);
		this.children = new ArrayList<CalcTreeNode>();
	}
	
	public CalcTreeNode(String varName) {
		this.varName = varName;
		this.children = new ArrayList<CalcTreeNode>();
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
