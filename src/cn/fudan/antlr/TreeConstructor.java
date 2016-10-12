package cn.fudan.antlr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import cn.fudan.domain.*;


public class TreeConstructor {
	
	private static Map<String, CalcTreeNode> map = new HashMap<>();
	
	private static void process_base(ParseTree tree) {
		tree = tree.getChild(0);
		CalcTreeNode node = new CalcTreeNode(
				tree.getChild(0).getText(),
				tree.getChild(2).getText(),
				tree.getChild(4).getText(),
				Long.parseLong(tree.getChild(7).getText()),
				Long.parseLong(tree.getChild(9).getText()));
		map.put(tree.getChild(0).getText(), node);
	}
	
	private static void process_upper(ParseTree tree) {
		tree = tree.getChild(0);
		CalcTreeNode node = new CalcTreeNode(
				tree.getChild(0).getText());
		// 层次遍历
		Queue<ParseTree> queue = new LinkedList<ParseTree>();
		queue.offer(tree);
		while (!queue.isEmpty()) {
			ParseTree tmpTree = queue.poll();
			if (tmpTree.getChildCount() == 0)
				if (map.containsKey(tmpTree.getText())) {
					System.out.println(tmpTree.getText());
					node.addChild(map.get(tmpTree.getText()));
					map.get(tmpTree.getText()).setParent(node);
				}
			for (int i=0; i<tmpTree.getChildCount(); i++) {
				queue.offer(tmpTree.getChild(i));
			}
		}
		map.put(tree.getChild(0).getText(), node);
	}
	
	public static void main(String[] args) {
		
		String input = "out_MD471Z=avg(\"5AB001-DY\",5000,1000);\n" +

		"out_MD472Z=max(\"5AB002-DY\",5000,1000);\n" +

		"out_MD473Z=min(\"5AB003-DY\",5000,1000);\n" +

		"out_MD474Z=sum(\"5AB004-DY\",5000,1000);\n" +

		"out_MD471474_AZ=(out_MD471Z+out_MD472Z+out_MD473Z+out_MD474Z)/4;";
		
		ANTLRInputStream stream = new ANTLRInputStream(input);
		CalcLexer lexer = new CalcLexer(stream);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		CalcParser parser = new CalcParser(tokens);
		ParseTree tree = parser.goal();
		for (int i=0; i<tree.getChildCount(); i+=2) {
			String tmpString = tree.getChild(i).getText();
//			System.out.println(tmpString);
			if (tmpString.contains("=max(") ||
					tmpString.contains("=min(") ||
					tmpString.contains("=avg(") ||
					tmpString.contains("=sum(")) {
				process_base(tree.getChild(i));
			}
			else {
				process_upper(tree.getChild(i));
			}
			System.out.println(map.size());
		}
//			System.out.println(map.get(s).getChildrenNum());
		System.out.println(map.get("out_MD471474_AZ").getChildrenNum());
	}
}
