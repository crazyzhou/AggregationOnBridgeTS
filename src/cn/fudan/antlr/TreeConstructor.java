package cn.fudan.antlr;

import org.antlr.runtime.tree.TreeParser;
import org.antlr.runtime.tree.TreeVisitor;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.antlr.v4.runtime.tree.gui.TreeViewer;
import org.stringtemplate.v4.compiler.STParser.namedArg_return;

import com.inet.pool.l;

public class TreeConstructor {
	
	private static void process_base(ParseTree tree) {
		tree = tree.getChild(0);
		for (int i=0; i < tree.getChildCount(); i++) {
			System.out.println(tree.getChild(i).getText());
		}
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
			if (tmpString.contains("max(") ||
					tmpString.contains("min(") ||
					tmpString.contains("avg(") ||
					tmpString.contains("sum(")) {
				process_base(tree.getChild(i));
			}
		}
	}
}
