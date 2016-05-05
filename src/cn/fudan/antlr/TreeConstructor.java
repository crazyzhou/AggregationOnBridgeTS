package cn.fudan.antlr;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

public class TreeConstructor {
	public static void main(String[] args) {
		
		String input = "out_MD471Z=avg(\"5AB001-DY\",5000,1000);\n" +

		"out_MD472Z=max(\"5AB002-DY\",5000,1000);\n" +

		"out_MD473Z=min(\"5AB003-DY\",5000,1000);\n" +

		"out_MD474Z=sum(\"5AB004-DY\",5000,1000);\n" +

		"double[] max={out_MD471Z,out_MD472Z,out_MD473Z,out_MD474Z};\n" +

		"java.util.Arrays.sort(max);\n" +

		"out_MD471474_MZ=max[max.length-1];\n" +

		"out_MD471474_AZ=(out_MD471Z+out_MD472Z+out_MD473Z+out_MD474Z)/4;";
		
		ANTLRInputStream stream = new ANTLRInputStream(input);
		TokenSource lexer = new CalcLexer(stream);
		TokenStream tokens = new CommonTokenStream(lexer);
		CalcParser parser = new CalcParser(tokens);
		parser.setBuildParseTree(true);
		ParseTree tree = parser.goal();
		// TODO: Tree get.
		// plan 1: use the original ParseTree to do the next step
		// plan 2: use walker & walk() to construct a simpler AST.
	}
}
