package cn.fudan.tools.util;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import bsh.EvalError;
import bsh.Interpreter;
import cn.fudan.domain.GetQueryMap;
import cn.fudan.tools.util.StringHelp;

public class NewGenerate
{
	public static Set<String> result;
	public static GetQueryMap getQueryMap;
	
	public static void main(String[] args) throws Exception
	{
		/*
		generate("out_MD471Z=avg(\"5AB001-DY\",5000,1000);\n" +

		"out_MD472Z=max(\"5AB002-DY\",5000,1000);\n" +

		"out_MD473Z=min(\"5AB003-DY\",5000,1000);\n" +

		"out_MD474Z=sum(\"5AB004-DY\",5000,1000);\n" +

		"double[] max={out_MD471Z,out_MD472Z,out_MD473Z,out_MD474Z};\n" +

		"java.util.Arrays.sort(max);\n" +

		"out_MD471474_MZ=max[max.length-1];\n" +

		"out_MD471474_AZ=(out_MD471Z+out_MD472Z+out_MD473Z+out_MD474Z)/4;");
		*/
	}
	
	public static int generate(String expression, int num)
	{
		if (result == null || result.isEmpty())
			result = new HashSet<>();
		if (getQueryMap == null)
			getQueryMap = new GetQueryMap();
		String[] ss = expression.split(";");
		for (int i = 0; i < ss.length; i++)
		{
			String[] s1 = ss[i].split("=");
			if (s1.length == 2 && s1[0].trim().contains("out"))
			{
				result.add(s1[0].trim());
			}
		}
		
		StringBuilder sb = new StringBuilder();
		Interpreter interpreter = new Interpreter();
		Method[] methods = getAllStatMethods();
		sb.append("import cn.fudan.tools.util.*;\n");
		sb.append("import cn.fudan.domain.*;\n");
		sb.append("private static final int num = " + num + ";\n");
		sb.append("private static int calcCount = 0;\n");
		for (int i = 0; i < methods.length; i++)
		{
			String s = "private {0} {1}(String channel,long windowSize, long moveSize)\n"
					+ "'{'\n"
					+ "    calcCount++;\n"
					+ "    NewGenerate.{1}(channel,windowSize,moveSize);\n"
					+ "    if (getQueryMap.getGroupingMap().containsKey(channel)) {\n"
					+ "    getQueryMap.getGroupingMap().get(channel).add(num);\n"
					+ "    } else {\n"
					+ "        getQueryMap.getGroupingMap().put(channel, new HashSet<Integer>(){{add(num);}});\n"
					+ "    }"
					+ "    return 0;\n" + "'}'";
			Method method = methods[i];
			s = StringHelp.format(s, new String[] {
					method.getReturnType().getName(), method.getName() });
			sb.append(s).append("\n");
		}
		sb.append(expression);
		System.out.println(sb.toString());
		try {
			interpreter.eval(sb.toString());
			return (int) interpreter.get("calcCount");
		} catch (EvalError e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}
	
	public static Method[] getAllStatMethods()
	{
		Method[] methods = NewGenerate.class.getMethods();
		Class[] paramClasses = new Class[] { String.class, Long.TYPE, Long.TYPE };
		ArrayList arrayList = new ArrayList();
		for (int i = 0; i < methods.length; i++)
		{
			Class[] parameterTypes = methods[i].getParameterTypes();

			if (paramClasses.length == parameterTypes.length)
			{
				boolean b = true;
				for (int j = 0; j < paramClasses.length; j++)
				{
					if (paramClasses[j] != parameterTypes[j])
					{
						b = false;
						break;
					}
				}

				if (b)
				{
					arrayList.add(methods[i]);
				}
			}
		}

		Method[] ret = new Method[arrayList.size()];
		arrayList.toArray(ret);
		return ret;
	}

	public static int avg(String channel, long windowSize, long moveSize)
			throws Exception
	{
		getQueryMap.AddOneQuery(channel, windowSize, moveSize, "avg");
		return 0;
	}

	public static int max(String channel, long windowSize, long moveSize)
			throws Exception
	{
		getQueryMap.AddOneQuery(channel, windowSize, moveSize, "max");
		return 0;
	}

	public static int min(String channel, long windowSize, long moveSize)
			throws Exception
	{
		getQueryMap.AddOneQuery(channel, windowSize, moveSize, "min");
		return 0;
	}
	
	public static int sum(String channel, long windowSize, long moveSize)
			throws Exception
	{
		getQueryMap.AddOneQuery(channel, windowSize, moveSize, "sum");
		return 0;
	}
}
