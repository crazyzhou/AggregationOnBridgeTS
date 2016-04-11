package cn.fudan.tools.util;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import bsh.Interpreter;
import cn.fudan.domain.GetQueryMap;
import cn.fudan.tools.util.StringHelp;

public class NewGenerate
{
	public static void main(String[] args) throws Exception
	{
		NewGenerate.generate("out_MD471Z=avg(\"5AB001-DY\",5000,1000);\n" +

		"out_MD472Z=max(\"5AB002-DY\",5000,1000);\n" +

		"out_MD473Z=min(\"5AB003-DY\",5000,1000);\n" +

		"out_MD474Z=sum(\"5AB004-DY\",5000,1000);\n" +

		"double[] max={out_MD471Z,out_MD472Z,out_MD473Z,out_MD474Z};\n" +

		"java.util.Arrays.sort(max);\n" +

		"out_MD471474_MZ=max[max.length-1];\n" +

		"out_MD471474_AZ=(out_MD471Z+out_MD472Z+out_MD473Z+out_MD474Z)/4;");
	}
	
	public static void generate(String expression) throws Exception
	{
		StringBuilder sb = new StringBuilder();
		Interpreter interpreter = new Interpreter();
		Method[] methods = getAllStatMethods();
		sb.append("import cn.fudan.tools.util.*;\n");
		sb.append("import cn.fudan.domain.*;\n");
		for (int i = 0; i < methods.length; i++)
		{
			String s = "private {0} {1}(String channel,long windowSize, long moveSize)\n"
					+ "'{'\n"
					+ "    NewGenerate.{1}(channel,windowSize,moveSize);\n"
					+ "    return 0;\n" + "'}'";
			Method method = methods[i];
			s = StringHelp.format(s, new String[] {
					method.getReturnType().getName(), method.getName() });
			sb.append(s).append("\n");
		}
		sb.append(expression);
		System.out.println(sb.toString());
		interpreter.eval(sb.toString());
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
		GetQueryMap.AddOneQuery(channel, windowSize, moveSize, "avg");
		return 0;
	}

	public static int max(String channel, long windowSize, long moveSize)
			throws Exception
	{
		GetQueryMap.AddOneQuery(channel, windowSize, moveSize, "max");
		return 0;
	}

	public static int min(String channel, long windowSize, long moveSize)
			throws Exception
	{
		GetQueryMap.AddOneQuery(channel, windowSize, moveSize, "min");
		return 0;
	}
	
	public static int sum(String channel, long windowSize, long moveSize)
			throws Exception
	{
		GetQueryMap.AddOneQuery(channel, windowSize, moveSize, "sum");
		return 0;
	}
}