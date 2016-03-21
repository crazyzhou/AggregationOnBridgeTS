package cn.fudan.tools.bolt;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import bsh.Interpreter;
import cn.fudan.tools.util.StringHelp;

public class FilterFunction
{
	public static Map<String, Set<String>> map = new HashMap<String, Set<String>>();// 记录每个通道对应哪些功能bolt
	public static Set<String> set = new HashSet<String>();// 记录有哪些function好用于declareStream

	public static void generateFilter(String expression) throws Exception
	{
		StringBuilder sb = new StringBuilder();
		Interpreter interpreter = new Interpreter();
		Method[] methods = getAllStatMethods();
		sb.append("import cn.fudan.tools.bolt.*;\n");
		for (int i = 0; i < methods.length; i++)
		{
			String s = "private {0} {1}(String channel,long windowSize, long moveSize)\n"
					+ "'{'\n"
					+ "    FilterFunction.{1}(channel,windowSize,moveSize);\n"
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
		Method[] methods = FilterFunction.class.getMethods();
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
		if (map.containsKey(channel))
		{
			map.get(channel).add("avg");
		} else
		{
			Set<String> set = new HashSet<String>();
			set.add("avg");
			map.put(channel, set);
		}
		set.add("avg");
		return 0;
	}

	public static int max(String channel, long windowSize, long moveSize)
			throws Exception
	{
		if (map.containsKey(channel))
		{
			map.get(channel).add("max");
		} else
		{
			Set<String> set = new HashSet<String>();
			set.add("max");
			map.put(channel, set);
		}
		set.add("max");
		return 0;
	}

	public static int min(String channel, long windowSize, long moveSize)
			throws Exception
	{
		if (map.containsKey(channel))
		{
			map.get(channel).add("min");
		} else
		{
			Set<String> set = new HashSet<String>();
			set.add("min");
			map.put(channel, set);
		}
		set.add("min");
		return 0;
	}
	
	public static int sum(String channel, long windowSize, long moveSize)
			throws Exception
	{
		if (map.containsKey(channel))
		{
			map.get(channel).add("sum");
		} else
		{
			Set<String> set = new HashSet<String>();
			set.add("sum");
			map.put(channel, set);
		}
		set.add("sum");
		return 0;
	}
}
