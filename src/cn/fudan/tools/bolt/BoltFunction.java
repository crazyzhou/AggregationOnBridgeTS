package cn.fudan.tools.bolt;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import bsh.EvalError;
import bsh.Interpreter;

import cn.fudan.tools.util.BoltFunctionItem;
import cn.fudan.tools.util.StringHelp;

public class BoltFunction
{
	public static Map<String, Set<BoltFunctionItem>> avgMap;
	public static Map<String, Set<BoltFunctionItem>> maxMap;
	public static Map<String, Set<BoltFunctionItem>> minMap;
	public static Map<String, Set<BoltFunctionItem>> sumMap;

	public static void generate(String expression) throws Exception
	{
		StringBuilder sb = new StringBuilder();
		Interpreter interpreter = new Interpreter();
		Method[] methods = getAllStatMethods();
		sb.append("import cn.fudan.tools.bolt.*;\n");
		for (int i = 0; i < methods.length; i++)
		{
			String s = "private {0} {1}(String channel,long windowSize, long moveSize)\n"
					+ "'{'\n"
					+ "    BoltFunction.{1}(channel,windowSize,moveSize);\n"
					+ "    return 0;\n" + "'}'";
			Method method = methods[i];
			s = StringHelp.format(s, new String[] {
					method.getReturnType().getName(), method.getName() });
			sb.append(s).append("\n");
		}
		sb.append(expression);
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
		if (null == avgMap)
		{
			avgMap = new HashMap<String, Set<BoltFunctionItem>>();
			Set<BoltFunctionItem> tempSet = new HashSet<BoltFunctionItem>();
			tempSet.add(new BoltFunctionItem(windowSize, moveSize));
			avgMap.put(channel, tempSet);
			return 0;
		}
		if (avgMap.containsKey(channel))
		{
			avgMap.get(channel).add(new BoltFunctionItem(windowSize, moveSize));
		} else
		{
			Set<BoltFunctionItem> tempSet = new HashSet<BoltFunctionItem>();
			tempSet.add(new BoltFunctionItem(windowSize, moveSize));
			avgMap.put(channel, tempSet);
		}
		return 0;
	}

	public static int max(String channel, long windowSize, long moveSize)
			throws Exception
	{
		if (null == maxMap)
		{
			maxMap = new HashMap<String, Set<BoltFunctionItem>>();
			Set<BoltFunctionItem> tempSet = new HashSet<BoltFunctionItem>();
			tempSet.add(new BoltFunctionItem(windowSize, moveSize));
			maxMap.put(channel, tempSet);
			return 0;
		}
		if (maxMap.containsKey(channel))
		{
			maxMap.get(channel).add(new BoltFunctionItem(windowSize, moveSize));
		} else
		{
			Set<BoltFunctionItem> tempSet = new HashSet<BoltFunctionItem>();
			tempSet.add(new BoltFunctionItem(windowSize, moveSize));
			maxMap.put(channel, tempSet);
		}
		return 0;
	}

	public static int min(String channel, long windowSize, long moveSize)
			throws Exception
	{
		if (null == minMap)
		{
			minMap = new HashMap<String, Set<BoltFunctionItem>>();
			Set<BoltFunctionItem> tempSet = new HashSet<BoltFunctionItem>();
			tempSet.add(new BoltFunctionItem(windowSize, moveSize));
			minMap.put(channel, tempSet);
			return 0;
		}
		if (minMap.containsKey(channel))
		{
			minMap.get(channel).add(new BoltFunctionItem(windowSize, moveSize));
		} else
		{
			Set<BoltFunctionItem> tempSet = new HashSet<BoltFunctionItem>();
			tempSet.add(new BoltFunctionItem(windowSize, moveSize));
			minMap.put(channel, tempSet);
		}
		return 0;
	}

	public static int sum(String channel, long windowSize, long moveSize)
			throws Exception
	{
		if (null == sumMap)
		{
			sumMap = new HashMap<String, Set<BoltFunctionItem>>();
			Set<BoltFunctionItem> tempSet = new HashSet<BoltFunctionItem>();
			tempSet.add(new BoltFunctionItem(windowSize, moveSize));
			sumMap.put(channel, tempSet);
			return 0;
		}
		if (sumMap.containsKey(channel))
		{
			sumMap.get(channel).add(new BoltFunctionItem(windowSize, moveSize));
		} else
		{
			Set<BoltFunctionItem> tempSet = new HashSet<BoltFunctionItem>();
			tempSet.add(new BoltFunctionItem(windowSize, moveSize));
			sumMap.put(channel, tempSet);
		}
		return 0;
	}
}
