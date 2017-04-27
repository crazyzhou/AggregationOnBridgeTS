package cn.fudan.tools.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import bsh.EvalError;
import bsh.Interpreter;
import cn.fudan.domain.ResultDataItem;
import cn.fudan.tools.util.StringHelp;

public class GetResultFunction
{
	public static Set<ResultDataItem> set = new HashSet<ResultDataItem>();// 记录要缓存的所有信息

	public static void generateGetResult(String expression) throws Exception
	{
		StringBuilder sb = new StringBuilder();
		Interpreter interpreter = new Interpreter();
		Method[] methods = getAllStatMethods();
		sb.append("import cn.fudan.tools.bolt.*;\n");
		for (int i = 0; i < methods.length; i++)
		{
			String s = "private {0} {1}(String channel,long windowSize, long moveSize)\n"
					+ "'{'\n"
					+ "    GetResultFunction.{1}(channel,windowSize,moveSize);\n"
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
		Method[] methods = GetResultFunction.class.getMethods();
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
		set.add(new ResultDataItem("avg", channel, windowSize, moveSize));
		return 0;
	}

	public static int max(String channel, long windowSize, long moveSize)
			throws Exception
	{
		set.add(new ResultDataItem("max", channel, windowSize, moveSize));
		return 0;
	}

	public static int min(String channel, long windowSize, long moveSize)
			throws Exception
	{
		set.add(new ResultDataItem("min", channel, windowSize, moveSize));
		return 0;
	}
	
	public static int sum(String channel, long windowSize, long moveSize)
			throws Exception
	{
		set.add(new ResultDataItem("sum", channel, windowSize, moveSize));
		return 0;
	}

	public static void generateCode(String expression) throws Exception
	{
		String staticCode = "import java.util.HashMap;"
				+ "\n"
				+ "import java.util.HashSet;"
				+ "\n"
				+ "import java.util.Map;"
				+ "\n"
				+ "import java.util.Set;"
				+ "\n"
				+ "import java.util.TreeMap;"
				+ "\n"
				+ "import java.util.LinkedList;"
				+ "\n"
				+ "import java.util.List;"
				+ "\n"
				+

				"import org.apache.storm.task.OutputCollector;"
				+ "\n"
				+ "import org.apache.storm.task.TopologyContext;"
				+ "\n"
				+ "import org.apache.storm.topology.IRichBolt;"
				+ "\n"
				+ "import org.apache.storm.topology.OutputFieldsDeclarer;"
				+ "\n"
				+ "import org.apache.storm.tuple.Tuple;"
				+ "\n"
				+ "import bsh.EvalError;"
				+ "\n"
				+ "import bsh.Interpreter;"
				+ "\n"
				+ "import cn.fudan.domain.ResultDataItem;"
				+ "\n"
				+

				"public class GetResult implements IRichBolt{"
				+ "\n"
				+ "OutputCollector _collector;Map<ResultDataItem, Map<Long, Float>> map;"
				+ "\n"
				+ "long maxSeconds;long deleteStep;long needDealSeconds;Interpreter interpreter; "
				+ "\n"
				+ "Set<String> result;"
				+ "\n"
				+

				"@Override"
				+ "\n"
				+ "public void cleanup(){}"
				+ "\n"
				+ "public boolean canRun(){"
				+ "\n"
				+ "for (Map.Entry<ResultDataItem, Map<Long, Float>> entry : map.entrySet()){"
				+ "\n"
				+ "Map<Long, Float> value = entry.getValue();Long minSeconds = null;"
				+ "\n"
				+ "for (Map.Entry<Long, Float> entry1 : value.entrySet()){"
				+ "\n"
				+ "minSeconds = entry1.getKey();break;}"
				+ "\n"
				+ "if (minSeconds != null){"
				+ "\n"
				+ "for (long i = minSeconds; i <= maxSeconds - deleteStep; i += deleteStep){"
				+ "\n"
				+ "if (value.containsKey(i)){"
				+ "\n"
				+ "value.remove(i);}}}}"
				+ "\n"
				+ "Set<Long> dealSeconds = new HashSet<Long>();int count = 0;"
				+ "\n"
				+ "for (Map.Entry<ResultDataItem, Map<Long, Float>> entry : map.entrySet()){"
				+ "\n"
				+ "if (0 == count){"
				+ "\n"
				+ "dealSeconds.addAll(entry.getValue().keySet());count++;continue;}"
				+ "\n"
				+ "if (dealSeconds.size() == 0){break;}"
				+ "\n"
				+ "Map<Long, Float> value = entry.getValue();"
				+ "\n"
				+ "Set<Long> dealSecondsTemp = new HashSet(dealSeconds);"
				+ "\n"
				+ "for (Long l : dealSecondsTemp)	{"
				+ "\n"
				+ "if (value.containsKey(l)){	continue;} else	{"
				+ "\n"
				+ "dealSeconds.remove(l);}}}"
				+ "\n"
				+ "if (0 == dealSeconds.size()){return false;}"
				+ "\n"
				+ "for (Long l : dealSeconds){needDealSeconds = l;}return true;}"
				+ "\n"
				+ "@Override"
				+ "\n"
				+ "public void declareOutputFields(OutputFieldsDeclarer declarer){	}"
				+ "\n"
				+

				"@Override"
				+ "\n"
				+ "public Map<String, Object> getComponentConfiguration(){return null;}"
				+ "\n"
				+

				"public void put(String functionName, String channelCode, long windowSize,long moveSize){"
				+ "\n"
				+ "if (map == null){map = new HashMap<ResultDataItem, Map<Long, Float>>();}"
				+ "\n"
				+ "map.put(new ResultDataItem(functionName, channelCode, windowSize,moveSize), new TreeMap<Long, Float>());}";
		String executeCode = "@Override"
				+ "\n"
				+ "public void execute(Tuple tuple){"
				+ "\n"
				+ "String functionName = tuple.getStringByField(\"functionName\");"
				+ "\n"
				+ "String channelCode = tuple.getStringByField(\"channelCode\");"
				+ "\n"
				+ "long windowSize = tuple.getLongByField(\"windowSize\");"
				+ "\n"
				+ "long moveSize = tuple.getLongByField(\"moveSize\");"
				+ "\n"
				+ "long startTime = tuple.getLongByField(\"startTime\");"
				+ "\n"
				+ "float average = tuple.getFloatByField(\"average\");"
				+ "\n"
				+ "ResultDataItem resultDataItem = new ResultDataItem(functionName,channelCode, windowSize, moveSize);"
				+ "\n"
				+ "map.get(resultDataItem).put(startTime, average);deleteStep = moveSize * 10;"
				+ "\n" + "if (startTime > maxSeconds){maxSeconds = startTime;}"
				+ "\n" + "if (canRun()){try{" + "\n"
				+ "interpreter.set(\"getResultmap\", map);" + "\n"
				+ "interpreter.set(\"needDealSeconds\", needDealSeconds);"
				+ "\n"
				+ "interpreter.set(\"temp\", new TreeMap<Long, Float>());";

		String expressionTemp = "";
		String expressionReal = new String(expression);// 这个expressionReal是为了后面prepare用的，因为exuecute会改变用户expression的真实内容
		String[] string = expression.split("\n");
		for (int i = 0; i < string.length; i++)
		{
			expressionTemp = expressionTemp + "\"";
			String[] string1 = string[i].split("\"");
			if (string1.length > 1)// 如果有引号
			{
				for (int j = 0; j < string1.length; j++)
				{
					if (j == string1.length - 1)
					{
						expressionTemp = expressionTemp + "\"" + string1[j];
					} else if (0 == j)
					{
						expressionTemp = expressionTemp + string1[j] + "\"+";
					} else if (j % 2 == 0)
					{
						expressionTemp = expressionTemp + "\"" + string1[j]
								+ "\"+";
					} else if (j % 2 == 1)
					{
						expressionTemp = expressionTemp + "\"" + "\\" + "\""
								+ string1[j] + "\\" + "\"\"+";
					}
				}
			} else
			{
				expressionTemp += string[i];
			}
			expressionTemp = expressionTemp + "\"" + "\n+";
		}
		expression = expressionTemp;
		// 这里是把真正的avg等一切函数拿进去，和用户提交的expression进行合并，注意我在GetResult里面写的那些avg函数只会在interpreter里面会被用到
		expression = expression.substring(0, expression.length() - 1);
		expression = "\"public float avg(String channel, long windowSize, long moveSize){\"+"
				+ "\n"
				+ "\"temp = getResultmap.get(new ResultDataItem(\\\"avg\\\", channel,windowSize, moveSize));\"+"
				+ "\n"
				+ "\"float result = temp.get(needDealSeconds);\"+"
				+ "\n"
				+ "\"return result;}\"+"
				+ "\"public float max(String channel, long windowSize, long moveSize){\"+"
				+ "\n"
				+ "\"temp = getResultmap.get(new ResultDataItem(\\\"max\\\", channel,windowSize, moveSize));\"+"
				+ "\n"
				+ "\"float result = temp.get(needDealSeconds);\"+"
				+ "\n"
				+ "\"return result;}\"+"
				+ "\"public float sum(String channel, long windowSize, long moveSize){\"+"
				+ "\n"
				+ "\"temp = getResultmap.get(new ResultDataItem(\\\"sum\\\", channel,windowSize, moveSize));\"+"
				+ "\n"
				+ "\"float result = temp.get(needDealSeconds);\"+"
				+ "\n"
				+ "\"return result;}\"+"
				+ "\"public float min(String channel, long windowSize, long moveSize){\"+"
				+ "\n"
				+ "\"temp = getResultmap.get(new ResultDataItem(\\\"min\\\", channel,windowSize, moveSize));\"+"
				+ "\n"
				+ "\"float result = temp.get(needDealSeconds);\"+"
				+ "\n" + "\"return result;}\"+" + expression;
		expression = "\"import java.util.HashMap;\"\n+\"\\n\"+"
				+ "\"import cn.fudan.domain.ResultDataItem;\"\n+\"\\n\"+"
				+ "\"import java.util.Map;\"\n+\"\\n\"+" + expression;// 把那些先决条件加上去
		System.out.println(expression);
		String executeCoreCode = "interpreter.eval(" + expression + ");";
		executeCoreCode = executeCoreCode
				+ "for (Map.Entry<ResultDataItem, Map<Long, Float>> entry : map.entrySet()){entry.getValue().remove(needDealSeconds);}      for (String resultItem : result){System.out.println(resultItem + \"在\" + needDealSeconds+ \"秒钟结果为:\" + interpreter.get(resultItem));}";
		// 真正执行的核心代码取名为executerCoreCode
		executeCode += executeCoreCode;
		executeCode = executeCode
				+ "} catch (EvalError e){e.printStackTrace();}}}\n";

		String prepareCode = "@Override"
				+ "\n"
				+ "public void prepare(Map arg0, TopologyContext arg1,OutputCollector collector){	this._collector = collector;interpreter = new Interpreter();result = new HashSet<String>();"
				+ "\n";
		// 这里开始放相关结果集

		String[] s = expressionReal.split(";");
		for (int i = 0; i < s.length; i++)
		{
			String[] s1 = s[i].split("=");
			if (s1.length == 2 && s1[0].trim().contains("out"))
			{
				prepareCode = prepareCode + "result.add(\"" + s1[0].trim()
						+ "\");";
			}
		}

		// 在这里放put
		for (ResultDataItem rdi : set)
		{
			prepareCode = prepareCode + "put(\"" + rdi.getFunctionName()
					+ "\",\"" + rdi.getChannelCode() + "\","
					+ rdi.getWindowSize() + "," + rdi.getMoveSize() + ");";
		}
		prepareCode += "}";
		BufferedWriter bw = new BufferedWriter(new FileWriter("GetResult.java"));
		StringBuilder ss = new StringBuilder();
		bw.append(staticCode + prepareCode + executeCode);
		bw.append("}");
		bw.close();
	}
}
