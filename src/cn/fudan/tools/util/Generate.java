package cn.fudan.tools.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;

import cn.fudan.tools.bolt.AvgFunction;
import cn.fudan.tools.bolt.BoltFunction;
import cn.fudan.tools.bolt.FilterFunction;
import cn.fudan.tools.bolt.GetResultFunction;

public class Generate
{
	public static void main(String[] args) throws Exception
	{
		new Generate().generate("out_MD471Z=avg(\"5AB001-DY\",5000,1000);\n" +

		"out_MD472Z=max(\"5AB002-DY\",5000,1000);\n" +

		"out_MD473Z=min(\"5AB003-DY\",5000,1000);\n" +

		"out_MD474Z=sum(\"5AB004-DY\",5000,1000);\n" +

		"double[] max={out_MD471Z,out_MD472Z,out_MD473Z,out_MD474Z};\n" +

		"java.util.Arrays.sort(max);\n" +

		"out_MD471474_MZ=max[max.length-1];\n" +

		"out_MD471474_AZ=(out_MD471Z+out_MD472Z+out_MD473Z+out_MD474Z)/4;");
	}

	public void generate(String expression) throws Exception
	{
		generateSpout();
		generateFilterBolt(expression);
		generateBolt(expression);
		generateGetResult(expression);
	}

	public void generateFilterBolt(String expression) throws Exception
	{
		FilterFunction.generateFilter(expression);
		String permanentCode = "import java.util.ArrayList;"
				+ "\n"
				+ "import java.util.HashMap;"
				+ "\n"
				+ "import java.util.List;"
				+ "\n"
				+ "import java.util.Map;"
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
				+ "import org.apache.storm.tuple.Fields;"
				+ "\n"
				+ "import org.apache.storm.tuple.Tuple;"
				+ "\n"
				+ "import org.apache.storm.tuple.Values;"
				+ "\n"
				+

				"public class StaticFilter implements IRichBolt"
				+ "\n"
				+ "{"
				+ "\n"
				+ "OutputCollector _collector;"
				+ "\n"
				+ "Map<String, List<String>> map;"
				+ "\n"
				+

				"@Override"
				+ "\n"
				+ "public void cleanup()"
				+ "\n"
				+ "{"
				+ "\n"
				+

				"}"
				+ "\n"
				+

				"@Override"
				+ "\n"
				+ "public void execute(Tuple tuple)"
				+ "\n"
				+ "{"
				+ "\n"
				+ "String ChannelCode = tuple.getStringByField(\"channelCode\");"
				+ "\n"
				+ "long timeStamp = tuple.getLongByField(\"timeStamp\");"
				+ "\n" + "float value = tuple.getFloatByField(\"value\");"
				+ "\n" + "if (map.containsKey(ChannelCode))" + "\n" + "{"
				+ "\n" + "_collector.emit(new Values(ChannelCode," + "\n"
				+ "timeStamp, value));" + "\n" + "}" + "\n" + "}" + "\n"
				+ "@Override" + "\n"
				+ "public Map<String, Object> getComponentConfiguration()"
				+ "\n" + "{" + "\n" +

				"return null;" + "\n" + "}" + "\n"
				+ "public void put(String channelCode, String function)" + "\n"
				+ "{" + "\n" + "if (map.containsKey(channelCode))" + "\n" + "{"
				+ "\n" + "map.get(channelCode).add(function);" + "\n"
				+ "} else" + "\n" + "{" + "\n"
				+ "List<String> list = new ArrayList<String>();" + "\n"
				+ "list.add(function);" + "\n" + "map.put(channelCode, list);"
				+ "\n" + "}" + "\n" + "}";

		/************** 开始生成prepareCode ************/
		String prepareCode = "@Override"
				+ "\n"
				+ "public void prepare(Map arg0, TopologyContext arg1,OutputCollector collector)"
				+ "\n" + "{" + "\n" + "this._collector = collector;" + "\n"
				+ "map = new HashMap<String, List<String>>();" + "\n";
		// 这里要添加put语句
		for (Map.Entry<String, Set<String>> entry : FilterFunction.map
				.entrySet())
		{
			String key = entry.getKey();
			Set<String> value = entry.getValue();
			for (String functionName : value)
			{
				prepareCode = prepareCode + "put(\"" + key + "\",\""
						+ functionName + "\");";
			}
		}
		prepareCode = prepareCode + "\n}";
		/************** 开始生成declare ************/
		String declareCode = "@Override"
				+ "\n"
				+ "public void declareOutputFields(OutputFieldsDeclarer declarer)"
				+ "\n" + "{" + "\n";
		// for (String streamId : FilterFunction.set)
		// {
		declareCode = declareCode + "declarer.declare("
				// + streamId
				+ " new Fields(\"channelCode\",\"timeStamp\", \"value\"));"
				+ "\n";
		// }
		declareCode = declareCode + "}";
		BufferedWriter bw = new BufferedWriter(new FileWriter(
				"StaticFilter.java"));
		bw.append(permanentCode + prepareCode + declareCode);
		bw.append("}");
		bw.close();
	}

	public void generateSpout() throws Exception
	{
		BufferedReader br = new BufferedReader(new FileReader(
				new File("").getAbsolutePath()
						+ "/src/cn/fudan/spout/GetData.java"));
		BufferedWriter bw = new BufferedWriter(new FileWriter("GetData.java"));
		StringBuilder ss = new StringBuilder();
		String line = br.readLine();
		while (line != null)
		{
			line = br.readLine();
			bw.append(line);
			bw.append("\n");
		}
		bw.close();
		br.close();
	}

	public void generateBolt(String expression) throws Exception
	{
		BoltFunction.generate(expression);
		if (BoltFunction.avgMap != null)
		{
			AvgFunction.generate();
		}
	}

	public void generateGetResult(String expression) throws Exception
	{
		GetResultFunction.generateGetResult(expression);
		GetResultFunction.generateCode(expression);
	}

	public void cluster(List<String> list)
	{
		for (int i = 0; i < list.size() - 1; i++)
		{
			String s = list.get(i);
			for (int j = i + 1; j < list.size(); j++)
			{
				String temp = list.get(j);
			}
		}
	}
}
