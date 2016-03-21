package cn.fudan.tools.bolt;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.juyee.health.webservice.DataFragmentUnit;
import com.juyee.health.webservice.DataUnit;

public class CalData implements IRichBolt
{
	OutputCollector _collector;
	@Override
	public void cleanup()
	{
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple tuple)
	{
		List<DataFragmentUnit> dataFragmentUnits = (List<DataFragmentUnit>) tuple
				.getValueByField("dataFragmentUnits");
		try
		{
			long endtime;
			for (DataFragmentUnit dataFragmentUnit1 : dataFragmentUnits)
			{
				String channelCode = dataFragmentUnit1.getChannelcode();

				ArrayList<DataUnit> dataUnitArrayList = new ArrayList<DataUnit>();

				endtime = dataFragmentUnit1.getEndttime();
				Long dt = dataFragmentUnit1.getDt();// 周期
				float[] values = dataFragmentUnit1.getValues();
				int i = 0;
				int size = values.length;
				for (float value : values)
				{
					DataUnit dataUnit = new DataUnit();
					dataUnit.setTime(new Timestamp(endtime - (size - i - 1)
							* dt).toString());// 推算之前每个数据的时间戳
					dataUnit.setValue(value);
					dataUnitArrayList.add(dataUnit);
					i++;

					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss.SSS");
					long timeStart = sdf.parse(dataUnit.getTime()).getTime();
					_collector.emit(new Values(channelCode, timeStart, dataUnit
							.getValue()));
				}
			}

		} catch (Exception e)
		{

		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector)
	{
		this._collector = collector;
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		// TODO Auto-generated method stub
		declarer.declare(new Fields("channelCode", "timeStamp", "value"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration()
	{
		// TODO Auto-generated method stub
		return null;
	}

}
