package cn.fudan.spout;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.cxf.jaxrs.client.ClientConfiguration;
import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.transport.common.gzip.GZIPInInterceptor;
import org.apache.cxf.transport.common.gzip.GZIPOutInterceptor;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import cn.fudan.domain.Dtomap;

import com.juyee.health.core.TimeHelper;
import com.juyee.health.webservice.DataFragment;
import com.juyee.health.webservice.DataFragmentUnit;

public class GetData extends BaseRichSpout
{
	SpoutOutputCollector _collector;
	WebClient client;
	private static int DURATION = 5 * 1000;
	public DataFragment dataFragment;
	public Dtomap dtomap;
	public HashMap<String, Long> map;
	public static int exceptionTimes = 0;// 出现异常次数
	public static int outTimes = 0;// 出现中断半小时以上次数
	long s = TimeHelper.truncTime(System.currentTimeMillis(), 1 * 1000);

	@Override
	public void nextTuple()
	{
		if (System.currentTimeMillis() - s > DURATION)
		{
			s = System.currentTimeMillis();// 一次循环取数据开始时间
			System.out.println("map中已有通道数:" + map.size());

			long endtime;
			boolean outtime = false;
			for (String key : map.keySet())
			{// 限值设置,如果上次取到的最后一个数据到现在的时间差超过半小时,则从当前时间前5秒开始取数据
				endtime = map.get(key);
				if (s - endtime > 30 * 60 * 1000)
				{
					outtime = true;
					endtime = s - 5 * 1000;
					map.put(key, endtime);
				}
			}
			if (outtime)
			{
				outTimes++;
			}

			dtomap.setChannelMap(map);
			try
			{
				dataFragment = client.post(dtomap, DataFragment.class);
			} catch (Exception e)
			{// 出现异常,退出本次循环
				exceptionTimes++;
				return;
			}
			for (DataFragmentUnit[] dataFragmentUnits : dataFragment
					.getDatafragments())
			{// 将本次获取到的每个通道的最后一条数据的时间戳放入map,作为下次访问的时间起点
				DataFragmentUnit dataFragmentUnit = dataFragmentUnits[dataFragmentUnits.length - 1];// 通道的最后一个碎片
				map.put(dataFragmentUnit.getChannelcode(),
						dataFragmentUnit.getEndttime());
				_collector.emit(new Values(dataFragmentUnit.getChannelcode(),
						Arrays.asList(dataFragmentUnits)));
			}
		}
	}

	@Override
	public void open(Map arg0, TopologyContext arg1,
			SpoutOutputCollector collector)
	{
		this.dtomap = new Dtomap();
		this.map = new HashMap<String, Long>();
		_collector = collector;
		client = WebClient.create("http://health.donghai-bridge.com.cn/web/");
		ClientConfiguration config = WebClient.getConfig(client);
		config.getInInterceptors().add(new GZIPInInterceptor());
		config.getOutInterceptors().add(new GZIPOutInterceptor());
		client.path("restful/dataservice/alllastdata")
				.accept("application/xml");

		// 初始化存放数据的容器

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields("channelCode", "dataFragmentUnits"));
	}

}

