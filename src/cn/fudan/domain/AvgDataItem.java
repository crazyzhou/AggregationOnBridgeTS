package cn.fudan.domain;

public class AvgDataItem
{
	private float sum;// 当前总和
	private float num;// 当前个数
	private float max;// 当前最大值
	private float min;// 当前最小值
	private long startTime;

	public AvgDataItem(float sum, float num, float max, float min, long startTime)
	{
		this.sum = sum;
		this.num = num;
		this.max = max;
		this.min = min;
		this.startTime = startTime;
	}
	
	public AvgDataItem(float sum, float num, float max, float min)
	{
		this.sum = sum;
		this.num = num;
		this.max = max;
		this.min = min;
		this.startTime = 0;
	}

	public AvgDataItem()
	{
		this.sum = 0;
		this.num = 0;
	}

	public float getSum()
	{
		return sum;
	}

	public void setSum(float sum)
	{
		this.sum = sum;
	}

	public float getNum()
	{
		return num;
	}

	public void setNum(float num)
	{
		this.num = num;
	}

	public float getMax()
	{
		return max;
	}

	public void setMax(float max)
	{
		this.max = max;
	}

	public float getMin()
	{
		return min;
	}

	public void setMin(float min)
	{
		this.min = min;
	}
	

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public String toString()
	{
		return sum + ":" + num;
	}
}
