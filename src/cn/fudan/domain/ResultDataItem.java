package cn.fudan.domain;

public class ResultDataItem
{
	private String functionName;
	private String channelCode;
	private long windowSize;
	private long moveSize;
	
	public ResultDataItem(String functionName, String channelCode,
			long windowSize, long moveSize)
	{
		this.functionName = functionName;
		this.channelCode = channelCode;
		this.windowSize = windowSize;
		this.moveSize = moveSize;
	}

	public String getFunctionName()
	{
		return functionName;
	}

	public void setFunctionName(String functionName)
	{
		this.functionName = functionName;
	}

	public String getChannelCode()
	{
		return channelCode;
	}

	public void setChannelCode(String channelCode)
	{
		this.channelCode = channelCode;
	}

	public long getWindowSize()
	{
		return windowSize;
	}

	public void setWindowSize(long windowSize)
	{
		this.windowSize = windowSize;
	}

	public long getMoveSize()
	{
		return moveSize;
	}

	public void setMoveSize(long moveSize)
	{
		this.moveSize = moveSize;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((channelCode == null) ? 0 : channelCode.hashCode());
		result = prime * result
				+ ((functionName == null) ? 0 : functionName.hashCode());
		result = prime * result + (int) (moveSize ^ (moveSize >>> 32));
		result = prime * result + (int) (windowSize ^ (windowSize >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ResultDataItem other = (ResultDataItem) obj;
		if (channelCode == null)
		{
			if (other.channelCode != null)
				return false;
		} else if (!channelCode.equals(other.channelCode))
			return false;
		if (functionName == null)
		{
			if (other.functionName != null)
				return false;
		} else if (!functionName.equals(other.functionName))
			return false;
		if (moveSize != other.moveSize)
			return false;
		if (windowSize != other.windowSize)
			return false;
		return true;
	}

}
