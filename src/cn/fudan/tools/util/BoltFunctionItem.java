package cn.fudan.tools.util;

public class BoltFunctionItem
{
	private long windowSize;
	private long moveSize;

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
		BoltFunctionItem other = (BoltFunctionItem) obj;
		if (moveSize != other.moveSize)
			return false;
		if (windowSize != other.windowSize)
			return false;
		return true;
	}

	public BoltFunctionItem(long windowSize, long moveSize)
	{
		super();
		this.windowSize = windowSize;
		this.moveSize = moveSize;
	}

	public String toString()
	{
		return windowSize + ":" + moveSize;
	}
}
