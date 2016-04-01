package cn.fudan.domain;

public class PairedWindow {
	private String channel;
	private long windowSize;
	private long moveSize;
	private long leftSize;
	private long rightSize;
	
	public PairedWindow(String channel, long windowSize, long moveSize) {
		this.channel = channel;
		this.windowSize = windowSize;
		this.moveSize = moveSize;
		this.leftSize = windowSize % moveSize;
		this.rightSize = moveSize - this.leftSize;
	}
	
	public String getChannel() {
		return channel;
	}


	public void setChannel(String channel) {
		this.channel = channel;
	}


	public long getWindowSize() {
		return windowSize;
	}


	public void setWindowSize(long windowSize) {
		this.windowSize = windowSize;
	}


	public long getMoveSize() {
		return moveSize;
	}


	public void setMoveSize(long moveSize) {
		this.moveSize = moveSize;
	}


	public long getLeftSize() {
		return leftSize;
	}

	public void setLeftSize(long leftSize) {
		this.leftSize = leftSize;
	}

	public long getRightSize() {
		return rightSize;
	}

	public void setRightSize(long rightSize) {
		this.rightSize = rightSize;
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((channel == null) ? 0 : channel.hashCode());
		result = prime * result + (int) (moveSize ^ (moveSize >>> 32));
		result = prime * result + (int) (windowSize ^ (windowSize >>> 32));
		result = prime * result + (int) (leftSize ^ (leftSize >>> 32));
		result = prime * result + (int) (rightSize ^ (rightSize >>> 32));
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
		PairedWindow other = (PairedWindow) obj;
		if (channel == null)
		{
			if (other.channel != null)
				return false;
		} else if (!channel.equals(other.channel))
			return false;
		if (moveSize != other.moveSize)
			return false;
		if (windowSize != other.windowSize)
			return false;
		if (leftSize != other.leftSize)
			return false;
		if (rightSize != other.rightSize)
			return false;
		return true;
	}
}
