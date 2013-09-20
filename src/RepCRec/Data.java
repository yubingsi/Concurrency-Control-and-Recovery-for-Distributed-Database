package RepCRec;

public class Data {
	private final int index;
	private int value;
	
	public Data (int index) {
		this.index = index;
		this.value = index;
	}
	
	public int getIndex() {
		return index;
	}
	
	public int getValue() {
		return value;
	}
	
	public Data setValue(int value) {
		this.value = value;
		return this;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if(!(obj instanceof Data))
			return false;
		Data d = (Data) obj;
		return (index == d.getIndex());
	}
	
	@Override
	public int hashCode() {
		return index;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("x").append(index);
		return sb.toString();
	}
	
	public String toStringwithSite(Site site) {
		StringBuffer sb = new StringBuffer();
		sb.append("x").append(index).append(".").append(site.getSid());
		return sb.toString();
	}

}
