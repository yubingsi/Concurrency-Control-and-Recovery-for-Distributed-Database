package RepCRec;

public class Lock {
	private Transaction transaction;
	private Data data;
	private boolean isRead;
	
	public Lock(Transaction t, Data d, boolean isRead) {
		this.transaction = t;
		this.data = d;
		this.isRead = isRead;
	}
	
	public boolean isRead() {
		return isRead;
	}
	
	public Transaction getTransaction() {
		return transaction;
	}
	
	public Data getData() {
		return data;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if(!(obj instanceof Lock))
			return false;
		Lock l = (Lock) obj;
		return (transaction.equals(l.getTransaction()) && data.equals(l.getData()) && isRead == l.isRead());
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		result = 31 * result + transaction.hashCode();
		result = 31 * result + data.hashCode();
		if (isRead){
			result = 31 * result + 19;
		}
		return result;
	}

	
}
