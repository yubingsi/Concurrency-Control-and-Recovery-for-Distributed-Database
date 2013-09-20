package RepCRec;

import java.util.*;

public class Transaction {
	private final int tid;
	private final long startTime;
	enum State {ACTIVE, ABORT, COMMITTED}
	private State state;
	private boolean readOnly = false;
	private Set<Data> writedData;
	
	public Transaction (int tid, long start) {
		this.tid = tid;
		startTime = start;
		state = State.ACTIVE;
		writedData = new HashSet<Data>();
	}
	public Transaction (int tid, boolean RO) {
		this.tid = tid;
		startTime = System.nanoTime();
		state = State.ACTIVE;
		readOnly = RO;
		writedData = new HashSet<Data>();
	}
	
	public void addWritedDatas(Data variable) {
		writedData.add(variable);
	}
	
	public Set<Data> getWritedData() {
		return this.writedData;
	}
	
	public int getTid() {
		return tid;
	}
	
	public long getStartTime() {
		return startTime;
	}
	
	public boolean isOlder (Transaction tr) {
		return startTime < tr.getStartTime();
	}
	
	public boolean isYounger (Transaction tr) {
		return startTime > tr.getStartTime();
	}
	
	public boolean isReadOnly() {
		return this.readOnly;
	}
	
	public void abort() {
		state = State.ABORT;
	}
	
	
	public boolean isAbort() {
		return state.equals(State.ABORT);
	}
	
	public boolean isActive() {
		return state.equals(State.ACTIVE);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if(!(obj instanceof Transaction))
			return false;
		Transaction t = (Transaction) obj;
		return (tid == t.getTid());
	}
	
	@Override
	public int hashCode() {
		return tid;
	}

}
