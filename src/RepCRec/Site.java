package RepCRec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Site {
	private final int sid;
	private Map<Integer, Data> data;
	private Map<Integer, Data> dataSnapshot;
	private List<Lock> locks;
	private Map<Data, Boolean> isWritten;  // want to use is to tell if the site did recovery before  
	private boolean isFailed;
	private Set<Transaction> accessedTable;

	public Site(int sid) {
		this.sid = sid;
		initializeData();
		this.isFailed = false;
		accessedTable = new HashSet<Transaction>();
		locks = new ArrayList<Lock>();
	}

	private void initializeData() {
		data = new HashMap<Integer, Data>();
		dataSnapshot = new HashMap<Integer, Data>();
		for(int i = 1; i<=10; i++) {
			data.put(2*i, new Data(20*i));
			dataSnapshot.put(2*i, new Data(20*i));
		}
		if((sid%2) == 0){
			data.put((sid-1), new Data(10*(sid-1)));
			data.put((sid+9), new Data(10*(sid+9)));
			dataSnapshot.put((sid-1), new Data(10*(sid-1)));
			dataSnapshot.put((sid+9), new Data(10*(sid+9)));
		}		
	}
	
	public Set<Transaction> getAccessedTable() {
		return accessedTable;
	}
	
	public void addTransactionToAccessedTable(Transaction transaction) {
		accessedTable.add(transaction);
	}
	

	
	public void writeData(int index, int value){
		data.get(index).setValue(value);
	}
	
	public boolean hasData(int index) {
		return data.containsKey(index);
	}
	
	//  add  
	public boolean hasDataInSnapshot(int index) {
		return dataSnapshot.containsKey(index);
	}
	
	//	add
	public int readDataFromSnapshot(int index) {
		return dataSnapshot.get(index).getValue();
	}
	
	public int readData(int index) {
		return data.get(index).getValue();
	}

	public int getSid() {
		return sid;
	}
	
	public void addLock(Transaction t, Data d, boolean isRead) {
		locks.add(new Lock(t, d, isRead));
	}
	
	public void removeLock(Lock lock) {
		locks.remove(lock);
	}
	
	public boolean isDataWLocked(Data data) {
		for (Lock lock: locks) {
			if(lock.getData().equals(data) && !lock.isRead()) {
				return true;
			}
		}
		return false;
	}
	
	public void fail() {
		isFailed = true;
		locks.clear();
		accessedTable.clear();
	}
	
	public boolean isFailed() {
		return isFailed;
	}
	
	public void recover() {
		this.isFailed = false;
		isWritten = new HashMap<Data, Boolean>();
		for (int i: data.keySet()) {
			isWritten.put(data.get(i), false);
		}
	}
	
	public boolean isReadyRead(Data d) {
		return isWritten.get(d);
	}
	
	public void commit() {
		for(int i: data.keySet()) {
			dataSnapshot.put(i, data.get(i));
		}
	}
	
	public List<Lock> getLockTable() {
		return locks;
	}
	
	public Map<Integer,Data> getSnapshot()   {
		Map<Integer,Data> snap = new HashMap<Integer, Data>();
		Set<Integer> keySet = dataSnapshot.keySet();
		for(Integer key : keySet) {
			snap.put(key,new Data(key).setValue(dataSnapshot.get(key).getValue()));
			
		}	
		return snap;
	}
	
	public Map<Integer,Data> getData()   {
		return data;
	}
	
	public void writeDataSnapshot(int index, int value){
		dataSnapshot.get(index).setValue(value);
	}
	
	public boolean isRecovering() {
		return isWritten != null;
	}
	
	public void writeAfterRecovey(Data variable) {
		isWritten.put(variable, true);
	}
	
	public String outputSiteCommitedData() {
		StringBuffer sb = new StringBuffer();
		Set<Integer> keyset = dataSnapshot.keySet();
		for(Integer key : keyset) {
			sb = sb.append("X" + key + ":" + dataSnapshot.get(key).getValue() + "\t" );
		}
		return sb.toString();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if(!(obj instanceof Site))
			return false;
		Site s = (Site) obj;
		return (sid == s.getSid());
	}
	
	@Override
	public int hashCode() {
		return sid;
	}
}
