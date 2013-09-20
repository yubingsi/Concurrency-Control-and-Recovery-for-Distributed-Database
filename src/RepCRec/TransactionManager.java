package RepCRec;

import java.util.*;

import org.omg.IOP.TransactionService;

import RepCRec.Operation.Type;

public class TransactionManager {

	private List<Site> sites; 
	private List<Operation> buffredOperations;
	private Map<Transaction,Map<Integer,Data>> snapshots;
	private Map<Integer,Transaction> transactions;
	
	public TransactionManager() {
		initialSites();
		buffredOperations = new ArrayList<Operation>();
		snapshots = new HashMap<Transaction,Map<Integer,Data>>();
		transactions = new HashMap<Integer,Transaction>();
	}
	
	private void initialSites() {
		sites = new ArrayList<Site>();
		for(int i=1; i<=10; i++) {
			sites.add(new Site(i));
		}
	}
	
	public static void main(String[] args) throws Exception {
		String filePath = "test6.txt";
		TransactionManager tm = new TransactionManager();
		List<Operation> operations = FileManager.getOperations(filePath);
		
		for(int tick=1; tick<=tm.getTickNum(operations); tick++){
			List<Operation> willDeletedOperations = new ArrayList<Operation>();
			for(Operation bufferedOperation : tm.buffredOperations) {
				boolean isBuffered = true;
				List<Integer> rwInfo = bufferedOperation.getRWinfo();
				Transaction transaction = tm.transactions.get(rwInfo.get(0));
				if(transaction.isReadOnly() && !transaction.isAbort()) {
					Map<Integer,Data> newSnapShot = tm.getAllCommitedDataForRO();
					tm.snapshots.put(transaction, newSnapShot);
					int readResult = tm.translateReadForRO(transaction, new Data(rwInfo.get(1)),bufferedOperation,isBuffered);
					if(readResult != Integer.MAX_VALUE) {
						//bufferedOperation.setDone();
						System.out.println("Read ONly T" + rwInfo.get(0) + " read X" + rwInfo.get(1) + "=" + readResult);
						willDeletedOperations.add(bufferedOperation);
					}	
				} else if (!transaction.isAbort() && bufferedOperation.getType().equals(Type.READ)) {
					int readResult = tm.translateRead(transaction, new Data(rwInfo.get(1)),bufferedOperation,isBuffered);
					if(readResult != Integer.MAX_VALUE) {
						//bufferedOperation.setDone();
						System.out.println("T" + rwInfo.get(0) + " read X" + rwInfo.get(1) + "=" + readResult);
						willDeletedOperations.add(bufferedOperation);
					}
				} else if (!transaction.isAbort() && bufferedOperation.getType().equals(Type.WRITE)){
					Transaction transactionW = tm.transactions.get(rwInfo.get(0));
					if (tm.translateWrite(transactionW, new Data(rwInfo.get(1)).setValue(rwInfo.get(2)),bufferedOperation,isBuffered)) {
						System.out.println("T" + rwInfo.get(0) + " write X" + rwInfo.get(1) + "to" + rwInfo.get(2));
						willDeletedOperations.add(bufferedOperation);
					}
				}
			}
			tm.updateBufferedOperations(willDeletedOperations);
			List<Operation> newOperations = tm.getCurrentOperations(operations, tick);
			for (Operation newOperation : newOperations) {
				boolean isBuffered = false;
				if(newOperation.getType().equals(Type.READ)) {
					List<Integer> readInfo = newOperation.getRWinfo();
					Transaction transaction = tm.transactions.get(readInfo.get(0));
					if(!transaction.isAbort() && transaction.isReadOnly()) {
						int readResult = tm.translateReadForRO(transaction, new Data(readInfo.get(1)),newOperation,isBuffered);
						if(readResult != Integer.MAX_VALUE) {
							newOperation.setDone();
							System.out.println("Read ONly T" + readInfo.get(0) + " read X" + readInfo.get(1) + "=" + readResult);
						}
					} else if (!transaction.isAbort()){
						int readResult = tm.translateRead(transaction, new Data(readInfo.get(1)),newOperation,isBuffered);
						if(readResult != Integer.MAX_VALUE) {
							newOperation.setDone();
							System.out.println("T" + readInfo.get(0) + " read X" + readInfo.get(1) + "=" + readResult);
						}
					}
					
				} else if (newOperation.getType().equals(Type.WRITE)) {
					List<Integer> writeInfo = newOperation.getRWinfo();
					Transaction transaction = tm.transactions.get(writeInfo.get(0));
					if (!transaction.isAbort() && tm.translateWrite(transaction, new Data(writeInfo.get(1)).setValue(writeInfo.get(2)),newOperation,isBuffered)) {
						System.out.println("T" + writeInfo.get(0) + " write X" + writeInfo.get(1) + "to" + writeInfo.get(2));
					}
				} else if (newOperation.getType().equals(Type.BEGIN)) {
					int tid = newOperation.getSoleNumeric();
					tm.begin(tid);
				} else if (newOperation.getType().equals(Type.BEGINRO)) {
					int tid = newOperation.getSoleNumeric();
					tm.beginRO(tid);
				} else if (newOperation.getType().equals(Type.DUMPALL)) {
					tm.dump();
				} else if(newOperation.getType().equals(Type.DUMPDATA)) {
					int dataIndex = newOperation.getSoleNumeric();
					tm.dump(new Data(dataIndex));
				} else if(newOperation.getType().equals(Type.DUMPSITE)) {
					int sid = newOperation.getSoleNumeric();
					tm.dump(sid);
				} else if (newOperation.getType().equals(Type.END)) {
					int tid = newOperation.getSoleNumeric();
					Transaction t = tm.transactions.get(tid);
					tm.end(t);
				} else if (newOperation.getType().equals(Type.FAIL)) {
					for(Site site : tm.sites) {
						if (site.getSid() == newOperation.getSoleNumeric()) {
							tm.fail(site);
							//System.out.println("Site" + site.getSid() + " is failed");
							break;
						}
					}	
				} else if (newOperation.getType().equals(Type.RECOVER)) {
					for(Site site : tm.sites) {
						if (site.getSid() == newOperation.getSoleNumeric()) {
							tm.recover(site);
							//System.out.println("Site" + site.getSid() + " is recoving");
							break;
						}
					}
				}
			}
		}
	}
	
	
	private List<Operation> getCurrentOperations(List<Operation> operations, int tick) {
		List<Operation> currentOperation = new ArrayList<Operation> ();
		for(Operation operation : operations) {
			if(operation.gettimeStamp() == tick) {
				currentOperation.add(operation);
			}
		}
		return currentOperation;
	}
	
	private int getTickNum(List<Operation> operations) {
		return operations.get(operations.size()-1).gettimeStamp();
	}
	
	public void beginRO(int tid) {
		boolean isReadOnly = true;
		Transaction transaction = new Transaction(tid,isReadOnly);
		Map<Integer,Data> newSnapShot = getAllCommitedDataForRO();
		snapshots.put(transaction, newSnapShot);
		transactions.put(tid, transaction);
	}
	
	public void begin(int tid) {
		Transaction transaction = new Transaction(tid, System.nanoTime());
		transactions.put(tid, transaction);
	}
	
	private Map<Integer,Data> getAllCommitedDataForRO() {
		Map<Integer,Data> newSnapShot = new HashMap<Integer, Data>();
		for(Site site : sites) { 
			if(!site.isFailed()) {
				Map<Integer,Data> snapShot = new HashMap<Integer, Data>();
				snapShot = site.getSnapshot();
				Set<Integer> keySet = snapShot.keySet();
				for(Integer key : keySet) {
					newSnapShot.put(key, snapShot.get(key));
				}
			}
		}
		return newSnapShot;
	}
	// write
	public boolean translateWrite(Transaction transaction,Data variable,Operation operation,boolean isBuffered){
		transaction.addWritedDatas(variable);
		return write(transaction,variable,operation,isBuffered);
	}
	
	// read for RO
	public int translateReadForRO(Transaction transaction,Data variable,Operation operation,boolean isBuffered) {
		Map<Integer,Data> newSnapshot = snapshots.get(transaction);
		Set<Integer> keySet = newSnapshot.keySet();
//		for(Integer key : keySet) {
//			System.out.println(newSnapshot.get(key).getValue());
//		}
		return readForRO(transaction,variable,newSnapshot,operation,isBuffered);
	}
	
	// read
	public int translateRead(Transaction t,Data variable,Operation operation,boolean isBuffered) {
		return readForRWT(t,variable,operation,isBuffered);
	}
	
	public void end(Transaction transaction) {
		if(transaction.isAbort()) {
			//System.out.println(transaction + "is Aborted!");
		} else if (transaction.isActive() && !hasBufferedOperation(transaction)) {
			System.out.println("transaction" + transaction.getTid() +  " is Ready to Commit!");
			updateLockAndAccessTable(transaction);
			updateSnapshot(transaction);
			for(Site site : sites) {
				if(site.isRecovering()) {
					for(Data variable : transaction.getWritedData()) {
						site.writeAfterRecovey(variable); 
					}
				}
			} 
		}
	}
	
	public void abortTransaction(Transaction transaction) {
		System.out.println("Transaction " + transaction.getTid() + " is aborted!");
		updateLockAndAccessTable(transaction);
		updateBufferedOperation(transaction);
		for(Data variable : transaction.getWritedData()) {
			for(Site site : sites) {
				if(site.hasData(variable.getIndex())){
					site.writeData(variable.getIndex(),site.getSnapshot().get(variable.getIndex()).getValue());	
				}
			}
		}
		transaction.abort();
	}
	
	private void updateLockAndAccessTable(Transaction transaction) {
		List<Lock> removedLocks = new ArrayList<Lock>();
		for(Lock lock : sites.get(0).getLockTable()) {
			if(lock.getTransaction().equals(transaction)) {
				removedLocks.add(lock);
			} 
		} 
		
		for(Site site : sites) {
			site.getAccessedTable().remove(transaction);
			site.getLockTable().removeAll(removedLocks); 
		}
	}
	
	private void updateBufferedOperations(List<Operation> operations) {
		buffredOperations.removeAll(operations);
	}
	
	private void updateBufferedOperation(Transaction transaction) {
		List<Operation> removedOperations = new ArrayList<Operation>();
		for (Operation wait : buffredOperations) {
			List<Integer> info = wait.getRWinfo();
			Transaction transaction1 = transactions.get(info.get(0));
			if (transaction1.equals(transaction)) {
				removedOperations.add(wait);
			}
		}
		buffredOperations.removeAll(removedOperations);
	}
	
	private void updateSnapshot(Transaction transaction) {
		Set<Data> writedDatas = transaction.getWritedData();
		for(Data data : writedDatas) {
			for(Site site : sites) {
				if(site.hasData(data.getIndex())) {
					site.writeDataSnapshot(data.getIndex(), data.getValue());
				}
			}
		}
	}
	
//	public List<Operation> checkBufferedOperations() {
//		List<Operation> operations = new ArrayList<Operation>();
//		for (WaitingOperation wait : buffredOperations) {
//			operations.add(wait.getOperation());
//		}
//		return operations;
//	}
	
	public void fail(Site site) {
		Set<Transaction> transactions = site.getAccessedTable();
		for(Transaction transaction : transactions){
			abortTransaction(transaction);
		}
		site.fail();
	}
	
	public void dump() {
		for(Site site : sites) {
			System.out.println("For site " + site.getSid());
			System.out.println(site.outputSiteCommitedData());
		}	
	}
	
	public void dump(int sid) {
		for(Site site : sites) {
			if(site.getSid() == sid) {
				System.out.println("For site " + site.getSid());
				System.out.println(site.outputSiteCommitedData());
			}
		}	
	}
	
	public void dump(Data variable) {
		for(Site site : sites) {
			System.out.print("For site " + site.getSid() + "\t");
			System.out.println("x" + variable.getIndex() + "=" + site.getData().get(variable.getIndex()));
		}	
	}
	
	public void recover(Site site) {
		site.recover();
	}
	
	
	public int readForRO(Transaction transaction, Data variable,Map<Integer,Data> newSnapshot, Operation operation,boolean isBuffered) {	
		if(newSnapshot.get(variable.getIndex()) == null && !isBuffered) {
			buffredOperations.add(operation);
			return Integer.MAX_VALUE;
		} else {
			return newSnapshot.get(variable.getIndex()).getValue(); 
		}
	}
	
	public int readForRWT(Transaction transaction, Data variable,Operation operation,boolean isBuffered) {
		final Boolean isRead = true;
		boolean allSiteFailed = true;
		for(Site site : sites) {
			if(canBeReadForReadWriteT(site,transaction,variable) && site.hasData(variable.getIndex())) {	
				if(!isReadConflict(transaction,variable,site)){
					site.addLock(transaction,variable, isRead);
					site.addTransactionToAccessedTable(transaction); 
					addRLockForAllSites(sites, transaction,variable);
					return site.readData(variable.getIndex());
				} else if (!isBuffered && isAllowedWait(transaction, variable,site)){	
					buffredOperations.add(operation);
					return Integer.MAX_VALUE;
				} else if (isReadConflict(transaction,variable,site) && 
						!isAllowedWait(transaction, variable, site)) {
					abortTransaction(transaction);
					return Integer.MAX_VALUE;
				}
				allSiteFailed = false;
			} 
		}
		
		if(allSiteFailed && !isBuffered){
			buffredOperations.add(operation);
		}
		return Integer.MAX_VALUE; // did not get the value	NEED TO DO
	}
	
	public boolean write(Transaction transaction, Data variable,Operation operation,boolean isBuffered) {
		boolean allFailed = true;
		boolean isRead = false;
		boolean isWrited = false;
		for(Site site : sites) {
			if(! site.isFailed() && site.hasData(variable.getIndex())) {
				if(!isWriteConflict(transaction,variable,site)) {
					site.addLock(transaction, variable, isRead);
					site.addTransactionToAccessedTable(transaction);
					site.writeData(variable.getIndex(),variable.getValue());
//					if(site.isRecovering()) {
//						site.writeAfterRecovey(variable);
//					} 
					allFailed = false;
					isWrited = true;
				} else if (!isBuffered && isAllowedWait(transaction, variable,site)){
					buffredOperations.add(operation);
					allFailed = false;
					return false;
				} else if (!isAllowedWait(transaction, variable,site)){
					abortTransaction(transaction);
					return false;
				}	
			} 
		}
		if(!isBuffered && allFailed) {
			//buffredOperations.add(operation);
		}
		return isWrited;
	}
	
	private boolean isReadConflict(Transaction transaction, Data variable, Site site) {
		List<Lock> locks = site.getLockTable();
		for(Lock lock : locks) {
			if(lock.getData().equals(variable) && !lock.isRead()) {
				return true;
			}
		}
		return false;
	}
	
	private boolean isWriteConflict(Transaction transaction, Data variable, Site site) {
		return site.isDataWLocked(variable);
	}	
	
//	private boolean canBeReadForReadOnlyT (Site site, Transaction transaction, Data variable) {
//		return (!site.isFailed() && ! isReplicated(variable) && site.hasData(variable.getIndex())) || 
//			(!site.isFailed() && site.hasDataInSnapshot(variable.getIndex()) && !site.isRecovering()) || 
//			(!site.isFailed() && site.hasDataInSnapshot(variable.getIndex()) && site.isReadyRead(variable));	
//	}
	
	private boolean canBeReadForReadWriteT (Site site, Transaction transaction, Data variable) {
		return (!site.isFailed() && ! isReplicated(variable)) ||
			(!site.isFailed() && site.hasData(variable.getIndex()) && isReplicated(variable) &&!site.isRecovering()) || 
			(!site.isFailed() && site.hasData(variable.getIndex()) && isReplicated(variable) && site.isReadyRead(variable));	
	} 
	
	private static void addRLockForAllSites(List<Site> sites, Transaction t, Data variable) {
		for(Site site : sites) {
			if(site.hasData(variable.getIndex())) {
				site.addLock(t, variable, true);
				site.addTransactionToAccessedTable(t);
			}
		}
	}
	
	private boolean isReplicated(Data variable) {
		int copiesNum = 0;
		for(Site site : sites) {
			if(site.hasData(variable.getIndex())) {
				copiesNum ++;
			}
			if(copiesNum > 1) {
				return true;
			}
		}
		return false;
	}
	
	public boolean isAllowedWait(Transaction transaction, Data variable, Site site) {
		List<Lock> locks = site.getLockTable();
		for(Lock lock : locks) {
			if(lock.getData().equals(variable) && transaction.isYounger(lock.getTransaction())) {
					return false;
			} 
		}
		for(Operation wait : buffredOperations) {
			List<Integer> rwInfo = wait.getRWinfo();
			Transaction transactionW = transactions.get(rwInfo.get(0));
			if(rwInfo.get(1) == variable.getIndex() && transaction.isYounger(transactionW)) {
				return false;
			} 
		}
		return true;
	}
	
	private boolean hasBufferedOperation(Transaction t) {
		for(Operation op : buffredOperations) {
			if(op.getRWinfo().get(0) == t.getTid()){
				return true;
			} 
		}
		return false;
	}
	
}
