//package RepCRec;
//
//import java.util.List;
//import java.util.Map;
//
//public class AvailableCopiesAlgorithm {
//	
//	
//	
//	private static List<Site> sites;
//	
//	
//	 
//	public static int readForRO(List<Site> sites,Transaction transaction, Data variable,Map<Integer,Data> newSnapshot) {	
//		for(Site site : sites) {
//			if(canBeReadForReadOnlyT(site,transaction,variable)) {
//				return newSnapshot.get(variable.getIndex()).getValue();
//			} 
//		}
//		return Integer.MAX_VALUE; // did not get the value NEED TO DO STH.
//	}
//	
//	// read from snapshot
//	public static int readForRWT(List<Site> sites, Transaction transaction, Data variable) {
//		final Boolean isRead = true;
//		int returnValue = Integer.MAX_VALUE;
//		for(Site site : sites) {
//			if(canBeReadForReadWriteT(site,transaction,variable)) {	
//				if(!isReadConflict(transaction,variable,site)){
//					site.addLock(transaction,variable, isRead);
//					site.addTransactionToAccessedTable(transaction); 
//					addRLockForAllSites(sites, transaction,variable);
//					return site.readData(variable.getIndex());
//				} else if (WaitDie.isAllowedWait(transaction, variable,site)){	
//					buffredOperations.add(new WaitingOperation(transaction, variable,
//							new Operation(isRead,transaction,variable)));
//				} else if (isReadConflict(transaction,variable,site) && 
//						!WaitDie.isAllowedWait(transaction, variable, site)) {
//					/*abort(transaction);*/
//				}
//			} 
//		}
//		return Integer.MAX_VALUE; // did not get the value	NEED TO DO
//	}
//	
//	public static void write(List<Site> sites, Transaction transaction, Data variable) {
//		boolean write = false;
//		boolean isRead = false;
//		for(Site site : sites) {
//			if(! site.isFailed() && site.hasData(variable.getIndex())) {
//				if(!isWriteConflict(transaction,variable,site)) {
//					site.addLock(transaction, variable, isRead);
//					site.addTransactionToAccessedTable(transaction);
//					site.writeData(variable.getIndex(),variable.getValue());
//					write = true;
//				} else if (WaitDie.isAllowedWait(transaction, variable,site)){
//					buffredOperations.add(new WaitingOperation(transaction, variable,
//							new Operation(isRead,transaction,variable).setDataValue(variable.getValue())));
//				} else {
//					//if (isWriteConflict(transaction,variable,site) && !WaitDie.isAllowedWait(transaction, variable, site)) {
//					/*abort();*/
//					break;
//				}	
//			} 
//		}
//		if(!write) {
//			// NEED TO DO STH
//		}
//	}
//	
//	private static boolean isReadConflict(Transaction transaction, Data variable, Site site) {
//		List<Lock> locks = site.getLockTable();
//		for(Lock lock : locks) {
//			if(lock.getData().equals(variable) && !lock.isRead()) {
//				return true;
//			}
//		}
//		return false;
//	}
//	
//	private static boolean isWriteConflict(Transaction transaction, Data variable, Site site) {
//		List<Lock> locks = site.getLockTable();
//		for(Lock lock : locks) {
//			if(lock.getData().equals(variable)) {
//				return true;
//			}
//		}
//		return false;
//	}	
//	
//	private static boolean canBeReadForReadOnlyT (Site site, Transaction transaction, Data variable) {
//		return (!site.isFailed() && ! isReplicated(variable) ) || 
//			(!site.isFailed() && site.hasDataInSnapshot(variable.getIndex()) && site.isWritten == null) || 
//			(!site.isFailed() && site.hasDataInSnapshot(variable.getIndex()) && site.isWritten.get(variable));	
//	}
//	
//	private static boolean canBeReadForReadWriteT (Site site, Transaction transaction, Data variable) {
//		return (!site.isFailed() && ! isReplicated(variable)) &&  site.hasData(variable.getIndex()) ||
//			(!site.isFailed() && site.hasData(variable.getIndex()) && isReplicated(variable) &&site.isWritten == null) || 
//			(!site.isFailed() && site.hasData(variable.getIndex()) && isReplicated(variable) && site.isWritten.get(variable));	
//	} 
//	
//	private static void addRLockForAllSites(List<Site> sites, Transaction t, Data variable) {
//		for(Site site : sites) {
//			if(site.hasData(variable.getIndex())) {
//				site.addLock(t, variable, true);
//				site.addTransactionToAccessedTable(t);
//			}
//		}
//	}
//	
//	private static boolean isReplicated(Data variable) {
//		int copiesNum = 0;
//		for(Site site : sites) {
//			if(site.hasData(variable.getIndex())) {
//				copiesNum ++;
//			}
//			if(copiesNum > 1) {
//				return true;
//			}
//		}
//		return false;
//	}
//}
