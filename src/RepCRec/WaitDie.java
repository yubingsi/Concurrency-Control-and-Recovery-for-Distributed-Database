//package RepCRec;
//
//import java.util.List;
//
//
//public class WaitDie {
//	
//	public static boolean isAllowedWait(Transaction transaction, Data variable, Site site) {
//		List<Lock> locks = site.getLockTable();
//		for(Lock lock : locks) {
//			if(lock.getData().equals(variable) && transaction.isYounger(lock.getTransaction())) {
//					return false;
//			} 
//		}
//		
//		/*
//		 * waiting talbe 
//		 * loop waiting table  
//		 * 
//		 * for(W wait : ws) {
//			if(wait.getData().equals(variable) && transaction.isYounger(wait.getTransaction())) {
//					return false;
//			} 
//		}
//		 */
//		
//		return true;
//	}
//}
