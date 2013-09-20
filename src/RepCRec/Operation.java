package RepCRec;

import java.util.ArrayList;
import java.util.List;

public class Operation {
//	private boolean isRead;
//	private Transaction transaction;
//	private Data variable;
//	private int dataValue;
//	private enum OperationType {BEGIN,READ,WRITE,};
//	
//	public Operation(boolean isRead, Transaction transaction, Data variable) {
//		this.isRead = isRead;
//		this.transaction = transaction;
//		this.variable = variable;
//	}
//	
//	public Operation setDataValue(int value) {
//		return this;
//	}
//	
//	public Transaction getTransaction() {
//		return transaction;
//	}
//	
//	public Data getVariable() {
//		return variable;
//	}
	
	private String operation;
	private int timeStamp;
	enum Type {BEGIN, BEGINRO, WRITE, READ, DUMPALL, DUMPSITE, DUMPDATA, END, FAIL, RECOVER }
	private Type type;
	private boolean isDone = false; // true if is done or the transaction abort or false 
	
	public Operation(String operation, int timeStamp) {
		this.operation = operation;
		this.timeStamp = timeStamp;
		setType();
	}
	
	public Type getType() {
		return type;
	}
	
	public int gettimeStamp() {
		return timeStamp;
	}
	
	public void setDone(){
		isDone = true;
	}
	
	public boolean isDone() {
		return isDone;
	}
	
	private void setType() {
		if(operation.startsWith("begin(T")) {
			type = Type.BEGIN;
		}
		else if(operation.startsWith("beginRO(T")) {
			type = Type.BEGINRO;
		}
		else if(operation.startsWith("R")) {
			type = Type.READ;
		}
		else if(operation.startsWith("W")) {
			type = Type.WRITE;
		}
		else if(operation.startsWith("dump(")) {
			if(operation.equals("dump()")) {
				type = Type.DUMPALL;
			}
			else if(operation.startsWith("dump(x")) {
				type = Type.DUMPDATA;
			}
			else {
				type = Type.DUMPSITE;
			}
		}
		else if(operation.startsWith("end")) {
			type = Type.END;
		}
		else if(operation.startsWith("fail")) {
			type = Type.FAIL;
		}
		else if(operation.startsWith("recover")) {
			type = Type.RECOVER;
		}
		else {
			type = Type.END;
		}		
	}
	
	public String getString() {
		return operation;
	}
	
	
	/**
	 * different meaning in different operation type, but meaningless in READ or WRITE type
	 * @return the sole numeric elements in operation
	 */
	public int getSoleNumeric() {
		return getNumeric(operation);
	}
	
	public List<Integer> getRWinfo() {
		if(type.equals(Type.READ) || type.equals(Type.WRITE)) {
			return getNumericList(operation);
		}
		return null;
	}
	
	private int getNumeric(String s) {
		return Integer.parseInt(s.replaceAll("[^\\d.]", ""));
	}
	
	private List<Integer> getNumericList(String s) {
		List<Integer> numericList = new ArrayList<Integer> ();
		String[] tmp = s.split(",");
		for(String ss: tmp) {
			ss = ss.trim();
			numericList.add(getNumeric(ss));
		}
		return numericList;
	}
	
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if(!(obj instanceof Operation))
			return false;
		Operation op = (Operation) obj;
		return (operation.equals(op.getString()) && timeStamp == op.gettimeStamp());
	}
	
	@Override
	public int hashCode() {
		int result = 17;
		result = 31 * result + operation.hashCode();
		result = 31 * result + timeStamp;
		return result;
	}
	
	
	
	

}
