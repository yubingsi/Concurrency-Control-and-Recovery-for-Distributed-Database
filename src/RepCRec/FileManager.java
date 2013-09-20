package RepCRec;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public abstract class FileManager {
	private static List<String> instrString;
		
	public static void readFile(String filePath) throws Exception {
		instrString = new ArrayList<String>();		
		try {
			BufferedReader inputf = new BufferedReader(new FileReader(filePath));
			String tmp = null;
			while ((tmp = inputf.readLine()) != null) {
				tmp = tmp.trim();
				instrString.add(tmp);
			}
			inputf.close();
		} catch (IOException io) {}				
	}
	
	public static List<Operation> getOperations(String filePath) throws Exception {
		readFile(filePath);
		List<Operation> operations = new ArrayList<Operation>();
		for(int i = 0; i<instrString.size(); i++) {
			String s = instrString.get(i);
			if (s.contains(";")) {
				String[] tmp = s.split(";");
				for(String st: tmp) {
					st = st.trim();
					operations.add(new Operation(st, i+1));
				}
			}
			else {
				s = s.trim();
				operations.add(new Operation(s, i+1));
			}
		}
		return operations;
	}

}
