package heap;

import chainexception.ChainException;
import global.RID;

public class HeapScan {
	
	protected HeapScan(HeapFile hf) {
		
	}
	
	protected void finalize​() throws Throwable {
		
		
	}
	
	public void close​() throws ChainException {
		
	}
	
	public boolean hasNext​() {
		return true;
	}
	
	public Tuple getNext​(RID rid) {
		Tuple arr = new Tuple();
		return arr;
		
	}

}
