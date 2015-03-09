package heap;

import global.RID;

import chainexception.ChainException;

public class HeapFile {
	
	public HeapFile(String name) {
		
	}
	
	public RID insertRecord​(byte[] record) throws ChainException {
		RID a = new RID();
		
		return a;
	}
	
	public Tuple getRecord​(RID rid) {
		Tuple tp = new Tuple();
		return tp;
	}
	
	public boolean updateRecord​(RID rid, Tuple newRecord) throws ChainException {
		
		return true;
	}
	
	public boolean deleteRecord​(RID rid) {
		
		return true;
	}

	public int getRecCnt() {
		
		return 0;
	}
	
	public HeapScan openScan​() {
		
		HeapFile hf = new HeapFile("test");
		HeapScan sc = new HeapScan(hf);
		
		return sc;
	}

}
