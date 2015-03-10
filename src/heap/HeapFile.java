package heap;

import SpaceNotAvailableException;
import global.*;
import bufmgr.*;
import diskmgr.*;

import java.util.*;

import heap.*;
import chainexception.ChainException;

public class HeapFile {
	private PageId headerPageId;
	private HFPage headerPage;
	private String fileName;
	private int numRecords;
	Hashtable<RID,PageId> heapTree;
	TreeMap<Integer,PageId> spaceTree;
	
	public HeapFile(String name) {
		headerPageId = new PageId();
		fileName = name;
		numRecords = 0;
		heapTree = new Hashtable<RID,PageId>();
		headerPage = new HFPage();
		//get the pageid of headfile
		headerPageId = Minibase.DiskManager.get_file_entry(fileName);
		
		if(headerPageId == null){
			//initial a newpage
			
			
			//allocate the newpage and get pageno
			headerPageId = Minibase.BufferManager.newPage(headerPage, 1);
			
			//add file entry as fileName using pageno
			Minibase.DiskManager.add_file_entry(fileName,headerPageId);
			
			//initial a new heapfilepage
			
			//!!!!!!!!!!!!!I don't know the next prev page
			headerPage.setCurPage(headerPageId);
			headerPage.setPrevPage(null);
			headerPage.setNextPage(null);
			spaceTree.put((int)headerPage.getFreeSpace(),headerPageId);
			Minibase.BufferManager.unpinPage(headerPageId, false);
			
			
		}
		else{
			//get the headerfile into headerPage
			Minibase.BufferManager.pinPage(headerPageId, headerPage, false);
			Minibase.BufferManager.unpinPage(headerPageId, false);
			
			//the traverse header file
			HFPage tempHeader = headerPage;
			PageId tempHeaderId = headerPageId;
			//the first record
			RID rid = tempHeader.firstRecord();
			if(rid != null)
				heapTree.put(rid, tempHeaderId);
			//update space hashmap
			spaceTree.put((int) tempHeader.getFreeSpace(), tempHeaderId);
			numRecords++;
			while(true){
				//get how many records 
				if(rid != null){
					numRecords++;
					rid = tempHeader.nextRecord(rid);
					if(rid != null)
						heapTree.put(rid, tempHeaderId);
					
				}
				// there may be several pages in headerFile
				else if(rid == null && tempHeader.getNextPage().pid != -1){
					tempHeaderId = tempHeader.getNextPage();
					Minibase.BufferManager.pinPage(tempHeaderId, tempHeader, false);
					Minibase.BufferManager.unpinPage(tempHeaderId,true);
					rid = tempHeader.firstRecord();
					//update our space hashmap
					spaceTree.put((int) tempHeader.getFreeSpace(), tempHeaderId);
				}
				else
					break;
			}
		}
	}
		
		
	
		
	
	
	public RID insertRecord(byte[] record) throws ChainException {
		if(record.length > GlobalConst.MAX_TUPSIZE)
			throw new SpaceNotAvailableException(null, "SpaceNotAvailableException");
		RID rid = null;
		int recLen = record.length;
		PageId curId = new PageId(headerPageId.pid);
		HFPage curHfPage = null;
		if(spaceMap)
		
		
		
		
		boolean flag = false;
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		while(curId.pid != -1){
			if(curHfPage.getFreeSpace() >= recLen){
				numRecords++;
				rid = curHfPage.insertRecord(record);
				Minibase.BufferManager.unpinPage(curId, true);
				rid.pageno = curId;
				return rid;
			}
			else if(curHfPage.getNextPage().pid != -1){
				PageId newId = curHfPage.getNextPage();
				Page newPage = new Page();
				Minibase.BufferManager.pinPage(newId, newPage, false);
				curHfPage  = new HFPage(newPage);
				curId = newId;
			}
			else{
				//allocate a new page
				PageId newId = new PageId();
				Page newPage = new Page();
				newId = Minibase.BufferManager.newPage(newPage, 1);
				
				//set old page's next page
				curHfPage.setNextPage(newId);
				//save old page's next page
				Minibase.BufferManager.unpinPage(curId, true);
				
				//move forward the old page to new page
				curHfPage = new HFPage(newPage);
				//update new page's previous page
				curHfPage.setPrevPage(curId);
				curId = newId;
				//save new page's previous page
				Minibase.BufferManager.unpinPage(curId, true);
				
			}
		}
		return rid;
	}
	
	public Tuple getRecord(RID rid) {
		Tuple tp = null;
		HFPage temp = new HFPage();
		Minibase.BufferManager.pinPage(rid.pageno, temp, false);
		byte[] input = temp.selectRecord(rid);
		tp = new Tuple(input);
		return tp;
	}
	
	public boolean updateRecord(RID rid, Tuple newRecord) throws ChainException {
		HFPage temp = new HFPage();
		Minibase.BufferManager.pinPage(rid.pageno, temp, false);
		temp.updateRecord(rid, newRecord);
		Minibase.BufferManager.unpinPage(rid.pageno, true);
		return true;
	}
	
	public boolean deleteRecord(RID rid) {
		
		return true;
	}

	public int getRecCnt() {
		
		return 0;
	}
	
	public HeapScan openScan() {
		
		HeapFile hf = new HeapFile("test");
		HeapScan sc = new HeapScan(hf);
		
		return sc;
	}

}
