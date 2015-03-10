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
	private PageId endPageId;
	Hashtable<RID,PageId> heapTree;
	TreeMap<Integer,Queue<PageId>> spaceTree;
	
	public HeapFile(String name) {
		headerPageId = new PageId();
		fileName = name;
		numRecords = 0;
		heapTree = new Hashtable<RID,PageId>();
		headerPage = new HFPage();
		endPageId = null;
		spaceTree = new TreeMap<Integer,Queue<PageId>>();
		

		//Queue initialize
			
		//Queue<PageId> obj = new LinkedList<PageId>();
		
		
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
			
			//initialize queue<Pageid>
			Queue<PageId> temp = new LinkedList<PageId>();
			temp.add(headerPageId);
			
			spaceTree.put((int)headerPage.getFreeSpace(),temp);
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
			
			Queue<PageId> temp = new LinkedList<PageId>();
			temp.add(headerPageId);
			spaceTree.put((int) tempHeader.getFreeSpace(), temp);
			
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
				else{
					endPageId = tempHeaderId;
					break;
				}
					
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
		
		//if there is no page available to insert we have to allocate a new page
		if(spaceTree.size() == 0 || spaceTree.lastKey() < recLen){
			// allocate a new page
			HFPage lastPage = new HFPage();
			HFPage newLastPage = new HFPage();
			Minibase.BufferManager.pinPage(endPageId, lastPage, false);
			
			PageId newLastId = Minibase.BufferManager.newPage(newLastPage, 1);
			rid = newLastPage.insertRecord(record);
			lastPage.setNextPage(newLastId);
			newLastPage.setPrevPage(endPageId);
			
			//update heapTree
			heapTree.put(rid, newLastId);
			//update spaceTree
			spaceTree.put((int) newLastPage.getFreeSpace(), newLastId);
			
			//upin those
			Minibase.BufferManager.unpinPage(endPageId, true);
			Minibase.BufferManager.unpinPage(newLastId, true);
		
			return rid;
		}
		//if there is, just insert
		else{
			PageId hfPageId = spaceTree.get(spaceTree.lastKey());
			HFPage hfPage = new HFPage();
			Minibase.BufferManager.pinPage(hfPageId, hfPage, false);
			rid = hfPage.insertRecord(record);
			//update heapTree
			heapTree.put(rid, hfPageId);
			//update spaceTree
			//remove the last one
			spaceTree.remove(spaceTree.lastKey());
			//update the previous one
			spaceTree.put((int) hfPage.getFreeSpace(), hfPageId);
			Minibase.BufferManager.unpinPage(hfPageId, true);
		}
	}
		
		
	
	public Tuple getRecord(RID rid) {
		Tuple tp = null;
		PageId targetId = heapTree.get(rid);
		HFPage targetPage = new HFPage();
		Minibase.BufferManager.pinPage(targetId, targetPage, false);
		
		byte[] data = targetPage.selectRecord(rid);
		tp = new Tuple(data);
		Minibase.BufferManager.unpinPage(targetId, false);
		return tp;
	}
	
	public boolean updateRecord(RID rid, Tuple newRecord) throws ChainException {
		HFPage targetPage = new HFPage();
		PageId targetId = heapTree.get(rid);
		Minibase.BufferManager.pinPage(targetId, targetPage, false);
		
		targetPage.updateRecord(rid, newRecord);
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
