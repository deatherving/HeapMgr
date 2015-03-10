package heap;

import bufmgr.*;
import global.*;
import chainexception.*;
import heap.HeapFile;
public class HeapScan implements GlobalConst {
    
    private DirectoryPage _dirPage;
	private short _count;
	private RID _rid;
	private int _index;
	private HFPage _dataPage;
	
	public HeapScan(HeapFile hf)
	{
		_index = -1;
		_dataPage = null;
		_rid = null;
		_dirPage = new DirectoryPage();
		Minibase.BufferManager.pinPage(hf._headId, _dirPage, false);
		_count = _dirPage.getEntryCount();
	}
	
	public void close() throws ChainException
	{
		
		_dataPage = null;
		_dirPage = null;
		_count = -1;
		_index = -1;
		_rid = null;
	}
	
	public void finalize() throws Throwable
	{
		if(_dirPage != null)
			close();
	}
	
	public Tuple getNext(RID rid)
	{
		byte[] res;	
		if(_index < _count -1)
		{
			_index ++;
			if(_dataPage == null)
			{
				_dataPage = new HFPage();
			}
			Minibase.BufferManager.pinPage(_dirPage.getPageID(_index), _dataPage, false);
			_rid = _dataPage.firstRecord();
			if(_rid != null)
			{
				res = _dataPage.selectRecord(_rid);
				rid.copyRID(_rid);
				if(res == null)
					return null;
				return new Tuple(res, 0, res.length);
			}
		}
		if(_rid!=null)
		{
			_rid = _dataPage.nextRecord(_rid);
			if(_rid != null)
			{
				res = _dataPage.selectRecord(_rid);
				rid.copyRID(_rid);
				if(res == null)
					return null;
				return new Tuple(res, 0, res.length);
			}
			Minibase.BufferManager.unpinPage(_dataPage.getCurPage(), false);
		}
		if(_dirPage.getNextPage().pid == -1)
		{	
			Minibase.BufferManager.unpinPage(_dirPage.getCurPage(), false);
			return null;
		}
		_index = -1;
		_rid = null;
		PageId next = _dirPage.getNextPage();
		Minibase.BufferManager.unpinPage(_dirPage.getCurPage(), false);
		Minibase.BufferManager.pinPage(next, _dirPage, false);
		_count = _dirPage.getEntryCount();
		
		return getNext(rid);
	}
	public boolean hasNext()
	{

		if((_index < _count -1)||(_rid != null && _dataPage.nextRecord(_rid) !=null ))
			return true;
		
		boolean res = false;
		if(_dirPage.getNextPage().pid !=-1)
			res = false;
		else
			res = true;
		return res;
	}
	
	
}
