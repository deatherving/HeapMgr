package heap;
import global.GlobalConst;

public class Tuple implements GlobalConst{
	
	public byte[] data;
	public int length;
	public int offset;
	private static final int maxSize = GlobalConst.MAX_TUPSIZE;
	public Tuple() 
	{
		data = new byte[maxSize];
		length = 0;
		offset = 0;
	}
	public Tuple(byte[] indata, int inoff, int inlength)
	{
		data = indata;
		offset = inoff;
		length = inlength;
	}
	public int getLength()
	{
		return length;
	}
	public byte[] getTupleByteArray()
	{
		return data;
	}
	public byte[] getData()
	{
		return data;
	}
	
}
