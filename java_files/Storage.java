import java.util.HashMap;


public class Storage
{
	public String[] fiedlNm; 
	public int[] format;
	public HashMap<Integer, String[]> data;
	public int tupleCnt; 
	 
	

	public Storage()
	{
		
		data = new HashMap<Integer, String[]>();
		tupleCnt = 0;
	}

	public void insert(int rowid, String[] val)
	{
		data.put(rowid, val);
		tupleCnt = tupleCnt + 1;
	}

	public void changeForm()
	{
		for(int j = 0; j < format.length; j++)
			format[j] = fiedlNm[j].length();
		for(String[] iter : data.values()){
			for(int j = 0; j < iter.length; j++)
				if(format[j] < iter[j].length())
					format[j] = iter[j].length();
		}
	}

	public String pattern(int sz, String str) 
	{
		return String.format("%-"+(sz+3)+"s", str);
	}

	public String sentence(String s,int len) 
	{
		String str = "";
		for(int j=0;j<len;j++) 
		{
			str += s;
		}
		return str;
	}

	public void print(String[] fields)
	{
		if(tupleCnt == 0)
		{
			System.out.println("");
		}
		else
		{
			changeForm();
			if(fields[0].equals("*"))
			{
				for(int f: format)
					System.out.print(sentence("-", f+3));
				System.out.println();
				for(int i = 0; i < fiedlNm.length; i++)
					System.out.print(pattern(format[i], fiedlNm[i])+"|");
				System.out.println();
				for(int f: format)
					System.out.print(sentence("-", f+3));
				System.out.println();
				for(String[] iter : data.values()){
					if(iter[0].equals("-10000"))
						continue;
					for(int k = 0; k < iter.length; k++)
						System.out.print(pattern(format[k], iter[k])+"|");
					System.out.println();
				}
				System.out.println();
			}
			else
			{
				int[] array = new int[fields.length];
				for(int k = 0; k < fields.length; k++)
					for(int j = 0; j < fiedlNm.length; j++)
						if(fields[k].equals(fiedlNm[j]))
							array[k] = j;
				for(int j = 0; j < array.length; j++)
					System.out.print(sentence("-", format[array[j]]+3));
				System.out.println();
				for(int i= 0; i < array.length; i++)
					System.out.print(pattern(format[array[i]], fiedlNm[array[i]])+"|");
				System.out.println();
				for(int i = 0; i < array.length; i++)
					System.out.print(sentence("-", format[array[i]]+3));
				System.out.println();
				for(String[] iter : data.values())
				{
					for(int k = 0; k < array.length; k++)
						System.out.print(pattern(format[array[k]], iter[array[k]])+"|");
					System.out.println();
				}
				System.out.println();
			}
		}
	}
}