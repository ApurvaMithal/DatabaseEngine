import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.text.SimpleDateFormat;

public class Utility {
	public static int pgSize = PageSizeConstant.pageSize;
	
	public static String[] fetchPL(RandomAccessFile file, long off)
	{
		String[] pl = new String[0];
		try{
			file.seek(off);
			file.readShort();
			int id = file.readInt();
			int numOfCol = file.readByte();
			byte[] colSize = new byte[numOfCol];
			file.read(colSize);
			pl = new String[numOfCol+1];
			pl[0] = Integer.toString(id);
			for(int col=1; col <= numOfCol; col++){
				switch(colSize[col-1]){
					case 0x00:  
						file.readByte();
						pl[col] = "null";
						break;

					case 0x01:  
						file.readShort();
						pl[col] = "null";
						break;

					case 0x02:  
						file.readInt();
						pl[col] = "null";
						break;

					case 0x03:  
						file.readLong();
						pl[col] = "null";
						break;

					case 0x04:  
						pl[col] = Integer.toString(file.readByte());
						break;

					case 0x05:  
						pl[col] = Integer.toString(file.readShort());
						break;

					case 0x06:  
						pl[col] = Integer.toString(file.readInt());
						break;

					case 0x07:  
						pl[col] = Long.toString(file.readLong());
						break;

					case 0x08:  
						pl[col] = String.valueOf(file.readFloat());
						break;

					case 0x09:  
						pl[col] = String.valueOf(file.readDouble());
						break;

					case 0x0A:  
						pl[col] = new SimpleDateFormat ("yyyy-MM-dd_HH:mm:ss").format(new Date(file.readLong()));
						break;

					case 0x0B:  
						pl[col] = new SimpleDateFormat ("yyyy-MM-dd_HH:mm:ss").format(new Date(file.readLong())).substring(0,10);
						break;

					default:    
						int k = new Integer(colSize[col-1]-0x0C);
						byte[] array = new byte[k];
						for(int j = 0; j < k; j++)
							array[j] = file.readByte();
						pl[col] = new String(array);
						break;
				}
			}

		}
		catch(Exception e)
		{
			System.out.println("Error while fetching payload data:\n"+ e);
		}
		return pl;
	}


	
	public static int computePlSz(String name, String[] data, byte[] arr)
	{
		String[] dt = fetchDt(name);
		int sz = dt.length;
		for(int iter = 1; iter < dt.length; iter++){
			byte b = serialTypeCode(data[iter], dt[iter]);
			arr[iter - 1] = b;
			sz = sz + colSize(b);
		}
		return sz;
	}
	
	public static byte serialTypeCode(String str, String dt)
	{
		if(str.equals("null"))
		{
			if(dt.equals("TINYINT"))
				return 0x00;
			else if(dt.equals("SMALLINT"))
				return 0x01;
			else if(dt.equals("INT"))
				return 0x02;
			else if(dt.equals("BIGINT"))
				return 0x03;
			else if(dt.equals("REAL"))
				 return 0x02;
			else if(dt.equals("DOUBLE"))
				return 0x03;
			else if(dt.equals("DATETIME"))
				return 0x03;
			else if(dt.equals("DATE"))
				return 0x03;
			else if(dt.equals("TEXT"))
				return 0x03;
			else
				return 0x00;
									
		}
		else
		{
			if(dt.equals("TINYINT"))
				return 0x04;
			else if(dt.equals("SMALLINT"))
				return 0x05;
			else if(dt.equals("INT"))
				return 0x06;
			else if(dt.equals("BIGINT"))
				return 0x07;
			else if(dt.equals("REAL"))
				 return 0x08;
			else if(dt.equals("DOUBLE"))
				return 0x09;
			else if(dt.equals("DATETIME"))
				return 0x0A;
			else if(dt.equals("DATE"))
				return 0x0B;
			else if(dt.equals("TEXT"))
				return (byte)(str.length()+0x0C);
			else
				return 0x00;
		}
	}

	public static short colSize(byte b)
	{
		switch(b)
		{
			case 0x00: return 1;
			case 0x01: return 2;
			case 0x02: return 4;
			case 0x03: return 8;
			case 0x04: return 1;
			case 0x05: return 2;
			case 0x06: return 4;
			case 0x07: return 8;
			case 0x08: return 4;
			case 0x09: return 8;
			case 0x0A: return 8;
			case 0x0B: return 8;
			default:   return (short)(b - 0x0C);
		}
	}

	
	public static int findId(RandomAccessFile file, int id)
	{
		
		try{
			int count = countPages(file);
			for(int pg = 1; pg <= count; pg++){
				file.seek((pg - 1)*pgSize);
				byte b = file.readByte();
				if(b == 0x0D){
					int[] id_array = BplusTree.fetchKeys(file, pg);
					if(id_array.length == 0)
						return 0;
					
					int right = BplusTree.fetchLastRight(file, pg);
					
					if((right == 0 && id_array[id_array.length - 1] < id)||(id_array[0] <= id && id <= id_array[id_array.length - 1])){
						return pg;
					}
				}
			}
		}
		catch(Exception exception)
		{
			System.out.println("Error while searching key: "+exception);
		}

		return 1;
	}


	public static String[] fetchDt(String name)
	{
		RandomAccessFile file = null;
		String[] dt = new String[0];
		Storage st = new Storage();
		ArrayList<String> list = new ArrayList<String>();
		try{
			file = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			String[] cols = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!name.equals("davisbase_tables") && !name.equals("davisbase_columns"))
				name = DavisBasePrompt.curDatabase+"."+name;
			String[] arr = {"table_name","=",name};
			DatabaseOperations.filter(file, arr, cols, st);
			HashMap<Integer, String[]> data = st.data;

			for(String[] iter : data.values()){
				list.add(iter[3]);
			}
			dt = list.toArray(new String[list.size()]);
			return dt;
		}
		catch(Exception e)
		{
			System.out.println("Error in getting the data type");
			System.out.println(e);
		}
		finally{
			if(file!=null)
				try {
					file.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return dt;
	}

	public static String[] fetchFieldNm(String tableName)
	{
		String[] col_array = new String[0];
		RandomAccessFile file = null;
		try{
			file = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			Storage bf = new Storage();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!tableName.equals("davisbase_tables") && !tableName.equals("davisbase_columns"))
				tableName = DavisBasePrompt.curDatabase+"."+tableName;
			String[] arr = {"table_name","=",tableName};
			DatabaseOperations.filter(file, arr, columnName, bf);
			ArrayList<String> array = new ArrayList<String>();
			for(String[] d : bf.data.values()){
				array.add(d[2]);
			}
			col_array = array.toArray(new String[array.size()]);
			return col_array;
		}
		catch(Exception exc)
		{
			System.out.println("Error while fetching the column name:\n"+exc);
		}
		finally{
			if(file!=null)
				try {
					file.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return col_array;
	}

	public static String[] isColNullable(String table)
	{
		RandomAccessFile file = null;
		String[] isNull_array = new String[0];
		Storage bf = new Storage();
		try{
			file = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			String[] fieldNm = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!table.equals("davisbase_tables") && !table.equals("davisbase_columns"))
				table = DavisBasePrompt.curDatabase+"."+table;
			String[] arr = {"table_name","=",table};
			DatabaseOperations.filter(file, arr, fieldNm, bf);
			ArrayList<String> list = new ArrayList<String>();
			for(String[] d : bf.data.values())
			{
				list.add(d[5]);
			}
			isNull_array = list.toArray(new String[list.size()]);
			return isNull_array;
		}catch(Exception excep){
			System.out.println("Error in isColNullable:\n"+excep);
		}
		finally{
			if(file!=null)
				try {
					file.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return isNull_array;
	}

	public static int countPages(RandomAccessFile file)
	{
		int count = 0;
		try
		{
			count = (int)(file.length()/(new Long(pgSize)));
		}
		catch(Exception e)
		{
			System.out.println("Error while counting pages");
		}

		return count;
	}

	public static boolean opCheck(String[] payL, int key, String[] arr, String[] fields)
	{

		boolean flag = false;
		if(arr.length == 0)
		{
			flag = true;
		}
		else{
			int loc = 1;
			for(int j = 0; j < fields.length; j++){
				if(fields[j].equals(arr[0])){
					loc = j + 1;
					break;
				}
			}
			String operator = arr[1];
			String str = arr[2];
			if(loc == 1)
			{
				if(operator.equals("=")){
					if(Integer.parseInt(str) == key) 
						flag = true;
					else
						flag = false;
				}
				if(operator.equals(">")){
					if(Integer.parseInt(str) < key) 
						flag = true;
					else
						flag = false;
				}
				if(operator.equals("<")){
					if(Integer.parseInt(str) > key) 
						flag = true;
					else
						flag = false;
				}
				if(operator.equals(">=")){
					if(Integer.parseInt(str) <= key) 
						flag = true;
					else
						flag = false;	
				}
				if(operator.equals("<=")){
					if(Integer.parseInt(str) >= key) 
						flag = true;
					else
						flag = false;	
				}
				if(operator.equals("<>")){
					if(key != Integer.parseInt(str))  
						flag = true;
					else
						flag = false;	
				}
			
			}else{
				if(str.equals(payL[loc-1]))
					flag = true;
				else
					flag = false;
			}
		}
		return flag;
	}
}


