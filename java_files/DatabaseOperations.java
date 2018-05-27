import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

public class DatabaseOperations
{
	public static int pgSize = PageSizeConstant.pageSize;
	
	public static void showDatabase()
	{
		String[] dbList = new File("data").list();
		for(String iter:dbList){
			if(iter.equals("catalog"))
				continue;
			System.out.println(iter);
		}
	}
	
	public static void show()
	{
		String[] arr = new String[0];
		String[] davisBase_table_col = {"table_name"};
		selectFrom("data\\catalog\\davisbase_tables.tbl","davisbase_tables", davisBase_table_col, arr);
	}
	
	public static void createDatabase(String dbName){
		try {
			File dbPath = new File("data\\"+dbName);
			if(dbPath.exists()){
				System.out.println("Database "+ dbName +" already exists");
				return;
			}
			dbPath.mkdir();
			DavisBasePrompt.curDatabase=dbName;
			System.out.println("Database "+dbName+" successfully created.\n");
		}
		catch (SecurityException exception) 
		{
			System.out.println("Error while creating database: "+exception);
		}
	}
	
	public static void createTable(String tableName, String[] columns)
	{
		try{
			String tabPath = "data\\"+DavisBasePrompt.curDatabase+"\\"+tableName;
			File f = new File(tabPath);
			f.mkdir();
			RandomAccessFile raf = new RandomAccessFile(tabPath+"\\"+tableName+".tbl", "rw");
			raf.setLength(pgSize);
			raf.seek(0);
			raf.writeByte(0x0D);
			raf.close();
			
			RandomAccessFile  raf_tab = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");
			int numPages = Utility.countPages(raf_tab);
			int pg = 1;
			for(int i = 1; i <= numPages; i++)
			{
				int right = BplusTree.fetchLastRight(raf_tab, i);
				if(right == 0)
					pg = i;
			}
			int[] list = BplusTree.fetchKeys(raf_tab, pg);
			int key_value = list[0];
			for(int i = 0; i < list.length; i++){
				if(key_value < list[i])
					key_value = list[i];
			}
			raf_tab.close();
			
			String[] arr = {};
			String[] array = {Integer.toString(key_value+1), DavisBasePrompt.curDatabase+"."+tableName};
			insertIntoCatalog("davisbase_tables", array);

			RandomAccessFile raf_col = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			Storage bf = new Storage();
			String[] fields = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			filter(raf_col, arr, fields, bf);
			key_value = bf.data.size();

			for(int c = 0; c < columns.length; c++){
				key_value = key_value + 1;
				String[] col = columns[c].split(" ");
				String var = "YES";
				if(col.length > 2)
					var = "NO";
				String[] insert = {Integer.toString(key_value), DavisBasePrompt.curDatabase+"."+tableName, col[0], col[1].toUpperCase(), Integer.toString(c+1), var};
				insertIntoCatalog("davisbase_columns", insert);
			}
			raf_col.close();
			
			
			
		}catch(Exception exception){
			System.out.println("Error Creating table:\n" + exception);
		}
	}

	
	public static void dropDatabase(String dbName){
		File f= new File("data\\"+dbName);
		for(String iter:f.list()){
			if(iter.equals("catalog") || iter.equals("user_data"))
				continue;
			dropTable(iter,dbName);
		}
		File file = new File("data", dbName); 
		file.delete();
	}

	public static void dropTable(String table,String db)
	{
		RandomAccessFile file = null;
		try{
			file = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");
			int cnt = Utility.countPages(file);
			for(int pg = 1; pg <= cnt; pg ++)
			{
				file.seek((pg-1)*pgSize);
				byte type = file.readByte();
				if(type == 0x05)
					continue;
				else{
					short[] record_array = BplusTree.fetchRows(file, pg);
					int iter = 0;
					for(int rec = 0; rec < record_array.length; rec++){
						long record_loc = BplusTree.fetchRecPos(file, pg, rec);
						String[] payLoad = Utility.fetchPL(file, record_loc);
						String tab = payLoad[1];
						if(!tab.equals(DavisBasePrompt.curDatabase+"."+table)){
							BplusTree.setRecOffset(file, pg, iter, record_array[rec]);
							iter++;
						}
					}
					BplusTree.setRecordNo(file, pg, (byte)iter);
				}
			}

			file = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			cnt = Utility.countPages(file);
			for(int pg = 1; pg <= cnt; pg ++){
				file.seek((pg-1)*pgSize);
				byte type = file.readByte();
				if(type == 0x05)
					continue;
				else{
					short[] record_array = BplusTree.fetchRows(file, pg);
					int iter = 0;
					for(int rec = 0; rec < record_array.length; rec++){
						long record_loc = BplusTree.fetchRecPos(file, pg, rec);
						String[] payLoad = Utility.fetchPL(file, record_loc);
						String tab = payLoad[1];
						if(!tab.equals(DavisBasePrompt.curDatabase+"."+table))
						{
							BplusTree.setRecOffset(file, pg, iter, record_array[rec]);
							iter++;
						}
					}
					BplusTree.setRecordNo(file, pg, (byte)iter);
				}
			}
			file.close();
			
			File tb = new File("data\\"+db+"\\"+table);
			for(String f:tb.list()){
				File files = new File("data\\"+db+"\\"+table,f);
				files.delete();
			}
			tb = new File("data\\"+db, table); 
			tb.delete();
		}
		catch(Exception exception)
		{
			System.out.println("Error dropping the table: "+ exception);
		}

	}
	
	public static void insertIntoTable(RandomAccessFile file, String name, String[] fields)
	{
		String[] nullCol = Utility.isColNullable(name);

		for(int i = 0; i < nullCol.length; i++)
			if(nullCol[i].equals("NO") && fields[i].equals("null")){
				System.out.println("NULL Value Constraint Violation\n");
				return;
			}
		int id = new Integer(fields[0]);
		int pgNo = Utility.findId(file, id);
		if(pgNo != 0)
			if(BplusTree.hasKey(file, pgNo, id))
			{
				System.out.println("Uniqueness (Primary key) Constraint Violation");
				return;
			}
		if(pgNo == 0)
			pgNo = 1;

		String[] dt = Utility.fetchDt(name);
		byte[] arr = new byte[dt.length-1];
		short payLoadSz = (short) Utility.computePlSz(name, fields, arr);
		int offset = BplusTree.isLeafSpace(file, pgNo, payLoadSz + 6);

		if(offset != -1)
		{
			BplusTree.insertLeafRecord(file, pgNo, offset, payLoadSz, id, arr, fields,name);
		}
		else
		{
			BplusTree.divideLeafTable(file, pgNo);
			insertIntoTable(file, name, fields);
		}
	}

	public static void insertIntoCatalog(String table, String[] values)
	{
		RandomAccessFile file = null;
		try
		{
			file = new RandomAccessFile("data\\catalog\\"+table+".tbl", "rw");
			insertIntoTable(file, table, values);

		}
		catch(Exception e)
		{
			System.out.println("Error in inserting the data:\n"+e);
		}
		finally{
			if(file!=null){
				try {
					file.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void deleteRow(String name, String[] array)
	{
		RandomAccessFile file = null;
		int id = -1;
		Storage bf = new Storage();
		boolean flag=false;
		try {
			String[] fields =Utility.fetchFieldNm(name), dataTy = Utility.fetchDt(name);
			file = new RandomAccessFile("data\\"+DavisBasePrompt.curDatabase+"\\"+name+"\\"+name+".tbl", "rw");
			selectCondition(file, array, fields, dataTy, bf);
			for(String[] iter : bf.data.values()){
				if(flag)
					break;
				for(int j = 0; j < iter.length; j++)
					if(bf.fiedlNm[j].equals(array[0]) && iter[j].equals(array[2])){
						id =(Integer.parseInt(iter[0]));							
						flag = true;
						break;
					}
			
			}
				
			int cnt = Utility.countPages(file);
			int pg = 1;

			for(int pgs = 1; pgs <= cnt; pgs++)
				if(BplusTree.hasKey(file, pgs, id)){
					pg = pgs;
				}
			int key_val = 0;
			int[] keys= BplusTree.fetchKeys(file, pg);
			for(int i = 0; i < keys.length; i++){
				if(keys[i] == id)
					key_val = i;
			}
			int loc = BplusTree.fetchRecOffset(file, pg, key_val);
			long record_pos = BplusTree.fetchRecPos(file, pg, key_val);
			String[] col_nms = Utility.fetchFieldNm(name);
			String[] payL_array = Utility.fetchPL(file, record_pos);
			byte[] arr = new byte[col_nms.length-1];
			int payL_size = Utility.computePlSz(name, payL_array, arr);
			file.seek((pg-1)*pgSize+loc);
			file.writeShort(payL_size);
			file.writeInt(-10000);
			

		} catch (Exception excep) {
			excep.printStackTrace();
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
		
	}
	
	public static void selectFrom(String file, String tableName, String[] columns, String[] arr)
	{
		RandomAccessFile raf = null;
		Storage bf = new Storage();
		try
		{
			raf = new RandomAccessFile(file, "rw");
			String[] cols = Utility.fetchFieldNm(tableName);
			String[] dt = Utility.fetchDt(tableName);
			selectCondition(raf, arr, cols, dt, bf);
			bf.print(columns);
			raf.close();
		}
		catch(Exception exception)
		{
			System.out.println("Error while executing select query: "+exception);
		}
	}
	
	public static void updateTable(String name, String[] str, String[] arr)
	{
		RandomAccessFile file = null;
		try{
			file = new RandomAccessFile("data\\"+DavisBasePrompt.curDatabase+"\\"+name+"\\"+name+".tbl", "rw");
			Storage bf = new Storage();
			ArrayList<Integer> list = new ArrayList<Integer>();
			String[] cols = Utility.fetchFieldNm(name);
			String[] dt = Utility.fetchDt(name);
			selectCondition(file, arr,cols, dt, bf);
			
			for(String[] iter : bf.data.values()){
				
				for(int k = 0; k < iter.length; k++)
					if(bf.fiedlNm[k].equals(arr[0]) && iter[k].equals(arr[2])){
						list.add(Integer.parseInt(iter[0]));							
						break;
					}
			}
				
			
			for(int id:list){
				int cnt = Utility.countPages(file);
				int pg = 1;
	
				for(int pgs = 1; pgs <= cnt; pgs++)
					if(BplusTree.hasKey(file, pgs, id)){
						pg = pgs;
					}
				int val = 0;
				int[] keys = BplusTree.fetchKeys(file, pg);
				for(int k = 0; k < keys.length; k++)
					if(keys[k] == id)
						val = k;
				int loc = BplusTree.fetchRecOffset(file, pg, val);
				long record_pos = BplusTree.fetchRecPos(file, pg, val);
				String[] col_nms = Utility.fetchFieldNm(name);
				String[] payLoad = Utility.fetchPL(file,record_pos );
				for(int i=0; i < dt.length; i++)
					if(dt[i].equals("DATE") || dt[i].equals("DATETIME"))
						payLoad[i] = "'"+payLoad[i]+"'";
	
				for(int i = 0; i < col_nms.length; i++)
					if(col_nms[i].equals(str[0]))
						val = i;
				payLoad[val] = str[2];
	
				String[] nullCol = Utility.isColNullable(name);
	
				for(int i = 0; i < nullCol.length; i++){
					if(payLoad[i].equals("null") && nullCol[i].equals("NO")){
						System.out.println("NULL value constraint violation\n");
						return;
					}
				}
				byte[] ar = new byte[col_nms.length-1];
				BplusTree.updateLeafRecord(file, pg, loc, Utility.computePlSz(name, payLoad, ar), id, ar, payLoad,name);
			}
			file.close();

		}catch(Exception excep){
			System.out.println("Error at update:\n"+excep);
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
	}
	
	
	public static boolean isDBExists(String dbName){
		File f = new File("data\\"+dbName);
		if(f.exists()){
			DavisBasePrompt.curDatabase=dbName;
			return true;
		}
		return false;
	}

	
	public static void selectCondition(RandomAccessFile file, String[] arr, String[] col_name, String[] datatype, Storage store){
		try{
			int cnt = Utility.countPages(file);
			for(int pg = 1; pg <= cnt; pg++)
			{
				file.seek((pg-1)*pgSize);
				byte pgType = file.readByte();
				if(pgType== 0x05)
					continue;
				else{
					byte recs_cnt = BplusTree.fetchRecordNo(file, pg);
					for(int iter=0; iter <recs_cnt ; iter++){
						long record_loc = BplusTree.fetchRecPos(file, pg, iter);
						file.seek(record_loc+2); 
						int rowid = file.readInt();
						file.readByte();
						String[] pl = Utility.fetchPL(file, record_loc);
						for(int j=0; j < datatype.length; j++)
							if(datatype[j].equals("DATETIME") || datatype[j].equals("DATE"))
								pl[j] = "'"+pl[j]+"'";
						
						boolean check = Utility.opCheck(pl, rowid, arr, col_name);

						for(int j=0; j < datatype.length; j++)
							if(datatype[j].equals("DATETIME") || datatype[j].equals("DATE"))
								pl[j] = pl[j].substring(1, pl[j].length()-1);

						if(check)
							store.insert(rowid, pl);
					}
				}
			}

			store.fiedlNm = col_name;
			store.format = new int[col_name.length];

		}
		catch(Exception e)
		{
			System.out.println("Error at selectFilter");
			e.printStackTrace();
		}

	}

	public static void filter(RandomAccessFile file, String[] arr, String[] col_name, Storage store){
		try{
			int cnt = Utility.countPages(file);
			for(int pg = 1; pg <= cnt; pg++){
				file.seek((pg-1)*pgSize);
				if(file.readByte() == 0x05)
					continue;
				else{
					byte recs_cnt = BplusTree.fetchRecordNo(file, pg);
					for(int i=0; i < recs_cnt; i++){
						long record_pos = BplusTree.fetchRecPos(file, pg, i);
						file.seek(record_pos+2);
						int rowid = file.readInt(); 
						int temp = new Integer(file.readByte());
						String[] pl = Utility.fetchPL(file, record_pos);

						boolean check = Utility.opCheck(pl, rowid, arr, col_name);
						if(check)
							store.insert(rowid, pl);
					}
				}
			}

			store.fiedlNm = col_name;
			store.format = new int[col_name.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}

	
}
