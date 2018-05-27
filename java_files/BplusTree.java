
import java.io.RandomAccessFile;
import java.util.Date;
import java.text.SimpleDateFormat;

public class BplusTree{
	public static int pgSize = PageSizeConstant.pageSize;


	public static int createInteriorPg(RandomAccessFile file)
	{
		int cnt = 0;
		try{
			cnt = (int)(file.length()/(new Long(pgSize)));
			cnt += 1;
			file.setLength(pgSize * cnt);
			file.seek((cnt-1)*pgSize);
			file.writeByte(0x05);  
		}catch(Exception e){
			System.out.println("Error while creating Interior Pages\n" + e);
		}

		return cnt;
	}

	public static int createLeafPage(RandomAccessFile file)
	{
		int cnt = 0;
		try{
			cnt = (int)(file.length()/(new Long(pgSize)));
			cnt += 1;
			file.setLength(pgSize * cnt);
			file.seek((cnt-1)*pgSize);
			file.writeByte(0x0D);  
		}
		catch(Exception e)
		{
			System.out.println("Error while creating leaf pages\n"+e);
		}

		return cnt;

	}

	public static void divideInteriorPg(RandomAccessFile file, int cur_page, int new_page)
	{
		try{
			int recs_cnt = fetchRecordNo(file, cur_page);
			int middle = (int) Math.ceil((double) recs_cnt / 2);
			int left = middle - 1;
			int right = recs_cnt - left - 1;
			short data_sz = 512;

			for(int iter = left+1; iter < recs_cnt; iter++){
				short rec_sz = 8;
				long rec_pos = fetchRecPos(file, cur_page, iter);
				data_sz = (short)(data_sz - rec_sz);
				file.seek(rec_pos);
				byte[] rec = new byte[rec_sz];
				file.read(rec);
				file.seek((new_page-1)*pgSize+data_sz);
				file.write(rec);
				file.seek(rec_pos);
				int page = file.readInt();
				setParent(file, page, new_page);
				setRecOffset(file, new_page, iter - (left + 1), data_sz);
			}
			int  right_most= fetchLastRight(file, cur_page);
			setLastRight(file, new_page, right_most);
			long centRec_pos = fetchRecPos(file, cur_page, middle - 1);
			file.seek(centRec_pos);
			right_most = file.readInt();
			setLastRight(file, cur_page, right_most);
			file.seek((new_page-1)*pgSize+2);
			file.writeShort(data_sz);
			short loc = fetchRecOffset(file, cur_page, left-1);
			file.seek((cur_page-1)*pgSize+2);
			file.writeShort(loc);
			int par = getParent(file, cur_page);
			setParent(file, new_page, par);
			byte b = (byte) left;
			setRecordNo(file, cur_page, b);
			b = (byte) right;
			setRecordNo(file, new_page, b);
		}catch(Exception e){
			System.out.println("Error while splitting interior page");
		}
	}
	
	
	public static int divideInteriorTable(RandomAccessFile file, int page){
		int cur_pg = createInteriorPg(file);
		int mid_key = searchMidId(file, page);
		divideInteriorPg(file, page, cur_pg);
		int par = getParent(file, page);
		if(par == 0){
			int root = createInteriorPg(file);
			setParent(file, page, root);
			setParent(file, cur_pg, root);
			setLastRight(file, root, cur_pg);
			addInteriorRecord(file, root, page, mid_key);
			return root;
		}else{
			long ptr = fetchPgPtr(file, page, par);
			setPgPtr(file, ptr, par, cur_pg);
			addInteriorRecord(file, par, page, mid_key);
			sorting(file, par);
			return par;
		}
	}
	

	public static void divideLeafPage(RandomAccessFile file, int cur_pg, int new_page){
		try{
			int data_sz = 512;
			int recs_cnt = fetchRecordNo(file, cur_pg);
			int mid = (int) Math.ceil((double) recs_cnt / 2);
			int left = mid - 1;
			int right = recs_cnt - left;
			
			for(int iter = left; iter < recs_cnt; iter++){
				long rec_pos = fetchRecPos(file, cur_pg, iter);
				file.seek(rec_pos);
				int rec_sz = file.readShort()+6;
				data_sz = data_sz - rec_sz;
				file.seek(rec_pos);
				byte[] rec = new byte[rec_sz];
				file.read(rec);
				file.seek((new_page-1)*pgSize+data_sz);
				file.write(rec);
				setRecOffset(file, new_page, iter - left, data_sz);
			}
			file.seek((new_page-1)*pgSize+2);
			file.writeShort(data_sz);
			short loc = fetchRecOffset(file, cur_pg, left-1);
			file.seek((cur_pg-1)*pgSize+2);
			file.writeShort(loc);

			int right_most = fetchLastRight(file, cur_pg);
			setLastRight(file, new_page, right_most);
			setLastRight(file, cur_pg, new_page);

			int par = getParent(file, cur_pg);
			setParent(file, new_page, par);

			byte b = (byte) left;
			setRecordNo(file, cur_pg, b);
			b = (byte) right;
			setRecordNo(file, new_page, b);
		}catch(Exception e){
			System.out.println("Error while splitting leaf page:\n"+e);
		}
	}
	
	
	public static void divideLeafTable(RandomAccessFile file, int pg){
		int cur_pg = createLeafPage(file);
		int mid_key = searchMidId(file, pg);
		divideLeafPage(file, pg, cur_pg);
		int par = getParent(file, pg);
		if(par == 0)
		{
			int root = createInteriorPg(file);
			setParent(file, pg, root);
			setParent(file, cur_pg, root);
			setLastRight(file, root, cur_pg);
			addInteriorRecord(file, root, pg, mid_key);
		}
		else
		{
			long ptr = fetchPgPtr(file, pg, par);
			setPgPtr(file, ptr, par, cur_pg);
			addInteriorRecord(file, par, pg, mid_key);
			sorting(file, par);
			while(isInteriorSpace(file, par)){
				par = divideInteriorTable(file, par);
			}
		}
	}

	public static void addInteriorRecord(RandomAccessFile file, int page, int child, int key)
	{
		try{
			file.seek((page-1)*pgSize+2);
			short data = file.readShort();
			if(data == 0)
				data = 512;
			data = (short)(data - 8);
			file.seek((page-1)*pgSize+data);
			file.writeInt(child);
			file.writeInt(key);
			file.seek((page-1)*pgSize+2);
			file.writeShort(data);
			byte recs_cnt = fetchRecordNo(file, page);
			setRecOffset(file, page ,recs_cnt, data);
			recs_cnt = (byte) (recs_cnt + 1);
			setRecordNo(file, page, recs_cnt);

		}catch(Exception e){
			System.out.println("Error while inserting interior cell");
		}
	}

	public static void insertLeafRecord(RandomAccessFile file, int pg, int loc, short payLoadSz, int id, byte[] arr, String[] data, String name){
		try{
			String str;
			file.seek((pg-1)*pgSize+loc);
			String[] field = Utility.fetchFieldNm(name);
			if(!name.equals("davisbase_columns") && !name.equals("davisbase_tables")){
				
				RandomAccessFile rafInd = new RandomAccessFile("data\\"+DavisBasePrompt.curDatabase+"\\"+name+"\\"+field[0]+".ndx", "rw");
				rafInd.seek(rafInd.length());
				rafInd.writeInt(id);
				rafInd.writeLong(file.getFilePointer());
				rafInd.close();
				
				for(int j= 1; j < data.length; j++)					
				{
					rafInd = new RandomAccessFile("data\\"+DavisBasePrompt.curDatabase+"\\"+name+"\\"+field[j]+".ndx", "rw");
					rafInd.seek(rafInd.length());
					switch(arr[j-1]){
						case 0x00:
							rafInd.writeByte(0);
							break;
						case 0x01:
							rafInd.writeShort(0);
							break;
						case 0x02:
							rafInd.writeInt(0);
							break;
						case 0x03:
							rafInd.writeLong(0);
							break;
						case 0x04:
							rafInd.writeByte(new Byte(data[j]));
							break;
						case 0x05:
							rafInd.writeShort(new Short(data[j]));
							break;
						case 0x06:
							rafInd.writeInt(new Integer(data[j]));
							break;
						case 0x07:
							rafInd.writeLong(new Long(data[j]));
							break;
						case 0x08:
							rafInd.writeFloat(new Float(data[j]));
							break;
						case 0x09:
							rafInd.writeDouble(new Double(data[j]));
							break;
						case 0x0A:
							str = data[j];
							Date dat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").parse(str);
							rafInd.writeLong(dat.getTime());
							break;
						case 0x0B:
							str = data[j];
							str = str+"_00:00:00";
							Date dat2 = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").parse(str);
							rafInd.writeLong(dat2.getTime());
							break;
						default:
							file.writeBytes(data[j]);
							break;
						
					}
					
					rafInd.writeLong(file.getFilePointer());
					rafInd.close();
				}
				
			}
			
			
			file.seek((pg-1)*pgSize+loc);
			file.writeShort(payLoadSz);
			file.writeInt(id);
			int fieldN = data.length - 1;
			
			file.writeByte(fieldN);
			file.write(arr);
			
			for(int i = 1; i < data.length; i++)
				
			{	
				switch(arr[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(data[i]));
						break;
					case 0x05:
						file.writeShort(new Short(data[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(data[i]));
						break;
					case 0x07:
						file.writeLong(new Long(data[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(data[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(data[i]));
						break;
					case 0x0A:
						str = data[i];
						Date temp = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").parse(str);
						file.writeLong(temp.getTime());
						break;
					case 0x0B:
						str = data[i];
						str = str+"_00:00:00";
						Date dat2 = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").parse(str);
						file.writeLong(dat2.getTime());
						break;
					default:
						file.writeBytes(data[i]);
						break;
				}
			}
			int num = fetchRecordNo(file, pg);
			byte b = (byte) (num+1);
			setRecordNo(file, pg, b);
			file.seek((pg-1)*pgSize+12+num*2);
			file.writeShort(loc);
			file.seek((pg-1)*pgSize+2);
			int val = file.readShort();
			if(val >= loc || val == 0){
				file.seek((pg-1)*pgSize+2);
				file.writeShort(loc);
			}
		}catch(Exception e)
		{
			System.out.println("Error while inserting leaf cell\n "+e);
		}
	}

	public static void updateLeafRecord(RandomAccessFile file, int pg, int loc, int payLoadSz, int id, byte[] arr, String[] array, String name){
		try{
			String str;
			file.seek((pg-1)*pgSize+loc);
			file.writeShort(payLoadSz);
			file.writeInt(id);
			int fieldN = array.length - 1;
			file.writeByte(fieldN);
			file.write(arr);
			for(int i = 1; i < array.length; i++){
				switch(arr[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(array[i]));
						break;
					case 0x05:
						file.writeShort(new Short(array[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(array[i]));
						break;
					case 0x07:
						file.writeLong(new Long(array[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(array[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(array[i]));
						break;
					case 0x0A:
						str = array[i];
						Date dat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").parse(str.substring(1, str.length()-1));
						long time = dat.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						str = array[i];
						str = str.substring(1, str.length()-1);
						str = str+"_00:00:00";
						Date dat2 = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").parse(str);
						file.writeLong(dat2.getTime());
						break;
					default:
						file.writeBytes(array[i]);
						break;
				}
			}
		}catch(Exception e){
			System.out.println("Error while updating leaf cell\n"+e);
		}
	}

	public static int searchMidId(RandomAccessFile file, int page)
	{
		int middle = 0;
		try{
			file.seek((page-1)*pgSize);
			byte pg_type = file.readByte();
			int recs_cnt = fetchRecordNo(file, page);
			int mid = (int) Math.ceil((double) recs_cnt / 2);
			long rec_pos = fetchRecPos(file, page, mid-1);
			file.seek(rec_pos);

			switch(pg_type){
				case 0x05:
					middle = file.readInt(); 
					middle = file.readInt();
					break;
				case 0x0D:
					middle = file.readShort();
					middle = file.readInt();
					break;
			}

		}catch(Exception e)
		{
			System.out.println("Error while finding mid key");
		}

		return middle;
	}

	

	public static void sorting(RandomAccessFile file, int page)
	{
		 byte rec_cnt = fetchRecordNo(file, page);
		 int[] arr = fetchKeys(file, page);
		 short[] recs = fetchRows(file, page);
		 int left;
		 short right;

		 for (int r = 1; r < rec_cnt; r++) 
		 {
            for(int c = r ; c > 0 ; c--)
            {
                if(arr[c] < arr[c-1])
                {

                	left = arr[c];
                	arr[c] = arr[c-1];
                	arr[c-1] = left;

                    right = recs[c];
                    recs[c] = recs[c-1];
                    recs[c-1] = right;
                }
            }
         }

         try
         {
         	file.seek((page-1)*pgSize+12);
         	for(int i = 0; i < rec_cnt; i++)
         	{
				file.writeShort(recs[i]);
			}
         }catch(Exception e)
         {
         	System.out.println("Error while sorting records array");
         }
	}

	
	public static boolean isInteriorSpace(RandomAccessFile file, int page)
	{
		byte rec_cnt = fetchRecordNo(file, page);
		if(rec_cnt > 30)
			return true;
		else
			return false;
	}

	public static int isLeafSpace(RandomAccessFile file, int page, int size)
	{
		int flag = -1;

		try
		{
			file.seek((page-1)*pgSize+2);
			int data = file.readShort();
			if(data == 0)
				return pgSize - size;
			int recs_cnt = fetchRecordNo(file, page);
			int k = data - 20 - 2*recs_cnt;
			if(size < k)
				return data - size;
			
		}catch(Exception e)
		{
			System.out.println("Error while checking leaf space");
		}

		return flag;
	}

	public static boolean hasKey(RandomAccessFile file, int page, int id)
	{
		int[] keys = fetchKeys(file, page);
		for(int k : keys)
			if(id == k)
				return true;
		return false;
	}
	
	public static int[] fetchKeys(RandomAccessFile file, int page){
		int recs_cnt = new Integer(fetchRecordNo(file, page));
		int[] keys = new int[recs_cnt];
		byte loc = 0;
		try{
			file.seek((page-1)*pgSize);
			byte temp = file.readByte();
			
			switch(temp){
				case 0x05:
					loc = 4;
					break;
				case 0x0d:
					loc = 2;
					break;
				default:
					loc = 2;
					break;
			}

			for(int i = 0; i < recs_cnt; i++){
				long rec_pos = fetchRecPos(file, page, i);
				file.seek(rec_pos+loc);
				keys[i] = file.readInt();
			}

		}catch(Exception e){
			System.out.println("Error while getting key array");
		}

		return keys;
	}

	public static short[] fetchRows(RandomAccessFile file, int page){
		int recs_cnt = new Integer(fetchRecordNo(file, page));
		short[] rec = new short[recs_cnt];

		try{
			file.seek((page-1)*pgSize+12);
			for(int r = 0; r < recs_cnt; r++){
				rec[r] = file.readShort();
			}
		}catch(Exception e){
			System.out.println("Error while getting Cell Array");
		}

		return rec;
	}

	public static int getParent(RandomAccessFile file, int pg){
		int par = 0;

		try{
			file.seek((pg-1)*pgSize+8);
			par = file.readInt();
		}catch(Exception e){
			System.out.println("Error while getting paremt");
		}

		return par;
	}

	public static void setParent(RandomAccessFile file, int pg, int par)
	{
		try{
			file.seek((pg-1)*pgSize+8);
			file.writeInt(par);
		}catch(Exception e){
			System.out.println("Error while setting parent");
		}
	}

	public static long fetchPgPtr(RandomAccessFile file, int page, int parent){
		long ptr_loc = 0;
		try{
			int recs_cnt = new Integer(fetchRecordNo(file, parent));
			for(int i=0; i < recs_cnt; i++){
				long rec_pos = fetchRecPos(file, parent, i);
				file.seek(rec_pos);
				int child = file.readInt();
				if(child == page){
					ptr_loc = rec_pos;
				}
			}
		}catch(Exception e){
			System.out.println("Error while getting pointer location");
		}

		return ptr_loc;
	}

	public static void setPgPtr(RandomAccessFile file, long pos, int par, int pg){
		try{
			if(pos == 0){
				file.seek((par-1)*pgSize+4);
			}else{
				file.seek(pos);
			}
			file.writeInt(pg);
		}catch(Exception e){
			System.out.println("Error while setting pointer location");
		}
	} 

	
	public static int fetchLastRight(RandomAccessFile file, int page)
	{
		int rm = 0;

		try
		{
			file.seek((page-1)*pgSize+4);
			rm = file.readInt();
		}
		catch(Exception e)
		{
			System.out.println("Error while getting rightmost page");
		}

		return rm;
	}

	public static void setLastRight(RandomAccessFile file, int page, int rightMost)
	{
		try
		{
			file.seek((page-1)*pgSize+4);
			file.writeInt(rightMost);
		}
		catch(Exception e)
		{
			System.out.println("Error while setting rightmost page");
		}

	}

	public static byte fetchRecordNo(RandomAccessFile file, int page)
	{
		byte rec = 0;
		try
		{
			file.seek((page-1)*pgSize+1);
			rec = file.readByte();
		}
		catch(Exception e)
		{
			System.out.println("Error while getting record number\n" + e);
		}
		return rec;
	}

	public static void setRecordNo(RandomAccessFile file, int page, byte num)
	{
		try{
			file.seek((page-1)*pgSize+1);
			file.writeByte(num);
		}catch(Exception e){
			System.out.println("Error while setting Cell Number");
		}
	}



	public static long fetchRecPos(RandomAccessFile file, int page, int id)
	{
		long rec_pos = 0;
		try{
			file.seek((page-1)*pgSize+12+id*2);
			short loc = file.readShort();
			rec_pos = (page-1)*pgSize + loc;
		}
		catch(Exception e)
		{
			System.out.println("Error while getting record location");
		}
		return rec_pos;
	}

	public static short fetchRecOffset(RandomAccessFile file, int page, int id)
	{
		short rec_pos = 0;
		try{
			file.seek((page-1)*pgSize+12+id*2);
			rec_pos = file.readShort();
		}
		catch(Exception e)
		{
			System.out.println("Error while getting Cell Offset");
		}
		return rec_pos;
	}

	public static void setRecOffset(RandomAccessFile file, int page, int id, int offset){
		try{
			file.seek((page-1)*pgSize+12+id*2);
			file.writeShort(offset);
		}
		catch(Exception e)
		{
			System.out.println("Error while setting Cell Offset");
		}
	}
	
}