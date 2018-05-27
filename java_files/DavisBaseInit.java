
import java.io.File;
import java.io.RandomAccessFile;

/* Creates folder structure
 * - data
 * ---catalog
 * -----davisbase_tables.tbl
 * -----davisbase_columns.tbl
 * 
 */
public class DavisBaseInit {
	public static int pgSize = PageSizeConstant.pageSize;
	private static RandomAccessFile tab_metaData;
	private static RandomAccessFile col_metaData;
	
	public static void init() 
	{
		try {
			File dataDir = new File("data");
			File catalogDir = new File("data\\catalog");
			boolean isDataExists = dataDir.exists();
			boolean bool = false;
			
			if (!isDataExists) {
				// create directory 'data'
				dataDir.mkdir();
			}
			
			if (!catalogDir.mkdir()){
				String[] catalogTables = catalogDir.list();
				
				for (int table = 0; table < catalogTables.length; table++) {
					if (catalogTables[table].equals("davisbase_tables.tbl"))
						bool = true;
				}
				if (!bool) {
					initCatalog();
				}
				
				bool = false;
				for (int table = 0; table < catalogTables.length; table++) {
					if (catalogTables[table].equals("davisbase_columns.tbl"))
						bool = true;
				}
				if (!bool) {
					initCatalog();
				}
				
				
			}
			else {
				initCatalog();
			} 
		} catch (SecurityException exception) {
			System.out.println("Error creating meta data files " + exception);
	
		}

	}
	
	public static void initCatalog() 
	{
		
		File f = new File("data\\catalog");
		for (int cat=0; cat<f.list().length; cat++) 
		{
			File file = new File(f, f.list()[cat]); 
			file.delete();
		}
	
		try {
			tab_metaData = new RandomAccessFile("data\\catalog\\davisbase_tables.tbl", "rw");
			tab_metaData.setLength(pgSize);
			tab_metaData.seek(0);
			tab_metaData.write(0x0D);
			tab_metaData.write(0x02);
			int[] location=new int[2];
			int size1=24;
			int size2=25;
			location[0]=pgSize-size1;
			location[1]=location[0]-size2;
			tab_metaData.writeShort(location[1]);
			tab_metaData.writeInt(0);
			tab_metaData.writeInt(10);
			tab_metaData.writeShort(location[1]);
			tab_metaData.writeShort(location[0]);
			tab_metaData.seek(location[0]);
			tab_metaData.writeShort(20);
			tab_metaData.writeInt(1); 
			tab_metaData.writeByte(1);
			tab_metaData.writeByte(28);
			tab_metaData.writeBytes("davisbase_tables");
			tab_metaData.seek(location[1]);
			tab_metaData.writeShort(21);
			tab_metaData.writeInt(2); 
			tab_metaData.writeByte(1);
			tab_metaData.writeByte(29);
			tab_metaData.writeBytes("davisbase_columns");
			
			col_metaData = new RandomAccessFile("data\\catalog\\davisbase_columns.tbl", "rw");
			col_metaData.setLength(pgSize);
			col_metaData.seek(0);       
			col_metaData.writeByte(0x0D); 
			col_metaData.writeByte(0x08); 
			int[] loc=new int[10];
			loc[0]=pgSize-43;
			loc[1]=loc[0]-47;
			loc[2]=loc[1]-44;
			loc[3]=loc[2]-48;
			loc[4]=loc[3]-49;
			loc[5]=loc[4]-47;
			loc[6]=loc[5]-57;
			loc[7]=loc[6]-49;
			loc[8]=loc[7]-49;
			col_metaData.writeShort(loc[8]);
			col_metaData.writeInt(0); 
			col_metaData.writeInt(0);
			for(int i=0;i<9;i++)
				col_metaData.writeShort(loc[i]);

			col_metaData.seek(loc[0]);
			col_metaData.writeShort(33);
			col_metaData.writeInt(1); 
			col_metaData.writeByte(5);
			col_metaData.writeByte(28);
			col_metaData.writeByte(17);
			col_metaData.writeByte(15);
			col_metaData.writeByte(4);
			col_metaData.writeByte(14);
			col_metaData.writeBytes("davisbase_tables"); 
			col_metaData.writeBytes("rowid"); 
			col_metaData.writeBytes("INT");
			col_metaData.writeByte(1);
			col_metaData.writeBytes("NO"); 
			col_metaData.seek(loc[1]);
			col_metaData.writeShort(39); 
			col_metaData.writeInt(2); 
			col_metaData.writeByte(5);
			col_metaData.writeByte(28);
			col_metaData.writeByte(22);
			col_metaData.writeByte(16);
			col_metaData.writeByte(4);
			col_metaData.writeByte(14);
			col_metaData.writeBytes("davisbase_tables"); 
			col_metaData.writeBytes("table_name"); 
			col_metaData.writeBytes("TEXT"); 
			col_metaData.writeByte(2);
			col_metaData.writeBytes("NO");
			col_metaData.seek(loc[2]);
			col_metaData.writeShort(34); 
			col_metaData.writeInt(3); 
			col_metaData.writeByte(5);
			col_metaData.writeByte(29);
			col_metaData.writeByte(17);
			col_metaData.writeByte(15);
			col_metaData.writeByte(4);
			col_metaData.writeByte(14);
			col_metaData.writeBytes("davisbase_columns");
			col_metaData.writeBytes("rowid");
			col_metaData.writeBytes("INT");
			col_metaData.writeByte(1);
			col_metaData.writeBytes("NO");
			col_metaData.seek(loc[3]);
			col_metaData.writeShort(40); 
			col_metaData.writeInt(4); 
			col_metaData.writeByte(5);
			col_metaData.writeByte(29);
			col_metaData.writeByte(22);
			col_metaData.writeByte(16);
			col_metaData.writeByte(4);
			col_metaData.writeByte(14);
			col_metaData.writeBytes("davisbase_columns");
			col_metaData.writeBytes("table_name");
			col_metaData.writeBytes("TEXT");
			col_metaData.writeByte(2);
			col_metaData.writeBytes("NO");
			col_metaData.seek(loc[4]);
			col_metaData.writeShort(41);
			col_metaData.writeInt(5); 
			col_metaData.writeByte(5);
			col_metaData.writeByte(29);
			col_metaData.writeByte(23);
			col_metaData.writeByte(16);
			col_metaData.writeByte(4);
			col_metaData.writeByte(14);
			col_metaData.writeBytes("davisbase_columns");
			col_metaData.writeBytes("column_name");
			col_metaData.writeBytes("TEXT");
			col_metaData.writeByte(3);
			col_metaData.writeBytes("NO");
			col_metaData.seek(loc[5]);
			col_metaData.writeShort(39);
			col_metaData.writeInt(6); 
			col_metaData.writeByte(5);
			col_metaData.writeByte(29);
			col_metaData.writeByte(21);
			col_metaData.writeByte(16);
			col_metaData.writeByte(4);
			col_metaData.writeByte(14);
			col_metaData.writeBytes("davisbase_columns");
			col_metaData.writeBytes("data_type");
			col_metaData.writeBytes("TEXT");
			col_metaData.writeByte(4);
			col_metaData.writeBytes("NO");
			col_metaData.seek(loc[6]);
			col_metaData.writeShort(49); 
			col_metaData.writeInt(7); 
			col_metaData.writeByte(5);
			col_metaData.writeByte(29);
			col_metaData.writeByte(28);
			col_metaData.writeByte(19);
			col_metaData.writeByte(4);
			col_metaData.writeByte(14);
			col_metaData.writeBytes("davisbase_columns");
			col_metaData.writeBytes("ordinal_position");
			col_metaData.writeBytes("TINYINT");
			col_metaData.writeByte(5);
			col_metaData.writeBytes("NO");
			col_metaData.seek(loc[7]);
			col_metaData.writeShort(41);
			col_metaData.writeInt(8); 
			col_metaData.writeByte(5);
			col_metaData.writeByte(29);
			col_metaData.writeByte(23);
			col_metaData.writeByte(16);
			col_metaData.writeByte(4);
			col_metaData.writeByte(14);
			col_metaData.writeBytes("davisbase_columns");
			col_metaData.writeBytes("is_nullable");
			col_metaData.writeBytes("TEXT");
			col_metaData.writeByte(6);
			col_metaData.writeBytes("NO");
		}
		catch (Exception exception) 
		{
			System.out.println("Error creating meta data tables and columns file: "+exception);
		}
	}
	
	public static boolean isTabPresent(String nameTab) {
		boolean isTableExists = false;
		try {
			File tab = new File("data\\"+DavisBasePrompt.curDatabase);
			if (tab.mkdir()) {
			}	
			
			String[] tabs = tab.list();
			for (int table = 0; table < tabs.length; table++) {
				if (tabs[table].equals(nameTab))
					return true;
			}
		} catch (SecurityException exception) {
			System.out.println("Error while creating data\\user_data" + exception);
		}

		return isTableExists;
	}
	
}
