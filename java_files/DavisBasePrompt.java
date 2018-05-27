import static java.lang.System.out;
import java.util.Scanner;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;

/**
 * @author Apurva Mithal
 * @version 1.0 <b>
 *          <p>
 *          This is an example of how to create an interactive prompt
 *          </p>
 *          <p>
 *          There is also some guidance to get started wiht read/write of binary
 *          data files using RandomAccessFile class
 *          </p>
 *          </b>
 * 
 */
public class DavisBasePrompt {

	static String prompt = "davisql> ";
	static String version = "v1.0b";
	static String copyright = "©2016 Apurva Mithal";
	static boolean isExit = false;
	static String curDatabase = "user_data";
	/*
	 * Page size for alll files is 512 bytes by default. You may choose to make
	 * it user modifiable
	 */

	/*
	 * The Scanner class is used to collect user commands from the prompt There
	 * are many ways to do this. This is just one.
	 * 
	 * Each time the semicolon (;) delimiter is entered, the userCommand String
	 * is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	/**
	 * ***********************************************************************
	 * Main method
	 */
	public static void main(String[] args) {

		DavisBaseInit.init();
		/* Display the welcome screen */
		splashScreen();

		/* Variable to collect user input from the prompt */
		String userCommand = "";

		while (!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "")
					.trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");
		System.out.println("Exited");

	}

	/**
	 * Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}

	/**
	 * @param s
	 *            The String to be repeated
	 * @param num
	 *            The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself
	 *         num times.
	 */

	/** return the DavisBase version */

	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}

	public static String getVersion() {
		return version;
	}

	public static String getCopyright() {
		return copyright;
	}

	public static void parseUserCommand(String userCommand) {

		/*
		 * commandTokens is an array of Strings that contains one token per
		 * array element The first token can be used to determine the type of
		 * command The other tokens can be used to pass relevant parameters to
		 * each command-specific method inside each case statement
		 */
		// String[] commandTokens = userCommand.split(" ");
		String[] commandTokens = userCommand.split(" ");

		/*
		 * This switch handles a very small list of hardcoded commands of known
		 * syntax. You will want to rewrite this method to interpret more
		 * complex commands.
		 */
		String s = commandTokens[0];
		int com = 0;
		if (s.equals("use"))
			com = 1;
		if (s.equals("show"))
			com = 2;
		if (s.equals("create"))
			com = 3;
		if (s.equals("drop"))
			com = 4;
		if (s.equals("insert"))
			com = 5;
		if (s.equals("delete"))
			com = 6;
		if (s.equals("select"))
			com = 7;
		if (s.equals("update"))
			com = 8;
		if (s.equals("help"))
			com = 9;
		if (s.equals("version"))
			com = 10;
		if (s.equals("exit"))
			com = 11;

		switch (com) {
		case 1:
			useDb(userCommand, commandTokens);
			break;
		case 2:
			show(userCommand, commandTokens);
			break;
		case 3:
			parseCreate(userCommand, commandTokens);
			break;
		case 4:
			drop(userCommand, commandTokens);
			break;
		case 5:
			insert(userCommand, commandTokens);
			break;
		case 6:
			delete(userCommand, commandTokens);
			break;
		case 7:
			select(userCommand, commandTokens);
			break;
		case 8:
			update(userCommand, commandTokens);
			break;
		case 9:
			System.out.println();
			out.println(line("*", 80));
			out.println("SUPPORTED COMMANDS\n");
			out.println("All commands below are case insensitive\n");
			out.println("USE <database_name>;");
			out.println("\tSwitch to specified database;");
			out.println("SHOW TABLES;");
			out.println("\tDisplay the names of all tables.\n");
			out.println("CREATE TABLE <table_name> (rowid int primary key, <column2 datatype not null>,....<columnk datatype>;");
			out.println("\tCreate table.\n");
			out.println("DROP TABLE <table_name>;");
			out.println("\tRemove table data (i.e. all records) and its schema.\n");
			out.println("INSERT INTO <table_name> (column_list) VALUES (value1,value2,value3, ...);");
			out.println("\tInsert records into table.\n");
			out.println("DELETE FROM <table_name> where <condition>;");
			out.println("\tDelete records from table.\n");
			out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
			out.println("\tDisplay table records whose optional <condition>");
			out.println("UPDATE <table_name> SET <column_name> = <value> [WHERE <condition>];");
			out.println("\tModify records data whose optional <condition> is\n");
			out.println("VERSION;");
			out.println("\tDisplay the program version.\n");
			out.println("HELP;");
			out.println("\tDisplay this help information.\n");
			out.println("EXIT;");
			out.println("\tExit the program.\n");
			out.println(line("*", 80));
			break;

		case 10:
			System.out.println();
			displayVersion();
			break;

		case 11:
			System.out.println();
			isExit = true;
			break;

		default:
			System.out.println("I didn't understand the command: \""
					+ userCommand + "\"");
			break;

		}
	}

	public static void useDb(String userCommand, String[] commandTokens) {
		if (commandTokens[1].equals("")) {

			System.out.println("Database name cannot be NULL ");
		} else {
			if (!DatabaseOperations.isDBExists(commandTokens[1])) {
				System.out.println("Database not found");

			} else {
				curDatabase = commandTokens[1];
				System.out.println("Database changed to " + curDatabase);
			}
		}

	}

	public static void show(String userCommand, String[] commandTokens) {
		String showObj = commandTokens[1];
		if (showObj.equals("databases")) {
			DatabaseOperations.showDatabase();
		} else if (showObj.equals("tables")) {
			DatabaseOperations.show();
		}

		System.out.println();
	}

	public static void parseCreate(String userCommand, String[] commandTokens) {

		if (commandTokens[1].equals("database")) {
			String dbName = commandTokens[2];
			DatabaseOperations.createDatabase(dbName);

		} else if (commandTokens[1].equals("table")) {
			String tableName = commandTokens[2];
			String[] array = userCommand.split(tableName);
			String[] columns = (array[1].trim()).substring(1,
					(array[1].trim()).length() - 1).split(",");
			for (int i = 0; i < columns.length; i++)
				columns[i] = columns[i].trim();
			if (DavisBaseInit.isTabPresent(tableName)) {
				System.out
						.println(tableName + " Table " + " already exists.\n");
			} else {
				DatabaseOperations.createTable(tableName, columns);
				System.out.println(tableName + " Table "
						+ "successfully created.");
			}
		}

		else {
			System.out.println("I didn't understand the command: \""
					+ userCommand + "\"");
		}
	}

	public static void drop(String userCommand, String[] commandTokens) {

		if (commandTokens[1].equals("database")) {
			if (!DatabaseOperations.isDBExists(commandTokens[2])) {
				System.out.println("Database " + commandTokens[2]
						+ " does not exist.\n");
			} else {
				DatabaseOperations.dropDatabase(commandTokens[2]);
				System.out.println("Database " + commandTokens[2]
						+ " successfully dropped .");
			}
		}
		if (commandTokens[1].equals("table")) {
			if (!DavisBaseInit.isTabPresent(commandTokens[2])) {
				System.out.println("Table " + commandTokens[2]
						+ " does not exist.\n");

			} else {
				DatabaseOperations.dropTable(commandTokens[2], curDatabase);
				System.out.println("Table " + commandTokens[2]
						+ " successfully dropped.\n");
			}
		} else {
			System.out.println("I didn't understand the command: \""
					+ userCommand + "\"");
		}
		System.out.println();
	}

	public static void insert(String userCommand, String[] commandTokens) {

		String values = "";
		String filePath = "";
		values = userCommand.split("values")[1].trim();
		values = values.substring(1,values.length() - 1);
		String[] val_array = values.split(",");
		for (int i = 0; i < val_array.length; i++)
			val_array[i] = val_array[i].trim();

		if (DavisBaseInit.isTabPresent(commandTokens[2])) {
			RandomAccessFile file = null;
			try {
				filePath = "data\\" + curDatabase + "\\" + commandTokens[2]
						+ "\\" + commandTokens[2] + ".tbl";
				file = new RandomAccessFile(filePath, "rw");
				DatabaseOperations.insertIntoTable(file, commandTokens[2], val_array);
				
			} catch (FileNotFoundException e) {
				System.out.println("Path not found " + filePath);
				e.printStackTrace();
			}
		} else if (!DavisBaseInit.isTabPresent(commandTokens[2])) {
			System.out.println("Table " + commandTokens[2]
					+ " does not exist.\n");
		}

	}

	public static void delete(String userCommand, String[] commandTokens) {
		String[] splitWhere = userCommand.split("where");
		String tableName = (splitWhere[0].split("from"))[1].trim();

		if (DavisBaseInit.isTabPresent(tableName)) {
			String[] cond = null;
			if (splitWhere.length > 1) {
				cond = splitByOperator(splitWhere[1].trim());
			} else {
				cond = new String[0];
			}
			DatabaseOperations.deleteRow(tableName, cond);
			System.out.println("Delete from " + commandTokens[2]
			                       						+ " successful.");
		} else {

			System.out.println("Table " + tableName + " does not exist.");
		}
	}

	public static void select(String userCommand, String[] commandTokens) {

		String[] splitWhere = userCommand.split("where");
		String[] colNames;
		String[] condition;
		String selectTable = (splitWhere[0].split("from"))[1].trim();
		String selectColumns = (splitWhere[0].split("from"))[0].replace(
				"select", "").trim();
		int flag = 0;
		if (selectTable.equals("davisbase_tables")) {
			if (selectColumns.contains("*")) {
				colNames = new String[1];
				colNames[0] = "*";
			} else {
				colNames = selectColumns.split(",");
				for (int i = 0; i < colNames.length; i++)
					colNames[i] = colNames[i].trim();
			}
			if (splitWhere.length > 1) {
				condition = splitByOperator(splitWhere[1].trim());
			} else {
				condition = new String[0];
			}
			DatabaseOperations.selectFrom("data\\catalog\\davisbase_tables.tbl", selectTable,
					colNames, condition);
			System.out.println();
			flag = 1;
		}

		else if (selectTable.equals("davisbase_columns")) {
			if (selectColumns.contains("*")) {
				colNames = new String[1];
				colNames[0] = "*";
			} else {
				colNames = selectColumns.split(",");
				for (int i = 0; i < colNames.length; i++)
					colNames[i] = colNames[i].trim();
			}
			if (splitWhere.length > 1) {
				condition = splitByOperator(splitWhere[1].trim());
			} else {
				condition = new String[0];
			}
			DatabaseOperations.selectFrom("data\\catalog\\davisbase_columns.tbl", selectTable,
					colNames, condition);
			System.out.println();
			flag = 1;
		}

		else {
			if (!DavisBaseInit.isTabPresent(selectTable)) {

				System.out.println("Table " + selectTable + " not found.\n");
				flag = 1;
			}
		}
		if (flag == 0) {
			if (splitWhere.length > 1) {
				condition = splitByOperator(splitWhere[1].trim());
			} else {
				condition = new String[0];
			}

			if (selectColumns.contains("*")) {
				colNames = new String[1];
				colNames[0] = "*";
			} else {
				colNames = selectColumns.split(",");
				for (int i = 0; i < colNames.length; i++)
					colNames[i] = colNames[i].trim();
			}

			DatabaseOperations.selectFrom("data\\" + curDatabase + "\\" + selectTable + "\\"
					+ selectTable + ".tbl", selectTable, colNames, condition);
		}

	}

	public static void update(String userCommand, String[] commandTokens) {
		String[] updateSet = (userCommand.split("set"))[1].split("where");
		if (!DavisBaseInit.isTabPresent(commandTokens[1])) {
			System.out.println("Table " + commandTokens[1]
					+ " does not exist.\n");

		} else {
			DatabaseOperations.updateTable(commandTokens[1], splitByOperator(updateSet[0]),
					splitByOperator(updateSet[1]));
			System.out.println("Table " + commandTokens[1]
					+ " successfully updated.\n");
		}
	}

	/**
	 * Help: Display supported commands
	 */
	public static void help() {
		out.println(line("*", 80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("All commands below are case insensitive\n");
		out.println("USE <database_name>;");
		out.println("\tSwitch to specified database;");
		out.println("SHOW TABLES;");
		out.println("\tDisplay the names of all tables.\n");
		out.println("CREATE TABLE <table_name> (rowid int primary key, <column2 datatype not null>,....<columnk datatype>;");
		out.println("\tCreate table.\n");
		out.println("DROP TABLE <table_name>;");
		out.println("\tRemove table data (i.e. all records) and its schema.\n");
		out.println("INSERT INTO <table_name> (column_list) VALUES (value1,value2,value3, ...);");
		out.println("\tInsert records into table.\n");
		out.println("DELETE FROM <table_name> where <condition>;");
		out.println("\tDelete records from table.\n");
		out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
		out.println("\tDisplay table records whose optional <condition>");
		out.println("UPDATE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		out.println("\tModify records data whose optional <condition> is\n");
		out.println("VERSION;");
		out.println("\tDisplay the program version.\n");
		out.println("HELP;");
		out.println("\tDisplay this help information.\n");
		out.println("EXIT;");
		out.println("\tExit the program.\n");
		out.println(line("*", 80));
	}
	
	public static String[] splitByOperator(String userCmnd) {
		String operator = "";
		String arr[] = new String[2];
		
		if (userCmnd.contains("=")) {
			arr = userCmnd.split("=");
			operator = "=";
		}

		else if (userCmnd.contains("<>")) {
			arr = userCmnd.split("<>");
			operator = "<>";
		}

		else if (userCmnd.contains(">")) {
			arr = userCmnd.split(">");
			operator = ">";
		}

		else if (userCmnd.contains(">=")) {
			arr = userCmnd.split(">=");
			operator = ">=";
		}

		else if (userCmnd.contains("<")) {
			arr = userCmnd.split("<");
			operator = "<";
		}

		else if (userCmnd.contains("<=")) {
			arr = userCmnd.split("<=");
			operator = "<=";
		}
		
		String leftOpRight[] = new String[3];
		leftOpRight[0] = arr[0].trim();
		leftOpRight[1] = operator;
		leftOpRight[2] = arr[1].trim();
		
		return leftOpRight;
	}

	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}

}
