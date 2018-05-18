import java.io.IOException;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import static java.lang.System.out;

/**
 *  @author Ashish Kumar
 *  @version 1.0
 *  <b>
 *  <p>This is an example of how to create an interactive prompt</p>
 *  <p>There is also some guidance to get started wiht read/write of
 *     binary data files using RandomAccessFile class</p>
 *  </b>
 *
 */
class AshishBaseMain {
	
	/* 
	 *  The Scanner class is used to collect user commands from the prompt
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the userCommand 
	 *  String is re-populated.
	 */
	
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	static AshishBaseOperation oper;
	
	/** ***********************************************************************
	 *  Main method
	 * @throws IOException 
	 */
    public static void main(String[] args) throws IOException {

		/* Display the welcome screen */
		splashScreen();

		/* Variable to collect user input from the prompt */
		String userCommand = ""; 
		oper = new AshishBaseOperation();
		oper.initialize();

		while(!AshishBaseConstants.isExit) {
			System.out.print(AshishBaseConstants.prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}


	}

	/** ***********************************************************************
	 *  Method definitions
	 */

	/**
	 *  Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to AshishBaseLite"); // Display the string.
		System.out.println("AshishBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	public static void printCmd(String s) {
		System.out.println("\n\t" + s + "\n");
	}
	public static void printDef(String s) {
		System.out.println("\t\t" + s);
	}
	
		/**
		 *  Help: Display supported commands
		 */
		public static void help() {
			out.println(line("*",80));
			out.println("SUPPORTED COMMANDS\n");
			out.println("All commands below are case insensitive\n");
			out.println("SHOW TABLES;");
			out.println("\tDisplay the names of all tables.\n");
			//printCmd("SELECT * FROM <table_name>;");
			//printDef("Display all records in the table <table_name>.");
			out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
			out.println("\tDisplay table records whose optional <condition>");
			out.println("\tis <column_name> = <value>.\n");
			out.println("DROP TABLE <table_name>;");
			out.println("\tRemove table data (i.e. all records) and its schema.\n");
			out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
			out.println("\tModify records data whose optional <condition> is\n");
			out.println("VERSION;");
			out.println("\tDisplay the program version.\n");
			out.println("HELP;");
			out.println("\tDisplay this help information.\n");
			out.println("EXIT;");
			out.println("\tExit the program.\n");
			out.println(line("*",80));
		}

	/** return the DavisBase version */
	public static String getVersion() {
		return AshishBaseConstants.version;
	}
	
	public static String getCopyright() {
		return AshishBaseConstants.copyright;
	}
	
	public static void displayVersion() {
		System.out.println("AshishBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}
		
	public static void parseUserCommand (String userCommand) throws IOException {
		
		/* commandTokens is an array of Strings that contains one token per array element 
		 * The first token can be used to determine the type of command 
		 * The other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement */
		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		

		/*
		*  This switch handles a very small list of hardcoded commands of known syntax.
		*  You will want to rewrite this method to interpret more complex commands. 
		*/
		switch (commandTokens.get(0)) {
			case "insert":
				oper.insertIntoTable(userCommand);
				//System.out.println("insert command");
				break;
//			case "delete":
//				oper.delete(userCommand);
//				System.out.println("delete command");
//				break;
			case "update":
				oper.update(userCommand);
				break;
			case "select":
				oper.parseQueryString(userCommand, true);
				break;
			case "show":
//				if(commandTokens.get(1).toLowerCase().trim().equals("database")){
//					oper.showDatabase(userCommand);
//				}else{
					oper.showTables(userCommand);
//				}
			    break;
			case "drop":
//				if(commandTokens.get(1).toLowerCase().trim().equals("database")){
//					oper.dropDatabase(userCommand);
//				}else{
				String cmd[] = userCommand.toLowerCase().split(" ");
				if(cmd.length <3  || (cmd.length >2 && !cmd[1].equals("table"))) {
					System.out.println("Err!! in drop table command");
					break;
				}
					oper.dropTable(userCommand);
//				}
				break;
			case "create":
//				if(commandTokens.get(1).toLowerCase().trim().equals("database")){
//					oper.createDatabase(userCommand);
//				}else{
					oper.createTable(userCommand);
//				}
				break;
//			case "use":
//				oper.usedatabase(userCommand);
//				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
				AshishBaseConstants.isExit = true;
				break;
			case "quit":
				AshishBaseConstants.isExit = true;
			default:
				System.out.println("Command not valid. Please check: \"" + userCommand + "\"");
				break;
		}
	}
}