import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
//import java.util.HashMap;
import java.util.List;

public class AshishBaseOperation {
	static TableClass ashishBasetable;
	static TableClass ashishBasecolumn;
	static String Dbpath;
	static String DBname;

	public void setfoldername(String name){
		DBname = name;
		Dbpath = "data/"+name+"/";
	}

	public void initialize() throws IOException{

		try {
			File dir = new File("data/catalog");
			if(dir != null){
				boolean success = dir.mkdirs();
				if(!success){
					//System.out.println("could not create directories");
					//return;
				}
			}
			dir = new File("data/user_data");
			if(dir != null){
				boolean success = dir.mkdirs();
				if(!success){
					//System.out.println("could not create directories");
					//return;
				}

			}
			setfoldername("user_data");
			ashishBasetable = new TableClass("data/catalog/ashishbase_tables.tbl","ashishbase_tables","rw");
			if(ashishBasetable.length() == 0){
				ashishBasetable.setDataBaseName("catalog");
				ashishBasetable.modifySystemTables();
			}
			ashishBasetable.setDataBaseName("catalog");

			ashishBasecolumn = new TableClass("data/catalog/ashishbase_columns.tbl","ashishbase_columns","rw");
			if(ashishBasecolumn.length() == 0){
				ashishBasecolumn.setDataBaseName("catalog");
				ashishBasecolumn.modifySystemTables();
			}
			ashishBasecolumn.setDataBaseName("catalog");

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//	public void usedatabase(String usercommand) throws IOException{
	//		String[] command = usercommand.split(" ");
	//		setfoldername(command[2].toLowerCase().trim());
	//	}

	//	public void createDatabase(String usercommand) throws IOException{
	//		String[] command = usercommand.split(" ");
	//		File dir = new File("data/"+command[2].toLowerCase().trim());
	//		if(dir.exists()){
	//			System.out.println("Database already exists");
	//			return;
	//		}else{
	//			dir.mkdir();
	//		}
	//	}
	//	
	//	public void dropDatabase(String usercommand) throws IOException{
	//		String[] command = usercommand.split(" ");
	//		File dir = new File("data/"+command[2].toLowerCase().trim());
	//		if(dir.exists()){
	//			for(File f:dir.listFiles()){
	//				String path = f.getAbsolutePath();
	//				String db = path.split("/")[path.split("/").length-2];
	//				//String tblname = filename.split(".")[0];
	//				ashishBasetable.delete(db, 3, "=");
	//				ashishBasecolumn.delete(db, 8, "=");
	//				f.delete();
	//			}
	//			dir.delete();
	//		}
	//		setfoldername("defaultdb");
	//	}

	//	public void showDatabase(String usercommand) throws IOException{
	//		List<String[]> list = ashishBasetable.getcolumnvalue(ashishBasecolumn,2,"ashishbase_tables","=");		
	//		List<String[]> list2 = ashishBasetable.findAll();
	//		System.out.println(list.get(2)[2]);
	//		System.out.println("----------");
	//		HashMap<String,Integer> map = new HashMap<>();
	//		for(int i = 0; i < list2.size(); i++){
	//			String[] cell = list2.get(i);
	//			map.put(cell[2], 1);
	//		}
	//		
	//		/*for(Entry<String,Integer> entry: map.entrySet()){
	//			//System.out.println(entry.getKey());
	//		}*/
	//		File dir = new File("data/");
	//		if(dir.exists()){
	//			for(String f:dir.list()){
	//				System.out.println(f);
	//			}
	//		}
	//	}

	public void showTables(String usercommand) throws IOException{
		List<String[]> list = ashishBasetable.getRecordColumnValue(ashishBasecolumn,"ashishbase_tables","=",2);		
		List<String[]> list2 = ashishBasetable.findAllRecords();
		System.out.print(list.get(1)[2]);
		System.out.print("\t"+list.get(2)[2]);
		System.out.print("\n");
		for(int i = 0; i < list2.size(); i++){
			String[] cell = list2.get(i);
			System.out.print(cell[1]);
			System.out.print("\t"+cell[2]);
			System.out.println("\n");
		}
	}

	public void insertIntoTable(String cmd) throws IOException{
		String tableName;
		String[] data;
		String[] colmList = {};
		if(cmd.indexOf("(") == cmd.lastIndexOf("(")){
			String str1 = cmd.substring(0,cmd.toLowerCase().indexOf("values"));
			String str2 = cmd.substring(cmd.toLowerCase().indexOf("values")+7,cmd.length());

			String[] str1arr = str1.trim().split(" ");
			tableName = str1arr[2].toLowerCase().trim();

			str2 = str2.replaceAll("[()']", "");
			data = str2.split(",");
		}else{
			colmList = cmd.substring(cmd.indexOf("(") + 1, cmd.indexOf(")")).split(",");
			data = cmd.substring(cmd.lastIndexOf("(") + 1, cmd.lastIndexOf(")")).split(",");
			tableName = cmd.substring(cmd.indexOf(")")+1,cmd.toLowerCase().indexOf("values")-1).trim();
			if(colmList.length>0 && !colmList[0].trim().equals("row_id")) {
				System.out.println("Err!! primary key missing in insert statement");
				return;
			}else {
				String cmd2 = "select * from " + tableName +" where "+ colmList[0].trim() +" = "+data[0].trim(); 
				if(parseQueryString(cmd2, false)>0) {
					System.out.println("Err!! Can not Insert , Record with same "+ colmList[0].trim()+ " already present in table");
					return;
				}
			}

		}


		List<String[]> templist = ashishBasecolumn.getPageHeaderdetails(tableName);
		List<String[]> list = new ArrayList<>();

		for(int i = 0; i < templist.size(); i++){
			String[] cell = templist.get(i);
			if(cell[7].equals(DBname)){
				list.add(cell);
			}
		}

		TableClass table = new TableClass(Dbpath+tableName+".tbl",tableName,"rw");
		//List<String[]> list = daviscolm.getHeaderdetails(tableName);
		if(list.size() == 0){
			System.out.println("The Table you are inserting is not available");
			table.close();
			return;
		}

		String[] columnType = new String[data.length-1];
		String[] columnValue= new String[data.length-1]; 
		int cellsize = 0;
		int rowid = 0;

		for(int i = 0; i < list.size(); i++){
			String[] col = list.get(i);
			if(colmList.length != 0){
				if(!col[2].equals(colmList[i].trim())){
					System.out.println("The column list provided does not match the table columns");
					table.close();
					return;
				}
			}
			if(col[5].toLowerCase().equals("no") && data[i].trim().toLowerCase().equals("null")){
				System.out.println("Value for "+col[2]+" cannot be NULL");
				table.close();
				return;
			}

			if(i == 0){
				if(data[i].contains("[^0-9]")){
					System.out.println("The primary key has to be Integer");
				}else{
					rowid = Integer.parseInt(data[i].trim());
				}
			}else{
				if(col[3].equals("float")){
					col[3] = "double";
				}
				columnType[i-1] = col[3];
				columnValue[i-1] = data[i].trim();
				if(DataTypes.dataTypeGetTypeSize(col[3]) != 0){
					cellsize += DataTypes.dataTypeGetTypeSize(col[3]);
				}else{
					cellsize += data[i].trim().length();
				}
			}
		}

		table.insertNewCell(rowid,cellsize,columnType,columnValue);
		table.close();
	}


	public void update(String command) throws IOException{
		String str1 = command.substring(0, command.toLowerCase().indexOf("where"));
		String str2 = command.substring(command.toLowerCase().indexOf("where")+6, command.length());

		String[] str1arr = str1.split(" ");
		String[] str2arr = str2.split(" ");

		String tableName = str1arr[1];
		String colm = str1arr[3];
		String val = str1arr[5];
		int ordnum = 0;

		String[] condcolm = new String[str2arr.length/3];
		String[] oper1 = new String[str2arr.length/3];
		String[] values = new String[str2arr.length/3];
		String[] oper2 = new String[(str2arr.length/3)-1];
		int[] ordinalnum = new int[str2arr.length/3];
		int j = 0;
		for(int i = 0; i < str2arr.length; i+=4){
			condcolm[j] = str2arr[i];
			oper1[j] = str2arr[i+1];
			values[j] = str2arr[i+2];
			if((i+3) < str2arr.length){
				oper2[j] = str2arr[i+3];
			}
			j++;
		}
		String tableFileName = Dbpath+tableName+".tbl";
		TableClass file = new TableClass(tableFileName,tableName,"rw");

		List<String[]> templist = ashishBasecolumn.getPageHeaderdetails(tableName);
		List<String[]> list = new ArrayList<>();

		for(int i = 0; i < templist.size(); i++){
			String[] cell = templist.get(i);
			if(cell[7].equals(DBname)){
				list.add(cell);
			}
		}

		int numOrd = 0;
		for(int i = 0; i < list.size(); i++){
			String[] cell = list.get(i);
			String colmhead = cell[2];
			if(colm.equals(colmhead)){
				ordnum = Integer.parseInt(cell[4]);
			}
			for(int k=0; k < condcolm.length; k++){
				if(colmhead.equals(condcolm[k])){
					ordinalnum[k] = Integer.parseInt(cell[4]);
					numOrd++;
				}
			}
		}
		if((numOrd != (str2arr.length/3)) || ordnum == 0){
			System.out.println("column name given is not valid column name");
			file.close();
			return;
		}

		file.update(val, ordnum, ordinalnum,values, oper1, oper2);
		file.close();
	}

	/**
	 *  Stub method for dropping tables
	 *  @param dropTableString is a String of the user input
	 * @throws IOException 
	 */
	public void dropTable(String dropTableString) throws IOException {
		
		String tablename = dropTableString.toLowerCase().split(" ")[2];
		ashishBasetable.delete(tablename,2,"=",DBname,3,"=");
		ashishBasecolumn.delete(tablename,2,"=",DBname,8,"=");
		File delfile = new File(Dbpath+tablename+".tbl");
		if(delfile.exists()){
			delfile.delete();
		}

	}

	public void delete(String command) throws IOException {
		String[] commandstr = command.split(" ");
		if(commandstr.length < 8){
			System.out.println("Delete command is not in right format");
			return;
		}
		String tableName = commandstr[3];
		String colmName = commandstr[5];
		String oper = commandstr[6];
		String pattern = commandstr[7];
		String tableFileName = Dbpath+tableName+".tbl";
		TableClass file = new TableClass(tableFileName,tableName,"rw");
		int ordinalnum = 0;
		List<String[]> templist = ashishBasecolumn.getPageHeaderdetails(tableName);
		List<String[]> list = new ArrayList<>();

		for(int i = 0; i < templist.size(); i++){
			String[] cell = templist.get(i);
			if(cell[7].equals(DBname)){
				list.add(cell);
			}
		}
		for(int i = 0; i < list.size(); i++){
			String[] cell = list.get(i);
			if(colmName.equals(cell[2])){
				ordinalnum= Integer.parseInt(cell[4]);
			}
		}
		if(ordinalnum == 0){
			System.out.println("column name given is not valid column name");
			file.close();
			return;
		}
		file.delete(pattern, ordinalnum, oper);

		file.close();
	}

	/**
	 *  Stub method for executing queries
	 *  @param queryString is a String of the user input
	 * @throws IOException 
	 */
	public int parseQueryString(String command, boolean print) throws IOException {
		String[] commandstr = command.split(" ");
		if(commandstr.length < 4) {
			System.out.println("Error in select command . Please check");
			return 0;
		}
		String tableFileName ;
		String dispcolm = commandstr[1].toLowerCase().trim();
		String tableName = commandstr[3].toLowerCase().trim();
		if(tableName.equals("ashishbase_tables") || tableName.equals("ashishbase_columns")){
			tableFileName = "data/catalog/"+tableName+".tbl";
		}else {
			tableFileName = Dbpath+tableName+".tbl";
		}
		TableClass file = new TableClass(tableFileName,tableName,"rw");
		List<String[]> templist = ashishBasecolumn.getPageHeaderdetails(tableName);
		List<String[]> list = new ArrayList<>();

		for(int i = 0; i < templist.size(); i++){
			String[] cell = templist.get(i);
			if(tableName.equals("ashishbase_tables") ||tableName.equals("ashishbase_columns") || cell[7].equals(DBname) ){
				list.add(cell);
			}
		}
		int ordinalnum = 0;
		List<String[]> list2;
		if(commandstr.length > 4){
			String colmname = commandstr[5].toLowerCase().trim();
			String operator = commandstr[6].toLowerCase().trim();
			String value = commandstr[7].toLowerCase().trim();
			for(int i = 0; i < list.size(); i++){
				String[] cell = list.get(i);
				if(colmname.equals(cell[2])){
					ordinalnum= Integer.parseInt(cell[4]);
				}
			}
			if(ordinalnum == 0){
				System.out.println("column name given is not valid column name");
				file.close();
				return -1;
			}
			list2 = file.getRecordColumnValue(file,value,operator,ordinalnum);

		}else{
			list2 = file.findAllRecords();
		}

		if(print) {
			if(1==0 && (list2== null || (list2!= null && list2.size() == 0))) {
				System.out.println("Empty set");
			} else {
				if(dispcolm.equals("*")){
					for(int i = 0; i < list.size(); i++){
						String[] cell = list.get(i);
						System.out.print(""+cell[2]+"\t");
					}
					System.out.print("\n");
					if(list2!=null && list2.size()>0) {
						for(int i = 0; i < list2.size(); i++){
							String[] cell = list2.get(i);
							for(int j = 0; j < cell.length; j++){
								System.out.print(""+cell[j]+"\t");
							}
							System.out.print("\n");

						}
					}
				}
			}
		} else {
			file.close();
			if(list2 == null) {
				return 0;
			}
			return list2.size();
		}
		file.close();
		return 0;
	}

	/**
	 *  Stub method for creating new tables
	 *  @param command is a String of the user input
	 */
	public void createTable(String createTableCommand) {


		String createTableStr = "create table";
		createTableCommand = createTableCommand.replaceAll("\\s{2,}", " ");
		if(createTableCommand.indexOf("(") == -1) {
			System.out.println(createTableStr + "( missing ");
			return ;
		}
		if(createTableCommand.charAt(createTableCommand.length()-1) != ')' ) {
			System.out.println(createTableStr + ") missing ");
			return ;
		}
		// extract table names
		String tableName = createTableCommand.substring(createTableStr.length(), createTableCommand.indexOf("(")).trim();
		// extract table coulmns
		String tableColumns = createTableCommand.substring(createTableCommand.indexOf("(") + 1, createTableCommand.lastIndexOf(")"));

		/*  Code to create a .tbl recordFileName to contain table data */
		try {

			List<String[]> templist = ashishBasecolumn.getPageHeaderdetails(tableName);
			List<String[]> list = new ArrayList<>();

			for(int i = 0; i < templist.size(); i++){
				String[] cell = templist.get(i);
				if(cell[7].equals(DBname)){
					list.add(cell);
				}
			}

			if(list.size() > 0){
				System.out.println("Table already exists");
				return;
			}
			String tableFileName = Dbpath+tableName+".tbl";
			TableClass file = new TableClass(tableFileName,tableName,"rw");

			file.setDataBaseName(DBname);
			file.createTable(ashishBasetable,ashishBasecolumn,tableColumns);
			file.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}

		/*  Code to insert a row in the davisbase_tables table 
		 *  i.e. database catalog meta-data 
		 */

		/*  Code to insert rows in the davisbase_columns table  
		 *  for each column in the new table 
		 *  i.e. database catalog meta-data 
		 */
	}

}
