import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.List;

public class TableClass extends RandomAccessFile{

	/**
	 * @param name
	 * @param tableName
	 * @param mode
	 * @throws FileNotFoundException
	 */
	public TableClass(String name, String tableName,String mode) throws FileNotFoundException {
		super(name, mode);
		this.tablename = tableName;
		dataRecOper = new DataRecordsOperation(this);
		table = this;
		// TODO Auto-generated constructor stub
	}

	
	/**
	 * @param name
	 */
	public void setDataBaseName(String name){
		DBname = name;
	}



	public void modifySystemTables() throws IOException{
		if(tablename.equals("ashishbase_tables")){
			updateSystemTable();
		} else if(tablename.equals("ashishbase_columns")){
			updateSystemColumns();
		}
	}

	public void updateSystemTable() throws IOException {
//		System.out.println(tablename);
		String columnType[] = new String[2];
		String columnValue[] = new String[2];
		int recordCellSize = 0;

		columnType[0] = "TEXT";
		columnValue[0] = "ashishbase_tables";
		recordCellSize += columnValue[0].length();
		columnType[1] = "TEXT";
		columnValue[1] = DBname;
		recordCellSize += columnValue[1].length();
		dataRecOper.insertNewCell(1,columnType,columnValue,recordCellSize,0,false);
		recordCellSize = 0;

		columnType[0] = "TEXT";
		columnValue[0] = "ashishbase_columns";
		recordCellSize += columnValue[0].length();
		columnType[1] = "TEXT";
		columnValue[1] = DBname;
		recordCellSize += columnValue[1].length();
		dataRecOper.insertNewCell(2,columnType,columnValue,recordCellSize,0,false);
	}
	
	public void updateSystemColumns() throws IOException {
		Integer rowId = 1;
		String columnType[] = new String[6];
		String columnValue[] = new String[6];
		int cellSize = 0;
		Integer ordinalNumber = 1;

		columnType[0] = "TEXT";
		columnType[1] = "TEXT";
		columnType[2] = "TEXT";
		columnType[3] = "TINYINT";
		columnType[4] = "TEXT";
		columnType[5] = "TEXT";
		columnValue[0] = "ashishbase_tables";
		cellSize += columnValue[0].length();
		columnValue[1] = "rowid";
		cellSize += columnValue[1].length();
		columnValue[2] = "INT";
		cellSize += columnValue[2].length();
		columnValue[3] = ordinalNumber.toString();
		cellSize += columnValue[3].length();
		columnValue[4] = "NO";
		cellSize += columnValue[4].length();
		columnValue[5] = "PRI";
		cellSize += columnValue[5].length();
		dataRecOper.insertNewCell(rowId,columnType,columnValue,cellSize,0,false);

		rowId++;
		ordinalNumber++;
		cellSize = 0;
		columnValue[0] = "ashishbase_tables";
		cellSize += columnValue[0].length();
		columnValue[1] = "table_name";
		cellSize += columnValue[1].length();
		columnValue[2] = "TEXT";
		cellSize += columnValue[2].length();
		columnValue[3] = ordinalNumber.toString();
		cellSize += columnValue[3].length();
		columnValue[4] = "NO";
		cellSize += columnValue[4].length();
		columnValue[5] = "NULL";
		cellSize += columnValue[5].length();
		dataRecOper.insertNewCell(rowId,columnType,columnValue,cellSize,0,false);

		rowId++;
		ordinalNumber++;
		cellSize = 0;
		columnValue[0] = "ashishbase_tables";
		cellSize += columnValue[0].length();
		columnValue[1] = "DBname";
		cellSize += columnValue[1].length();
		columnValue[2] = "TEXT";
		cellSize += columnValue[2].length();
		columnValue[3] = ordinalNumber.toString();
		cellSize += columnValue[3].length();
		columnValue[4] = "NO";
		cellSize += columnValue[4].length();
		columnValue[5] = "NULL";
		cellSize += columnValue[5].length();
		dataRecOper.insertNewCell(rowId,columnType,columnValue,cellSize,0,false);

		ordinalNumber = 1;
		rowId++;
		cellSize = 0;
		columnValue[0] = "ashishbase_columns";
		cellSize += columnValue[0].length();
		columnValue[1] = "rowid";
		cellSize += columnValue[1].length();
		columnValue[2] = "INT";
		cellSize += columnValue[2].length();
		columnValue[3] = ordinalNumber.toString();
		cellSize += columnValue[3].length();
		columnValue[4] = "NO";
		cellSize += columnValue[4].length();
		columnValue[5] = "PRI";
		cellSize += columnValue[5].length();
		dataRecOper.insertNewCell(rowId,columnType,columnValue,cellSize,0,false);

		rowId++;
		ordinalNumber++;
		cellSize = 0;
		columnValue[0] = "ashishbase_columns";
		cellSize += columnValue[0].length();
		columnValue[1] = "table_name";
		cellSize += columnValue[1].length();
		columnValue[2] = "TEXT";
		cellSize += columnValue[2].length();
		columnValue[3] = ordinalNumber.toString();
		cellSize += columnValue[3].length();
		columnValue[4] = "NO";
		cellSize += columnValue[4].length();
		columnValue[5] = "NULL";
		cellSize += columnValue[5].length();
		dataRecOper.insertNewCell(rowId,columnType,columnValue,cellSize,0,false);

		rowId++;
		ordinalNumber++;
		cellSize = 0;
		columnValue[0] = "ashishbase_columns";
		cellSize += columnValue[0].length();
		columnValue[1] = "column_name";
		cellSize += columnValue[1].length();
		columnValue[2] = "TEXT";
		cellSize += columnValue[2].length();
		columnValue[3] = ordinalNumber.toString();
		cellSize += columnValue[3].length();
		columnValue[4] = "NO";
		cellSize += columnValue[4].length();
		columnValue[5] = "NULL";
		cellSize += columnValue[5].length();
		dataRecOper.insertNewCell(rowId,columnType,columnValue,cellSize,0,false);

		rowId++;
		ordinalNumber++;
		cellSize = 0;
		columnValue[0] = "ashishbase_columns";
		cellSize += columnValue[0].length();
		columnValue[1] = "data_type";
		cellSize += columnValue[1].length();
		columnValue[2] = "TEXT";
		cellSize += columnValue[2].length();
		columnValue[3] = ordinalNumber.toString();
		cellSize += columnValue[3].length();
		columnValue[4] = "NO";
		cellSize += columnValue[4].length();
		columnValue[5] = "NULL";
		cellSize += columnValue[5].length();
		dataRecOper.insertNewCell(rowId,columnType,columnValue,cellSize,0,false);

		rowId++;
		ordinalNumber++;
		cellSize = 0;
		columnValue[0] = "ashishbase_columns";
		cellSize += columnValue[0].length();
		columnValue[1] = "ordinal_position";
		cellSize += columnValue[1].length();
		columnValue[2] = "TINYINT";
		cellSize += columnValue[2].length();
		columnValue[3] = ordinalNumber.toString();
		cellSize += columnValue[3].length();
		columnValue[4] = "NO";
		cellSize += columnValue[4].length();
		columnValue[5] = "NULL";
		cellSize += columnValue[5].length();
		dataRecOper.insertNewCell(rowId,columnType,columnValue,cellSize,0,false);

		rowId++;
		ordinalNumber++;
		cellSize = 0;
		columnValue[0] = "ashishbase_columns";
		cellSize += columnValue[0].length();
		columnValue[1] = "is_nullable";
		cellSize += columnValue[1].length();
		columnValue[2] = "TEXT";
		cellSize += columnValue[2].length();
		columnValue[3] = ordinalNumber.toString();
		cellSize += columnValue[3].length();
		columnValue[4] = "NO";
		cellSize += columnValue[4].length();
		columnValue[5] = "NULL";
		cellSize += columnValue[5].length();
		dataRecOper.insertNewCell(rowId,columnType,columnValue,cellSize,0,false);

		rowId++;
		ordinalNumber++;
		cellSize = 0;
		columnValue[0] = "ashishbase_columns";
		cellSize += columnValue[0].length();
		columnValue[1] = "COLUMN_KEY";
		cellSize += columnValue[1].length();
		columnValue[2] = "TEXT";
		cellSize += columnValue[2].length();
		columnValue[3] = ordinalNumber.toString();
		cellSize += columnValue[3].length();
		columnValue[4] = "NO";
		cellSize += columnValue[4].length();
		columnValue[5] = "NULL";
		cellSize += columnValue[5].length();
		dataRecOper.insertNewCell(rowId,columnType,columnValue,cellSize,0,false);

		rowId++;
		ordinalNumber++;
		cellSize = 0;
		columnValue[0] = "ashishbase_columns";
		cellSize += columnValue[0].length();
		columnValue[1] = "DBname";
		cellSize += columnValue[1].length();
		columnValue[2] = "TEXT";
		cellSize += columnValue[2].length();
		columnValue[3] = ordinalNumber.toString();
		cellSize += columnValue[3].length();
		columnValue[4] = "NO";
		cellSize += columnValue[4].length();
		columnValue[5] = "NULL";
		cellSize += columnValue[5].length();
		dataRecOper.insertNewCell(rowId,columnType,columnValue,cellSize,0,false);

	}


	public void createTable(TableClass ashishBaseTable,TableClass ashishBaseColumn,String command) throws IOException{
		int ashishTableRowid =  ashishBaseTable.dataRecOper.findIdOfLastRow(0) +1;
		String columnTypeArray[] = new String[2];
		String columnValueArray[] = new String[2];
		int recordCellSize = 0;

		columnTypeArray[0] = "TEXT";
		columnValueArray[0] = tablename;
		recordCellSize += columnValueArray[0].length();
		columnTypeArray[1] = "TEXT";
		columnValueArray[1] = DBname;
		recordCellSize += columnValueArray[1].length();
		ashishBaseTable.dataRecOper.insertNewCell(ashishTableRowid,columnTypeArray,columnValueArray,recordCellSize,0,false);


		int ashishColumnRowid = ashishBaseColumn.dataRecOper.findIdOfLastRow(0) + 1;
		String[] columns = command.split(",");
		String columnName, dataType;
		String isNullable, columnKey;
		Integer ordinalPosition = 0;


		columnTypeArray = new String[7];
		columnValueArray = new String[7];
		columnTypeArray[0] = "TEXT";
		columnTypeArray[1] = "TEXT";
		columnTypeArray[2] = "TEXT";
		columnTypeArray[3] = "TINYINT";
		columnTypeArray[4] = "TEXT";
		columnTypeArray[5] = "TEXT";
		columnTypeArray[6] = "TEXT";

		for (int i = 0; i < columns.length; i++) {
			String column = columns[i];
			String[] tokensArray = column.trim().split(" ");
			columnName = tokensArray[0].trim();
			dataType = tokensArray[1].trim();
			column = column.toLowerCase();
			isNullable = column.contains("not null") ? "NO" : "YES";
			columnKey = column.contains("primary key") ? "PRI" : "NULL";
			ordinalPosition++;
			if(i == 0 && !columnName.equals("row_id" )){
				System.out.println("Err!! first column in create cmd is not row_id");
				return ;
			}

			recordCellSize = 0;
			columnValueArray[0] = tablename;
			recordCellSize += columnValueArray[0].length();
			columnValueArray[1] = columnName;
			recordCellSize += columnValueArray[1].length();
			columnValueArray[2] = dataType;
			recordCellSize += columnValueArray[2].length();
			columnValueArray[3] = ordinalPosition.toString();
			recordCellSize += columnValueArray[3].length();
			columnValueArray[4] = isNullable;
			recordCellSize += columnValueArray[4].length();
			columnValueArray[5] = columnKey;
			recordCellSize += columnValueArray[5].length();
			columnValueArray[6] = DBname;
			recordCellSize += columnValueArray[6].length();
			ashishBaseColumn.dataRecOper.insertNewCell(ashishColumnRowid,columnTypeArray,columnValueArray,recordCellSize,0,false);
			ashishColumnRowid++;
		}

	}

	public List<String[]> getPageHeaderdetails(String name) throws IOException{
		List<String[]> recordList = table.dataRecOper.findRecordColumnEntries(name);
		return recordList;
	}

	public List<String[]> getRecordColumnValue(TableClass tableFile,String regexPattern,String operator,int ordinalNumber) throws IOException{
		//List<String[]> list = daviscolm.btree.findHeader(name);
		List<String[]> recordColumnValueList = tableFile.dataRecOper.findRecordColumnEntries(regexPattern,ordinalNumber,operator);
		return recordColumnValueList;
	}

	public List<String[]> findAllRecords() throws IOException{
		List<String[]> recordColumnValueList = table.dataRecOper.findAllRecords();
		return recordColumnValueList;
	}

	public void insertNewCell(int rowid,int cellSize, String[] columnTypeArray, String[] columnValueArray) throws IOException{
		table.dataRecOper.insertNewCell(rowid, columnTypeArray, columnValueArray, cellSize, 0, false);
	}

	public void delete(String pattern) throws IOException{
		table.dataRecOper.findAndDeleteRegexPattern(pattern);
	}

	public void delete(String regexPattern,int ordinalNumber, String operator) throws IOException{
		table.dataRecOper.findAndDeleteRegexPattern(regexPattern,ordinalNumber,operator);
	}

	public void delete(String regexPattern1,int ordinalNumber1, String operator1,String regexPattern2,int ordinalNumber2, String operator2) throws IOException{
		table.dataRecOper.findAndDeleteRegexPattern(regexPattern1,ordinalNumber1,operator1,regexPattern2,ordinalNumber2,operator2);
	}

	public void update(String val, int ordnum, int[] Ordinalnum,String[] values, String[] oper1, String[] oper2) throws IOException{
		table.dataRecOper.updateRecords(val, ordnum, Ordinalnum,values, oper1, oper2);
	}

	public boolean compareValueMethod(byte type,String value, String pattern, String oper) throws IOException{
		boolean retValue = false;
		int intValue;
		Integer integerClassValue;
		Float floatValue;
		Double doubleValue;
		Long longValue;
		if (type == DataTypes.dataTypeGetTypeCode(DataTypes.TINYINT)) {
			intValue = Integer.parseInt(value);
			int val = Integer.parseInt(pattern);
			if(oper.equals("<")){
				return (intValue < val);
			}else if(oper.equals(">")){
				return (intValue > val);
			}else if(oper.equals(">=")){
				return (intValue >= val);
			}else if(oper.equals("<=")){
				return (intValue <= val);
			}else if(oper.equals("=")){
				return (intValue == val);
			}
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.SMALLINT)) {
			intValue =  Integer.parseInt(value);
			integerClassValue = intValue;
			int val = Integer.parseInt(pattern);
			if(oper.equals("<")){
				return (integerClassValue < val);
			}else if(oper.equals(">")){
				return (integerClassValue > val);
			}else if(oper.equals(">=")){
				return (integerClassValue >= val);
			}else if(oper.equals("<=")){
				return (integerClassValue <= val);
			}else if(oper.equals("=")){
				return (integerClassValue == val);
			}
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.INT)) {
			integerClassValue = Integer.parseInt(value);
			int val = Integer.parseInt(pattern);
			if(oper.equals("<")){
				return (integerClassValue < val);
			}else if(oper.equals(">")){
				return (integerClassValue > val);
			}else if(oper.equals(">=")){
				return (integerClassValue >= val);
			}else if(oper.equals("<=")){
				return (integerClassValue <= val);
			}else if(oper.equals("=")){
				return (integerClassValue == val);
			}
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.BIGINT)) {
			longValue = Long.parseLong(value);
			long val = Long.parseLong(pattern);
			if(oper.equals("<")){
				return (longValue < val);
			}else if(oper.equals(">")){
				return (longValue > val);
			}else if(oper.equals(">=")){
				return (longValue >= val);
			}else if(oper.equals("<=")){
				return (longValue <= val);
			}else if(oper.equals("=")){
				return (longValue == val);
			}
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.REAL)) {
			floatValue = Float.parseFloat(value);
			float val = Float.parseFloat(pattern);
			if(oper.equals("<")){
				return (floatValue < val);
			}else if(oper.equals(">")){
				return (floatValue > val);
			}else if(oper.equals(">=")){
				return (floatValue >= val);
			}else if(oper.equals("<=")){
				return (floatValue <= val);
			}else if(oper.equals("=")){
				return (floatValue == val);
			}
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.DOUBLE)) {
			doubleValue = Double.parseDouble(value);
			float val = Float.parseFloat(pattern);
			if(oper.equals("<")){
				return (doubleValue < val);
			}else if(oper.equals(">")){
				return (doubleValue > val);
			}else if(oper.equals(">=")){
				return (doubleValue >= val);
			}else if(oper.equals("<=")){
				return (doubleValue <= val);
			}else if(oper.equals("=")){
				return (doubleValue == val);
			}
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.DATETIME)) {
			longValue = Long.parseLong(value);
			long val = Long.parseLong(pattern);
			if(oper.equals("<")){
				return (longValue < val);
			}else if(oper.equals(">")){
				return (longValue > val);
			}else if(oper.equals(">=")){
				return (longValue >= val);
			}else if(oper.equals("<=")){
				return (longValue <= val);
			}else if(oper.equals("=")){
				return (longValue == val);
			}
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.DATE)) {
			longValue = Long.parseLong(value);
			long val = Long.parseLong(pattern);
			if(oper.equals("<")){
				return (longValue < val);
			}else if(oper.equals(">")){
				return (longValue > val);
			}else if(oper.equals(">=")){
				return (longValue >= val);
			}else if(oper.equals("<=")){
				return (longValue <= val);
			}else if(oper.equals("=")){
				return (longValue == val);
			}
		} else {
			if(oper.equals("=")){
				return (value.equals(pattern));
			}
		}
		return retValue;
	}



	public String readValueByType(byte type,int val) throws IOException{
		int intValue;
		Integer integerClassValue;
		Float floatValue;
		Double doubleValue;
		Long longValue;
		if (type == DataTypes.dataTypeGetTypeCode(DataTypes.TINYINT)) {
			intValue = this.readByte();
			integerClassValue = intValue;
			return integerClassValue.toString();
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.SMALLINT)) {
			intValue =  this.readShort();
			integerClassValue = intValue;
			return integerClassValue.toString();
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.INT)) {
			integerClassValue = this.readInt();
			return integerClassValue.toString();
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.BIGINT)) {
			longValue = this.readLong();
			return longValue.toString();
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.REAL)) {
			floatValue = this.readFloat();
			return floatValue.toString();
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.DOUBLE)) {
			doubleValue = this.readDouble();
			return doubleValue.toString();
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.DATE)) {
			longValue = this.readLong();
			DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");

			long milliSeconds= longValue;
//			System.out.println(milliSeconds);

			Calendar calendar = Calendar.getInstance();
			calendar.setTimeInMillis(milliSeconds);
//			System.out.println(formatter.format(calendar.getTime())); 
			String tempDate = formatter.format(calendar.getTime());
			return tempDate;
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.DATETIME)) {
			longValue = this.readLong();
			DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

			long milliSeconds= longValue;
//			System.out.println(milliSeconds);

			Calendar calendar = Calendar.getInstance();
			calendar.setTimeInMillis(milliSeconds);
//			System.out.println(formatter.format(calendar.getTime())); 
			String tempDate = formatter.format(calendar.getTime());
			return tempDate;
		} else {
			int length = type - 12;
			return this.readString(length);
		}
	}




	public Object readValueByType(byte type) throws IOException{
		if (type == DataTypes.dataTypeGetTypeCode(DataTypes.TINYINT)) {
			return this.readByte();
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.SMALLINT)) {
			return this.readShort();
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.INT)) {
			return this.readInt();
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.BIGINT)) {
			return this.readLong();
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.REAL)) {
			return this.readFloat();
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.DOUBLE)) {
			return this.readDouble();
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.DATETIME)) {
			return this.readLong();
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.DATE)) {
			return this.readLong();
		} else {
			int length = type - 12;
			return this.readString(length);
		}
	}

	public void writeValueByType(Object val) throws IOException {
		if (val instanceof Byte) {
			this.writeByte(((Byte) val).byteValue());
		} else if (val instanceof Short) {
			this.writeShort(((Short) val).shortValue());
		} else if (val instanceof Integer) {
			this.writeInt(((Integer) val).intValue());
		} else if (val instanceof Long) {
			this.writeLong(((Long) val).longValue());
		} else if (val instanceof Float) {
			this.writeFloat(((Float) val).floatValue());
		} else if (val instanceof Double) {
			this.writeDouble(((Double) val).doubleValue());
		} else {
			this.writeBytes(val.toString());
		}
	}

	public void writeValueByType(String v,Byte type) throws IOException {
		if (type == DataTypes.dataTypeGetTypeCode(DataTypes.TINYINT)) {
			this.writeByte(Integer.parseInt(v));
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.SMALLINT)) {
			this.writeShort(Short.parseShort(v));
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.INT)) {
			this.writeInt(Integer.parseInt(v));
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.BIGINT)) {
			this.writeLong(Long.parseLong(v));
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.REAL)) {
			this.writeFloat(Float.parseFloat(v));
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.DOUBLE)) {
			this.writeDouble(Double.parseDouble(v));
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.DATE)) {
			String myDate = v.replace('-', '/') + " 00:00:00";
			LocalDateTime localDateTime = LocalDateTime.parse(myDate,
			    DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss") );
			/*
			  With this new Date/Time API, when using a date, you need to
			  specify the Zone where the date/time will be used. For your case,
			  seems that you want/need to use the default zone of your system.
			  Check which zone you need to use for specific behaviour e.g.
			  CET or America/Lima
			*/
			long millis = localDateTime
			    .atZone(ZoneId.systemDefault())
			    .toInstant().toEpochMilli();
//			System.out.println("ashish " + millis);
			this.writeLong(millis);
		} else if (type == DataTypes.dataTypeGetTypeCode(DataTypes.DATETIME)) {
			String myDate = v.replace('-', '/');
			myDate = myDate.replace('_', ' ');
			LocalDateTime localDateTime = LocalDateTime.parse(myDate,
			    DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss") );
			/*
			  With this new Date/Time API, when using a date, you need to
			  specify the Zone where the date/time will be used. For your case,
			  seems that you want/need to use the default zone of your system.
			  Check which zone you need to use for specific behaviour e.g.
			  CET or America/Lima
			*/
			long millis = localDateTime
			    .atZone(ZoneId.systemDefault())
			    .toInstant().toEpochMilli();
//			System.out.println("ashish " + millis);
			this.writeLong(millis);
		} else {
			this.writeBytes(v.toString());
		}
	}

	public String readString(int length) throws IOException {
		byte[] b = new byte[length];
		this.read(b);
		return new String(b);
	}
	
	// member variables of Table Class
	String tablename;
	DataRecordsOperation dataRecOper;
	TableClass table;
	String DBname;



}
