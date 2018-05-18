import java.util.HashMap;

/*
Serial	TypeCode	Database	Data Type	Name	Content	Size(bytes) 	Description
0x00 NULL 1 Value is a 1-byte NULL (used for NULL TINYINT)
0x01 NULL 2 Value is a 2-byte NULL (used for NULL SMALLINT)
0x02 NULL 4 Value is a 4-byte NULL (used for NULL INT or REAL)
0x03 NULL 8 Value is a 8-byte NULL (used for NULL DOUBLE, DATETIME, or DATE
0x04 TINYINT 1 Value is a big-endian 1-byte twos-complement integer.
0x05 SMALLINT 2 Value is a big-endian 2-byte twos-complement integer.
0x06 INT 4 Value is a big-endian 4-byte twos-complement integer.
0x07 BIGINT 8 Value is an big-endian 8-byte twos-complement integer.
0x08 REAL 4 A big-endian single precision IEEE 754 floating point number
0x09 DOUBLE 8 A big-endian double precision IEEE 754 floating point number
0x0A DATETIME 8	A big-endian unsigned LONG integer that represents the specified
number of milliseconds since the standard base time known as "the
epoch”. It should display as a formatted string string:
YYYY-MM-DD_hh:mm:ss, e.g. 2016-03-23_13:52:23.
0x0B DATE 8 A datetime whose time component is 00:00:00, but does not display.
0x0C + n TEXT
Value is a string in ASCI encoding (range 0x00-0x7F) of length n. For
the purposes of this database you may consider that the empty string
is a NULL value, i.e. empty strings do not exist. The null terminator is
not stored.
 */


/*
 * 
Offset from
beginning
of page    Content Size(bytes)		 			Description
	0x00     1		The one-byte flag at offset 0 indicating the b-tree page type.
				• A value of 2 (0x02) means the page is an interior index b-tree page.
				• A value of 5 (0x05) means the page is an interior table b-tree page.
				• A value of 10 (0x0a) means the page is a leaf index b-tree page.
				• A value of 13 (0x0d) means the page is a leaf table b-tree page.
				Any other value for the b-tree page type is an error.
	0x01     1 		The one-byte integer at offset 1 gives the number of cells on the page.
	0x02     2 		The two-byte integer at offset 2 designates the start of the cell content area. A zero
				value for this integer is interpreted as 65536.
	0x04     4		The four-byte integer page pointer at offset 4 has a different role depending on the
				page type:
				• Table/Index interior page - rightmost child page pointer
				• Table/Index leaf page - right sibling page pointer
	0x08    2n		An array of 2-byte integers that indicate the page offset location of each data cell.
				The array size is 2n, where n is the number of cells on the page. The array is
				maintained in key-sorted order
 */
public class DataTypes {
	public static final String INT = "int";
	public static final String BIGINT = "bigint";

	public static final String TINYINT = "tinyint";
	public static final String SMALLINT = "smallint";
	
	public static final String REAL = "real";
	public static final String DOUBLE = "double";
	
	public static final String TEXT = "text";
	public static final String DATETIME = "datetime";
	public static final String DATE = "date";

	public static HashMap<String, Byte> columnDataTypeMap = new HashMap<>();
	static {
		columnDataTypeMap.put(TINYINT, (byte) 0x04);
		columnDataTypeMap.put(SMALLINT, (byte) 0x05);
		columnDataTypeMap.put(INT, (byte) 0x06);
		columnDataTypeMap.put(BIGINT, (byte) 0x07);
		columnDataTypeMap.put(REAL, (byte) 0x08);
		columnDataTypeMap.put(DOUBLE, (byte) 0x09);
		columnDataTypeMap.put(DATETIME, (byte) 0x0A);
		columnDataTypeMap.put(DATE, (byte) 0x0B);
		columnDataTypeMap.put(TEXT, (byte) 0x0C);
	}
	
	public static HashMap<Byte, String> columnDatacodeMap = new HashMap<>();
	static {
		columnDatacodeMap.put( (byte) 0x04,TINYINT);
		columnDatacodeMap.put( (byte) 0x05,SMALLINT);
		columnDatacodeMap.put( (byte) 0x06,INT);
		columnDatacodeMap.put( (byte) 0x07,BIGINT);
		columnDatacodeMap.put( (byte) 0x08,REAL);
		columnDatacodeMap.put( (byte) 0x09,DOUBLE);
		columnDatacodeMap.put( (byte) 0x0A,DATETIME);
		columnDatacodeMap.put( (byte) 0x0B,DATE);
		columnDatacodeMap.put( (byte) 0x0C,TEXT);
	}

	public static HashMap<String, Integer> dataTypeSizeMap = new HashMap<>();
	static {
		dataTypeSizeMap.put(TINYINT, 1);
		dataTypeSizeMap.put(SMALLINT, 2);
		dataTypeSizeMap.put(INT, 4);
		dataTypeSizeMap.put(BIGINT, 8);
		dataTypeSizeMap.put(REAL, 4);
		dataTypeSizeMap.put(DOUBLE, 8);
		dataTypeSizeMap.put(DATETIME, 8);
		dataTypeSizeMap.put(DATE, 8);
		dataTypeSizeMap.put(TEXT, 0);
	}

	public static HashMap<String, Object> dataTypeNullValueMap = new HashMap<>();
	static {
		dataTypeNullValueMap.put(TINYINT, 0);
		dataTypeNullValueMap.put(SMALLINT, 0);
		dataTypeNullValueMap.put(INT, 0);
		dataTypeNullValueMap.put(BIGINT, 0);
		dataTypeNullValueMap.put(REAL, 0);
		dataTypeNullValueMap.put(DOUBLE, 0);
		dataTypeNullValueMap.put(DATETIME, 0);
		dataTypeNullValueMap.put(DATE, 0);
		dataTypeNullValueMap.put(TEXT, "");
	}

	public static HashMap<String, Byte> dataTypeNullMap = new HashMap<>();
	static {
		dataTypeNullMap.put(TINYINT, (byte) 0x00);
		dataTypeNullMap.put(SMALLINT, (byte) 0x01);
		dataTypeNullMap.put(INT, (byte) 0x02);
		dataTypeNullMap.put(BIGINT, (byte) 0x03);
		dataTypeNullMap.put(REAL, (byte) 0x02);
		dataTypeNullMap.put(DOUBLE, (byte) 0x08);
		dataTypeNullMap.put(DATETIME, (byte) 0x08);
		dataTypeNullMap.put(DATE, (byte) 0x08);
		dataTypeNullMap.put(TEXT, (byte) 0x01);
	}
	
	public static Object dataTypeGetNullValue(String dataType) {
		return dataTypeNullValueMap.get(dataType.toLowerCase());
	}

	public static byte dataTypeGetTypeCode(String dataType) {
		return columnDataTypeMap.get(dataType.toLowerCase());
	}
	
	public static String dataTypeGetCodeval(Byte code) {
		if(code >= 0x0C){
			return "TEXT";
		}
		return columnDatacodeMap.get(code);
	}

	public static byte dataTypeGetNullType(String dataType) {
		return dataTypeNullMap.get(dataType.toLowerCase());
	}

	public static int dataTypeGetTypeSize(String dataType) {
		return dataTypeSizeMap.get(dataType.toLowerCase());
	}
}
