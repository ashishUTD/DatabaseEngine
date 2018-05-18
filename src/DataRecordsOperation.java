
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class DataRecordsOperation {

	public TableClass recordFileName;

	/**
	 * constructor
	 *	
	 * @param recordFile
	 */
	public DataRecordsOperation(TableClass file){
		this.recordFileName = file;
	}






	/**
	 * @param regexPattern
	 * @param ordinalNumber
	 * @param operator
	 * @throws IOException
	 */
	public void findAndDeleteRegexPattern(String regexPattern, int ordinalNumber, String operator) throws IOException{
		int cellPosition = 0;
		int numberOfColumns = 0;
		// right page pointer
		int rightPointer = recordFileName.readInt();
		// page address
		int position = searchFirstLeafNodeInFile() * AshishBaseConstants.pageSize;
		

		int regexPatternFound = 0;
		int ordinalNumberCheck = 0;
		do {
			// number of cells
			recordFileName.seek(position+1);
			int numOfCells = recordFileName.readByte();
			// right page pointer
			recordFileName.seek(position+4);
			rightPointer = recordFileName.readInt();
			// first record address
			int newPosition = position +8;
			cellPosition = 0;
			// iterating over number of records
			for(int i = 0; i < numOfCells; i++){
				regexPatternFound = 0;
				ordinalNumberCheck = 1;
				recordFileName.seek(newPosition+(2*i));
				cellPosition = recordFileName.readShort();
				if(cellPosition == 0){
					continue;
				}
				// number of columns
				recordFileName.seek(position+cellPosition+6);
				numberOfColumns = recordFileName.readByte();
				// row id number
				recordFileName.seek(position+cellPosition+2);
				Integer rowId = recordFileName.readInt();
				String columnValue = rowId.toString();
				if(ordinalNumberCheck == ordinalNumber){
					if(recordFileName.compareValueMethod((byte)0x06, columnValue, regexPattern, operator)){
						regexPatternFound = 1;
					}
				}
				// col  type 
				int columnTypePosition = position+cellPosition+7;
				// column value type
				int columnValuePosition = position+cellPosition+7+numberOfColumns;
				// iterating over number of columns
				for(int j = 0; j < numberOfColumns; j++){
					ordinalNumberCheck++;
					// column type position
					columnTypePosition = position+cellPosition+7+j;
					recordFileName.seek(columnTypePosition);
					// reading data type 
					Byte dataType = recordFileName.readByte();
					recordFileName.seek(columnValuePosition);
					// reading column value
					columnValue = recordFileName.readValueByType(dataType,1);
					if(ordinalNumberCheck == ordinalNumber){
						if(recordFileName.compareValueMethod(dataType, columnValue, regexPattern, operator)){
							regexPatternFound = 1;
						}
					}
					// get datatype size from hashmap
					int dataTypeSize = DataTypes.dataTypeGetTypeSize(DataTypes.dataTypeGetCodeval(dataType));
					if(dataTypeSize == 0){
						columnValuePosition += columnValue.length();
					}else{
						columnValuePosition += dataTypeSize;
					}
				}
				// if pattern found
				if(regexPatternFound == 1){
					recordFileName.seek(position+cellPosition);
					int recordCellSize = recordFileName.readShort()+6;
					for(int k = 0; k < recordCellSize; k++){
						recordFileName.seek(position+cellPosition+k);
						recordFileName.writeByte(0x00);
					}
					recordFileName.seek(newPosition+(2*i));
					recordFileName.writeShort(0);
					if(i == numOfCells-1){
						recordFileName.seek(position+1);
						recordFileName.writeByte(numOfCells-1);
					}
				}
			}
			position = (rightPointer == -1) ? position :rightPointer*AshishBaseConstants.pageSize;
		} while (rightPointer != -1);
	}


	/**
	 * @param regexPattern
	 * @throws IOException
	 */
	public void findAndDeleteRegexPattern(String regexPattern) throws IOException{
		int cellPosition = 0;
		int numOfColumns = 0;
		// get the position of first leaf node
		int position = searchFirstLeafNodeInFile() * AshishBaseConstants.pageSize;
	
		// get right page pointer
		int rightPagePointer = 0;

		int regexPatternFound = 0;
		do {
			// read number of cells
			recordFileName.seek(position+1);
			int numberOfCellsInRecord = recordFileName.readByte();
			// read right page pointer
			recordFileName.seek(position+4);
			rightPagePointer = recordFileName.readInt();
			// firs record address 
			int newPosition = position +8;
			cellPosition = 0;
			// iterating over records
			for(int i = 0; i < numberOfCellsInRecord; i++){
				regexPatternFound = 0;
				// seeking records 
				recordFileName.seek(newPosition+(2*i));
				cellPosition = recordFileName.readShort();
				// seek at position
				recordFileName.seek(position+cellPosition+6);
				// read number of columns
				numOfColumns = recordFileName.readByte();
				String columnValue;
				// col  type 
				int columnTypePosition = position+cellPosition+7;
				// col  value type 
				int columnValuePosition = position+cellPosition+7+numOfColumns;
				// iterating over number of columns
				for(int j = 0; j < numOfColumns; j++){
					// colm type position
					columnTypePosition = position+cellPosition+7+j;
					recordFileName.seek(columnTypePosition);
					// read type
					Byte type = recordFileName.readByte();
					recordFileName.seek(columnValuePosition);
					columnValue = recordFileName.readValueByType(type,1);
					if(columnValue.equals(regexPattern)){
						regexPatternFound = 1;
					}
					int dataTypeSize = DataTypes.dataTypeGetTypeSize(DataTypes.dataTypeGetCodeval(type));
					if(dataTypeSize == 0){
						columnValuePosition += columnValue.length();
					}else{
						columnValuePosition += dataTypeSize;
					}
				}
				if(regexPatternFound == 1){
					recordFileName.seek(position+cellPosition);
					int cellsize = recordFileName.readShort()+6;
					for(int k = 0; k < cellsize; k++){
						recordFileName.seek(position+cellPosition+k);
						// write null
						recordFileName.writeByte(0x00);
					}
					recordFileName.seek(newPosition+(2*i));
					// write short
					recordFileName.writeShort(0);
					if(i == numberOfCellsInRecord-1){
						recordFileName.seek(position+1);
						recordFileName.writeByte(numberOfCellsInRecord-1);
					}
				}
			}
			position = (rightPagePointer == -1) ? position :rightPagePointer*AshishBaseConstants.pageSize;
		} while(rightPagePointer != -1);
	}


	/**
	 * @param rowId
	 * @param columnType
	 * @param columnValue
	 * @param recordSize
	 * @param pageNumber
	 * @return
	 * @throws IOException
	 */
	public int searchAndInsertLeafNodeForCell(int rowId,String[] columnType, String[] columnValue, int recordSize,int pageNumber) throws IOException{
		int returnVal = 0;
		Byte pageType = 0;
		if(pageNumber == -1){
			// do nothing
		}else{
			int position = (pageNumber)*AshishBaseConstants.pageSize;
			recordFileName.seek(position);
			// get page type
			pageType = recordFileName.readByte();
		}
		// depending upon page type do search
		if(pageType == 0){
			int pageNumberNew = (int)recordFileName.length()/AshishBaseConstants.pageSize;
			recordFileName.setLength(recordFileName.length()+AshishBaseConstants.pageSize);
			// create new page
			Page leafPage = new Page(recordFileName,rowId,recordSize,pageNumberNew,columnType,columnValue,(byte)0x0D);
			// write leaf
			leafPage.writePayloadOnLeaf();
			return pageNumberNew;
		}else if(pageType == 0x05){ // interior page type
			// create page
			Page interiorPage = new Page(recordFileName);
			// search leaf node
			int leafRecord = interiorPage.searchLeafNode(rowId,recordSize,pageNumber);
			// create page
			interiorPage = new Page(recordFileName,rowId,recordSize,leafRecord,columnType,columnValue,(byte)0x0D);
			// write on leaf
			returnVal = interiorPage.writePayloadOnLeaf();
			if(returnVal == -1){
				int oldleafRecord = leafRecord;
				// search for leaf node and insert
				leafRecord = searchAndInsertLeafNodeForCell(rowId,columnType,columnValue,recordSize,-1);
				int position = (oldleafRecord)*AshishBaseConstants.pageSize;
				recordFileName.seek(position+4);
				// write integer
				recordFileName.writeInt(leafRecord);
			}
			return leafRecord;
		}

		return returnVal;
	}



	/**
	 * @param rowId
	 * @param columnType
	 * @param columnValue
	 * @param cellSize
	 * @param pageNumber
	 * @param isLeaf
	 * @throws IOException
	 */
	public void insertNewCell(int rowId,String[] columnType, String[] columnValue, int cellSize,int pageNumber,boolean isLeaf) throws IOException{
		if(!isLeaf){ // not leaf
			if(recordFileName.length() < ((pageNumber+1)*AshishBaseConstants.pageSize)){
				recordFileName.setLength((pageNumber+1)*AshishBaseConstants.pageSize);
			}
			// check for interior cell address 
			if(checkInteriorCellPositions(pageNumber)){
				int leafpageno = searchAndInsertLeafNodeForCell(rowId,columnType,columnValue,cellSize,pageNumber);
				// insert new data
				insertNewCell(pageNumber,leafpageno,rowId);
			}
		}
	}
	/**
	 * @param pageNumber
	 * @param leafPageNumber
	 * @param rowId
	 * @throws IOException
	 */
	public void insertNewCell(int pageNumber,Integer leafPageNumber,Integer rowId) throws IOException{
		// get starting address
		int position = pageNumber * AshishBaseConstants.pageSize;
		recordFileName.seek(position);
		
		String[] columnValue = new String[2];
		columnValue[1] = rowId.toString();
		columnValue[0] = leafPageNumber.toString();
		// create a new page
		Page interiorPage = new Page(recordFileName, 0x05,pageNumber, columnValue);
		// write in the interior page
		interiorPage.writeInInteriorPage(rowId,leafPageNumber);

	}


	/**
	 * @return
	 * @throws IOException
	 */
	public int searchFirstLeafNodeInFile() throws IOException{
		int position = 0;
		int leafRecord = 0;
		int cellAddress = 0;
		try {
			while(true){
				//iterate over cells address
				recordFileName.seek(position+8);
				cellAddress = recordFileName.readShort();
				recordFileName.seek(position+cellAddress);
				// record at leaf
				leafRecord = recordFileName.readInt();
				int newAddress = leafRecord * AshishBaseConstants.pageSize;
				recordFileName.seek(newAddress);
				// get page type
				int pageType = recordFileName.readByte();
				if(pageType == 0x0D){
					break;
				}else{
					position = newAddress;
				}
			}

			return leafRecord;
		} catch (Exception e) {
			//System.out.println("error in performing operation");
		}
		return 0;
	}


	/**
	 * @return
	 * @throws IOException
	 */
	public List<String[]> findAllRecords() throws IOException{
		// find all records in table
		try {
			// leaf node
			int position = searchFirstLeafNodeInFile() * AshishBaseConstants.pageSize;
			recordFileName.seek(position+4);
			// cell Address
			int cellAddress = 0;
			int numOfColumns = 0;
			// right page pointer
			int rightPagePointer = recordFileName.readInt();
			List<String[]> recordList = new ArrayList<String[]>();
			String columnValueArray[];
			do{
				// right page pointer
				recordFileName.seek(position+4);
				rightPagePointer = recordFileName.readInt();
				// number of records
				recordFileName.seek(position+1);
				int numberOfCells = recordFileName.readByte();
				// first record address
				int newPosition = position +8;
				cellAddress = 0;
				// iterate over all cells
				for(int i = 0; i < numberOfCells; i++){
					recordFileName.seek(newPosition+(2*i));
					// getting cell address
					cellAddress = recordFileName.readShort();
					if(cellAddress == 0){
						continue;
					}
					// number of columns
					recordFileName.seek(position+cellAddress+6);
					numOfColumns = recordFileName.readByte();
					columnValueArray = new String[numOfColumns+1];
					recordFileName.seek(position+cellAddress+2);
					// row number
					Integer rowId = recordFileName.readInt();
					columnValueArray[0] = rowId.toString();
					// reading column types 
					int columnTypePosition = position+cellAddress+7;
					// reading column value
					int columnValuePosition = position+cellAddress+7+numOfColumns;
					// iterating over number of columns
					for(int j = 0; j < numOfColumns; j++){
						columnTypePosition = position+cellAddress+7+j;
						recordFileName.seek(columnTypePosition);
						// reading datatype
						Byte dataType = recordFileName.readByte();
						recordFileName.seek(columnValuePosition);
						// reading value by type
						columnValueArray[j+1] = recordFileName.readValueByType(dataType,1);
						int dataTypeSize = DataTypes.dataTypeGetTypeSize(DataTypes.dataTypeGetCodeval(dataType));
						if(dataTypeSize == 0){
							columnValuePosition += columnValueArray[j+1].length();
						}else{
							columnValuePosition += dataTypeSize;
						}
					}
					recordList.add(columnValueArray);

				}
				position = (rightPagePointer == -1) ? position :rightPagePointer*AshishBaseConstants.pageSize;
			}while(rightPagePointer != -1);

			return recordList;
		} catch (Exception e) {
			return null;
		}
	}


	/**
	 * @param regexPattern
	 * @param ordinalNumber
	 * @param operator
	 * @return
	 * @throws IOException
	 */
	public List<String[]> findRecordColumnEntries(String regexPattern,int ordinalNumber, String operator) throws IOException{
		// get page address
		int position = searchFirstLeafNodeInFile() * AshishBaseConstants.pageSize;
		if(position == 0 ) {
			return null;
		}
		int cellPosition = 0;
		int numberOfColumns = 0;
		// read right page pointer
		int rightPagePointer = recordFileName.readInt();
		List<String[]> recordList = new ArrayList<String[]>();
		String columnValueArray[];
		int pfound = 0;  // pattern found
		int ordinalNumberCheck = 0;
		do{
			// number of cells
			recordFileName.seek(position+1);
			int numberOfCells = recordFileName.readByte();
			// right page pointer
			recordFileName.seek(position+4);
			rightPagePointer = recordFileName.readInt();
			// starting records address
			int newPosition = position +8;
			cellPosition = 0;
			// iterating over all records
			for(int i = 0; i < numberOfCells; i++){
				pfound = 0;
				ordinalNumberCheck = 1;
				recordFileName.seek(newPosition+(2*i));
				cellPosition = recordFileName.readShort();
				if(cellPosition == 0){
					continue;
				}
				// reading num of col
				recordFileName.seek(position+cellPosition+6);
				numberOfColumns = recordFileName.readByte();
				// col val array
				columnValueArray = new String[numberOfColumns+1];
				recordFileName.seek(position+cellPosition+2);
				// reading row id
				Integer rowId = recordFileName.readInt();
				columnValueArray[0] = rowId.toString();
				if(ordinalNumberCheck == ordinalNumber){
					if(recordFileName.compareValueMethod((byte)0x06, columnValueArray[0], regexPattern, operator)){
						pfound = 1;
					}
				}
				// reading colm type
				int columnTypePosition = position+cellPosition+7;
				// readign colm value
				int columnValuePosition = position+cellPosition+7+numberOfColumns;
				// iterating over all col
				for(int j = 0; j < numberOfColumns; j++){
					ordinalNumberCheck++;
					columnTypePosition = position+cellPosition+7+j;
					recordFileName.seek(columnTypePosition);
					// read data type
					Byte dataType = recordFileName.readByte();
					// read colm value
					recordFileName.seek(columnValuePosition);
					columnValueArray[j+1] = recordFileName.readValueByType(dataType,1);
					if(ordinalNumberCheck == ordinalNumber){
						if(recordFileName.compareValueMethod(dataType, columnValueArray[j+1], regexPattern, operator)){
							pfound = 1;
						}
					}
					int dataSize = DataTypes.dataTypeGetTypeSize(DataTypes.dataTypeGetCodeval(dataType));
					if(dataSize == 0){
						columnValuePosition += columnValueArray[j+1].length();
					}else{
						columnValuePosition += dataSize;
					}
				}
				if(pfound == 1){
					recordList.add(columnValueArray);
				}else{
					continue;
				}

			}
			position = (rightPagePointer == -1) ? position :rightPagePointer*AshishBaseConstants.pageSize;
		}while(rightPagePointer != -1);

		return recordList;

	}

	/**
	 * @param pageNumber
	 * @return
	 * @throws IOException
	 */
	public boolean checkInteriorCellPositions(int pageNumber) throws IOException {
		int position = (pageNumber) * AshishBaseConstants.pageSize;
		position += 1;

		recordFileName.seek(position);
		Byte numberOfCells = recordFileName.readByte();
		if(numberOfCells < ((AshishBaseConstants.pageSize-8)/10)){
			return true;
		}else{
			return false;
		}
	}
	/**
	 * @param val
	 * @param ordinalNumber
	 * @param ordinalNumberArray
	 * @param valArray
	 * @param operator1Array
	 * @param operator2Array
	 * @throws IOException
	 */
	public void updateRecords(String val, int ordinalNumber, int[] ordinalNumberArray,String[] valArray, String[] operator1Array, String[] operator2Array) throws IOException{
		int[] foundArray = new int[operator1Array.length];
		int position = searchFirstLeafNodeInFile() * AshishBaseConstants.pageSize;
		int cellPosition = 0;
		int numberofColumns = 0;
		int rightPagePointer = recordFileName.readInt();
		//List<String[]> list = new ArrayList<String[]>();
		int regexPatternFound = 0;
		int ordinalNumberCheck = 0;
		int conditionSatisfied = 0;
		do{
			recordFileName.seek(position+1);
			int numOfCells = recordFileName.readByte();
			recordFileName.seek(position+4);
			rightPagePointer = recordFileName.readInt();
			int newPosition = position +8;
			cellPosition = 0;
			for(int i = 0; i < numOfCells; i++){
				regexPatternFound = 0;
				ordinalNumberCheck = 1;
				recordFileName.seek(newPosition+(2*i));
				cellPosition = recordFileName.readShort();
				if(cellPosition == 0){
					continue;
				}
				recordFileName.seek(position+cellPosition+6);
				numberofColumns = recordFileName.readByte();
				recordFileName.seek(position+cellPosition+2);
				Integer rowId = recordFileName.readInt();
				String columnValue = rowId.toString();
				for(int k=0; k < ordinalNumberArray.length; k++){
					int localOrdinalnumber = ordinalNumberArray[k];
					if(ordinalNumberCheck == localOrdinalnumber){
						if(recordFileName.compareValueMethod((byte)0x06, columnValue, valArray[k], operator1Array[k])){
							foundArray[k] = 1;
							regexPatternFound = 1;
						}
					}
				}
				int columnTypePosition = position+cellPosition+7;
				int columnnValuePosition = position+cellPosition+7+numberofColumns;
				for(int j = 0; j < numberofColumns; j++){
					ordinalNumberCheck++;
					columnTypePosition = position+cellPosition+7+j;
					recordFileName.seek(columnTypePosition);
					Byte type = recordFileName.readByte();
					recordFileName.seek(columnnValuePosition);
					columnValue = recordFileName.readValueByType(type,1);
					for(int k=0; k < ordinalNumberArray.length; k++){
						int ordinalnum = ordinalNumberArray[k];
						if(ordinalNumberCheck == ordinalnum){
							if(recordFileName.compareValueMethod(type, columnValue, valArray[k], operator1Array[k])){
								foundArray[k] = 1;
								regexPatternFound = 1;
							}
						}
					}
					int size = DataTypes.dataTypeGetTypeSize(DataTypes.dataTypeGetCodeval(type));
					if(size == 0){
						columnnValuePosition += columnValue.length();
					}else{
						columnnValuePosition += size;
					}
				}
				if(regexPatternFound == 1){
					conditionSatisfied = foundArray[0];
					for(int k=1; k < foundArray.length; k++){
						if(operator2Array[k-1].equals("and")){
							conditionSatisfied = conditionSatisfied & foundArray[k];
						}else{
							conditionSatisfied = conditionSatisfied | foundArray[k];
						}
					}

					if(conditionSatisfied == 1){
						ordinalNumberCheck = 1;
						recordFileName.seek(position+cellPosition+6);
						numberofColumns = recordFileName.readByte();
						recordFileName.seek(position+cellPosition+2);
						if(ordinalNumber == ordinalNumberCheck){
							recordFileName.writeInt(Integer.parseInt(val));
						}
						columnTypePosition = position+cellPosition+7;
						columnnValuePosition = position+cellPosition+7+numberofColumns;
						for(int j = 0; j < numberofColumns; j++){
							ordinalNumberCheck++;
							columnTypePosition = position+cellPosition+7+j;
							recordFileName.seek(columnTypePosition);
							Byte type = recordFileName.readByte();
							recordFileName.seek(columnnValuePosition);
							columnValue = recordFileName.readValueByType(type,1);
							if(ordinalNumber == ordinalNumberCheck){
								recordFileName.seek(columnnValuePosition);
								recordFileName.writeValueByType(val, type);
							}
							int dataTypesize = DataTypes.dataTypeGetTypeSize(DataTypes.dataTypeGetCodeval(type));
							if(dataTypesize == 0){
								columnnValuePosition += columnValue.length();
							}else{
								columnnValuePosition += dataTypesize;
							}
						}
					}
				}
			}
			position = (rightPagePointer == -1) ? position :rightPagePointer*AshishBaseConstants.pageSize;
		}while(rightPagePointer != -1);
	}

	/**
	 * @param regexPattern
	 * @param ordinalNumber
	 * @param operator
	 * @param regexPattern2
	 * @param ordinalNumber2
	 * @param operator2
	 * @throws IOException
	 */
	public void findAndDeleteRegexPattern(String regexPattern, int ordinalNumber, String operator,String regexPattern2,int ordinalNumber2, String operator2) throws IOException{
		int position = searchFirstLeafNodeInFile() * AshishBaseConstants.pageSize;
		int cellPosition = 0;
		int numOfColumns = 0;
		int rightPagePointer = recordFileName.readInt();
		int pfound = 0;  //pattern 1 flag
		int p2found = 0; // pattern 2 flag
		int ordinalNumberCheck = 0;
		do{
			recordFileName.seek(position+1);
			int numberOfCells = recordFileName.readByte();
			recordFileName.seek(position+4);
			rightPagePointer = recordFileName.readInt();
			int newPosition = position +8;
			cellPosition = 0;
			for(int i = 0; i < numberOfCells; i++){
				pfound = 0;
				p2found = 0;
				ordinalNumberCheck = 1;
				recordFileName.seek(newPosition+(2*i));
				cellPosition = recordFileName.readShort();
				if(cellPosition == 0){
					continue;
				}
				recordFileName.seek(position+cellPosition+6);
				numOfColumns = recordFileName.readByte();
				recordFileName.seek(position+cellPosition+2);
				Integer rowId = recordFileName.readInt();
				String columnValue = rowId.toString();
				if(ordinalNumberCheck == ordinalNumber){
					if(recordFileName.compareValueMethod((byte)0x06, columnValue, regexPattern, operator)){
						pfound = 1;
					}
				}
				if(ordinalNumberCheck == ordinalNumber2){
					if(recordFileName.compareValueMethod((byte)0x06, columnValue, regexPattern2, operator2)){
						p2found = 1;
					}
				}
				int columnTypePosition = position+cellPosition+7;
				int columnValuePosition = position+cellPosition+7+numOfColumns;
				for(int j = 0; j < numOfColumns; j++){
					ordinalNumberCheck++;
					columnTypePosition = position+cellPosition+7+j;
					recordFileName.seek(columnTypePosition);
					Byte type = recordFileName.readByte();
					recordFileName.seek(columnValuePosition);
					columnValue = recordFileName.readValueByType(type,1);
					if(ordinalNumberCheck == ordinalNumber){
						if(recordFileName.compareValueMethod(type, columnValue, regexPattern, operator)){
							pfound = 1;
						}
					}
					if(ordinalNumberCheck == ordinalNumber2){
						if(recordFileName.compareValueMethod(type, columnValue, regexPattern2, operator2)){
							p2found = 1;
						}
					}
					int dataTypeSize = DataTypes.dataTypeGetTypeSize(DataTypes.dataTypeGetCodeval(type));
					if(dataTypeSize == 0){
						columnValuePosition += columnValue.length();
					}else{
						columnValuePosition += dataTypeSize;
					}
				}
				if(pfound == 1 & p2found==1){
					recordFileName.seek(position+cellPosition);
					int recordCellSize = recordFileName.readShort()+6;
					for(int k = 0; k < recordCellSize; k++){
						recordFileName.seek(position+cellPosition+k);
						recordFileName.writeByte(0x00);
					}
					recordFileName.seek(newPosition+(2*i));
					recordFileName.writeShort(0);
					if(i == numberOfCells-1){
						recordFileName.seek(position+1);
						recordFileName.writeByte(numberOfCells-1);
					}
				}
			}
			position = (rightPagePointer == -1) ? position :rightPagePointer*AshishBaseConstants.pageSize;
		}while(rightPagePointer != -1);
	}
	/**
	 * @param pattern
	 * @return
	 * @throws IOException
	 */
	public List<String[]> findRecordColumnEntries(String pattern) throws IOException{
		int position = searchFirstLeafNodeInFile() * AshishBaseConstants.pageSize;
		int cellPosition = 0;
		int numberOfColumns = 0;
		int rightPagePointer = recordFileName.readInt();
		List<String[]> recordList = new ArrayList<String[]>();
		String columnValueArray[];
		int pFound = 0;  // pattern found flag
		do{
			recordFileName.seek(position+1);
			int numberOfCells = recordFileName.readByte();
			recordFileName.seek(position+4);
			rightPagePointer = recordFileName.readInt();
			int newPosition = position +8;
			cellPosition = 0;
			for(int i = 0; i < numberOfCells; i++){
				pFound = 0;
				recordFileName.seek(newPosition+(2*i));
				cellPosition = recordFileName.readShort();
				if(cellPosition == 0){
					continue;
				}
				recordFileName.seek(position+cellPosition+6);
				numberOfColumns = recordFileName.readByte();
				columnValueArray = new String[numberOfColumns+1];
				recordFileName.seek(position+cellPosition+2);
				Integer rowId = recordFileName.readInt();
				columnValueArray[0] = rowId.toString();
				int columnTypePosition = position+cellPosition+7;
				int columnValuePosition = position+cellPosition+7+numberOfColumns;
				for(int j = 0; j < numberOfColumns; j++){
					columnTypePosition = position+cellPosition+7+j;
					recordFileName.seek(columnTypePosition);
					Byte dataType = recordFileName.readByte();
					recordFileName.seek(columnValuePosition);
					columnValueArray[j+1] = recordFileName.readValueByType(dataType,1);
					if(columnValueArray[j+1].equals(pattern)){
						pFound = 1;
					}
					int recordSize = DataTypes.dataTypeGetTypeSize(DataTypes.dataTypeGetCodeval(dataType));
					if(recordSize == 0){
						columnValuePosition += columnValueArray[j+1].length();
					}else{
						columnValuePosition += recordSize;
					}
				}
				if(pFound == 1){
					recordList.add(columnValueArray);
				}else{
					continue;
				}

			}
			position = (rightPagePointer == -1) ? position :rightPagePointer*AshishBaseConstants.pageSize;
		}while(rightPagePointer != -1);

		return recordList;

	}
	/**
	 * @param pageNumber
	 * @return
	 * @throws IOException
	 */
	public int findIdOfLastRow(int pageNumber) throws IOException{
		int position = (pageNumber)*AshishBaseConstants.pageSize;
		recordFileName.seek(position);
		Byte pageType = recordFileName.readByte();
		if(pageType == 0){
			return 0;
		}else if(pageType == 0x05){
			recordFileName.seek(position+4);
			int rightpointer = recordFileName.readInt();
			if(rightpointer != -1){
				return findIdOfLastRow(rightpointer);
			}else{
				recordFileName.seek(position+2);
				short lastcellpos = recordFileName.readShort();
				recordFileName.seek(position+lastcellpos);
				int leafpage = recordFileName.readInt();
				return findIdOfLastRow(leafpage);
			}
		}else if(pageType == 0x0D){
			recordFileName.seek(position+1);
			short lastCellAddress;
			int numOfCells = recordFileName.readByte();
			while(true){
				recordFileName.seek(position+8+(numOfCells*2));
				lastCellAddress = recordFileName.readShort();
				if(lastCellAddress == 0){
					numOfCells--;
				}
				else{
					break;
				}
				if(numOfCells == 0){
					return -1;
				}
			}
			recordFileName.seek(position+lastCellAddress+2);
			return recordFileName.readInt();
		}

		return -1;
	}


}