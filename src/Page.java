
import java.io.IOException;

public class Page {
	static int pageSize = 512;
	public int cellSize;
	public int pageNumber;
	public String[] columnTypeArray;
	public String[] columnValueArray;
	
	TableClass recordFile;
	public int rowId;
	public int pageType;
	
	
	/**
	 * constructor
	 * @param file
	 */
	public Page(TableClass file){
		this.recordFile = file;
	}
	
	/**
	 * constructor
	 * @param file
	 * @param pageType
	 * @param pageNumber
	 * @param columnValueArray
	 */
	public Page(TableClass file, int pageType, int pageNumber, String[] columnValueArray){
		this.recordFile = file;
		this.pageNumber = pageNumber;
		this.columnValueArray = columnValueArray;
		this.pageType = pageType;
	}
	
	
	/**
	 * constructor
	 * @param file
	 * @param rowId 
	 * @param recordCellSize
	 * @param pageNumber
	 * @param columnTypeArray
	 * @param columnvalueArray
	 * @param pageType
	 */
	public Page(TableClass file,int rowId ,int recordCellSize, int pageNumber, String[] columnTypeArray,String[] columnvalueArray,int pageType){
		this.recordFile = file;
		this.columnTypeArray = columnTypeArray;
		this.columnValueArray = columnvalueArray;
		this.pageNumber = pageNumber;
		this.rowId = rowId ;
		this.cellSize = recordCellSize;
		this.pageType = pageType;
	}
	
	
	/**
	 * @return
	 * @throws IOException
	 */
	public int writePayloadOnLeaf() throws IOException{
		int position = pageNumber * pageSize;
		recordFile.seek(position);
		Byte currentPageType = recordFile.readByte();
		short recordPayloadSize=0;
		short cellPosition=0;
		if(currentPageType == 0){
			recordFile.seek(position);
			recordFile.writeByte(pageType);
			recordFile.seek(position+1);
			recordFile.writeByte(0x01);
			recordFile.seek(position+4);
			recordFile.writeInt(-1);
			recordPayloadSize = (short)(columnTypeArray.length+cellSize+1);
			cellPosition = (short)(pageSize - (6 + recordPayloadSize));		
		}else{
			int numOfCells = 0;
			recordFile.seek(position+1);
			numOfCells = recordFile.readByte();
			recordFile.seek(position+2);
			cellPosition = recordFile.readShort();
			if(cellPosition < (cellSize+columnTypeArray.length+6+1+8+((numOfCells+1)*2))){
				return -1;
			}
			recordFile.seek(position+1);
			numOfCells = recordFile.readByte();
			recordFile.seek(position+1);
			recordFile.writeByte(numOfCells+1);
			cellPosition -= (cellSize+columnTypeArray.length+6+1);
			recordPayloadSize = (short)(columnTypeArray.length+cellSize+1);
		}
		recordFile.seek(position+8);
		int recItr = 0;
		while(recordFile.readShort() != 0){
			recItr +=2;
			recordFile.seek(position+8+recItr);
		}
		recordFile.seek(position+8+recItr);
		recordFile.writeShort(cellPosition);
		recordFile.seek(position+2);
		recordFile.writeShort(cellPosition);
		recordFile.seek(position+cellPosition);
		recordFile.writeShort(recordPayloadSize);
		recordFile.seek(position+cellPosition+2);
		recordFile.writeInt(rowId);
		recordFile.seek(position+cellPosition+6);
		recordFile.writeByte(columnTypeArray.length);
		for(int colmTypeArrayItr=0; colmTypeArrayItr < columnTypeArray.length; colmTypeArrayItr++){
			Byte type = DataTypes.dataTypeGetTypeCode(columnTypeArray[colmTypeArrayItr]);
			if(type == 0x0C){
				int len = columnValueArray[colmTypeArrayItr].length() & 0xFF;
				type = (byte) (type + (len & 0xFF));
			}
			recordFile.seek(position+cellPosition+7+colmTypeArrayItr);
			recordFile.writeByte(type);
		}
		int newPosition = position +cellPosition+7+columnTypeArray.length;
		for(int itr = 0; itr < columnValueArray.length; itr++){
			recordFile.seek(newPosition);
			recordFile.writeValueByType(columnValueArray[itr], DataTypes.dataTypeGetTypeCode(columnTypeArray[itr]));
			newPosition += DataTypes.dataTypeGetTypeSize(columnTypeArray[itr]);
			if(DataTypes.dataTypeGetTypeCode(columnTypeArray[itr]) == 0x0C){
				newPosition += columnValueArray[itr].length();
			}
		}
		return 1;
	}
	
	
	/**
	 * @param recordRowId
	 * @param recordCellSize
	 * @param recordPageNumber
	 * @return
	 * @throws IOException
	 */
	public int searchLeafNode(int recordRowId,int recordCellSize,int recordPageNumber) throws IOException{
		pageNumber = recordPageNumber;
		rowId = recordRowId;
		
		int position = pageNumber * pageSize;
		recordFile.seek(position+1);
		int numOfCells = recordFile.readByte();
		int leafPage = 0;
		int cellPosition = 0;
		int cellRowId = 0;
		recordFile.seek(position+4);
		int leafFound = 0;
		int rightPagePointer = recordFile.readInt();
		recordFile.seek(position+8);
		for(int itr = 0; itr < numOfCells; itr++){
			recordFile.seek(position+8+(2*itr));
			cellPosition = recordFile.readShort();
			if(cellPosition == 0){
				continue;
			}
			recordFile.seek(position+cellPosition);
			leafPage = recordFile.readInt();
			recordFile.seek(position+cellPosition+4);
			cellRowId = recordFile.readInt();
			if(rowId > cellRowId){
				continue;
			}else{
				leafFound = 1;
				break;
			}
		}
		if(leafFound == 0){
			if(rightPagePointer!= -1){
				return rightPagePointer;
			}
		}
		return leafPage;	
		
	}

	
	/**
	 * @param cellRowNumber
	 * @param leafPageNumber
	 * @throws IOException
	 */
	public void writeInInteriorPage(int cellRowNumber,int leafPageNumber) throws IOException{
		int position = pageNumber * pageSize;
		recordFile.seek(position);
		Byte currentPageType = recordFile.readByte();
		short cellPosition = 0;
		int currentLeafNumber = 0;
		int currentRowId = 0;
		if(currentPageType != 0x05){
			recordFile.seek(position);
			recordFile.writeByte(pageType);
			recordFile.seek(position+1);
			recordFile.writeByte(0x01);
			recordFile.seek(position+4);
			recordFile.writeInt(-1);
			cellPosition = (short)(pageSize - 10);
		}else{
			recordFile.seek(position+4);
			int rightPagePointer = recordFile.readInt();
			if(leafPageNumber == rightPagePointer){
				return;
			}
			recordFile.seek(position+2);
			cellPosition = recordFile.readShort();
			if(rightPagePointer != -1){
				currentLeafNumber = rightPagePointer;
				int newPosition = currentLeafNumber * pageSize;
				recordFile.seek(newPosition+2);
				int newcellPosition = recordFile.readShort();
				recordFile.seek(newPosition+newcellPosition+2);
				currentRowId = recordFile.readInt();
				cellPosition -= 8;
				recordFile.seek(position+cellPosition);
				recordFile.writeInt(rightPagePointer);
				recordFile.seek(position+cellPosition+4);
				recordFile.writeInt(currentRowId);
				recordFile.seek(position+4);
				recordFile.writeInt(leafPageNumber);
				return;
			}else{
				recordFile.seek(position+2);
				cellPosition = recordFile.readShort();
				recordFile.seek(position+cellPosition);
				currentLeafNumber = recordFile.readInt();
				recordFile.seek(position+cellPosition+4);
				currentRowId = recordFile.readInt();
				if(currentLeafNumber == leafPageNumber){
					if(cellRowNumber > currentRowId){
						recordFile.seek(position+cellPosition+4);
						recordFile.writeValueByType(columnValueArray[1], DataTypes.dataTypeGetTypeCode(DataTypes.INT));
					}
					return;
				}else{
					recordFile.seek(position+4);
					rightPagePointer = recordFile.readInt();
					if(rightPagePointer == -1){
						recordFile.seek(position+4);
						recordFile.writeInt(leafPageNumber);
					}
					return;
				}
			}
		}
		recordFile.seek(position+8);
		int itr = 0;
		while(recordFile.readByte() != 0){
			itr +=2;
			recordFile.seek(position+8+itr);
		}
		recordFile.seek(position+8+itr);
		recordFile.writeShort(cellPosition);
		recordFile.seek(position+2);
		recordFile.writeShort(cellPosition);
		recordFile.seek(position+cellPosition);
		recordFile.writeValueByType(columnValueArray[0], DataTypes.dataTypeGetTypeCode(DataTypes.INT));
		recordFile.writeValueByType(columnValueArray[1], DataTypes.dataTypeGetTypeCode(DataTypes.INT));
	}

}
