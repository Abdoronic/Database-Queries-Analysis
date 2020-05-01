package data;

public class Column {
	private String tableName;
	private String colName;
	
	public Column(String tableName, String colName) {
		this.tableName = tableName;
		this.colName = colName;
	}

	public String getTableName() {
		return tableName;
	}

	public String getColName() {
		return colName;
	}
}
