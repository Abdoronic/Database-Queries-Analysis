package data;

public class Index {
	private String database;
	private String indexName;
	private String indexType;
	private String tableName;
	private String[] columns;
	
	public Index(String database, String indexName, String indexType, String tableName, String[] columns) {
		this.database = database;
		this.indexName = indexName;
		this.indexType = indexType;
		this.tableName = tableName;
		this.columns = columns;
	}

	public String getDatabase() {
		return database;
	}
	
	public String getIndexName() {
		return indexName;
	}

	public String getIndexType() {
		return indexType;
	}

	public String getTableName() {
		return tableName;
	}

	public String[] getColumns() {
		return columns;
	}
	
}
