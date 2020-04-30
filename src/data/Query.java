package data;

public class Query {
	private String database;
	private String query;
	private Column[] canditateColumns;
	
	public Query(String database, String query, Column[] canditateColumns) {
		this.database = database;
		this.query = query;
		this.canditateColumns = canditateColumns;
	}

	public String getDatabase() {
		return database;
	}

	public String getQuery() {
		return query;
	}

	public Column[] getCanditateColumns() {
		return canditateColumns;
	}
}
