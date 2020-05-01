package core;

import java.sql.Statement;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import data.Index;
import data.Query;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class DBHandler {
	
	PrintWriter out;
	
	public DBHandler(PrintWriter out) {
		this.out = out;
	}
	
	public void printDivider(int len) {
		out.println();
		for(int i = 0; i < len; i++)
			out.print("#");
		out.println();
		out.println();
	}
	public void printLightDivider(int len) {
		out.println();
		for(int i = 0; i < len; i++)
			out.print("-");
		out.println();
		out.println();
	}

	public void printJSON(JSONObject json) {
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonParser jp = new JsonParser();
		JsonElement je = jp.parse(json.toJSONString());
		String prettyJsonString = gson.toJson(je);
		out.println(prettyJsonString);
	}

	public double runQuery(Query queryObj, boolean showResults) {
		String database = queryObj.getDatabase();
		String query = queryObj.getQuery();
		if(showResults) {
			printLightDivider(30);
			printResults(database, query);
		} else {
			double totalCost = printPlanAndCost(database, "explain (format JSON) " + query);
			return totalCost;
		}
		printDivider(30);
		return 0;
	}
	
	public void printResults(String database, String query) {
		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + database,
				"postgres", "12345678")) {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(query);
			ResultSetMetaData metadata = resultSet.getMetaData();
			for(int i = 1; i <= metadata.getColumnCount(); i++)
				out.printf("%-30s", metadata.getColumnLabel(i) + " ");
			out.println();
			int sizeOfOutput = 0;
			while (resultSet.next()) {
				for(int i = 1; i <= metadata.getColumnCount(); i++)
					out.printf("%-30s", resultSet.getObject(i) + " ");
				out.println();
				++sizeOfOutput;
			}
			out.println();
			out.println();
			
			out.println("End of Results");
			out.println("The Size of the Result Set = " + sizeOfOutput);
		} catch (SQLException e) {
			System.err.println("Connection Faliure");
			e.printStackTrace(System.err);
		}
	}

	public double printPlanAndCost(String database, String query) {
		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + database,
				"postgres", "12345678")) {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(query);
			JSONParser jsonParser = new JSONParser();
			while (resultSet.next()) {
				JSONObject planContainer = (JSONObject) ((JSONArray) jsonParser.parse(resultSet.getString(1))).get(0);
				JSONObject plan = (JSONObject) planContainer.get("Plan");
				out.printf("1. The Total Cost of the Plan = %s\n", plan.get("Total Cost"));
				out.printf("2. The Estimated Size of the Result Set = %s\n", plan.get("Plan Rows"));
				out.printf("3. The Plan of the Query:\n");
				printJSON(plan);
				return Double.parseDouble(plan.get("Total Cost").toString());
			}
			out.println("End of Query Plan");
		} catch (SQLException e) {
			System.err.println("Connection Faliure");
			e.printStackTrace(System.err);
		} catch (ParseException e) {
			System.err.println("Error Parsing JSON");
			e.printStackTrace(System.err);
		}
		return -1;
	}

	public void createIndex(Index createdIndex) {
		String database = createdIndex.getDatabase();
		String indexName = createdIndex.getIndexName();
		String indexType = createdIndex.getIndexType();
		String tableName = createdIndex.getTableName();
		String[] columns = createdIndex.getColumns();
		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + database,
				"postgres", "12345678")) {
			Statement statement = connection.createStatement();
			String indexCommand = "CREATE INDEX " + indexName + " ON " + tableName + " USING " + indexType + " ( ";
			for (int i = 0; i < columns.length; i++) {
				indexCommand += columns[i] + (i == columns.length - 1 ? ");" : ", ");
			}
			statement.executeUpdate(indexCommand);
//			System.out.printf("Index %s Created\n", indexName);
		} catch (SQLException e) {
			System.err.println("Error Creating Index: " + indexName);
			e.printStackTrace(System.err);
		}
	}

	public void dropIndex(Index deletedIndex) {
		String database = deletedIndex.getDatabase();
		String indexName = deletedIndex.getIndexName();
		try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + database,
				"postgres", "12345678")) {
			Statement statement = connection.createStatement();
			String indexCommand = "DROP INDEX " + indexName + ";";
			statement.executeUpdate(indexCommand);
//			System.out.printf("Index %s Dropped\n", indexName);
		} catch (SQLException e) {
			System.err.println("Error Dropping Index: " + indexName);
			e.printStackTrace(System.err);
		}
	}
	
}
