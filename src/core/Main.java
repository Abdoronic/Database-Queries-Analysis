package core;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import data.Column;
import data.Index;
import data.Query;

public class Main {

	static String[] indicies = { "none", "btree", "hash" };
	static int configurationCount, optimalConfiguration;
	static double optimalCost;
	static PrintWriter out;
	static String outputPath = "/Users/abdulrahmanibrahim/eclipse-workspace/DBQueriesAnalysis/output/";
	static DBHandler dbHandler;

	public static void printDivider(int len) {
		System.out.println();
		for (int i = 0; i < len; i++)
			System.out.print("#");
		System.out.println();
		System.out.println();
	}

	public static void printLightDivider(int len) {
		System.out.println();
		for (int i = 0; i < len; i++)
			System.out.print("-");
		System.out.println();
		System.out.println();
	}

	public static void createIndicies(Query query, Column[] candidateColumns, String[] chosenIndex) {
		for (int i = 0; i < candidateColumns.length; i++) {
			if (chosenIndex[i].equals("none"))
				continue;
			String indexName = candidateColumns[i].getTableName() + "_" + candidateColumns[i].getColName();
			Index index = new Index(query.getDatabase(), "IDX_" + indexName, chosenIndex[i],
					candidateColumns[i].getTableName(), new String[] { candidateColumns[i].getColName() });
			dbHandler.createIndex(index);
		}
	}

	public static void dropIndicies(Query query, Column[] candidateColumns, String[] chosenIndex) {
		for (int i = 0; i < candidateColumns.length; i++) {
			if (chosenIndex[i].equals("none"))
				continue;
			String indexName = candidateColumns[i].getTableName() + "_" + candidateColumns[i].getColName();
			Index index = new Index(query.getDatabase(), "IDX_" + indexName, chosenIndex[i],
					candidateColumns[i].getTableName(), new String[] { candidateColumns[i].getColName() });
			dbHandler.dropIndex(index);
		}
	}

	public static void printIndiciesConfigurations(Query queryObj, Column[] candidateColumns, String[] chosenIndex) {
		dbHandler.printLightDivider(30);
		out.println("Indicies Configuration #" + ++configurationCount + ":");
		int numOfIndicies = 0;
		for (int i = 0; i < candidateColumns.length; i++) {
			if (chosenIndex[i].equals("none"))
				continue;
			out.printf("Index %s on column %s in table %s\n", chosenIndex[i], candidateColumns[i].getColName(),
					candidateColumns[i].getTableName());
			numOfIndicies++;
		}
		if (numOfIndicies == 0)
			out.println("No Indicies Used");
		dbHandler.printLightDivider(30);
	}

	public static void tryAllIndicies(Query queryObj, Column[] candidateColumns, String[] chosenIndex, int idx) {
		if (idx == candidateColumns.length) {
			createIndicies(queryObj, candidateColumns, chosenIndex);
			printIndiciesConfigurations(queryObj, candidateColumns, chosenIndex);
			double cost = dbHandler.runQuery(queryObj, false);
			if (cost < optimalCost) {
				optimalCost = cost;
				optimalConfiguration = configurationCount;
			}
			dropIndicies(queryObj, candidateColumns, chosenIndex);
			return;
		}

		for (int i = 0; i < indicies.length; i++) {
			chosenIndex[idx] = indicies[i];
			tryAllIndicies(queryObj, candidateColumns, chosenIndex, idx + 1);
		}

	}

	public static void tryAll(Query queryObj) {
		out.printf("Connected to Database: \"%s\"\n\n", queryObj.getDatabase());
		out.printf("Running Query:\n %s\n\n", queryObj.getQuery());
		out.println("Query Result Set:");
		dbHandler.runQuery(queryObj, true);
		configurationCount = 0;
		optimalCost = 1e9;
		Column[] candidateColumns = queryObj.getCanditateColumns();
		tryAllIndicies(queryObj, candidateColumns, new String[candidateColumns.length], 0);
		out.printf("The Optimal Configuration is Number %d with Total Cost = %f\n", optimalConfiguration, optimalCost);
		dbHandler.printDivider(30);
		out.println("End of Query Analyzation");
		dbHandler.printDivider(30);
	}

	public static Query[] getAllQueries() {
		Query[] queries = new Query[18];
		queries[1] = new Query("schema1",
				"select *\n" + "from (select *\n" + "from student\n" + "where \n"
						+ "department = \'CS1\') as CS1_student\n" + "natural full outer join\n" + "(select *\n"
						+ "from takes t inner join section s \n" + "on t.section_id = s.section_id \n"
						+ "where semester = 1 \n" + "and \n" + "year = 2019) as sem1_student;",
				new Column[] { new Column("student", "department"), new Column("takes", "section_id"),
						new Column("section", "section_id"), new Column("section", "semester"),
						new Column("section", "year") });

		queries[2] = new Query("schema2",
				"select distinct pnumber\n" + "from project\n" + "where pnumber in\n" + "(select pnumber\n"
						+ " from project, department d, employee e\n" + " where e.dno=d.dnumber \n" + " and\n"
						+ " d.mgr_snn=ssn \n" + " and \n" + " e.lname=\'employee1\' )\n" + "or\n" + "pnumber in\n"
						+ "(select pno\n" + " from works_on, employee\n" + " where essn=ssn and lname=\'employee1\' );",
				new Column[] { new Column("project", "pnumber"), new Column("employee", "dno"),
						new Column("department", "dnumber"), new Column("department", "mgr_snn"),
						new Column("employee", "lname"), new Column("works_on", "Essn"),
						new Column("employee", "ssn") });

		queries[3] = new Query("schema2",
				"select lname, fname\n" + "from employee\n" + "where salary > all ( \n" + "select salary\n"
						+ "from employee\n" + "where dno=5 ); ",
				new Column[] { new Column("employee", "salary"), new Column("employee", "dno") });

		queries[4] = new Query("schema2",
				"select e.fname, e.lname\n" + "from employee as e\n" + "where e.ssn in ( \n" + "select essn\n"
						+ "from dependent as d\n" + "where e.fname != d.dependent_name\n" + "and \n"
						+ "e.sex!=d.sex );",
				new Column[] { new Column("employee", "ssn"), new Column("dependent", "dependent_name"),
						new Column("employee", "sex"), new Column("dependent", "sex"),
						new Column("employee", "fname") });

		queries[5] = new Query("schema2",
				"select fname, lname from employee\n" + "where exists ( select *\n"
						+ "from dependent where ssn=essn );",
				new Column[] { new Column("employee", "ssn"), new Column("dependent", "essn") });

		queries[6] = new Query("schema2",
				"select dnumber, count(*)\n" + "from department, employee\n" + "where dnumber=dno \n" + "and \n"
						+ "salary > 40 \n" + "and\n" + "dno =  (\n" + "  select dno\n" + "  from employee\n"
						+ "  group by dno\n" + "  having count (*) > 2)\n" + "group by dnumber;",
				new Column[] { new Column("department", "dnumber"), new Column("employee", "dno"),
						new Column("employee", "salary") });

		queries[7] = new Query("schema3",
				"select s.sname\n" + "from sailors s\n" + "where\n" + "s.sid in(  select r.sid\n" + "from reserves r\n"
						+ "where r.bid = 103 );",
				new Column[] { new Column("sailors", "sid"), new Column("reserves", "bid") });

		queries[8] = new Query("schema3",
				"select s.sname\n" + "from sailors s\n" + "where s.sid in ( select r.sid\n" + "from reserves r\n"
						+ "where r. bid in (select b.bid\n" + "from boat b\n" + "where b.color = \'red\'));",
				new Column[] { new Column("sailors", "sid"), new Column("reserves", "bid"),
						new Column("boat", "color") });

		queries[9] = new Query("schema3",
				"select s.sname\n" + "from sailors s, reserves r, boat b\n" + "where\n" + "s.sid = r.sid \n" + "and \n"
						+ "r.bid = b.bid \n" + "and \n" + "b.color = 'red'\n" + "and \n" + "s.sid in ( select s2.sid\n"
						+ "from sailors s2, boat b2, reserves r2\n" + "where s2.sid = r2.sid \n" + "and \n"
						+ "r2.bid = b2.bid\n" + "and \n" + "b2.color = 'green');",
				new Column[] { new Column("sailors", "sid"), new Column("reserves", "bid"), new Column("boat", "color"),
						new Column("reserves", "sid"), new Column("boat", "bid") });

		queries[10] = new Query("schema4",
				"select *\n" + "from actor \n" + "where act_id in(\n" + "select act_id \n" + "from movie_cast \n"
						+ "where mov_id in(\n" + "select mov_id \n" + "from movie \n"
						+ "where mov_title =\'movie1\'));",
				new Column[] { new Column("actor", "act_id"), new Column("movie_cast", "mov_id"),
						new Column("movie", "mov_title") });

		queries[11] = new Query("schema4",
				"select dir_fname, dir_lname\n" + "from  director\n" + "where dir_id in(\n" + "select dir_id \n"
						+ "from movie_direction\n" + "where mov_id in(\n" + "select mov_id \n" + "from movie_cast \n"
						+ "where role =any( select role \n" + "from movie_cast \n"
						+ "   								where mov_id in(\n" + "select  mov_id \n" + "from movie \n"
						+ "where mov_title=\'movie2\'))));",
				new Column[] { new Column("director", "dir_id"), new Column("movie_direction", "mov_id"),
						new Column("movie_cast", "role"), new Column("movie_cast", "mov_id"),
						new Column("movie", "mov_title") });

		queries[12] = new Query("schema4",
				"select mov_title \n" + "from   movie \n" + "where  mov_id=(\n" + "select mov_id \n"
						+ "from movie_direction \n" + "where dir_id=\n" + "(select dir_id \n" + "from director \n"
						+ "where dir_fname=\'actor1\'\n" + "and \n" + "dir_lname=\'actor1\'));",
				new Column[] { new Column("movie", "mov_id"), new Column("movie_direction", "dir_id"),
						new Column("director", "dir_fname"), new Column("director", "dir_lname") });

		queries[13] = new Query("schema5",
				"select country_name as team \n" + "from soccer_country \n" + "where country_id in(\n"
						+ "select team_id \n" + "from match_details \n" + "where play_stage=\'a\'\n" + "and \n"
						+ "win_lose=\'w\');",
				new Column[] { new Column("soccer_country", "country_id"), new Column("match_details", "play_stage"),
						new Column("match_details", "win_lose") });

		queries[14] = new Query("schema5",
				"select match_no \n" + "from match_details \n" + "where team_id = (\n" + "select country_id \n"
						+ "from soccer_country \n" + "where country_name=\'germany1\')\n" + "or \n" + "team_id = (\n"
						+ "select country_id \n" + "from soccer_country \n" + "where country_name=\'germany2\')\n"
						+ "group by match_no \n" + "having count(distinct team_id)=1;",
				new Column[] { new Column("match_details", "team_id"), new Column("soccer_country", "country_name"),
						new Column("match_details", "match_no") });

		queries[15] = new Query("schema5", "select match_no, play_stage, play_date, results, goal_score\n"
				+ "from match_mast\n" + "where match_no in (\n" + "select match_no \n" + "from match_details \n"
				+ "where team_id = (\n" + "				select country_id \n" + "				from soccer_country \n"
				+ "				where country_name='germany1')\n" + "or team_id=(\n"
				+ "					  select country_id \n" + "					  from soccer_country \n"
				+ "					  where country_name='germany2')\n" + "group by match_no \n"
				+ "having count ( distinct team_id ) = 1 );",
				new Column[] { new Column("match_mast", "match_no"), new Column("match_details", "team_id"),
						new Column("soccer_country", "country_name") });

		queries[16] = new Query("schema5",
				"select country_name\n" + "from soccer_country\n" + "where country_id in(\n" + "select team_id \n"
						+ "from goal_details \n" + "where match_no=(\n" + "select match_no \n" + "from match_mast \n"
						+ "where audonce=(\n" + "select max(audonce)\n" + "from match_mast)\n"
						+ "order by audonce desc));",
				new Column[] { new Column("soccer_country", "country_id"), new Column("goal_details", "match_no"),
						new Column("match_mast", "audonce") });

		queries[17] = new Query("schema5",
				"select player_name \n" + "from player_mast \n" + "where player_id=(\n" + "select player_id \n"
						+ "from goal_details \n" + "where match_no=(\n" + "select match_no \n" + "from match_details \n"
						+ "where team_id=(\n" + "select country_id \n" + "from soccer_country \n"
						+ "where country_name=\'germany1\')\n" + "or \n" + "team_id=(\n" + "select country_id \n"
						+ "from soccer_country \n" + "where country_name=\'germany1\')\n" + "group by match_no \n"
						+ "having count(distinct team_id)=1)	\n" + "and \n" + "team_id=(\n" + "select team_id\n"
						+ "from soccer_country a, \n" + "soccer_team b\n" + "where a.country_id=b.team_id \n" + "and \n"
						+ "country_name=\'germany1\')\n" + "and \n" + "goal_time=(\n" + "select max(goal_time)\n"
						+ "from goal_details \n" + "where match_no=(\n" + "select match_no \n" + "from match_details \n"
						+ "where team_id=(\n" + "select country_id \n" + "from soccer_country \n"
						+ "where country_name =\'germany1\')\n" + "or \n" + "team_id=(\n" + "select country_id \n"
						+ "from soccer_country \n" + "where country_name=   \n" + "  \'germany1\')\n"
						+ "group by match_no \n" + "having count(\n" + "    distinct team_id)=1)\n" + "and \n"
						+ "team_id=(\n" + "select team_id\n" + "from soccer_country a,    \n" + "     soccer_team b\n"
						+ "where a.country_id=b.team_id and country_name=\'germany1\')));",
				new Column[] { new Column("player_mast", "player_id"), new Column("goal_details", "match_no"),
						new Column("match_details", "team_id"), new Column("soccer_country", "country_name"),
						new Column("soccer_team", "team_id"), new Column("soccer_country", "country_id"),
						new Column("goal_details", "goal_time")});

		return queries;
	}

	public static void main(String[] args) {

		Query[] queries = getAllQueries();
		long startTime = System.currentTimeMillis();
		for (int i = 1; i < queries.length; i++) {
			try {
				out = new PrintWriter(new FileWriter(new File(outputPath + "Query_" + i + ".txt")));
			} catch (IOException e) {
				System.err.println("Error Writing to file");
				e.printStackTrace(System.err);
			}
			dbHandler = new DBHandler(out);

			printLightDivider(30);
			long queryStartTime = System.currentTimeMillis();
			System.out.println("Analyzing Query #" + i);
			out.println("Query #" + i + ":");
			tryAll(queries[i]);
			System.out.println("Finished Analyzing Query #" + i);
			System.out.printf("Time taken for the query is %f s\n",
					(System.currentTimeMillis() - queryStartTime) / 1000.0);
			printLightDivider(30);
			out.flush();
		}
		System.out.printf("Total Time taken is %f s\n", (System.currentTimeMillis() - startTime) / 1000.0);
		printDivider(30);
		out.close();
	}
}
