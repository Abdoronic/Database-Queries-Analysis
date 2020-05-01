# Database-Queries-Analysis

Analyzing different SQL queries by measuring the cost and the expected result set size of each query before and after creating indices.

## Overview

There are seventeen queries distributed on five different schemas. For each schema various number of entries are inserted to ensure that no query outputs an empty result set. After that the code tries every possible index configuration using a brute-force approach for each query and finds the optimal configuration(s) that produces the logical plan with the lowest estimated cost.

<br/>

## Running the code localy

1. **Import the code as an Eclipse project.** It's recommended to use Eclipse as the it's exported as an Eclipse project.
1. **Install the dependencies.** Add the three jar files in the `./Dependencies` directory to the Eclipse projcet by adding them to the project build path.
1. **Run the code** from the `./src/core/Main.java` class's main method.


## Output

For each query there will be an analysis of the base configuration and the optimal configuration that was produced by the brute-force code. For the full list of each configuration details, analysis, plans and the query SQL code, check the plain text file (Query_X.txt; where X is the number of the query) for each query.

<br/>
<br/>

***Note:*** *Due to the fact that Postgres does not support direct BITMAP index creation (at the time of testing) and it can only be created by the Postgres optimizer. So in the queries files every configuration has the possibility to have a BITMAP index that was created on the fly for that specific query. In each query in the queries files we stated explicitly the indices that we directly created (BITMAP is not included).*
