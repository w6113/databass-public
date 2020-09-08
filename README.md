
<img src="./docs/logo-small.png" width=200 />

databass is a query compilation engine built for Columbia's database courses.
It parses, optimizes, and compiles SQL strings into a single Python function that performs the query execution.
It also has some experimental code that instruments the compiled code with lineage capture mechanisms based on the Smoke paper.

## Code organization

The system is split into parser, operator definitons and interpretor, optimizer, and compilation.  The overall design of databass can be found in [design.md](./design.md).

* [./databass/](./databass) defines the base operator classes, and the parsing and optimization processes.  They are responsible for turning a query string into a physical query plan
* [./databass/ops](./databass/ops) defines the logical/physical SQL operators.  We currently support join, groupby, project, where, limit, and orderby.  
* [./databass/optimizer](./databass/optimizer) defines optimization logic that turns a logical query plan into a physical plan.  This includes the join optimization logic and cost estimation.
* [./databass/compile](./databass/compile) defines the logic for turning a physical plan into a pipeline plan ala [Neumann11](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf), compiling the pipelined plan into target language (python currently), and instrumenting the compiled code with lineage tracking.  See the readme in the folder for details.


## Getting Started

Installation using Python 3

    git clone git@github.com:w6113/databass-public.git

    # key whatever command you need to turn on virtualenv

    # install the needed python packages
    pip install -r requirements.txt


The repo includes incomplete test cases that you can run using `pytest`.
For quick hacks, and to see how databass compiles different query plans, I use the scaffold in test.py:

    python test.py



### Using the databass API

The following is an example program.  This specific program compiles and runs
a simple group-by query, but will not correcty run until you have completed
assignment 3.

    from databass import *
    db = Database()
    q = PyCompiledQuery("SELECT a, count(1) FROM data GROUP BY a")

    print(q.print_code())
    for row in q(db):
      print(row)


### Using the Prompt

Do the following to run the prompt:

    python -m databass.prompt

Below is an example session using the prompt.  The user input is the text after the `> ` character.


	Welcome to databass
	Type "help" for help, and "q" to exit
	> help

	List of commands

    <query>                           runs query string
    COMPILE [AND RUN] <query>         compile and optionally run query string
    PARSE [query or expression str]   parse and print AST for expression or query
    TRACE                             print stack trace of last error
    SHOW TABLES                       print list of database tables
    SHOW <tablename>                  print schema for <tablename>


You can see how simple expressions are parsed.  Note that operator precedence needs to be specified explicitly using parens:

	> parse 1=2 and a=b
    1.0 == (2.0 and (a == b))

    > parse (1=2) and (a=b)
    (1.0 == 2.0) and (a == b)

	> parse (1+2*a) / 10
	(1.0 + (2.0 * a)) / 10.0

Or the parsed query plan of a SQL query

	> parse SELECT 1+2*a AS a FROM data WHERE a > 1
    Project(1.0 + (2.0 * a) AS a)
      Filter(a > 1.0)
        From
          Scan(data AS data)

When the program starts, databass automatically crawls all subdirectories and loads any CSV files that it finds into memory.  In our example, [databass/data](./databass/data) contains two CSV files: [data.csv](./databass/data/data.csv) and [iowa-liquor-sample.csv](./databass/data/iowa-liquor-sample.csv).

	> show tables
    data_orig
    data
    iowa-liquor-sample
    data2

	> show data
	Schema for data
    a       num
    b       num
    c       num
    d       num
    e       str
    f       num
    g       str

You can execute a simple query, and it will print the query plan and then the result rows.  

	> SELECT 1
    Yield()
      Project(1.0 AS attr0)
    (1.0)
    Interpreted query took 0.000019 seconds

	> SELECT * FROM data LIMIT 2
	Yield()
	  LIMIT(2.0 OFFSET 0)
		Project(data.a AS a, data.b AS b, data.c AS c, data.d AS d, data.e AS e, data.f AS f, data.g AS g)
		  Scan(data AS data)
	(0, 0, 0, 0, a, 2, c)
	(1, 1, 1, 0, b, 4, d)
	Interpreted query took 0.000053 seconds


To compile a query, prefix the query with `COMPILE` (case insensitive).  It will print the query plan, the compiled code as a function called `compiled_q()`, and also write it out to a python file that you can run.

    > COMPILE SELECT 1

	Yield()
	  Project(1.0 AS attr0)

	def compiled_q():
	  proj_row_0 = ListTuple(Schema([Attr('attr0', 'num', None)]))
	  tmp_0 = 1.0
	  proj_row_0.row[:] = [tmp_0]
	  yield proj_row_0

	wrote compiled query to ./_code.py

You can run the compiled query by prefixing the query with `COMPILE AND RUN` (case insensitive): 

	> COMPILE AND RUN SELECT 1                                                                  
	Collect()
	  Project(1.0 AS a0)
		DummyScan()

	000
	001 def compiled_q(db=None, lineage=None):
	002   from databass import UDFRegistry
	003   from datetime import date, datetime
	004   if not db:
	005     db = Database()
	006
	007   collect_buf_0 = []
	008
	009   # --- Pipeline 1 ---
	010   proj_row_0 = ListTuple(Schema([Attr('a0', 'num', None)]))
	011   proj_row_0.row[:] = [1.0]
	012   collect_tmp_0 = ListTuple(Schema([Attr('a0', 'num', None)]))
	013   collect_tmp_0.row = list(proj_row_0.row)
	014   collect_buf_0.append(collect_tmp_0)
	015
	016   return collect_buf_0


	wrote compiled query to ./_code.py.  Type `python _code.py` to run it.
	Running compiled query
	(1.0)
	Compiled query took 0.000151 seconds

	Lineage:


### Run Tests

To run tests, use the `pytest` python test framework by specifying which tests in the `test/` directory to run:

    pytest test/*.py


