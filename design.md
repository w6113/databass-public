## System Design

This document describes the design of the databass query engine. 

#### Background

Database is a reasonably featured, but simple, in-memory read-only row-wise database engine.  It can parse SQL queries, translate them into a query plan, disambiguate column references and check for ambiguities, perform simple optimizations such as join ordering, and run the query using pull-based iterator model.  It also includes an implementation of the [produce-consumer query compilation model](https://pdfs.semanticscholar.org/66a6/e8434ef51986cdf7669af526f9914c35d3a9.pdf), so that a query can be compiled into a python function that runs over the database and outputs the same query results.

The purpose is to provide students an overview of how the main parts of a data query engine work together and introduce database concepts within the context of an end-to-end engine.  Thus, the engine does not support many things, such as insert/update/delete queries, transactions, recovery, memory-management, correct null value support, etc.

The engine is composed of the modules defined in Python files in the [databass/](databass/) folder:

Basic files:

* [db.py](databass/db.py): this module manages the tables in the database.  It also keeps statistics about the tables that the optimizer can later use.
* [tables.py](databass/table.py): implementation of an in-memory row-oriented table, where each row is a ListTuple.
* [stats.py](databass/stats.py): computes statistics used for cardinality estimation in the optimizer.
* [schema.py](databass/schema.py): all tables, tuples, and operators expose schemas.  
* [tuples.py](databass/tuples.py):  implementation of tuples as Python arrays of values.  You will see that query compilation is intimately tied to this specific implementation of a tuple, and would need to change if data were represented as e.g., raw byte arrays or columnar.
* [baseops.py](databass/baseops.py): all logical and physical operators are subclasses of Op defined in this file.  The file includes helper classes for unary, binary, and nary operators.  The classes also provide traversal methods, and helpers for schema initialization, compilation, and pretty printing the operator tree.


Core engine files:

* [ops/](databass/ops): contains the core SQL operators.  
* [exprs.py](databass/exprs.py): contains implementations of expression operations.  Although they are also operators, they do not expose a schema, and their compilation procedure is different than the producer-consumer model used for relational operators, because an expression is always evaluated on a single (or array) or records.
* [optimizer/](databass/optimizer/): this module takes a logical query plan as input, figures out each operator's output schema, replaces attribute references in each operator with an index into its input tuple, and performs join ordering optimization.  
* [compile/](databass/compile): contains translation wrappers that implement the produce/consume query compilation logic
* [parse_expr.py](databass/parse_expr.py): this module is a simple parsing examples that only parses expressions and not queries.  You can play with it to get acquainted with how parsing works.
* [parse_sql.py](databass/parse_sql.py): this module implements the subset of the SQL language that databass supports.  The parsing grammar rules also include those in `parse_expr`.
* [parseops.py](databass/parseops.py): the output of the sql parser is a tree of SELECT queries.   This performs query validation, some minor optimizations, and transforms the parse tree into a query plan.

Misc files:

* [udfs.py](databass/udfs.py): a registry for user defined functions. Even native functions such as `count` are implemented as UDFs.   You can see how a UDF is referenced and executed in [exprs.py](databass/exprs.py)
* [prompt.py](databass/prompt.py): this is the databass client that you can use to write and execute SQL queries in the command line.

## Core Components

This section describes how core database concepts such as Tables, Operators, the iterator execution model are designed.  The next section describes how the pieces connect together.

#### Tuples, Tables, Catalog

Tuples are represented as ListTuple types in databass.  It is represented by a schema and a list of values.  The tuple provides accessors for retrieving attribute values via indexing into the list of values.  The schema helps translate attribute names to the lookup index. 

Table are provides an iterator access method to retrieve tuples.  An InMemoryTable is represented as a schema along with a list of ListTuples.  

The Database manages the catalog of tables that can be queried.  It is a singleton.  It is basically a hash table that maps the table name to the Table object.  To make life easier, it automatically crawls the subdirectories of the directory that you run Python from, and load all CSV files that it finds into memory.

Note that other implementations of tables and tuples are also possible. For instance you might mmap a binary data file, and implement ByteTable and ByteTuple classes to directly access attribute values from the binary file (aka byte buffer)

#### Parser

The parser uses a simple PEG grammar to parse SQL queries and expressions.  The AST generated by the grammar is transformed into a preliminary parse tree using Visitors defined in the parser files.   The nodes of the parse tree are defined in [parseops.py](./databass/parseops.py).  Essentially, each node is a SELECT query fragment, and subqueries correspond to children in this parse tree.  This format is used for validation and disambiguation, and is responsible for turning itself into a query plan.

The main issue to be aware of is that we have not implemented any sane notion of expression precedence, so you need to explicitly use parentheses to enforce your desired precedence.   The following are some examples of how expressions are parsed:

        -1 == 1            -->     -(1 == 1)
        (-1) == 1          -->     (-1) == 1
        a = b and c = d    -->     a == (b and (c == d))

#### Operators

`ops.py` defines three types of operators: Parse Operators, Query Operators and Expression Operators.  They all subclass `Op`.

`Op` is basically a tree node and provides a number of convenience functions for manipulating and traversing operator trees.  The main ones are:

* `collect(klasses)` traverses the tree and collects operators that are instances of the class names or objects in the argument
* `init_schema()`  initializes the operator's schema based on the schema of its child operators.   The schema should be initialized bottom up from leaves to the root in a query plan. This is only defined for relational operators (not parse operators, nor expressions).  See [optimizer.py](./databass/optimizer.py) for where it is called.
* `__str__()` turns the current operator into a string (ignoring child operators).
* `to_str()` performs a bunch of tree traversal magic to turn the query plan into a printable string.

The subclasses `UnaryOp`, `BinaryOp`, `NaryOp` are subclassed by the Query operators.  Under the covers, they manipulate the parent and child pointers to maintain the query plan.

##### Query Operators

Query Operators represent the logical and physical operators that we recognize, such as Filter (selection), Project, Join, LIMIT, etc.  You will notice that syntactic operators such as `From` is not actually executable.  The parser uses it to construct the parsed query plan, but the `From` operator needs to be replaced with a Join plan before the query can be run.  Similarly, there are also multiple implementations of the same logical operator.  For example, `ThetaJoin` and `HashJoin` are two implementations of Join.  

There are two ways to execute operators that you will eventually implement.  The first is to fill in the `__iter__()` methods to implement a pull-based iterator execution method.  The second is to fill in the `produce()` and `consume()` methods to generate compiled code.

#### Expression Operators

`exprs.py` defines Expression operators that are evaluated over a single input tuple.  The implementation is mostly straight forward with a couple parts to be aware ofe.


First, the aggregation function operator (AggFunc) takes as input a tuple whose schema contains a special `__group__` attribute that contains a list of actual tuples.  The point of the aggregation function is to extract the relevant attribute values from the tuples in `__group__` and pass those along to the function. 

Second, the `Attr` class represents attributes used as part of expressions in query operators (e.g., a = 1), as well as attributes in schemas (e.g., T(a, b, c)).   Attributes may be specified in a user query without declaring the table it should come from.  The optimizer's reference disambiguation step will identify the table and set the `Attr.tablename` attribute.    It is also responsible, after schema initialization and reference disambiguation, to know what index to use to lookup its attribute value in a ListTuple.


### Iterator Execution Model

Each query operator implements the `__iter__()` method, which returns an iterator over the subplan rooted at the operator.   Internally, the operator then iterates over its child operators to compute its results.  Take a look at how the `Scan` operator is implemented for a simple example.

In Python, using the `yield` keyword turns a function into an iterator.  This [stackoverflow answer is a good description](https://stackoverflow.com/questions/231767/what-does-the-yield-keyword-do).


## Putting It Together

databass executes queries using the following workflow:

        query --> [ parser ] --> parsed plan --> 
              --> [ validation ] --> query plan
              --> [ optimizer ] --> query plan with disambiguted attrs and
                                    initialized schemas
              --> [ optimizer ] --> physical query plan 
              --> [ interpretor ] --> result tuples

The interpretor step can be replaced with a compilation step instead:

                  ...           --> physical query plan
              --> [ compilation ] --> python code string
              --> [ python eval() ] --> python function --> result tuples



