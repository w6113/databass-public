# Query Compilation Code

Query compilation takes an optimized plan as input, and emits a function in 
the compiled language that executes the logic of the query.  The function 
takes a database handle `db` and a reference to a lineage manager,
and returns the query result.  `db` is basically a dictionary where the
keys are the table names, and the values are iterators over the data.

The following is the example output for the query

      SELECT a FROM data

All variables prefixed with `l_` are used for lineage capture.
The [CompiledQuery](./compiledquery.py) class is a helper that parses, 
optimizes, and compiles a query, and also sets up the lineage manager
on your behalf.  

**Note:** you can feel free to avoid the discussion and code related to lineage
as it is not needed until the optional A5.

```
'''

Collect()
  Project(data.a:num AS a)
    Scan(data AS data)
'''


def compiled_q(db=None, lineage=None):
  from databass import UDFRegistry
  from datetime import date, datetime
  if not db:
    db = Database()

  collect_buf_0 = []

  # --- Pipeline 0 ---
  proj_row_0 = ListTuple(Schema([Attr('a', 'num', None),Attr('a0', 'num', None)]))
  # scan data AS data
  for scan_row_0 in db['data']:
    proj_row_0.row[:] = [scan_row_0[0],(scan_row_0[0]) + (scan_row_0[1])]
    collect_tmp_0 = ListTuple(Schema([Attr('a', 'num', None),Attr('a0', 'num', None)]))
    collect_tmp_0.row = list(proj_row_0.row)
    collect_buf_0.append(collect_tmp_0)

  return collect_buf_0

```




## The compilation process

Query compilation proceeds in the following stages

* Start with optimized query plan
* Identify pipeline breakers in [./pipeline.py](./pipeline.py)
* Create a translation plan where each operator is wrapped with a translator
  responsible for generating the operator's query compiled code.
  There is one translator for each physical operator.
  * Pipeline breakers are split into two translators: one for collecting tuples
    and the other for processing and emiting operator outputs.   ([./pipeline.py#L137](./pipeline.py#L137))
* Walk the translation plan and create lineage indexes (lindexes) at translators
  that need to capture or materialize lineage.   (Skip this for A1-4)
  * The translators that capture lineage depends on the lineage policy.
    The policy specifies the pairs of operators that databass needs to
    materialize a lineage index for.  See [./lpolicy.py](./lpolicy.py).
  * An operator does not capture lineage if it is not along the path 
    between any pair of operators that the policy asks to materialize.  
  * An operator along a path needs to capture lineage, but can delete 
    the lineage once its information has been propagated to the next operator.
    See [translator.clean_prev_lineage_indexes()](./translator.py#L51) 
* Calling produce on the root of the translation plan generates the compiled
  code.  The main logic is embedded in the translator classes.  
* Notes on lineage instrumentation:
  * Lineage instrumentation logic in the compiled code is controlled by
    an `if self.l_capture` condition.   If a translator does not have `l_capture`
    set, then we don't instrument the generated code.
  * Adding to the lineage indexes is tricky because different operators 
    are 1-1, 1-N, or N-1.  Further, depending on the logic, the specific
    data structure for a forward or backward lineage index can differ.
    In addition, when we propagate lineage between operators, the arity
    of the resulting lindexes also changes.  All of this complexity
    is handled by the [lindex.py](./lindex.py) classes.


## Code organization

* The files in this folder contain abstract classes for tranlators, pipeline plan generation, and lineage instrumentation.  
* The files in [./py](./py) are subclasses of the tranlators, pipelines, and lineage instrumentation that are specific to
  the compilation target language.


## A primer on Query Compilation 

Our approach to query compilation follows the producer-consumer approach outlined in [Efficiently Compiling Efficient Query Plans for Modern Hardware](https://w6113.github.io/files/papers/p539-neumann.pdf).

The same idea as expression compilation applies to query plans.  Rather than using the Iterator model to interpret the query plan, we would like to generate raw Python code to run.  For example, the following query

        SELECT a + b AS val
        FROM data
        WHERE a > b

would ideally be compiled into the following program, where  `db` is a Database object that contains a sequential scan access method for the table `data`.

        def q(db):
          for row in db['data']:
            if row['a'] > row['b']:
              val = row['a'] + row['b']
              yield dict(val=val)

The challenge is we can't just perform compilation in the same way we evaluate a query plan using the pull-based iterator model.  Take a look at the query plan for the above query:

            Project(a + b AS val)
                   |
              Filter(a > b)
                   |
              Scan(data)

Notice that project is at the _top_ of the query plan, whereas it is in the innermost block in the compiled program above.  In contrast, the Scan operator is at the _bottom_ of the query plan, even the for loop to scan the table is the first line of the compiled function.  If we asked Project to generate its code, and then called its child to generate the Filter code, we would have generated code in the opposite order:

        val = row['a'] + row['b']
        yield dict(val = val)
        if row['a'] > row['b']:
        ...

This is why the [Generating code for holistic query evaluation](https://w6113.github.io/files/papers/krikellas-icde2010.pdf) paper generates its code by first topologically sorting the query plan from the bottom operators (access methods) to the root operator.  

The produce-consumer model is one way to address this issue.  


#### Compiling Expressions

Let's say we have the following expression in a SQL query

        a + (1 * 9)

This is parsed into an expression tree of the form:

            +
           / \
          a   *
             / \
            1   9

In a typical database, the expression is evaluated by interpreting this tree.
Each node in the tree is an Operator object.
The root of the expression is actually a binary operator whose operator is `+`,
and the left and right children are `a` and the subtree for `*`.
The expression is evaluated by recursively evaluating the children, getting their value, looking up the function to add the two values, and then returning:

      def eval():
        left_val = left_child.eval()
        right_val = right_child.eval()
        if op == "=":
          return left_val == right_val
        if op == "+":
          return left_val + right_val

This incurs the overhead of function calls, context switches, if/else statements, etc.
In contrast, if we know that the tuple is an array called  `row` with schema `(c, a, t),
then we could compile the tree into the following Python statement that would run much faster:

        row[1] + (1 * 9)

Most databases (not databass)  implement constant folding as well, which would result in:

        row[1] + 9


The code for compiling expressions can be found in the Base Python Translator class [PyTranslator](../databass/compile/py/translator.py).


#### The Producer Consumer Model

The main idea is that we want Scan to generate its code first, and then Filter, and finally Project.  To do so, split compilation into two phases. The _produce_ phase that follows the ↓ arrows, and its purpose is to initialize the required state for each operator (setup hash tables, temporary variables, etc), and to allocate variables to hold te tuples read by the access methods (e.g., Scan).  The _consume_ phase follows the ↑ arrows to use the populated variables actual data processing.  

                Project(a + b AS val)
                     |    ↑  
           produce   |    |    consume
                     ↓    |  
                  Filter(a > b)
                     |    ↑ 
           produce   |    |    consume
                     ↓    |  
                   Scan(data) 
     

This is implemented by wrapper operators with Translators that are responsible for
translating the operator logic into compiled code.  Each Translator defines
`produce()` and `consume()` methods that are called during compilation.  

          class Translator(object):
            def __init__(self, op):
              self.op = op

            def produce(self):
              # compile setup code for self.op, 
              # call child translator's produce()

            def consume(self):
              # compile operator logic,
              # call parent translator's consume()

Pipeline breakers and Join operators define two Translators.  Aggregation defines
bottom and top translators; the former is responsible for building the hash table, and 
the latter loops through the hash table to generate output tuples.  The top translator
serves as the source for the next pipeline (see `./pipeline.py:make_pipelines()`).
Joins define left and right translators, for each of its children.



### Compiling Queries

The [CompiledQuery](./compiledquery.py) class provides convenience methods to
parse, optimize, compile and run a query string or query plan.   For instance:

      from databass import *
      q = PyCompiledQuery("SELECT a, b FROM data")
      print(q.print_code())

      db = Database.db()
      for row in q(db):
        print(row)


When compiling a query plan, the root node must be a [Sink operator](../ops/root.py).
They are used to print the results, yield each result from the compiled function, or
collect the results into a buffer that the function will return.



### A few notes about the code

#### Context

The Context object `ctx` is passed between operators as an argument in the produce/consume calls.   It provides a way to communicate between translators.  It also provides helper methods for codegen.  

* See [../context.py](../context.py) to see how to use the Context object.  
  * `ctx.new_var("prefix")`  allocates a new var `"prefix_10"`, if its the 10th variable allocated with the same prefix.  
  * `ctx.request_vars()` requests variable names from descendent translators
* See [./compiler.py](./compiler.py) for the Compiler object that is used to programmatically construct the compiled program.  It provides methods to add new lines, indentation, declarations, etc.
* See example uses in the provided implementations for Scan, Filter, Distinct in [./py/](./py)


#### Passing Variable Names Around

As control flows down along the produce calls, translators can register variables they want the descendant translators to help populate.  This is primarily used so a filter translator can ask for the variable that the scan translator will use to store the current tuple.  

This is achieved by registering a varable request to the context.  As an example,

* Filter registers request for `row`: `ctx.request_vars(dict(row=None))`.  The dictionary argument defines keys that should be populated by some descendant.  Filter calls its child's produce.
* The Scan  sets `ctx['row'] = <row variable name>`, and calls Filter's consume.
* Filter gets the variable by reading `ctx['row']`


## Misc Notes


Resources for query compilation

* [Making compiling query engines practical](https://ieeexplore.ieee.org/document/8667737)
* Tiark's [Legobase-micro](https://github.com/TiarkRompf/legobase-micro/blob/master/minidb/src/main/scala/miniDB.scala)
* [dblab's query compilation transformers](https://github.com/epfldata/dblab/tree/develop/components-compiler/src/main/scala/ch/epfl/data/dblab/transformers)
* [Legobase](https://github.com/peterboncz/LegoBase)
* [afterburner](https://github.com/afterburnerdb/afterburner/blob/master/src/core/afterburner.js) and [its paper](https://arxiv.org/pdf/1804.08822.pdf)


If you look at the appendix of the [Neumann paper](https://w6113.github.io/files/papers/p539-neumann.pdf), it goes into more details about their context information, which passes down to the access methods the columns of interest, so it doesn't read more columns than needed.  Ours is a stripped down version.    The appendix also goes into more details with code snippets for different operator implementations.