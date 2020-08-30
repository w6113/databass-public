
Python-specific classes for query compilation.

* [./translator.py](./translator.py) contains helper logic for compiling expressions
* [./compiledquery.py](./compiledquery.py) defines PyCompiledQuery, which manages the process of
  parsing, optimizing, and compiling a query string/plan, executing the compiled code, and 
  providing access to the generated lineage.  Take a look at it to understand how the query compilation APIs work.
