import time
import traceback
import readline
import click
from . import *
from .compile import *

WELCOMETEXT = """Welcome to DataBass.  
Type "help" for help, and "q" to exit"""


HELPTEXT = """
List of commands

<query>                           runs query string
COMPILE [AND RUN] <query>         compile and optionally run query string
PARSE [query or expression str]   parse and print AST for expression or query
TRACE                             print stack trace of last error
SHOW TABLES                       print list of database tables
SHOW <tablename>                  print schema for <tablename>
"""

def write_code(compiled_q, fname="./_code.py"):
  header = """
from databass import *
import time
db = Database.db()
lineage = Lineage(None)"""
  footer = """
if __name__ == "__main__":
  start = time.perf_counter()
  for row in compiled_q(db): 
    print(row)
  end = time.perf_counter()
  print("Compiled query took %f seconds" % (end - start))
"""

  with open(fname, "w") as out:
    out.write(header)
    out.write("\n")
    out.write(compiled_q.code)
    out.write("\n")
    out.write(footer)

def parse_and_optimize(qstr):
  plan = parse(qstr).to_plan()
  plan = Yield(plan)
  opt = Optimizer()
  optimized_plan = opt(plan)
  return optimized_plan

if __name__ == "__main__":

  @click.command()
  def main():
    print(WELCOMETEXT)
    service_inputs()

  def service_inputs():
    cmd = input("> ").strip()

    _db = Database.db()

    if cmd == "q":
      return

    elif cmd == "":
      pass

    elif cmd.startswith("help"):
      print(HELPTEXT)

    elif cmd.upper().startswith("TRACE"):
      traceback.print_exc()

    elif cmd.upper().startswith("PARSE"):
      q = cmd[len("PARSE"):]
      ast = None
      try:
        ast = cond_to_func(q)
      except Exception as err_expr:
        try:
          ast = parse(q)
        except Exception as err:
          print("ERROR:", err)

      if ast:
        print(ast.pretty_print())


    elif cmd.upper().startswith("SHOW TABLES"):
      for tablename in _db.tablenames:
        print(tablename)
      
    elif cmd.upper().startswith("SHOW "):
      tname = cmd[len("SHOW "):].strip()
      if tname in _db:
        print("Schema for %s" % tname)
        for attr in _db[tname].schema:
          print(attr.aname, "\t", attr.typ)
      else:
          print("%s not in database" % tname)

    elif cmd.upper().startswith("COMPILE "):
      cmd = cmd[len("COMPILE "):].strip()
      b_run = False
      if cmd.upper().startswith("AND RUN "):
        b_run = True
        cmd = cmd[len("AND RUN "):].strip()

      try:
        compiled_q = PyCompiledQuery(cmd)

        print(compiled_q.pipelined_plan.pretty_print())
        code = write_code(compiled_q, "./_code.py")

        compiled_q.print_code()
        print()
        print("wrote compiled query to ./_code.py.  Type `python _code.py` to run it.")

        if b_run:
          print("Running compiled query")
          start = time.perf_counter()
          for row in compiled_q(_db):
            print(row)
          end = time.perf_counter()
          print("Compiled query took %f seconds" % (end - start))

          print()
          print("Lineage:")
          print(compiled_q.lineages[-1])

      except Exception as err:
        print("ERROR:", err)

    else:
      try:
        plan = parse_and_optimize(cmd)
        print(plan.pretty_print())
        start = time.perf_counter()
        for row in plan:
          print(row)
        end = time.perf_counter()
        print("Interpreted query took %f seconds" % (end - start))
      except Exception as err:
        print(("ERROR:", err))

    del _db
    service_inputs()


  main()
