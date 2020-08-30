from ...context import *
from ...udfs import *
from ..compiledquery import *
from ..compiler import Compiler
from .pipeline import *
from .lineage import Lineage

class PyCompiledQuery(CompiledQuery):

  def create_pipelined_plan(self, plan):
    return PyPipelines(plan)

  def compile_to_func(self, fname="f"):
    """
    Wrap the compiled query code with a function definition.
    """
    comp = Compiler()
    with comp.indent("def %s(db=None, lineage=None):" % fname):
      comp.add_lines([
        "from databass import UDFRegistry",
        "from datetime import date, datetime",
        "if not db:",
        "  db = Database()"
        ])

      comp.add_lines(self.ctx.compiler.compile_to_lines())

    return comp.compile()

  def print_code(self, funcname="compiled_q"):
    code = self.compile_to_func(funcname)
    #code = """'''\n%s\n'''\n\n%s\n""" % (
    #    self.plan.pretty_print(), code)
    lines = code.split("\n")
    lines = ["%03d %s" % (i, l) for i, l in enumerate(lines)]
    print()
    print("\n".join(lines))
    print()


  def __call__(self, db=None):
    if not db:
      db = Database()
    lineage = Lineage(self.plan)
    self.lineages.append(lineage)
    return self.f(db, lineage)


