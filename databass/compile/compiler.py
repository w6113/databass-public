from contextlib import contextmanager
from collections import *

class Indent(object):
  pass

class Unindent(object):
  pass

class Compiler(object):
  """
  Defines helper functions to construct code blocks
  """
  def __init__(self):
    self.var_ids = defaultdict(lambda: 0)
    self.lines = []
    self.declarations = []
    self.return_stmts = []

  def new_var(self, prefix="var"):
    """
    Allocate a new variable name for compmiler to use

    @prefix optionally provide a custom variable name prefix
    """
    var = "%s_%d" % (prefix, self.var_ids[prefix])
    self.var_ids[prefix] += 1
    return var

  def compile_to_code(self):
    """
    Generate the raw code from the ir nodes referenced by self.root
    """
    return self.compile()

  def compile(self):
    return "\n".join(self.compile_to_lines())

  def compile_to_lines(self):
    ret = []
    for lhs, rhs in self.declarations:
      if rhs is not None:
        ret.append("%s = %s" % (lhs, rhs))
      else:
        ret.append(lhs)
    ret.append("")

    ind = 0
    for line in self.lines:
      if isinstance(line, str):
        ret.append(("  " * ind) + line)
      elif isinstance(line, Indent):
        ind += 1
      elif isinstance(line, Unindent):
        ind -= 1

    ret.extend(self.return_stmts)
    return ret


  def add_line(self, line):
    self.lines.append(line)

  def add_lines(self, lines):
    self.lines.extend(lines)
  
  def set(self, lhs, rhs):
    self.add_line("%s = %s" % (lhs, rhs))


  def declare(self, lhs, rhs):
    self.declarations.append((lhs, rhs))

  def returns(self, line):
    self.return_stmts.append(line)

  @contextmanager
  def indent(self, cond, **formatargs):
    """
    Helper that lets caller enter and exit indented code block using a "with" expression:

        with ctx.compiler.indent() as compiler:
          compiler.add_line(...)

    Context has a helper indent() function, so you can use that directly:

        with ctx.indent(...):
          ...

    """
    self.lines.append(cond.format(**formatargs))
    self.lines.append(Indent())
    try:
      yield self
    finally:
      self.lines.append(Unindent())


if __name__ == "__main__":
  c = Compiler()
  c.add_line("a = 1")
  c.add_line("b = 2")
  with c.indent("for t in dataset:"):
    with c.indent("for s in dataset:"):
      c.add_line("t['a'] += 1")
    c.add_line("t['a'] -= 1")
  c.add_line("tup = {}")

  print(c.compile())



