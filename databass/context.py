from contextlib import contextmanager
from collections import *
from .compile.compiler import Compiler



class Context(object):
  """
  The context object used throughout query compilation, and is the main 
  communication channel between operators as it goes through the produce->consume
  compilation phases.

  It serves three main roles:
  1. reference to Compiler object which is used to actually write compiled code
  2. manage i/o variables for compiling expressions.  The caller of an Expr instance's
     compile() method should specify the variable name of the input tuple that the 
     expression is evaluated over, and the output variable name that the expression result
     should be written to.
  3. pass variable requests and definitions between relational algebra operators in the
     query plan.  For instance, Source operators define range variables that iterate
     over input relations, and the names of those variables need to be passed to 
     later operators read these range variables' attribute values.

  """
  def __init__(self, compiler=None):
    # stack of operators populated during produce phase
    self.compiler = compiler or Compiler()

    # Input and output variable names for compiling exprs.  
    # Exprs are special because the reference to the input
    # row is passed from parent to child
    self.io_vars = []


    # Requested vars during qplan produce/consume phase.
    # Source operators define variables that range over the input relation
    # Non-source operators request variables that they will process.
    # 
    # This is needed because the produce phase happens top-down, 
    # and the Project operator's produce is called before the 
    # range variables are allocated by the descendent Scan operator.  
    #
    # Thus, during produce, operators add requests to ops_var, 
    # and during consume, operators fulfill variable requests.
    #
    # Initialized with a dummy dict
    self.op_vars = [dict()]


  def add_line(self, line, **formatargs):
    if formatargs:
      line = line.format(**formatargs)
    self.compiler.add_line(line)

  def add_lines(self, lines, **formatargs):
    if formatargs:
      lines = [line.format(**formatargs) for line in lines]
    self.compiler.add_lines(lines)

  def set(self, lhs, rhs, **formatargs):
    if formatargs:
      lhs = lhs.format(**formatargs)
      rhs = rhs.format(**formatargs)
    self.compiler.set(lhs, rhs)

  def declare(self, lhs, rhs=None, **formatargs):
    """
    @rhs is optional.  If non-null, generates assignment statement
    """
    if formatargs:
      lhs = lhs.format(**formatargs)
      if rhs:
        rhs = rhs.format(**formatargs)
    self.compiler.declare(lhs, rhs)

  def returns(self, line, **formatargs):
    if formatargs:
      line = line.format(**formatargs)
    self.compiler.returns(line)

  def new_var(self, *args, **kwargs):
    """
    Wrapper to create a new variable name
    """
    return self.compiler.new_var(*args, **kwargs)

  def indent(self, cond, **formatargs):
    if formatargs:
      cond = cond.format(**formatargs)
    return self.compiler.indent(cond)

  def add_io_vars(self, in_var, out_var):
    """
    Add an io variable request for expression compilation.
    @in_var name of variable in compiled program that contains expression's input row
    @out_var name of variable in compiled program that expression result should write to
    """
    self.io_vars.append((in_var, out_var))

  def pop_io_vars(self):
    """
    After an expression has used its input and output variables for compilation, the 
    pair should be popped from the stack.
    """
    return self.io_vars.pop()

  def request_vars(self, d):
    """
    @d a dictionary with keys that the operator requests, whose values are None and
       will be filled in by the child operators.  The main requested key is "row"
    """
    self.op_vars.append(d)

  def pop_vars(self):
    return self.op_vars.pop()

  def __getitem__(self, name):
    """
    Get the value of the most recently requested variable.
    @name name of variable to get
    """
    return self.op_vars[-1].get(name, None)

  def __setitem__(self, name, val):
    """
    Set the value of the most recently requested variable
    @name name of variable to set
    @val  value of variable
    """
    self.op_vars[-1][name] = val


