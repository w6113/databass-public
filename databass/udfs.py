"""
Define the structure of aggregate and scalar UDFs, and the UDF registry.
"""
import numpy as np

class UDF(object):
  """
  Wrapper for registering metadata about UDFs.

  TODO: add setup function/dependencies so that compiler can generate the
        appropriate import statements and definitions.
  """
  def __init__(self, name, nargs):
    self.name = name
    self.nargs = nargs
    self.f = None

  @property
  def is_agg(self):
    return False

  @property
  def is_incremental(self):
    return False



class AggUDF(UDF):
  def __init__(self, name, nargs, f=None):
    UDF.__init__(self, name, nargs)
    self.f = f

  @property
  def is_agg(self):
    return True

  def __call__(self, *args):
    if len(args) != self.nargs:
      raise Exception("Number of arguments did not match expected number.  %s != %s" % (len(args), self.nargs))
    if not all(isinstance(arg, list) or isinstance(arg, tuple) for arg in args):
      print(args)
      raise Exception("AggUDF expects each argument to be a column.")
    return self.f(*args)

class ScalarUDF(UDF):
  def __init__(self, name, nargs, f=None):
    UDF.__init__(self, name, nargs)
    self.f = f

  def __call__(self, *args):
    if len(args) != self.nargs:
      raise Exception("Number of arguments did not match expected number.  %s != %s" % (len(args), self.nargs))
    return self.f(*args)

class IncAggUDF(AggUDF):
  """
  Aggregation UDF that is incremental. Bascially acts as a reducer.
  """
  def __init__(self, name, nargs, f=None, init=None, update=None, finalize=None):
    """
    @init Returns an initialized intermediate state
    @update Updates @state in place based on new tuple
    @finalize Return aggregation result
    """
    super(IncAggUDF, self).__init__(name, nargs, f)

    self.init = init
    self.update = update
    self.finalize = finalize

  @property
  def is_incremental(self):
    return True


class UDFRegistry(object):
  """
  Global singleton object for managing registered UDFs
  """
  _registry = None

  def __init__(self):
    self.scalar_udfs = {}
    self.agg_udfs = {}

  @staticmethod
  def registry():
    if not UDFRegistry._registry:
      UDFRegistry._registry = UDFRegistry()
    return UDFRegistry._registry

  def add(self, udf):
    if isinstance(udf, AggUDF):
      if udf.name in self.scalar_udfs:
        raise Exception("A Scalar UDF with same name already exists %s" % udf.name)
      self.agg_udfs[udf.name] = udf

    elif isinstance(udf, ScalarUDF):
      if udf.name in self.agg_udfs:
        raise Exception("A Agg UDF with same name already exists %s" % udf.name)
      self.scalar_udfs[udf.name] = udf

  def __getitem__(self, name):
    if name in self.scalar_udfs:
      return self.scalar_udfs[name]
    if name in self.agg_udfs:
      return self.agg_udfs[name]
    raise Exception("Could not find UDF named %s" % name)


#
# Prepopulate registry with simple functions
#
registry = UDFRegistry.registry()
registry.add(ScalarUDF("lower", 1, lambda s: str(s).lower()))
registry.add(ScalarUDF("upper", 1, lambda s: str(s).upper()))

#
# Prepopulate with incremental aggregation functions
#

registry.add(IncAggUDF("count", 1, len, lambda: 0, lambda s, v: s+1, lambda s:s))
registry.add(IncAggUDF("avg", 1, np.mean, 
  lambda: (0, 0), 
  lambda s, v: (s[0]+v, s[1]+1),
  lambda s: (s[0] / s[1]) if s[1] else float('nan')))
registry.add(IncAggUDF("sum", 1, np.sum, lambda: 0, lambda s, v: s+v, lambda s: s))

# Welford's algorithm for online std
std_init = lambda: [0, 0., 0]
def std_update(s, v):
  s[0] += 1
  d = v - s[1]
  s[1] += d / s[0]
  s[2] += d * (v - s[1])
  return s
def std_finalize(s):
  if s[0] < 2: return float('nan')
  return s[2] / (s[0] - 1)

registry.add(IncAggUDF("std", 1, np.std, std_init, std_update, std_finalize))
registry.add(IncAggUDF("stdev", 1, np.std, std_init, std_update, std_finalize))


if __name__ == "__main__":

  udf = registry["sum"]
  print(udf.f([1,2,3]))
