"""
You can use this as a script to run queries and test your code
-We have included some examples
"""
import time
import timeit
from databass import *
from databass.tables import InMemoryTable
from databass.ops import *
from databass.compile import *
from databass.exprutil import *

from pygg import *


def run_benchmark(workload_generator, db):
  opt = Optimizer()
  workload = workload_generator(range(1, 21, 2))

  data = []
  for x, q in workload:
    print(q)
    plan = opt(parse(q).to_plan())
    y_iterator = timeit.timeit(lambda: list(plan), number=20)
    data.append(dict(x=x, y=y_iterator, label="iterator"))

    cq = PyCompiledQuery(Yield(plan), None)
    y_compile = timeit.timeit(lambda: list(cq(db)), number=20)
    data.append(dict(x=x, y=y_compile, label="compile"))
  return data

def plot(data, xlabel, ylabel, fname):
  """
  @data should be a list of dict(x=, y=, label=) 
  """
  # this code uses http://github.com/sirrice/pygg which requires installing R and ggplot
  # replace with your favorite library
  p = ggplot(data, aes(x='x', y='y', color='label'))
  p += geom_line()
  p += geom_point(size=1.2)
  p += axis_labels(xlabel, ylabel)
  ggsave(fname, p, width=6, height=4)


if __name__ == "__main__":
  def fusion_workload_1(ns=range(1,11)):
    """
    @return list of (n, query) pairs, where n is the degree of fusion
    """
    for n_preds in ns:
      qual = " AND ".join(["a = %d" % j for j in range(n_preds)])
      q = "SELECT 1 FROM data4 WHERE %s" % qual
      yield (n_preds, q)

  def fusion_workload_2(ns=range(1,11)):
    """
    @return list of (n, query) pairs, where n is the degree of fusion
    """
    for n_preds in ns:
      qual = " AND ".join(["a > 0" for j in range(n_preds)])
      q = "SELECT 1 FROM data4 WHERE %s" % qual
      yield (n_preds, q)



  # we need to pass in the database so that the database isn't recreated
  # on each query
  db = Database.db()
  data = run_benchmark(fusion_workload_1, db)
  plot(data, "Degree of operator fusion", "Query Latency (s)","fusion_workload_1.png")
  data = run_benchmark(fusion_workload_2, db)
  plot(data, "Degree of operator fusion", "Query Latency (s)","fusion_workload_2.png")


  #
  # The following is for A4
  #

  def custom_workload(ns=range(1, 11)):
    """
    A4: Fill this in to construct your custom workload for A4
    """
    return []

  data = run_benchmark(custom_workload, db)
  # A4: REPLACE the x axis label text
  plot(data, "FILL IN X AXIS LABEL", "Query Latency (s)", "custom_workload.png")

