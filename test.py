"""
You can use this as a script to run queries and test your code
-We have included some examples
"""
import time
from databass import *
from databass.tables import InMemoryTable
from databass.ops import *
from databass.compile import *
from databass.exprutil import *


# Optimizer is needed to process a query plan before it can run
opt = Optimizer()


def optimize_and_run(plan):
  plan = opt(plan)
  print(plan.pretty_print())
  start = time.time()
  for row in plan:
    print(row)
  print("took %0.5f sec" % (time.time()-start))

def make_query(n):
  """
  @n number of input tables
  Generates simple join queries
  """
  tables = ["T%d" % i for i in range(n)]

  preds = []
  scans = []
  for i, t in enumerate(tables[:n]):
    scans.append(Scan("data", t))
    if i > 0:
      preds.append(cond_to_func("%s.a = %s.a" % (tables[i], tables[i-1])))
  plan = Filter(From(scans, preds), cond_to_func("%s.a = 1" % tables[0]))
  return plan


# Example of running a SQL string
parsetree = parse("""
    SELECT sum(b)
    FROM data
    GROUP BY a
    HAVING sum(c) > 0
    ORDER BY a*2""")
plan = parsetree.to_plan()
optimize_and_run(plan)

# Example of building a join query plan and running it
preds = predicate_to_cnf(cond_to_func("(A.a = B.a)"))
logicalplan = From([
  Scan("data", "A"),
  Scan("data", "B")
], preds)
optimize_and_run(logicalplan)


# Example runs Selinger and Exhaustive join optimizers on varying # of relations
db = Database.db()
opt1 = Optimizer(db, SelingerOpt)
opt2 = Optimizer(db, JoinOptExhaustive)

data = []
for i in range(1, 8):
  plan1 = opt1(make_query(i))
  plan2 = opt2(make_query(i))
  n1 = opt1.join_optimizer.plans_tested
  n2 = opt2.join_optimizer.plans_tested
  data.append(dict( x=i, y=n1, opt="Selinger"))
  data.append(dict( x=i, y=n2, opt="Exhaustive"))
  print((i, n1, n2))

try:
  # plot the curves
  from pygg import *
  from wuutils import *
  p = ggplot(data, aes(x='x', y='y', color='opt', group='opt'))
  p += geom_line()
  p += axis_labels("Num Relations", "Num Plans Tested")
  p += legend_bottom
  ggsave("join_costs.png", p, width=6, height=4)
except:
  pass


# Example of compiling a query
plan = Yield(opt(parse("SELECT a, a+b FROM data").to_plan()))
q = PyCompiledQuery(plan, None)
print(q.print_code())
for row in q():
  print(row)





