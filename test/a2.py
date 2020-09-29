import unittest
from .conftest import *
from databass import *
from databass.ops import *
from databass.exprutil import predicate_to_cnf

def make_query(n):
  tables = ["T%d" % i for i in range(n)]

  preds = []
  scans = []
  for i, t in enumerate(tables[:n]):
    scans.append(Scan("data", t))
    if i > 0:
      preds.append(cond_to_func("%s.a = %s.a" % (tables[i], tables[i-1])))
  plan = Filter(From(scans, preds), cond_to_func("%s.a = 1" % tables[0]))
  return plan


@pytest.mark.usefixtures("context")
def test_join(context):
  expected_iterations = {
      1:(1, 0),   # (selinger, exhaustive)
      2:(10, 8),
      3:(35, 32),
      4:(100, 126),
      5:(260, 574),
      6:(640, 3122),
      7:(1520, 20070)
    }
  margin = 0.2

  # add the dummy tables to the database
  # the data in the public tests use uniform distribution.
  # you can try other ditsributions!
  db = context['db']
  sqlite = context['sqlite']
  for i in range(10):
    tname = "T%s" % i
    sqlite.execute("DROP TABLE IF EXISTS %s" % tname)
    df = pd.DataFrame(np.random.randint(0, 100, size=(1000, 4)), columns=list("abcd"))
    db.register_dataframe(tname, df)
    db._df_registry[tname].to_sql(tname, sqlite, index=False)



  # check that the number of plans your selinger algorithm takes
  # is less than max(our selinger, our exhaustive) and within 20% of
  # our implementation
  for i in range(1, 8):
    opt = Optimizer(context['db'], SelingerOpt)
    plan = make_query(i)
    plan = opt(plan)
    youriters = opt.join_optimizer.plans_tested
    ourselinger, ourexhaustive = expected_iterations[i]
    assert(youriters <= max(ourselinger, ourexhaustive) or (youriters <= ourselinger * (1 + margin)))
