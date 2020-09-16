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
      2:(18, 8),
      3:(81, 32),
      4:(282, 126),
      5:(864, 574),
      6:(2448, 3122),
      7:(6576, 20070)
    }

  # check that the number of plans your selinger algorithm takes
  # is less than max(our selinger, our exhaustive) and within 10% of
  # our implementation
  for i in range(1, 8):
    opt = Optimizer(context['db'], SelingerOpt)
    plan = make_query(i)
    plan = opt(plan)
    youriters = opt.join_optimizer.plans_tested
    ourselinger, ourexhaustive = expected_iterations[i]
    assert(youriters <= max(ourselinger, ourexhaustive))
    assert(youriters <= ourselinger * 1.1)
