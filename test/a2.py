import unittest
from .util import *
from databass import *
from databass.ops import *
from databass.exprutil import predicate_to_cnf

class TestOptimizer(TestBase):
  def make_query(self, n):
    tables = ["T%d" % i for i in range(n)]

    preds = []
    scans = []
    for i, t in enumerate(tables[:n]):
      scans.append(Scan("data", t))
      if i > 0:
        preds.append(cond_to_func("%s.a = %s.a" % (tables[i], tables[i-1])))
    plan = Filter(From(scans, preds), cond_to_func("%s.a = 1" % tables[0]))
    return plan

  def test_join(self):
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
      opt = Optimizer(self.db, SelingerOpt)
      plan = self.make_query(i)
      plan = opt(plan)
      youriters = opt.join_optimizer.plans_tested
      ourselinger, ourexhaustive = expected_iterations[i]
      self.assertTrue(youriters <= max(ourselinger, ourexhaustive))
      self.assertTrue(youriters <= ourselinger * 1.1)
