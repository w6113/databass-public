import os
import json
import unittest
from .conftest import *
from databass import *
from databass.ops import *
from databass.exprutil import predicate_to_cnf

curdir = os.path.dirname(os.path.abspath(__file__))


with open(os.path.join(curdir, "lineage.json")) as f:
  truth = json.loads(f.read())




@pytest.mark.parametrize("q,alltruth,endtoendtruth", truth)
def test_lineage(q, alltruth, endtoendtruth):
  _all = collect_lineage(q, AllLineagePolicy())
  assert(str(_all) == str(alltruth))

  _endtoend = collect_lineage(q, EndtoEndLineagePolicy())
  assert(str(_endtoend) == str(endtoendtruth))

def collect_lineage(q, policy):
  cq = PyCompiledQuery(q, policy)
  results = cq()
  oids = list(range(len(results)))
  ret = []
  for scan in cq.optimized_plan.collect("Scan"):
    ret.append(scan.alias)
    for oid in oids:
      iids = cq.lineages[-1].back(oid, scan.id)
      ret.append([oid, iids])
  return ret


