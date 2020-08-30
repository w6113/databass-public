from .util import *
from databass.parse_sql import *

class TestA0(TestBase):
  def test_parsing(self):
    qs = [
        "SELECT 1 ORDER BY 1",
        "SELECT 1 ORDER BY a",
        "SELECT 1 ORDER BY a asc"

    ]
    for q in qs:
      Visitor().parse(q)

    badqs = [
        "SELECT 1 ORDER BY",
        "SELECT 1 ORDER ",
        "SELECT 1 ORDER BY 1(+1",
    ]
    for q in badqs:
      with self.assertRaises(Exception):
        Visitor().parse(q)


  def test_end2end(self):
    qs = [
        "SELECT * from data ORDER BY 1",
        "SELECT a+b FROM data ORDER BY a",
        "SELECT * from data ORDER BY a asc",
        "SELECT * from data ORDER BY a",
        "SELECT * from data ORDER BY a desc"
    ]

    for q in qs:
      self.run_query(q, True)

    badqs = [
        "SELECT 1 ORDER BY a"
    ]
    for q in badqs:
      with self.assertRaises(Exception):
        self.run_query(q, True)

