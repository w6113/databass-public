from .util import *
from databass.parse_sql import *

class TestA0(TestBase):
  def test_parsing(self):
    qs = [
        "SELECT 1 ORDER BY 1",
        "SELECT 1 ORDER BY a",
        "SELECT 1 ORDER BY a asc"
        ,
        "SELECT 1 ORDER BY 1+1",
        "SELECT 1 ORDER BY 3+1",
        "SELECT 1 ORDER BY (a)+1",
        "SELECT 1 ORDER BY (a+1)",
        "SELECT 1 ORDER BY a+b",
        "SELECT 1 ORDER BY a+b asc",
        "SELECT 1 ORDER BY a+b ASC",
        "SELECT 1 ORDER BY a+b desc",
        "SELECT 1 ORDER BY a+b, 1 desc",
        "SELECT 1 ORDER BY a+b, 1+1 asc",
        "SELECT 1 ORDER BY a+b, a+1 asc", 
        "SELECT 1 ORDER BY a+b, a+c asc",
        "SELECT 1 ORDER BY a asc, b desc"

    ]
    for q in qs:
      Visitor().parse(q)

    badqs = [
        "SELECT 1 ORDER BY",
        "SELECT 1 ORDER ",
        "SELECT 1 ORDER BY 1(+1",
        "SELECT 1 ORDER BY 3+1)",
        "SELECT 1 ORDER BY a+1-",
        "SELECT 1 ORDER BY a+/b",
        "SELECT 1 ORDER BY a+b asec",
        "SELECT 1 ORDER BY a+b ASc",
        "SELECT 1 ORDER BY a+b asc asc",
        "SELECT 1 ORDER BY a+b deSC",
        "SELECT 1 ORDER BY a+b 1 desc",
        "SELECT 1 ORDER BY a+b asc asc, a+1 asc",
        "SELECT 1 ORDER BY a a"
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

