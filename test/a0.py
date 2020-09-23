from .conftest import *
from databass.parse_sql import *

parse_qs = [
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

parse_badqs = [
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


end2end_qs = [
    "SELECT * from data ORDER BY 1",
    "SELECT a+b FROM data ORDER BY a",
    "SELECT * from data ORDER BY a asc",
    "SELECT * from data ORDER BY a",
    "SELECT * from data ORDER BY a desc"
    ,
    "SELECT * from data ORDER BY a, b",
    "SELECT * from data ORDER BY a asc, b asc",
    "SELECT * from data ORDER BY a asc, b desc",
    "SELECT * from data ORDER BY a desc, b asc",
    "SELECT * from data ORDER BY a desc, b desc",
    "SELECT * from data ORDER BY a+1 desc, b desc",
    "SELECT * from data ORDER BY a+1 desc, b+2 desc",
    "SELECT * from data ORDER BY -a, -b",
    "SELECT * from data ORDER BY -a, -b desc",
    "SELECT * from data ORDER BY -a desc, -b"
]

end2end_badqs = [
    "SELECT 1 ORDER BY a"
    ,
    "SELECT 1 ORDER BY (a+b1)+1 asc",
    "SELECT 1 FROM data ORDER BY (a+b1)+1 asc",
    "SELECT 1 FROM data ORDER BY (1, 1)+1 asc",
    "SELECT 1 FROM data ORDER BY (a+b, 1)+1 asc",
    "SELECT 1 ORDER BY a+b, a+c asc"
]


@pytest.mark.parametrize("q", parse_qs)
def test_parsing(q):
  Visitor().parse(q)

@pytest.mark.parametrize("q", parse_badqs)
def test_bad_parsing(q):
  with pytest.raises(Exception):
    Visitor().parse(q)

@pytest.mark.parametrize("q", end2end_qs)
@pytest.mark.usefixtures('context')
def test_q(context, q):
  run_query(context, q, True)

@pytest.mark.parametrize("q", end2end_badqs)
@pytest.mark.usefixtures('context')
def test_badq(context, q):
  with pytest.raises(Exception):
    run_query(context, q, True)


