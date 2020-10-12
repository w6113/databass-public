from .conftest import *
from databass import *
from databass.ops import *

MARGIN = 0.2

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


@pytest.fixture(scope='module')
@pytest.mark.usefixtures("context")
def datasets(context):
  """Datasets for testing"""
  N = 5
  table_size = [10, 20, 50, 100, 200, 500]
  tables = list(range(1, N + 1))

  db = context['db']
  sqlite = context['sqlite']
  table_names = []

  for ts in table_size:
    for id in tables:
      tname = f"test{id}_{ts}"
      sqlite.execute("DROP TABLE IF EXISTS %s" % tname)

      if os.path.exists(f"data/{tname}.csv"):
        df = pd.read_csv(f"data/{tname}.csv")
      else:
        df = pd.DataFrame(np.random.randint(0, ts, size=(ts, 4)), columns=list("abcd"))

      table_names.append(tname)
      db.register_dataframe(tname, df)
      db._df_registry[tname].to_sql(tname, sqlite, index=False)

  return table_names


def run_plan_after_opt(plan):
  """Runs a plan after it is optimized"""
  rows = list()
  for row in plan:
    vals = []
    for v in row:
      if isinstance(v, str):
        vals.append(v)
      else:
        vals.append(float(v))
    rows.append(vals)
  return rows


def get_cond(tbl_names):
  cond = []
  for (t1, t2) in zip(tbl_names, tbl_names[1:]):
    cond.append(f"{t1}.a = {t2}.a")
  return " AND ".join(cond)


@pytest.mark.usefixtures("context")
@pytest.mark.parametrize("query, ourexhaustive, ourselinger",[
  (f"SELECT test1_100.a FROM  test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test4_100', 'test5_100'])} AND test1_100.a = 10", 574, 260),
  (f"SELECT test3_100.b FROM  test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test4_100', 'test5_100'])} AND test3_100.b = 0", 574, 260),
  (f"SELECT test4_100.b FROM  test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test4_100', 'test5_100'])} AND test4_100.b = 0", 574, 260),
  (f"SELECT test5_100.b FROM  test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test4_100', 'test5_100'])} AND test5_100.b > 25 and test5_100.b < 75", 574, 260),
  (f"SELECT test3_100.c FROM  test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test4_100', 'test5_100'])} AND test3_100.c = 0", 574, 260),
  (f"SELECT test4_100.c FROM  test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test4_100', 'test5_100'])} AND test4_100.c = 0", 574, 260),
  (f"SELECT test5_100.c FROM  test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test4_100', 'test5_100'])} AND test5_100.c < 50", 574, 260),
  (f"SELECT test3_100.c FROM  test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test4_100', 'test5_100'])} AND test3_100.d = 'z'", 574, 260),
  (f"SELECT test4_100.c FROM  test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test4_100', 'test5_100'])} AND test4_100.d = 'z'", 574, 260),
  (f"SELECT test5_100.d FROM  test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test4_100', 'test5_100'])} AND test5_100.d > 50", 574, 260)
])
def test_condition(context, datasets, query, ourexhaustive, ourselinger):
  run_selinger_test(context, ourexhaustive, ourselinger, query)


@pytest.mark.usefixtures("context")
@pytest.mark.parametrize("query, ourexhaustive, ourselinger",[
  (f"SELECT test1_100.b, test5_100.b FROM  test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test4_100'])} AND test1_100.b = test5_100.b", 574, 260),
  (f"SELECT test1_100.c, test5_100.c FROM  test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test4_100'])} AND test1_100.c = test5_100.c", 574, 260),
  (f"SELECT test1_100.d, test5_100.d FROM  test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test4_100'])} AND test1_100.c = test5_100.c", 574, 260),
])
def test_data_with_skewed_distribution(context, datasets, query, ourexhaustive, ourselinger):
  run_selinger_test(context, ourexhaustive, ourselinger, query)


@pytest.mark.usefixtures("context")
@pytest.mark.parametrize("query, ourexhaustive, ourselinger",[
  (f"SELECT test3_100.a, test4_100.b FROM test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test5_100'])} AND test3_100.b = test4_100.b", 571, 258),
  (f"SELECT test3_100.c, test4_100.c FROM test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test5_100'])} AND test3_100.c = test4_100.c", 571, 258),
  (f"SELECT test3_100.d, test4_100.d FROM test1_100, test2_100, test3_100, test4_100, test5_100 WHERE {get_cond(['test1_100', 'test2_100', 'test3_100', 'test5_100'])} AND test3_100.d = test4_100.d", 571, 258),
])
def test_data_with_different_range(context, datasets, query, ourexhaustive, ourselinger):
  run_selinger_test(context, ourexhaustive, ourselinger, query)

@pytest.mark.usefixtures("context")
@pytest.mark.parametrize("query,ourexhaustive,ourselinger",[
  ("SELECT test1_10.a  FROM test1_10, test2_10, test3_10, test4_10, test5_10", 205, 9),
  ("SELECT test1_10.a  FROM test1_10, test1_20, test1_50, test1_100, test1_200", 205, 9),
  ("SELECT test1_100.a  FROM test1_100, test2_100, test3_100, test4_100, test5_100", 205, 9),
])
def test_cross_join(context, query, ourexhaustive, ourselinger):
  run_selinger_test(context, ourexhaustive, ourselinger, query)


def run_selinger_test(context, ourexhaustive, ourselinger, query):
  # start_time = datetime.now()
  exhaustive_opt = context['opt']
  exhaustive_plan = exhaustive_opt(parse(query).to_plan())
  oriexhaustive = exhaustive_opt.join_optimizer.plans_tested
  # run_plan_after_opt(exhaustive_plan)
  # elapsed_time = datetime.now() - start_time
  # print("exhaustive", exhaustive_opt.join_optimizer.plans_tested, elapsed_time)
  # print("exhaustive", oriexhaustive)

  # start_time = datetime.now()
  selinger_opt = Optimizer(context['db'], SelingerOpt)
  selinger_plan = selinger_opt(parse(query).to_plan())
  yourselinger = selinger_opt.join_optimizer.plans_tested
  # run_plan_after_opt(selinger_plan)
  # elapsed_time = datetime.now() - start_time
  # print("selinger", selinger_opt.join_optimizer.plans_tested, elapsed_time)
  # print("selinger", yourselinger)

  assert (yourselinger <= max(ourselinger, ourexhaustive) or (yourselinger <= ourselinger * (1 + MARGIN)))


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
