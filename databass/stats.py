class ColStats(object):
  def __init__(self, table_id, col_idx, 
      nrows, card, frac_null, 
      common_vals, common_freqs,
      histogram_bounds, is_base_table):
    self.nrows = nrows
    self.card = card
    self.frac_null = frac_null
    self.common_vals = common_vals
    self.common_freqs = common_freqs
    self.histogram_bounds = histogram_bounds
    self.is_base_table = is_base_table

class Stats(object):
  
  def __init__(self, table):
    self.table = table
    # A2: compute the exact table cardinality
    self.card = 10

    self.col_stats = dict()

  def __getitem__(self, attr):
    if attr not in self.col_stats:
      self.col_stats[attr] = self.compute_col_stats(attr)
    return self.col_stats[attr]

  def compute_col_stats(self, attr):
    """
    @return the domain of the @attr as a dictionary with keys:
            min, max, and distinct
    """
    # A2: compute the actual min, max, ndistinct for the column
    #     "str" type attributes don't have a min and max
    return dict(min=0, max=10, ndistinct=10)


