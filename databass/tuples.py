class ListTuple(object):
  """
  A tuple consists of a schema (should be same schema as the containing Table)
  and a list of attribute values.


  TODO: in general tuples should know how to generate code to access/write
        values to a tuple given a variable representing the tuple.
  """
  def __init__(self, schema, row=None):
    self.schema = schema
    self.row = row or []
    if len(self.row) < len(self.schema.attrs):
      self.row += [None] * (len(self.schema.attrs) - len(self.row))

  def copy(self):
    return ListTuple(self.schema.copy(), list(self.row))

  def __hash__(self):
    return hash(str(self.row))
 
  def __getitem__(self, idx):
    return self.row[idx]

  def __setitem__(self, idx, val):
    self.row[idx] = val

  def __str__(self):
    return "(%s)" % ", ".join(map(str, self.row))

