from ..ops import *

class Translator(object):
  """
  A translator wraps a physical operator and provides the compilation logic.
  It follows the producer/consumer model.

  It also contains information about the lineage it needs to capture.
  """

  _id = 0
  def __init__(self, op):
    self.id = Translator._id
    Translator._id += 1
    self.op = op
    self.child_translator = None
    self.parent_translator = None

    self.l_materialize = False
    self.l_capture = False
    self.l_prev_translator = None # previous translator that contains lindexes
    self.lindex = None
    self.lindexes = []

  @property
  def propagated_lindexes(self):
    """
          Join
        /     \
      Join     C
      |  |
      A  B

    """
    return self.lindexes

  def prepare(self, c, p, pipeline):
    self.child_translator = c
    self.parent_translator = p
    self.pipeline = pipeline

  def is_type(self, klasses):
    if not isinstance(klasses, list):
      klasses = [klasses]
    return any(isinstance(self, k) for k in klasses)

  def produce(self, ctx):
    pass

  def consume(self, ctx):
    pass

  def compile_expr(self, ctx, e):
    """
    @return var name containing expression result
    """
    raise Exception("Not implemented")

  def compile_exprs(self, ctx, exprs):
    """
    @return [varname,] list of expression results
    """
    raise Exception("Not implemented")

  def compile_new_tuple(self, ctx, schema):
    """
    @return varname containing the new tuple
    """
    raise Exception("Not implemented")

  def clean_prev_lineage_indexes(self):
    """
    Clean up (delete) previous lineage indexes, 
    if they are not materialized
    """
    if self.l_capture and self.l_prev_translator:
      self.l_prev_translator.clean_lineage_indexes()

  def clean_lineage_indexes(self):
    if self.l_capture and not self.l_materialize:
      for lindex in self.propagated_lindexes:
        lindex.clean_lineage_indexes()
      self.lindex = None
      self.lindexes = []

      if hasattr(self, "left") and self.left:
        self.left.lindex = None
        self.left.lindexes = []

  def pretty_print(self):
    return self.op.pretty_print()

  def __str__(self):
    return "%s: %s" % (self.id, self.__class__.__name__)


class BottomTranslator(Translator):
  """
  Unary operators that are pipeline breakers (groupby, orderby)
  are split into bottom and top translators.

  Bottom is responsible for buffering tuples in an appropriate data structure
  (hashtable for groupby, list for orderby)
  """
  def __init__(self, op):
    super(BottomTranslator, self).__init__(op)

    self.l_i = None

class TopTranslator(Translator):
  """
  Top is responsible for processing and walking the populated data struture
  from Bottom and generating output tuples for its parent tranlators
  """
  def __init__(self, op, bottom):
    super(TopTranslator, self).__init__(op)
    self.bottom = bottom
    self.l_i = None   # source rid
    self.l_o = None   # output rid

  def initialize_lineage_indexes(self, ctx):
    pass

  def populate_lineage_indexes(self, ctx, v_bucket):
    pass


class LeftTranslator(Translator):
  """
  Binary join operators are split into a left and right side.

  For hash joins, the left translator is a pipeline breaker that
  collects tuples in a hash table.
  For theta joins, the left is just a loop
  """
  def __init__(self, op):
    super(LeftTranslator, self).__init__(op)
    self.l_i = None


class RightTranslator(Translator):
  """
  Iterates over the right side of the join and probes the left side.
  """
  def __init__(self, op, left):
    super(RightTranslator, self).__init__(op)
    self.left = left
    assert(op.is_type(Join))

    self.l_i = None
    self.l_o = None

  @property
  def propagated_lindexes(self):
    ret = []
    ret.extend(self.left.propagated_lindexes)
    ret.extend(self.lindexes)
    return ret


