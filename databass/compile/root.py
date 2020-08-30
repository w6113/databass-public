from ..ops import GroupBy
from .translator import *

class SinkTranslator(Translator):
  def __init__(self, *args, **kwargs):
    super(SinkTranslator, self).__init__(*args, **kwargs)

    self.l_o = None  # rid of output row
    self.l_i = None  # rid of input row
    self.lindexes = []

  def produce(self, ctx):
    self.initialize_lineage_indexes(ctx)
    self.child_translator.produce(ctx)
    self.clean_prev_lineage_indexes()


class YieldTranslator(SinkTranslator):
  def __init__(self, *args, **kwargs):
    super(YieldTranslator, self).__init__(*args, **kwargs)


class CollectTranslator(SinkTranslator):
  def __init__(self, *args, **kwargs):
    super(CollectTranslator, self).__init__(*args, **kwargs)
    self.l_buffer = None


class PrintTranslator(SinkTranslator):
  def __init__(self, *args, **kwargs):
    super(PrintTranslator, self).__init__(*args, **kwargs)




