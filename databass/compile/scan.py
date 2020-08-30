from ..ops import GroupBy
from .translator import *

class SubQueryTranslator(Translator):
  def __init__(self, *args, **kwargs):
    super(SubQueryTranslator, self).__init__(*args, **kwargs)

  def produce(self, ctx):
    self.child_translator.produce(ctx)

  def consume(self, ctx):
    self.parent_translator.consume(ctx)



class ScanTranslator(Translator):
  def __init__(self, *args, **kwargs):
    super(ScanTranslator, self).__init__(*args, **kwargs)

    self.l_o = None

  def consume(self, ctx):
    self.parent_translator.consume(ctx)
