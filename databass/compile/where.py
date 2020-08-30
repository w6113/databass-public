from ..ops import GroupBy
from .translator import *


class FilterTranslator(Translator):
  def __init__(self, *args, **kwargs):
    super(FilterTranslator, self).__init__(*args, **kwargs)

  def produce(self, ctx):
    self.child_translator.produce(ctx)


