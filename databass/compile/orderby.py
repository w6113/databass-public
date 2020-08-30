from ..ops import GroupBy
from .translator import *

class OrderByBottomTranslator(BottomTranslator):
  def __init__(self, *args, **kwargs):
    super(OrderByBottomTranslator, self).__init__(*args, **kwargs)


class OrderByTopTranslator(TopTranslator):
  def __init__(self, *args, **kwargs):
    super(OrderByTopTranslator, self).__init__(*args, **kwargs)

