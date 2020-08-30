from ..ops import GroupBy
from .translator import *


class LimitTranslator(Translator):
  def __init__(self, *args, **kwargs):
    super(LimitTranslator, self).__init__(*args, **kwargs)

