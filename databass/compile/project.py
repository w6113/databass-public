from ..ops import GroupBy
from .translator import *

class ProjectTranslator(Translator):
  def __init__(self, *args, **kwargs):
    super(ProjectTranslator, self).__init__(*args, **kwargs)

    self.v_out = None

