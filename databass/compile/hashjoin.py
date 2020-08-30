from ..ops import HashJoin
from .translator import *


class HashJoinLeftTranslator(LeftTranslator):
  def __init__(self, *args, **kwargs):
    super(HashJoinLeftTranslator, self).__init__(*args, **kwargs)

    # allocate variables for all state shared between produce/consume
    self.v_ht = None   # hashtable

class HashJoinRightTranslator(RightTranslator):
  def __init__(self, *args, **kwargs):
    super(HashJoinRightTranslator, self).__init__(*args, **kwargs)

    self.v_irow = None

