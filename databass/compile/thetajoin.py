from ..ops import ThetaJoin
from .translator import *


class ThetaJoinLeftTranslator(LeftTranslator):
  """
  Inner loop
  """
  def __init__(self, *args, **kwargs):
    super(ThetaJoinLeftTranslator, self).__init__(*args, **kwargs)

    self.v_lrow = None


class ThetaJoinRightTranslator(RightTranslator):
  """
  Outer loop
  """
  def __init__(self, *args, **kwargs):
    super(ThetaJoinRightTranslator, self).__init__(*args, **kwargs)

    self.v_irow = None


