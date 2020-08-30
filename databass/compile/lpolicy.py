from collections import defaultdict
from .translator import LeftTranslator

class LineagePolicy(object):
  """
  Manages which paths in the query plan should capture and/or materialize
  the lineage
  """
  FW = 1
  BW = 2
  BOTH = 3


  def __init__(self):
    self.paths = []
    self.to_materialize = set()
    self.to_capture = set()

    # mapping from operator to the lineage direction to materialize
    self.opdirs = defaultdict(lambda: 0)

  def add_path(self, src_op, dst_op, dirs):
    """
    @dirs FW, BW, or FW | BW = BOTH
    """
    path = []
    cur = src_op
    while cur:
      path.append(cur)
      if cur == dst_op: break
      cur = cur.p

    if not cur:
      raise Exception("adding non-existent lineage path: %s -!-> %s" % (src_op, dst_op))

    self.paths.append(dict(path=path, dirs=dirs))
    self.to_materialize.update([src_op, dst_op])
    self.to_capture.update(path)
    for op in path:
      self.opdirs[op] |= dirs

  def bcapture(self, translator):
    if translator.op not in self.to_capture: 
      return False

    if isinstance(translator, LeftTranslator):
      return self.bcapture(translator.child_translator)
    return True

  def bmaterialize(self, translator):
    return translator.op in self.to_materialize

  def bfw(self, translator):
    """
    Should the translator construct a forward lindex?
    """
    return self.opdirs[translator.op] | LineagePolicy.FW

  def bbw(self, translator):
    """
    Should the translator construct a backward lindex?
    """
    return self.opdirs[translator.op] | LineagePolicy.BW


class NoLineagePolicy(LineagePolicy):
  def add_path(self, *args):
    pass

  def bcapture(self, translator):
    return False

  def bmaterialize(self, translator):
    return False

  def bfw(self, translator):
    return False

  def bbw(self, translator):
    return False


class AllLineagePolicy(LineagePolicy):
  def add_path(self, *args):
    pass

  def bcapture(self, translator):
    return True

  def bmaterialize(self, translator):
    return True

  def bfw(self, translator):
    return True

  def bbw(self, translator):
    return True

class EndtoEndLineagePolicy(LineagePolicy):
  def bcapture(self, translator):
    return True

  def bmaterialize(self, translator):
    return not translator.op.p 

  def bfw(self, translator):
    return True

  def bbw(self, translator):
    return True


