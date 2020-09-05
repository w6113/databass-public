from collections import defaultdict
from ..lindex import *



class PyLindex(Lindex):

  def __add__(self, o):
    if not o or not isinstance(o, Lindex):
      raise Exception("Lindex + %s doesn't make sense" % o)

    bw_n = Lindex.N if Lindex.N in (self.bw_n, o.bw_n) else Lindex.ONE
    fw_n = Lindex.N if Lindex.N in (self.fw_n, o.fw_n) else Lindex.ONE
    bw = PyBw(self.ctx, bw_n, prev=o)
    fw = PyFw(self.ctx, fw_n, prev=o)
    lindex = PyLindex(self.ctx, bw, fw, self.translator)
    lindex.src_t = o.src_t
    lindex.dst_t = self.dst_t
    return lindex

 
class PyLidx(Lidx):
  def __getitem__(self, key):
    return "{idx}[{key}]".format(idx=self.idx, key=key)

  def clean_lineage_indexes(self):
    self.ctx.add_line("del {idx}", idx=self.idx)

  @contextmanager
  def _loop(self, vals, varname):
    line = "for {var} in {vals}:"
    with self.ctx.indent(line, var=varname, vals=vals):
      yield self

  def _set(self, key, val):
    self.ctx.add_line("{idx}[{key}] = ({val})",
        idx=self.idx, key=key, val=val)

  def _add_1(self, key, val):
    self.ctx.add_line("{idx}[{key}].append({val})", 
        idx=self.idx, key=key, val=val)

  def _add_n(self, key, vals):
    self.ctx.add_line("{idx}[{key}].extend({vals})", 
        idx=self.idx, key=key, vals=vals)


class PyIdentityLidx(IdentityLidx, PyLidx):
  pass

 
class PyBw(Bw, PyLidx):
  def __init__(self, *args, **kwargs):
    super(PyBw, self).__init__(*args, **kwargs)
    self.preallocated = False

    # if we preallocate the lineage index, then need to
    # internally track which array element to set next
    # when self._append is called
    self.lindex_idx = None

  def initialize(self, size=None):
    """
    @size source cardinality
    """
    self.idx = self.ctx.new_var("l_bw")
    self.preallocated = (size is not None)
    if self.preallocated:
      self.lindex_idx = self.ctx.new_var("l_bw_idx")
      self.ctx.declare(self.lindex_idx, 0)

    if self.type == Lindex.ONE:
      # bw is always an array because output id always increments 
      if self.preallocated:
        self.ctx.add_line("{idx} = [None] * ({size})", idx=self.idx, size=size)
      else:
        self.ctx.add_line("{idx} = []", idx=self.idx)
      return 

    if self.preallocated:
      self.ctx.add_line("{idx} = [[] for i in range({size})]", 
          idx=self.idx, size=size)
    else:
      self.ctx.add_line("{idx} = []", idx=self.idx)

  def _append(self, val):
    """
    The access pattern for append is different depending on whether
    or not the index has been preallocated, so we can't use the default
    """
    if self.preallocated:
      self._set(self.lindex_idx, val)
      self.ctx.add_line("%s += 1" % self.lindex_idx)
    else:
      self.ctx.add_line("{idx}.append({val})", 
          idx=self.idx, val=val)



class PyFw(Fw, PyLidx):
  def __init__(self, *args, **kwargs):
    super(PyFw, self).__init__(*args, **kwargs)
    self.preallocated = False


  def initialize(self, size=None):
    """
    Should only be called once!
    @size cardinality of destination 
    """
    self.idx = self.ctx.new_var("l_fw")
    self.preallocated = (size is not None)

    if self.type == Lindex.ONE:
      if self.preallocated and not self.prev:
        self.ctx.add_line("{idx} = [None] * ({size})", idx=self.idx, size=size)
      else:
        self.ctx.add_line("{idx} = dict()", idx=self.idx)
      return

    if self.preallocated and not self.prev:
      self.ctx.add_line("{idx} = [[] for i in range({size})]",
          idx=self.idx, size=size)
    else:
      self.ctx.add_line("{idx} = defaultdict(list)", idx=self.idx)


