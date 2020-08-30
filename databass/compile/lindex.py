from contextlib import contextmanager
from collections import defaultdict
from .compiler import Indent, Unindent



class Lindex(object):
  """
  Manages backward (bw) and forward (fw) lineage indexes for a translator
  """
  # for backward lindex, N means N-1 operator (groupby)
  # for forward lindex, N means 1-N operator (join)
  N = 0    

  # for both, ONE means 1-1 operator
  ONE = 1

  def __init__(self, ctx, bw, fw, translator):
    self.ctx = ctx
    self.bw = bw
    self.fw = fw
    self.translator = translator
    self.src_t = self.translator  # input translator
    self.dst_t = self.translator   # output translator

  @property
  def bw_n(self):
    return self.bw.type

  @property
  def fw_n(self):
    return self.fw.type

  def __add__(self, o):
    raise Exception("Not implemented")

  def clean_lineage_indexes(self):
    self.bw.clean_lineage_indexes()
    self.fw.clean_lineage_indexes()

  def compile_print(self):
    self.ctx.add_line("print '# {t} | {src} -> {dst}'", 
        t=self.translator, src=self.src_t.op, dst=self.dst_t.op)
    self.bw.compile_print("BW")
    self.fw.compile_print("FW")

 
class Lidx(object):
  """
  Base class for a bw/fw lineage index.  Provides helpers to generate code for 
  adding to a lineage index and propogating lineage from a previous index

  The primary challenge in efficient implementations is to manage memory 
  and array resizing efficiently.  
  """
  def __init__(self, ctx, typ, prev=None):
    """
    @idx  compiled lindex variable name. result of ctx.new_var(...)
    @prev the previous Lindex object
    """
    self.ctx = ctx
    self.type = typ  # Lindex.N or Lindex.ONE
    self.prev = prev

  def __getitem__(self, key):
    raise Exception("Not implemented")


  def clean_lineage_indexes(self):
    raise Exception("Not implemented")

  def compile_print(self, name=''):
    """
    print information about the lidx to self.ctx
    """
    self.ctx.add_line("print '>> {name}, {idx}'", name=name, idx=self.idx)
    self.ctx.add_line("print {idx}", idx=self.idx)


  #
  # The following actually emit code, and need to be overridden
  # by language-specific subclasses.  These functions are
  # prefixed with "_"
  #

  def _append(self, val):
    """
    Assuming lineage index is an array, append @val to it
    """
    raise Exception("Not implemented")

  def _set(self, key, val):
    """
    Assuming lineage index is a hashtable/2d array, 
    ensure index[@key] = @val
    """
    raise Exception("Not implemented")

  def _add_1(self, key, val):
    """
    Assuming lineage index is a hashtable/2d array, 
    append @val to index[@key]
    """
    raise Exception("Not implemented")

  def _add_n(self, key, vals):
    """
    Assuming lineage index is a hashtable/2d array, 
    append all elements in @vals to index[@key]
    """
    raise Exception("Not implemented")

  @contextmanager
  def _loop(self, vals, varname):
    """
    Helper code to iterate through @vals and bind @varname to each element.
    Returns a context manager so caller can use this in a "with" statement:
    
    with self._loop(iids, 'var1'):
      .. code within the loop ..

    See below for usage
    """
    raise Exception("Not implemented")




class IdentityLidx(Lidx):
  def __init__(self, ctx):
    """
    @idx  compiled lindex variable name. result of ctx.new_var(...)
    @prev the previous Lindex object
    """
    self.ctx = ctx
    self.type = Lindex.ONE
  
  def __getitem__(self, key):
    return key

  def clean_lineage_indexes(self):
    pass

  def initialize(self, size=None): 
    return

  def compile_print(self, name=''):
    self.ctx.add_line("print '>> {name}, IdentityLidx'", name=name)

 
class Bw(Lidx):
  """
  Helpers for populating backward lineage indexes.
  Can be created with a reference to the previous lindex.  
  If so, will use the previous backward lineage index to 
  propogate its information to this index.

  The following is an example query plan.  
  A, I, O are the input, intermediate, and output relations

        (prev)          (self)
        bw idx 1       bw idx 2
    A --> OP1 ---- I --> OP2 ---> O

  Will ensure that bw idx 2 will map output record ids 
  to record ids in A instead of I

  """
  def __init__(self, *args, **kwargs):
    super(Bw, self).__init__(*args, **kwargs)
    self.idx = self.ctx.new_var("l_bw")

  def initialize(self, size=None):
    raise Exception("Not implemented")

  
  #
  # The following are public APIs for adding to a backward lineage index.
  # The implementations perform the appropriate translation if there is 
  # a previous lindex to propagate
  #

  def append_1(self, val, basecase=False):
    if basecase or not self.prev:
      self._append(val)
      return

    if self.prev.bw_n == Lindex.ONE:
      self._append(self.prev.bw[val])
    else:
      self._append(self.prev.bw[val])

  def append_n(self, vals, basecase=False):
    if basecase or not self.prev:
      self._append(vals)
      return

    _iid = self.ctx.new_var("l_bw_iid")
    with self._loop(vals, _iid):
      _prev_iid = self.prev.bw[_iid]
      if self.prev.bw_n == Lindex.ONE:
        self._append(_prev_iid)
      else:
        self._append(_prev_iid)



class Fw(Lidx):
  """
  Helpers for populating forward lineage indexes.
  Can be created with a reference to the previous lindex.  
  If so, will use the previous backward lineage index to 
  propogate its information to this index.

  The following is an example query plan.  
  A, I, O are the input, intermediate, and output relations

        (prev)          (self)
        fw idx 1       fw idx 2
    A --> OP1 ---- I --> OP2 ---> O

  Will ensure that fw idx 2 will map input record ids from A
  to output record ids in O, instead of from I to O
  """

  def __init__(self, *args, **kwargs):
    super(Fw, self).__init__(*args, **kwargs)
    self.idx = self.ctx.new_var("l_fw")

  def initialize(self, size=None):
    raise Exception("Not implemented")
 
  #
  # The following are public APIs for adding to a backward lineage index.
  # The implementations perform the appropriate translation if there is 
  # a previous lindex to propagate
  #


  def set_1(self, key, val, basecase=False):
    if basecase or not self.prev:
      self._set(key, val)
      return

    if self.prev.bw_n == Lindex.ONE:
      if self.prev.fw_n == Lindex.ONE:
        self._set(self.prev.bw[key], val)
      else:
        self._add_1(self.prev.bw[key], val)
    else:
      _iid = self.ctx.new_var("l_fw_iid")
      with self._loop(self.prev.bw[key], _iid):
        if self.prev.fw_n == Lindex.ONE:
          self._set(_iid, val)
        else:
          self._add_1(_iid, val)

  def add_1(self, key, val, basecase=False):
    if basecase or not self.prev:
      self._add_1(key, val)
      return

    if self.prev.bw_n == Lindex.ONE:
      self._add_1(self.prev.bw[key], val)
      return

    _iid = self.ctx.new_var("l_fw_iid")
    with self._loop(self.prev.bw[key], _iid):
      self._add_1(_iid, val)

  def add_n(self, key, vals, basecase=False):
    if basecase or not self.prev:
      self._add_n(vals, key)
      return

    if self.prev.bw_n == Lindex.ONE:
      self._add_n(self.prev.bw[key], vals)
      return

    _iid = self.ctx.new_var("l_fw_iid")
    with self._loop(self.prev.bw[key], _iid):
      self._add_n(_iid, vals)

