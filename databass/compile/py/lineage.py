"""
The contents of this file are used in user-space by the application, and NOT in
databass for query compilation.  It loadsthe captured lineage and provides
convenient lineage query APIs.  

This probably makes sense to move into a databass/apis folder, but
we will leave it here for now..

TODO: move to APIs folder

"""
from ...baseops import Op, UnaryOp
from .lindex import Lindex
from collections import defaultdict
from collections.abc import Iterable

class IdentityDict(object):
  def __getitem__(self, key):
    return key

  def __str__(self):
    return "IdentityDict"

class Lineage(object):
  """
  This class provide convenient APIs for the application to query the captured lineage via
  Backward and Forward lineage queries.  

  It is initialized with the lindexes that were populated during query execution.
  """
  def __init__(self, plan):
    self.plan = plan
    self.lindexes = []
    self.materialized_ids = set()
    self.fws = defaultdict(dict)
    self.bws = defaultdict(dict)

    id2op = dict()
    ops = [self.plan]
    while ops:
      op = ops.pop()
      id2op[op.id] = op
      ops.extend(op.children())
    self.id2op = id2op

  def add(self, src_pair, dstid, idx, direction, typ):
    """
    Add a forward or backward lineage index 

    @src   (operator id, index). Represents input of operator.
           Index by default is 0.
           Index is 1 if src is right child of a join operator. 
    @dstid operator id.  Represents output of operator.
    @idx   lineage data structure
    @direction "fw" | "bw"
    @typ   Lindex.ONE | Lindex.N
    """
    if idx is None:
      idx = IdentityDict()

    srcid, index = src_pair
    self.lindexes.append((src_pair, dstid, idx, direction, typ))
    self.materialized_ids.update([srcid, dstid])
    if direction == "fw":
      self.fws[src_pair][dstid] = (idx, typ)
    elif direction == "bw":
      self.bws[dstid][src_pair] = (idx, typ)
    else:
      raise Exception("direction %s should be 'fw' or 'bw'" % direction)

  def find_bw_path(self, start, end):
    """
    Returns the sequence of backward lineage indexes that connects 
    the output of start to the input in end

    @start  operator id of ancestor (sink) operator
    @end  (operator id, index) of descendent (source) operator
    """
    if start not in self.bws:
      return None

    neighbors = self.bws[start]
    if end in neighbors:
      print(self.id2op[start])
      return [neighbors[end]]

    for neighbor in neighbors:
      opid, index = neighbor
      op = self.id2op[opid]
      if not op.children(): continue
      child = op.children()[index]

      # Neighbor represents the input of the "neighbor" operator
      # Translate into the output of the descendent pipeline breaker.
      #
      # If neighbor = (Join, 0), then we translate into A
      #
      #          Join
      #     Filter    B
      #      A    
      #

      # skip non-pipeline breakers
      while child and child.id not in self.bws:
        if not child.is_type(UnaryOp):
          return None
        child = child.children()[0]
      if not child: 
        return None

      rest = self.find_bw_path(child.id, end)
      if rest:
        print(op)
        return [neighbors[neighbor]] + rest
    return None

  def back(self, oids, src_pair, dstid=None):
    """
    Given the output record ids of operator @dstid, 
    return rids in the @index'th input relation to @src 

    @src_pair (operator object or its ID, index)
    @dstid operator object or its ID.
           Assumed to be query output if None
    """
    if not isinstance(src_pair, tuple):
      src_pair = (src_pair, 0)
    if isinstance(src_pair[0], Op):
      src_pair = (src_pair[0].id, src_pair[1])
    if dstid is None:
      dstid = self.plan.id
    if isinstance(dstid, Op):
      dstid = dstid.id
    if not isinstance(oids, Iterable):
      oids = [oids]

    path = self.find_bw_path(dstid, src_pair)
    if not path:
      return None

    curoids = oids
    for lidx, typ in path:
      nextoids = []
      if typ == Lindex.ONE:
        for oid in curoids:
          nextoids.append(lidx[oid])
      else:
        for oid in curoids:
          nextoids.extend(lidx[oid])

      curoids = nextoids
    return curoids

  def index_in_parent(self, op):
    if op.p:
      return op.p.children().index(op)
    return 0

  def find_fw_path(self, start, end):
    """
    Returns the sequence of backward lineage indexes that connects 
    the input of start to the output of end

    @start (operator id, index) of source operator
    @end operator id of sink operator
    """
    if start not in self.fws:
      return None

    neighbors = self.fws[start]
    if end in neighbors:
      #print self.id2op[end]
      return [neighbors[end]]
    
    for neighbor in neighbors:
      # Neighbor represents the output of the "neighbor" operator
      # Translate into the input of the ancestor pipeline breaker
      # If neighbor = A, then translate it into (Join, 0)
      #
      #          Join
      #     Filter    B
      #      A    
      #
      cur = self.id2op[neighbor]
      new_start = None
      while cur.p:
        index = cur.p.children().index(cur)
        # if it is in the fws index, then it is a pipeline breaker
        if (cur.p.id, index) in self.fws:
          new_start = (cur.p.id, index)
          break
        cur = cur.p
      if not new_start:
        return None

      rest = self.find_fw_path(new_start, end)
      if rest:
        #print self.id2op[neighbor]
        return [neighbors[neighbor]] + rest
    return None

  def forw(self, iids, src_pair, dstid=None):
    """
    Given input rids of the source operator, return rids of the
    outputs of the destination operator that are influenced by the
    input rids.  

    @dstid assumed to be query output if None
    """

    # src_pair refers to the source operator & input  relation we 
    # are referring to.  Default for unary ops is 0
    if not isinstance(src_pair, tuple):
      src_pair = (src_pair, 0)
    if isinstance(src_pair[0], Op):
      src_pair = (src_pair[0].id, src_pair[1])

    # default is root operator
    if dstid is None:
      dstid = self.plan.id
    if isinstance(dstid, Op):
      dstid = dstid.id
    if not isinstance(iids, Iterable):
      iids = [iids]

    path = self.find_fw_path(src_pair, dstid)
    if not path:
      return None

    curiids = iids
    for lidx, typ in path:
      nextiids = []
      if typ == Lindex.ONE:
        if isinstance(lidx, list):
          for iid in curiids:
            nextiids.append(lidx[iid])
        else:
          for iid in curiids:
            if iid not in lidx: continue
            nextiids.append(lidx[iid])
      else:
        if isinstance(lidx, list):
          for iid in curiids:
            nextiids.extend(lidx[iid])
        else:
          for iid in curiids:
            if iid not in lidx: continue
            nextiids.extend(lidx[iid])

      curiids = nextiids
    return curiids

  def __str__(self):
    ret = []
    for (srcid, index), dstid, idx, direction, typ in self.lindexes:
      src, dst = (self.id2op[srcid], self.id2op[dstid])
      typ = "N" if typ  == Lindex.N else "1"
      ret.append("# %s:%s %s(%s) -> %s" % (direction, typ, src, index, dst))
      ret.append(str(idx))
      ret.append("")
    return "\n".join(ret)

