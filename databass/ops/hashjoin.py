from ..baseops import *
from ..exprs import *
from ..db import Database
from ..schema import *
from ..tuples import *
from ..util import cache, OBTuple
from .join import *
from itertools import chain

   
class HashJoin(Join):
  """
  Hash Join
  """
  def __init__(self, l, r, join_attrs):
    """
    @l    left table of the join
    @r    right table of the join
    @join_attrs two attributes to join on, hash join checks if the 
                attribute values from the left and right tables are
                the same.  Suppose:
                
                  l = iowa, r = iowa, join_attrs = ["STORE", "storee"]

                then we return all pairs of (l, r) where 
                l.STORE = r.storee
    """
    super(HashJoin, self).__init__(l, r)
    self.join_attrs = join_attrs



  def __iter__(self):
    """
    Build an index on the inner (right) source, then probe the index
    for each row in the outer (left) source.  
    
    Yields each join result
    """
    # initialize intermediate row to populate and pass to parent operators
    irow = ListTuple(self.schema)
    lidx = self.join_attrs[0].idx
    ridx = self.join_attrs[1].idx

    index = self.build_hash_index(self.r, ridx)

    for lrow in self.l:
      # probe the hash index
      lval = lrow[lidx]
      key = hash(lval)
      matches = index[key]

      # generate outputs for all matching tuples
      irow.row[:len(lrow.row)] = lrow.row
      for rrow in matches:
        irow.row[len(lrow.row):] = rrow.row
        # TODO: typically, check join condition again
        yield irow

  def build_hash_index(self, child_iter, idx):
    """
    @child_iter tuple iterator to construct an index over
    @attr attribute name to build index on

    Loops through a tuple iterator and creates an index based on
    the attr value
    """
    index = defaultdict(list)
    for row in child_iter:
      val = row[idx]
      key = hash(val)
      index[key].append(row.copy())
    return index


