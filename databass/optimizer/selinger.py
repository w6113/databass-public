from .joinopt import *

class SelingerOpt(JoinOpt):
  """
  Selinger is a dynamic programming algorithm that starts by 
  finding the optimal for single relation plans, and then iteratively
  finds the optimal for increasing sized plans (2, 3, etc)
  """

  def __call__(self, preds, sources):
    self.sources = sources
    self.preds = preds
    self.pred_index = self.build_predicate_index(preds)
    self.plans_tested = 0

    # number of relations --> all join info objects of that size
    join_infos = defaultdict(dict)

    # initialize single relation join infos, cost the plans, and cache them
    # A2: implement me

    # Find the optimal for plans of increasing size, starting from size 2 plans
    while len(join_infos) < len(sources):
      key = len(join_infos) + 1

      # Expand each candidate from the previous iteration by a single relation,
      # go over all possible plans, and find the lowest cost one
      for cand in join_infos[key-1].values():
        for item in join_infos[1].values():
          if cand.overlaps(item): continue
          ji = cand.merge(item)

          if not ji.predicates: continue
          if hash(ji) in join_infos[key]: continue

          self.evaluate_new_join_info(ji, join_infos)
          if ji.best_plan:
            join_infos[key][hash(ji)] = ji


      # If we didn't add any JoinInfos in this iteration, it means
      # we don't have any more join predicates.  Thus we need to find the
      # lowest cost join info from the previous iteration, and cross product it
      # with everything else.  
      #
      # The order you crossproduct the remaining relations doesn't matter
      # but join_infos should be updated in each iteration 
      if len(join_infos[key]) == 0:
        # A2: implement me
        break

    # there should only be one JoinInfo for all sources
    ji = next(iter(join_infos[len(sources)].values()))
    plan, cost = ji.best_plan, ji.best_cost

    self.fix_parent_pointers(plan)
    return plan

  def evaluate_new_join_info(self, ji, join_infos):
    """
    @ji the new JoinInfo that we will find the best plan for
    @join_infos the cache of previously costed JoinInfo objects 

    Try all possible ways to join relations in @ji.  Since we have already
    computed the optimal for any subset of @ji.rels, we only need to consider
    which subsets we will join, and the valid set of physical join operators.

    It is OK to only consider plans with one relation on one side of the join.
    Extra kudos if you also consider bushy plans

    When method returns, @ji.best_plan and best_cost should be set to the optimal
    """
    # A2: implement me
    #     get_join_info(), and valid_join_impls() may help
    pass

  def get_join_info(self, rels, join_infos):
    """
    @rels a list of relations
    @join_infos JoinInfo cache

    Lookup cache JoinInfo for set of relations @rels
    """
    aliases = [rel.alias for rel in rels]
    key = hash(tuple(list(sorted(aliases))))
    return join_infos[len(rels)].get(key, None)


