import logging
import copy
import itertools
import math
import os
from colorlog import ColoredFormatter
from collections import defaultdict
from nhd.Node import Node
from nhd.NHDCommon import NHDCommon
from nhd.CfgTopology import SMTSetting
from nhd.CfgTopology import TopologyMapType
from nhd.CfgTopology import CfgTopology
from typing import Dict, List


""" 
The Matcher class attempts to find the best pairing of a Node to a CfgTopology, if one exists. This
mapping can then be used by the orchestrator to place a workload and statically-assign the resources.
"""
class Matcher:
    def __init__(self):
        self.logger = NHDCommon.GetLogger(__name__)
        self.logger.info('Initializing matcher')


    def FindNode(self, nl, top) -> str:
        """ Main scheduling matcher function. Attempts to find the best node match based on a list of
        available nodes, plus the pod's topology configuration. For algorithm details, see README """

        self.logger.info(f'Attempting to find node for pod with {len(top.proc_groups)} process groups, '
                        f'{len(top.misc_cores)} misc cores')

        # First filter on the top-level pod resources that are built-in to Kubernetes
        nl = self.FilterPodResources(nl, top)

        # After the native resources are filtered, now do all the matching based on what type of map
        # we're using (NUMA/PCIe). For PCIe matching, it's a superset of NUMA matching. In other words, 
        # all NUMA requirements must still be met, but also the PCI requirements need to be met. For this
        # reason we can do the NUMA filtering first, and if there are no candidates after that, we're
        # done. The first step is filtering, which makes sure that for each of the three types, we find all possible
        # ways the resources can be placed on a node. After filtering, the intersection step occurs, where for each
        # of the three resource types and their combinations, we take the intersection of those combinations to figure
        # out which are valid for all resources types.
        if top.map_type not in (TopologyMapType.TOPOLOGY_MAP_NUMA, TopologyMapType.TOPOLOGY_MAP_PCI):
            self.logger.error(f'Invalid mapping type: {top.map_type}')
            return (None,)
        else:
            filts = self.FilterNumaTopology(nl, top)
            if filts[1] == None or len(filts[1]) == 0:
                self.logger.info('No candidate nodes found after filter step!')
                return (None,)

            self.logger.info(f'{len(filts[1])} candidate nodes found after filtering. Intersecting resources...')
            isect = self.IntersectResources(nl, filts, top.map_type)

            node = self.SelectNode(filts, top, nl)
            if node == '':
                self.logger.error('NUMA intersection step left no candidate nodes. Cannot schedule pod!')
                return (None,)
                
            midx = self.GetNumaGroupIdx(node, nl[node].numa_nodes, filts)
            return node, midx
        
        
        return None
        
    def FilterPodResources(self, nl: Dict[str, Node], top: CfgTopology) -> Dict[str,Node]:
        """ Filters the native pod resources from each node. Returns a list of all nodes left that have
            enough native resources
        """
        filtnodes = {}

        for k,v in nl.items():
            # Filter hugepages
            if top.hugepages_gb > v.mem.free_hugepages_gb:
                self.logger.info(f'Node {k} only has {v.mem.free_hugepages_gb} free 1GB hugepages free, but pod needs {top.hugepages_gb}. Removing node...')
            else:
                self.logger.info(f'Node {k} has {v.mem.free_hugepages_gb} free 1GB hugepages free, and pod needs {top.hugepages_gb}. Allowing node...')
                filtnodes[k] = v

        return filtnodes

    def FilterNumaTopology(self, nl, top):
        """ Match nodes based on NUMA topology. The only criteria here is that the GPUs, CPUs, and NICs fall
            on the same NUMA node for a given processing group. Each predicate filter step is intentionally
            not combined with others for ease of debugging. """

        cand_nodes = list(nl.keys())
        to_drop = []
        res_cands  = {}

        # First check if there's enough GPUs free. The amount returned from the topology is per group, meaning
        # that if two GPUs are in the same group they must be scheduled on the same NUMA node.
        self.logger.info('Gathering data about GPU requests')
        req_gpus = top.GetTotalGpusRequested()

        gpu_cands = {}
        for n,v in nl.items():
            stmp = set()
            self.logger.info(f'Checking node {n} for enough free GPUs')
            free_gpus = v.GetFreeNumaGPUs()

            # Make a list of all ways the GPUs can be assigned to NUMA nodes
            pr = list(itertools.product(range(v.numa_nodes), repeat=len(req_gpus)))

            # For each NUMA node, add up the total number of GPUs that would be on each node for this combination
            for p in pr: # combination
                ttl = [0] * v.numa_nodes # Hold the totals for this round to see if it passes
                for idx,r in enumerate(p): # numa index
                    ttl[r] += req_gpus[idx]

                # Check to see if this combo would work
                if all([ttl[xidx] <= free_gpus[xidx] for xidx in range(len(ttl))]):
                    self.logger.debug(f'Found a valid mapping of {ttl} with free GPUs {free_gpus}')
                    stmp.add(p)

            # Check if there are enough free GPUs
            if len(stmp) == 0:
                self.logger.info(f'Dropping node {n} from candidate list since not enough free GPUs (req={req_gpus}, free={free_gpus})')
                cand_nodes.remove(n)
            else:
                self.logger.info(f'Node {n} has {len(stmp)} possible GPU combinations to service request of {req_gpus}')

            # stmp contains a tuple of the NUMA node assigned to each processing group. If there's only once processing group, 
            # and both NUMA nodes have capacity, then stmp would have [(0,), (1,)]. If there were two processing groups and
            # two NUMA nodes with resources, it would be: [(0, 0), (0, 1), (1, 0), (1, 1)]
            gpu_cands[n] = list(stmp)
    
        if len(cand_nodes) > 0:
            self.logger.info(f'{len(cand_nodes)} nodes found with sufficient GPU resources')
        else:
            self.logger.info(f'No nodes found with sufficient GPU resources. Pod will remain unscheduled')
            return (None, None)

        res_cands['gpu'] = gpu_cands


        # We now have available and requested CPUs in a format we can use. Finding pairings here is quite a bit more complicated
        # than in the GPU case, since SMT creates many more variants than there would be without it. If the topology request
        # allows SMT, we should prefer that over separate cores since it uses fewer resources and allows better packing for
        # future requests.
        fcpu = {}
        cpu_cands = {}

        self.logger.info('Gathering data about CPU requests')
        req_cpus = top.GetTotalCpusRequested()
        self.logger.info(f'Requested CPUs={req_cpus}')
        
        for n,v in nl.items():
            fcpu[n] = v.GetFreeCpuCores()

        #self.logger.info(f'Free request={freereq}')
        self.logger.info(f'Free node CPUs={fcpu}')
      
        for n,v in nl.items():
            if n not in cand_nodes:
                self.logger.info(f'Not checking node {n} for CPUs since it\'s already excluded')
                continue

            clist = []
            stmp = set()

            # Since we treat the helper and processors cores as a separate entity that we don't necessarily want on the same SMT
            # siblings, we need to add them in separate groups
            for t in req_cpus['proc']:
                if v.SMTEnabled():
                    tot = 0
                    if t[0][1].value: # SMT enabled
                        tot += int(math.ceil(t[0][0]/2.0))
                    else:       # SMT disabled
                        tot += t[0][0]

                    if t[1][1].value: # SMT enabled
                        tot += int(math.ceil(t[1][0]/2.0))
                    else:       # SMT disabled
                        tot += t[1][0]

                    clist.append(tot)
                else:
                    clist.append(t[0][0] + t[1][0])

            # Misc cores
            if v.SMTEnabled():
                tot = int(math.ceil(req_cpus['misc'][0]/2.0)) if req_cpus['misc'][1] else req_cpus['misc'][0]
                clist.append(tot)
            else:
                clist.append(req_cpus['misc'][0])

            pr = list(itertools.product(range(v.numa_nodes), repeat=len(clist)))
            for p in pr: # combination
                ttl = [0] * v.numa_nodes # Hold the totals for this round to see if it passes
                for idx,r in enumerate(p): # numa index
                    ttl[r] += clist[idx]
                
                # Check to see if this combo would work
                if all([ttl[xidx] <= fcpu[n][xidx] for xidx in range(len(ttl))]):
                    self.logger.debug(f'Found a valid mapping of {ttl} with free CPUs {fcpu[n]} where node has SMT={v.SMTEnabled()}')
                    stmp.add(p)

            if len(stmp) == 0:
                self.logger.info(f'Dropping node {n} from candidate list since not enough free CPU cores (req={clist}, free={fcpu[n]})')
                cand_nodes.remove(n)
            else:
                self.logger.info(f'Node {n} has {len(stmp)} possible CPU combinations to service request of {clist}')

            cpu_cands[n] = list(stmp)

        res_cands['cpu'] = cpu_cands
            
        # And finally, the NIC availability. NICs are considered available if the total speeds requested is available across
        # all known interfaces in the system. This also must satisfy NUMA constraints, if any. We need to do matching on both
        # RX and TX since the demand on them may not be symmetric. Note that NICs have an extra level of searching beyond what
        # GPUs and CPUs did, since we can potentially have many NICs across many NUMA nodes with different speeds. This makes
        # the matching constraints not just on NUMA nodes, but available interface speeds as well.
        self.logger.info('Verifying nodes have the NIC resources available')

        req_nics = top.GetTotalNICsRequested()
        nnic_free = {} # Node available resources
        nic_cands = defaultdict(list)
        for n,v in nl.items():
            if n not in cand_nodes:
                self.logger.info(f'Not checking node {n} for NICs since it\'s already excluded')
                continue

            self.logger.info(f'Checking node {n} for NIC resources')
            nnic_free[n] = v.GetFreeNumaNicResources()

            pr = list(itertools.product(range(v.numa_nodes), repeat=len(req_nics)))
            for p in pr: # combination
                nic_combos = []
                for numa in range(v.numa_nodes): # Pick out all candidates from this NUMA node
                    nidx = [i for i,x in enumerate(p) if x == numa]
                    nic_combos.append(list(itertools.product(range(len(nnic_free[n][numa])), repeat=len(nidx)))) # All ways our pod can map to NICs on this node
                
                # Now we make a list of NUMA combinations that we can join together to check for feasible combinations, and we need to combine those to form all possibilities
                idx_combos = list(itertools.product(*[range(len(x)) for x in nic_combos]))

                for numa_combo_idx in idx_combos:
                    c = []
                    nic_ttls = copy.deepcopy(nnic_free[n])
                    for numaidx,numa in enumerate(numa_combo_idx):
                        c.append(list(nic_combos[numaidx][numa]))

                    ttl_list = [c[np].pop(0) for np in p]             

                    # Now go through each combo and assign throughput to the NICs, and see if it's viable based on current capacity
                    for xi,xv in enumerate(ttl_list):
                        nic_ttls[p[xi]][xv][0] -= req_nics[xi][0]
                        nic_ttls[p[xi]][xv][1] -= req_nics[xi][1]

                    # Check if there are any negative values, meaning the placement could not be done. If none of the numbers were
                    # negative after removing all requested capacity, this is a valid candidate mapping
                    if not any([x < 0 for y in nic_ttls for z in y for x in z]):
                        nic_cands[n].append(list(zip(p,ttl_list)))

            if len(nic_cands[n]) == 0:
                self.logger.info(f'Dropping node {n} from candidate list since not enough free NIC resources (req={req_nics}, free={nnic_free[n]})')
                cand_nodes.remove(n)
            else:
                self.logger.info(f'Node {n} has {len(nic_cands[n])} possible resource combinations to service request of {req_nics}')

        res_cands['nic'] = nic_cands
  
        # At this point we're removed any node that doesn't meet one or more of our resource requirements. The next stage is to match
        # the possibilities up with the best node, and the best hardware on that node
        return (res_cands, cand_nodes)   
     

    def IntersectResources(self, nl: List[Node], filts, map_type: TopologyMapType):
        """ At this point we should know all possible CPU, GPU, and NIC combinations that can be made for a given NUMA node.
            For each processing group, we now have to determine if there is a valid match of all three resource types
            on the same NUMA node. 

            For example, even though we may have had 4 GPUs free and 20 CPU cores, it's possible that all 4 GPUs are on NUMA
            node 0, and the 20 CPU cores are on node 1, so it's not a valid match.
        """
        cand_list = filts[1].copy()
        self.logger.info(f'Starting intersection with candidate list: {cand_list}')

        # If we're intersecting PCI switches, remove those candidates from the NIC list first.
        if map_type == TopologyMapType.TOPOLOGY_MAP_PCI:
            self.logger.info('Beginning PCI intersection')
            for n,v in nl.items():
                to_remove = []

                # Skip over our pre-filtered list
                if n not in cand_list:
                    continue

                self.logger.info(f'Intersecting PCIe resources on node {n}')

                # First find all the possible NUMA combinations of NIC + GPU that are on the same switch
                gsw = v.GetFreeGPUPCICount()
                nsw = v.GetNumaNICPCIResources()

                # For each NUMA node, go through and see how many NICs and GPUs are sharing the same switch that we can allocate. We use the NIC indices here
                # since we already have a list of filtered physical indices that are candidates
                for ntup in filts[0]['nic'][n]:
                    nicswcount = defaultdict(lambda: 0)
                    for nicopts in ntup: # Listing of all NIC options within this processing group
                        # Build up a list of all NIC combinations that don't have a matching free NIC on the same PCIe switch
                        nicswcount[nsw[nicopts[0]][nicopts[1]]] += 1 # Add this switch to NICs that need a match
                    
                    # Make sure we have enough GPUs on these switches to 
                    for ni,nv in nicswcount.items():
                        if gsw[ni] < nv: # Check if the number of GPUs on this NIC switch is lower than the NICs on it
                            self.logger.info(f'Total number of GPUs on switch {ni} is less than our NICs on it ({nv}). Removing as option')
                            to_remove.append(ntup)

                # Now we know all processing group combinations that don't meet our PCI switch requirements. Remove them from the NIC groups
                if len(to_remove) == 0:
                    self.logger.info('All NICs have a matching GPU on a PCI switch')
                    continue

                print(filts[0]['nic'][n])
                self.logger.info(f'Removing NIC groups {to_remove} since we don\'t have enough GPUs on those switches')
                for r in to_remove:
                    if r in filts[0]['nic'][n]:
                        del filts[0]['nic'][n][filts[0]['nic'][n].index(r)]
                    else:
                        self.logger.error(f'Found NIC group {r} from intersection, but couldn\'t find it in master list')

        for n in cand_list:
            # For the matching process, we need to go through CPU, GPU, and NIC mappings to find all group mappings that are met for
            # all three types. We use the GPU list as our loop filter, but it doesn't matter.
            to_remove = []

            self.logger.info(f'Beginning NUMA intersection on node {n}')

            gpu_tuples = [x for x in filts[0]['gpu'][n]]
            cpu_tuples = [x[:-1] for x in filts[0]['cpu'][n]] # CPU pairs reserve the last element for miscellaneous cores
            nic_tuples = [list(zip(*x))[0] for x in filts[0]['nic'][n]]
            self.logger.info(f'gpu_tuples={gpu_tuples}, cpu_tuples={cpu_tuples}, nic_tuples={nic_tuples}')

            intersect = list(set(gpu_tuples) & set(cpu_tuples) & set(nic_tuples)) # Intersect all tuples so we're left with valid combos
            self.logger.info(f'Set intersection is {intersect}')

            if len(intersect) == 0:
                self.logger.info(f'Node {n} has no valid intersection candidates. Removing from list')
                try:
                    filts[1].remove(n)
                except ValueError as e:
                    self.logger.error(f"Couldn't find node {n} in candidate list: {e}")

                continue

            # Each type may have to be deleted in a different way, since the format we intersect with is not necessarily the format
            # it's stored in. This is due to carrying over extra meta data, like interface number on NICs      

            # GPU first. This one is simple since it's a direct mapping
            diff = set(filts[0]['gpu'][n]) - set(intersect)
            if len(diff):
                self.logger.info(f'Removing {diff} from GPU sets since they\'re not in the intersection')
                filts[0]['gpu'][n] = intersect

            # CPU is a bit different since we have the extra miscellaneous cores that aren't part of a group
            for c in filts[0]['cpu'][n]:
                if c[:-1] not in intersect:
                    self.logger.info(f'Removing CPU combo {c} from set since it\'s not in intersection')
                    filts[0]['cpu'][n].remove(c)

            
            # NICs have an even more add mapping since it's a zip() of numa node and interface number
            for nicidx,nic in enumerate(filts[0]['nic'][n]):
                tup = list(zip(*nic))[0]
                if tup not in intersect:
                    self.logger.info(f'Removing NIC combo {tup} from set since it\'s not in intersection')
                    del filts[0]['nic'][n][nicidx]
            
            num_combos = [len(filts[0][t][n]) > 0 for t in ('gpu','cpu','nic')]
            if not all(num_combos):
                self.logger.info(f'Not enough resources of at least one type for node {n}. Removing from candidates')
                del filts[1][n]
            else:
                self.logger.info(f'Node {n} has {len(intersect)} valid resource combos after intersection. Leaving as candidate')

        return filts
        
    def SelectNode(self, filts, top: CfgTopology, nl: Dict[str, Node]):
        """ Selects the node that will be used for scheduling. At this point we've already made sure the node is adequate
            to service each type of resource. There are many ways to select which node we're using, but in general, we want
            to try to pack the pod into a node as tightly as possible, meaning with the fewest leftover resources. Since we
            have many types of resources, there can be competing objectives. """

        
        if len(filts[1]) == 0:
            self.logger.error('No candidate nodes found!')
            return ''

        # If the pod isn't asking for any GPU resources, try to prefer a non-GPU node first, and only place on a GPU node as
        # as a last resort.
        needsGpu = False
        for p in top.proc_groups:
            if len(p.group_gpus) > 0:
                needsGpu = True
                break

        if not needsGpu:
            for n in filts[1]:
                if nl[n].GetTotalGPUs() == 0:
                    self.logger.info(f'Found node {n} without GPUs to pair with a CPU-only pod')
                    return n
        
        # If we've gotten here, this is a pod that needs GPUs, or a CPU pod where no CPU-only nodes are available. Just return
        # the first node
        node = filts[1][0]
        return node

    def GetNumaGroupIdx(self, node, numa_nodes, filts):
        """ Find the best group index mapping to maximize later bin packing. For now, the criteria will be to maximize
            the number of GPUs on a particular node. """

        self.logger.info(f'Input to func {filts}')
        def node_delta(x):
            el = [filts[0]['gpu'][node][x].count(y) for y in range(numa_nodes)]
            return max(el) - min(el)

        gidx, gval = (0, node_delta(0))

        for tmpidx in range(1, len(filts[0]['gpu'][node])):
            tmp = node_delta(tmpidx)
            if tmp > gval:
                gidx, gval = tmpidx,tmp

        gtuple = filts[0]['gpu'][node][gidx]

        # CPUs
        cabbr  = [x[:-1] for x in filts[0]['cpu'][node]]
        cidx   = cabbr.index(gtuple)
        ctuple = filts[0]['cpu'][node][cidx]

        # NICs
        nabbr  = [list(zip(*x))[0] for x in filts[0]['nic'][node]]
        nidx   = nabbr.index(gtuple)
        ntuple = filts[0]['nic'][node][nidx]

        self.logger.info(f"Index matching done. GPU={gtuple}, cpu={ctuple}, nic={ntuple}")
        return {'gpu': gtuple, 'cpu': ctuple, 'nic': ntuple}
