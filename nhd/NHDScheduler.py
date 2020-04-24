import logging
import time
import os
import threading
import pkg_resources
from nhd.NHDCommon import NHDCommon
from nhd.K8SMgr import K8SEventType
from colorlog import ColoredFormatter
from nhd.Node import Node
from nhd.K8SMgr import K8SMgr
from nhd.Matcher import Matcher
from enum import Enum
from typing import Dict
from nhd.TriadCfgParser import TriadCfgParser
from queue import Queue
from queue import Empty
from nhd.NHDCommon import RpcMsgType
from collections import defaultdict

NHD_SCHED_NAME = "nhd-scheduler"

# Scheduler status for each pod 
class PodStatus(Enum):
    POD_STATUS_SCHEDULED = 0
    POD_STATUS_FAILED = 1
    POD_STATUS_SUCCEEDED = 2
    POD_STATUS_RUNNING = 3
    POD_STATUS_COMPLETED = 4

""" Main scheduler thread. The basic actions are:

    1) Read active node list from Kubernetes that pass our taint
    2) Read active pods that are either scheduled or pending from an attempted schedule
    3) Remove resources from nodes for any active pods
    4) Sit in reconciliation loop for pods waiting to be scheduled.
"""    
class NHDScheduler(threading.Thread):
    def __init__(self, q: Queue):
        threading.Thread.__init__(self)
        self.logger = NHDCommon.GetLogger(__name__)
        self.nodes = {}
        self.node_groups = defaultdict(list)
        self.k8s = K8SMgr.GetInstance()
        self.sched_name = NHD_SCHED_NAME
        self.whitelist = []
        self.matcher = Matcher()
        self.pod_state = {}
        self.mainq = q
        self.failed_schedule_count = 0

        self.ver = pkg_resources.get_distribution("nhd").version
        self.logger.warning(f'NHD version {self.ver}')

    def AddNodeWhiteList(self, nl):
        """
        Creates a white list of scheduling nodes. This is used for debug purposes when we don't want to 
        scan all the taints to find ones we're using.
        """
        self.whitelist = nl

    def InitNHDNodes(self):
        """
        Find all nodes handled by NHD given the node taints
        """        
        if len(self.whitelist) > 0:
            self.logger.info('Node whitelist is not empty, using nodes from that list as schedulable nodes')
            for n in self.whitelist:
                self.nodes[n] = Node(n)
        else:
            nodes = self.k8s.GetNodes()
            for n in nodes:
                if self.k8s.IsNHDTainted(n):
                    self.nodes[n] = Node(n)

        self.logger.info(f'{len(self.nodes)} nodes are marked as schedulable by NHD: {self.nodes.keys()}')

    def BuildInitialNodeList(self):
        """ Builds the initial list of available nodes on startup. In the future we need to watch for downed nodes and
            new nodes added later. """
        self.logger.info("Populating list of nodes") 
        self.InitNHDNodes()

        todel = []
        for n,v in self.nodes.items():
            try:
                v.SetNodeAddr(self.k8s.GetNodeAddr(n))

                if not v.ParseLabels(self.k8s.GetNodeLabels(n)):
                    self.logger.error(f'Error while parsing labels for node {n}, removing from list')
                    todel.append(n)

                (alloc, free) = self.k8s.GetNodeHugepageResources(n) 
                if alloc == 0 or not v.SetHugepages(alloc, free):
                    self.logger.error(f'Error while parsing allocatable resources for node {n}, removing from list')
                    todel.append(n)                    

            except Exception as e:
                print('Caught exception while setting up node {n}:', e)
                todel.append(n)

        # Delete all nodes that had issues when parsing
        if len(todel):
            self.logger.info('Removing bad nodes from list')
            for n in todel:
                del self.nodes[n]

        for k,v in self.nodes.items():
            self.logger.info(f'Adding node {k} to scheduling list')

        self.logger.info("Done building initial node list")

    def ClaimPodResources(self, podname, ns):
        """ Claims any pod resources from a given pod's configmap. This will remove any physical node resources consumed
            by the pod from being scheduled by other pods. """
        cmname, cfgstr = self.k8s.GetCfgMap(podname, ns)
        cfgtype = self.k8s.GetCfgType(podname, ns)
        tcfg = self.GetCfgParser(cfgtype, cfgstr)

        top = tcfg.CfgToTopology(True)
        if top is not None: # Start removing pod's resources from node
            n = self.k8s.GetPodNode(podname, ns)
            if not n:
                self.logger.error('Pulled pod\'s config, but it wasn\'t assigned a node!')
                return

            if n not in self.nodes:
                self.logger.error(f'Pod is mapping to node {n} but that node isn\'t in the current node list. Skipping')
                return

            if self.nodes[n].PodPresent(podname, ns):
                self.logger.error(f'Pod {ns}.{podname} already scheduled on node {n}! Cannot add again')
                return 

            # Passed all the tests. Now remove the resources from the cluster
            self.logger.info(f'Taking node resources from {n}')
            self.nodes[n].RemoveResourcesFromTopology(top)

            # (alloc, free) = self.k8s.GetNodeHugepageResources(n) 
            # if not self.nodes[n].SetHugepages(alloc, free):
            #     self.logger.error(f'Error while parsing allocatable resources for node {n}')

            self.nodes[n].AddScheduledPod(podname, ns, top)

    def ResetResources(self):
        """ Resets all known resources to those that have already been deployed. Useful when deployed resources
            get out of sync with what's believed to be scheduled. """
        self.logger.info('Resetting all resource knowledge')
        for n in self.nodes.values():
            n.ResetResources()

        self.logger.info('Loading all running configuration files')
        self.LoadDeployedConfigs()
        self.logger.info('Node resources after reset:')
        self.PrintAllNodeResources()
    
    def LoadDeployedConfigs(self):
        """ Loads any configs that were already deployed by this scheduler to be add as a used resource. This typically
            happens when NHD is restarted after pods have been deployed. """
        self.logger.info('Looking for any pods already deployed with used resources')
        pods = self.k8s.GetScheduledPods(self.sched_name)
        self.logger.info(f'Found scheduled pods: {pods}')
        for p in pods:
            if p[2] in ('Running', 'CrashLoopBackOff', 'Pending'):
                self.logger.info(f'Reclaiming resources for pod {p[1]}.{p[0]}')
                self.ClaimPodResources(p[0], p[1])


    def ReleasePodResources(self, podname, ns):
        """ Releases resources consumed by a pod that's completed or errored """
        self.logger.info(f'Releasing resources for pod {ns}.{podname}')
        cmname, cfgstr = self.k8s.GetCfgMap(podname, ns)
        if cmname == None:
            self.logger.warning(f'Pod {ns}.{podname} not found. Possibly already removed from a previous event, such as an error or a job completing. Triggering re-scan of all pods...')
            self.ResetResources()
            return

        cfgtype = self.k8s.GetCfgType(podname, ns)
        tcfg = self.GetCfgParser(cfgtype, cfgstr)        
        top = tcfg.CfgToTopology(True)

        if top is not None: # Start removing pod's resources from node
            n = self.k8s.GetPodNode(podname, ns)
            if not n:
                self.logger.error('Pulled pod\'s config, but it wasn\'t assigned a node!')
                return

            if n not in self.nodes:
                self.logger.error(f'Pod is mapping to node {n} but that node isn\'t in the current node list. Skipping')
                return

            if not self.nodes[n].PodPresent(podname, ns):
                self.logger.error(f'Pod {ns}.{podname} is not scheduled on node {n}! Cannot remove')
                return 

            # Passed all the tests. Now remove the resources from the cluster
            self.logger.info(f'Freeing node resources from {n}')
            self.nodes[n].AddResourcesFromTopology(top)
            self.nodes[n].RemoveScheduledPod(podname, ns)


    def PrintAllNodeResources(self):
        """
        Prints all node resource utilization (CPUs, GPUs, NICs)
        """
        for _,n in self.nodes.items():
            n.PrintResourceStats()

    def ParsePodResources(self, pod: str, ns: str) -> Dict[str, int]:
        """
        Parse the native pod resources of a node
        """
        res = self.k8s.GetRequestedPodResources(pod, ns)

        # We only care about a subset of the resources for a pod to schedule on
        trimmed = {}
        if 'hugepages-1Gi' in res:
            trimmed['hugepages-1Gi'] = int(res['hugepages-1Gi'][:res['hugepages-1Gi'].find('G')])

        return trimmed
    
    def GetCfgParser(self, cfgtype: str, cfgstr: str):
        if cfgtype == 'triad':
            return TriadCfgParser(cfgstr, False)

        # default
        return TriadCfgParser(cfgstr, False)

    def FilterNodeGroups(self, podname: str, ns: str):
        """ Each pod requests a node group to be in, or "default" if none is seen. Only schedule pods on nodes matching their node group.
            This allows users to segment some nodes for different purposes without affecting the main cluster. """
        ngroup = self.k8s.GetPodNodeGroup(podname, ns)
        nl = {}
        for n,v in self.nodes.items():
            if v.group == ngroup:
                nl[n] = v

        return nl

    def AttemptScheduling(self, podname, ns) -> bool:
        """
        Attempt to schedule a pod to a node. This is the main entry point for the scheduler, and is kicked off
        whenever a new pod appears in the cluster that wants to use this scheduler. 
        """
        # Get pod data from Kubernetes to populate event fields properly
        pobj = self.k8s.GetPodObj(podname, ns)
        if (pobj == None):
            self.logger.error(f"Could not get pod object for pod {ns}.{podname}")
            return False

        self.k8s.GeneratePodEvent(pobj, podname, ns, 'StartedScheduling', K8SEventType.EVENT_TYPE_NORMAL, \
                f'Started scheduling {ns}/{podname}')

        cmname, cfgstr = self.k8s.GetCfgMap(podname, ns)  
        cfgtype = self.k8s.GetCfgType(podname, ns)
        tcfg = self.GetCfgParser(cfgtype, cfgstr)

        top = tcfg.CfgToTopology(False)
        if top is None:
            self.k8s.GeneratePodEvent(pobj, podname, ns, 'FailedCfgParse', K8SEventType.EVENT_TYPE_WARNING, \
                    f'Error while processing config for pod {podname}')
            return False

        # # Some of the resource requirements are posted as part of a pod's spec and not the application config. 
        # # Pull those into the topology config here
        # pod_res = self.ParsePodResources(podname, ns)
        # top.AddPodReservations(pod_res)

        # Filter out nodes not belonging to the node group requested by this pod
        filt_nodes = self.FilterNodeGroups(podname, ns)
        self.logger.info(f"Nodes matching group filter: {filt_nodes}")
        
        match = self.matcher.FindNode(filt_nodes, top)
        nodename = match[0]

        if nodename == None:
            self.k8s.GeneratePodEvent(pobj, podname, ns, 'FailedScheduling', K8SEventType.EVENT_TYPE_WARNING, \
                    f'No valid candidate nodes found for scheduling pod {podname}')
            self.failed_schedule_count += 1
            return False

        self.k8s.GeneratePodEvent(pobj, podname, ns, 'Scheduling', K8SEventType.EVENT_TYPE_NORMAL, \
                f'Node {nodename} selected for scheduling')

        try:
           nic_list = self.nodes[nodename].SetPhysicalIdsFromMapping(match[1], top)
           if nic_list == None:
               self.logger.error('Failed to map physical resources from topology config!')
               return False
        except IndexError:
            # If we end up here, no resources have been mapped and we should not try to finish assigning the pod
            self.logger.error('Failed to map physical resources from topology config!')
            return False

        # Set the NetworkAttachmentDefinition
        nidx = list({x[0] for x in nic_list})

        self.nodes[nodename].ClaimPodNICResources(nidx)

        nadlist = self.nodes[nodename].GetNADListFromIndices(nidx)
        # Host-device plugin we want to stick with the same name of the if
        csnad = ','.join([f'{x}@{x}' for x in nadlist])

        if not self.k8s.AddNADToPod(podname, ns, csnad):
            self.logger.error('Failed to set NetworkAttachmentDefinition')
            self.ReleasePodResources(podname, ns)
            return False

        # Finally, we map the filled-in topology config back into the appropriate format for the pod
        topstr = tcfg.TopologyToCfg()

        # Annotate pod with new configuration
        if not self.k8s.AnnotatePodConfig(ns, podname, topstr):
            self.k8s.GeneratePodEvent(pobj, podname, ns, 'PodCfgFailed', K8SEventType.EVENT_TYPE_WARNING, \
                    f'Failed to annotate pod\'s configuration')
            self.ReleasePodResources(podname, ns)
            return False
        else:
            self.k8s.GeneratePodEvent(pobj, podname, ns, 'PodCfgSuccess', K8SEventType.EVENT_TYPE_NORMAL, \
                    f'Successfully added pod\'s configuration to annotations')

        # Now patch the pod's configmap with the new one. This is a legacy feature and will be removed once all pods use annotations
        if not self.k8s.PatchConfigMap(ns, cmname, topstr):
            self.k8s.GeneratePodEvent(pobj, podname, ns, 'CfgMapFailed', K8SEventType.EVENT_TYPE_WARNING, \
                    f'Failed to patch ConfigMap. Unwinding changes')
            self.ReleasePodResources(podname, ns)
            return False
        else:
            self.k8s.GeneratePodEvent(pobj, podname, ns, 'CfgMapSuccess', K8SEventType.EVENT_TYPE_NORMAL, \
                    f'Successfully patched ConfigMap contents. Binding pod to node')

        if not self.k8s.BindPodToNode(podname, nodename, ns):
            self.logger.info('Failed to bind pod to node. Unwinding...')
            self.k8s.GeneratePodEvent(pobj, podname, ns, 'FailedScheduling', K8SEventType.EVENT_TYPE_WARNING, \
                    f'Failed to schedule {ns}/{podname} to {nodename}')
            self.ReleasePodResources(podname, ns)
            return False
        else:
            self.logger.warning(f'Successfully bound pod {podname} to node {nodename}!')
            self.k8s.GeneratePodEvent(pobj, podname, ns, 'Scheduled', K8SEventType.EVENT_TYPE_NORMAL, \
                    f'Successfully assigned {ns}/{podname} to {nodename}')

        self.nodes[nodename].AddScheduledPod(podname, ns, top)

        return True

    def GetBasicNodeStats(self):
        """
        Return high-level node stats for use by gRPC requests
        """
        nodes = []
        for k,v in self.nodes.items():
            nodeinfo = {
                'name': k,
                'freegpu': v.GetFreeGpuCount(),
                'totalgpu': v.GetTotalGPUs(),
                'freecpu': v.GetFreeCpuCoreCount(),
                'totalcpu': v.GetTotalCPUs(),
                'freehuge_gb': v.GetFreeHugepages(),
                'totalhuge_gb': v.GetTotalHugepages(),
                'totalpods': v.GetTotalPods(),
                'nicstats': v.GetNICUsedSpeeds()
            }

            nodes.append(nodeinfo)

        return nodes

    def GetPodStats(self):
        """
        Returns statistics about all pods running
        """
        pinfo = []
        for k,v in self.nodes.items():
            self.logger.info(f'Processing node {k} with {len(v.pod_info)} pods')
            for pname,pval in v.pod_info.items():
                tmp = {
                     'namespace':   pname[1],
                     'podname':     pname[0],
                     'node':        k,
                     'annotations': self.k8s.GetPodAnnotations(pname[1], pname[0]),
                     'hugepages':   pval.hugepages_gb,
                     'proc_cores':  [pc.core for pg in pval.proc_groups for pc in pg.proc_cores],
                     'proc_helper_cores':  [pc.core for pg in pval.proc_groups for pc in pg.misc_cores],
                     'misc_cores':  [pc.core for pc in pval.misc_cores],
                     'gpus':        [g.device_id for pg in pval.proc_groups for g in pg.group_gpus],
                     'nics':        [np.mac for np in pval.nic_core_pairing],
                }
                pinfo.append(tmp)
        return pinfo

    def ParseRPCReq(self, msgid: RpcMsgType, q: Queue):
        """
        Parse an incoming gRPC message request by fetching the data from our Node objects, and
        putting the results into a queue for the RPC thread.
        """
        self.logger.info(f'Got RPC msg id: {msgid}')

        if msgid == RpcMsgType.TYPE_NODE_INFO:
            rsp = self.GetBasicNodeStats()
            q.put(rsp)
        elif msgid == RpcMsgType.TYPE_SCHEDULER_INFO:
            rsp = self.failed_schedule_count
            self.failed_schedule_count = 0
            q.put(rsp)
        elif msgid == RpcMsgType.TYPE_POD_INFO:
            rsp = self.GetPodStats()
            q.put(rsp)            

    def run(self):
        """ 
        Main entry point for NHD. Initialization pulls node information, and sets up all data structures needed
        for scheduling a pod.
        """
        self.BuildInitialNodeList()
        self.LoadDeployedConfigs()
        self.PrintAllNodeResources()

        self.logger.warning("Starting main scheduler loop")

        while True:
            # Start watching for pods that want to be scheduled or are waiting to be freed
            pods = self.k8s.ServicePods(self.sched_name)

            # Check if we need to delete any pods now
            todel = []
            for k,p in self.pod_state.items():
                if k not in pods:
                    todel.append(k)

            for v in todel:
                self.logger.info(f'Pod {v[0]}.{v[1]}[{v[2]}] no longer in cluster. Freeing resources')
                self.ReleasePodResources(v[1],v[0])
                del self.pod_state[v]

            # Schedule any new pods
            for k,p in pods.items():
                if p[0] == 'Pending' and p[1] == None and (k not in self.pod_state):
                    self.logger.info(f'Found new pending pod {k[0]}.{k[1]}[{k[2]}]')
                    # Normal pod that needs to be scheduled
                    if not self.AttemptScheduling(k[1],k[0]):
                        self.logger.error(f'Failed scheduling pod {k[0]}.{k[1]}[{k[2]}]')
                        self.pod_state[k] = PodStatus.POD_STATUS_FAILED
                    else:
                        self.pod_state[k] = PodStatus.POD_STATUS_SCHEDULED

                elif (p[0] == 'Failed') and (k in self.pod_state) and (self.pod_state[k] == PodStatus.POD_STATUS_SCHEDULED):
                    self.logger.info(f'Pod {k[0]}.{k[1]}[{k[2]}] failed to schedule. Removing consumed resources')
                    self.ReleasePodResources(k[1],k[0])
                    self.pod_state[k] = PodStatus.POD_STATUS_FAILED

            self.logger.debug(f'Done processing {len(pods)} pods. {len(self.pod_state)} pods in cache')

            # Check RPC before sleeping
            try:
                item = self.mainq.get(True, 5)

                self.ParseRPCReq(item[0], item[1])
                
            except Empty as e:
                self.logger.debug('Empty gRPC queue')     
                continue           
            

                





