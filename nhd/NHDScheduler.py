import logging
import time
import os
import threading
import pkg_resources
from nhd.NHDCommon import NHDCommon
from nhd.NHDCommon import NHDLock
from nhd.K8SMgr import K8SEventType
from colorlog import ColoredFormatter
from nhd.NHDWatchQueue import qinst
from nhd.NHDWatchQueue import NHDWatchTypes
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
IDLE_CNT_THRESH = 60
Q_BLOCK_TIME_SEC = 0.5


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
        self.k8s = K8SMgr.GetInstance()
        self.sched_name = NHD_SCHED_NAME
        self.matcher = Matcher()
        self.pod_state = {}
        self.rpcq = q
        self.failed_schedule_count = 0
        self.nqueue = qinst
        self.lock = NHDLock.GetInstance()

        self.ver = pkg_resources.get_distribution("nhd").version
        self.logger.warning(f'NHD version {self.ver}')


    def InitNHDNodes(self):
        """
        Find all nodes handled by NHD given the node taints
        """        
        nodes = self.k8s.GetNodes()
        for node in nodes:
            active = self.k8s.IsNodeActive(node)
            self.nodes[node] = Node(node, active)

        schedulable = sum([node.active for node in self.nodes.values()])

        self.logger.info(f'{schedulable} nodes are marked as schedulable by NHD out of {len(self.nodes)} total nodes: {self.nodes.keys()}')

    def BuildInitialNodeList(self):
        """
        Builds the initial list of available nodes on startup.
        In the future we need to watch for downed nodes and new nodes added later.
        """
        self.logger.info("Populating list of nodes") 
        self.InitNHDNodes()

        for n, v in self.nodes.items():
            try:
                v.SetNodeAddr(self.k8s.GetNodeAddr(n))

                if not v.ParseLabels(self.k8s.GetNodeLabels(n)):
                    self.logger.error(f'Error while parsing labels for node {n}, deactivating node')
                    v.active = False
                    continue

                (alloc, free) = self.k8s.GetNodeHugepageResources(n) 
                if alloc == 0 or not v.SetHugepages(alloc, free):
                    self.logger.error(f'Error while parsing allocatable resources for node {n}, deactivating node')
                    v.active = False                  

            except Exception as e:
                self.logger.error(f'Caught exception while setting up node {n}:\n    {e}')
                v.active = False

        # JVM: this is only printing a message - maybe should be part of InitNHDNodes() ?
        for k,v in self.nodes.items():
            if v.active:
                self.logger.info(f'Adding node {k} to scheduling list')

        self.logger.info("Done building initial node list")

    def ClaimPodResources(self, podname, ns, uid):
        """ Claims any pod resources from a given pod's configmap. This will remove any physical node resources consumed
            by the pod from being scheduled by other pods. """
        cfgstr = self.k8s.GetCfgAnnotations(podname, ns)
        if cfgstr == False:
            self.logger.error(f'Couldn\'t find pod resources for {ns}.{podname}')
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

            if self.nodes[n].PodPresent(podname, ns):
                self.logger.error(f'Pod {ns}.{podname} already scheduled on node {n}! Cannot add again')
                return 

            # Passed all the tests. Now remove the resources from the cluster
            self.logger.info(f'Taking node resources from {n}')
            if not self.nodes[n].RemoveResourcesFromTopology(top):
                self.logger.error("Failed removing resources")
                return

            # (alloc, free) = self.k8s.GetNodeHugepageResources(n) 
            # if not self.nodes[n].SetHugepages(alloc, free):
            #     self.logger.error(f'Error while parsing allocatable resources for node {n}')

            self.nodes[n].AddScheduledPod(podname, ns, top)
            self.pod_state[(ns, podname)] = {'state': PodStatus.POD_STATUS_SCHEDULED, 'time': time.time(), 'uid': uid}

    def ResetResources(self):
        """ Resets all known resources to those that have already been deployed. Useful when deployed resources
            get out of sync with what's believed to be scheduled. """
        self.logger.info('Resetting all resource knowledge')
        for n in self.nodes.values():
            n.ResetResources()

        # Reset our pod states stats
        self.pod_state.clear()

        self.logger.info('Loading all running configuration files')
        self.LoadDeployedConfigs()
        self.logger.info('Node resources after reset:')
        self.PrintAllNodeResources()
    
    def LoadDeployedConfigs(self):
        """ Loads any configs that were already deployed by this scheduler to be add as a used resource.
            This typically happens when NHD is restarted after pods have been deployed. """
        self.logger.info('Looking for any pods already deployed with used resources')
        pods = self.k8s.GetScheduledPods(self.sched_name)
        self.logger.info(f'Found scheduled pods: {pods}')
        for p in pods:
            if p[3] in ('Running', 'CrashLoopBackOff', 'Pending'):
                self.logger.info(f'Reclaiming resources for pod {p[1]}.{p[0]}')
                self.ClaimPodResources(p[0], p[1], p[2])
            else:
                self.logger.info(f'Pod {p[1]}.{p[0]} is in state: {p[3]}')

    def ReleasePodResources(self, podname, ns):
        """ Releases resources consumed by a pod that's completed or errored """
        self.logger.info(f'Releasing resources for pod {ns}.{podname}')
        cfgstr = self.k8s.GetCfgAnnotations(podname, ns)
        if cfgstr == False:
            self.logger.warning(f'Pod {ns}.{podname} not found. Possibly already removed from a previous event, such as an error or a job completing. Triggering re-scan of all pods...')
            self.ResetResources()
            return

        cfgtype = self.k8s.GetCfgType(podname, ns)
        tcfg = self.GetCfgParser(cfgtype, cfgstr)
        top = tcfg.CfgToTopology(True)

        if top is not None: # Start removing pod's resources from node
            node = self.k8s.GetPodNode(podname, ns)
            if not node:
                self.logger.error('Pulled pod\'s config, but it wasn\'t assigned a node!')
                return

            if node not in self.nodes:
                self.logger.error(f'Pod is mapping to node {node} but that node isn\'t in the current node list. Skipping')
                return

            if not self.nodes[node].PodPresent(podname, ns):
                self.logger.error(f'Pod {ns}.{podname} is not scheduled on node {node}! Cannot remove')
                return

            # Passed all the tests. Now remove the resources from the cluster
            self.logger.info(f'Freeing node resources from {node}')
            self.nodes[node].AddResourcesFromTopology(top)
            self.nodes[node].RemoveScheduledPod(podname, ns)
            self.nodes[node].SetBusy()

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
    
    # JVM: why is this always returning the same config parser? is there a missing config parser?
    def GetCfgParser(self, cfgtype: str, cfgstr: str):
        if cfgtype == 'triad':
            return TriadCfgParser(cfgstr, False)

        # default
        return TriadCfgParser(cfgstr, False)

    def InitialNodeFilter(self, podname: str, ns: str):
        """ Each pod requests a node group to be in, or "default" if none is seen. Only schedule pods on nodes matching their node group.
            This allows users to segment some nodes for different purposes without affecting the main cluster. """
        ngroups = self.k8s.GetPodNodeGroups(podname, ns)
        nl = {}
        for n,v in self.nodes.items():
            if len(set(v.groups) & set(ngroups)) > 0:
                if v.active:
                    nl[n] = v
                else:
                    self.logger.info(f'Node {n} is in the group, but not active. Skipping')

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

        _, cfgstr = self.k8s.GetCfgMap(podname, ns)
        cfgtype = self.k8s.GetCfgType(podname, ns)
        tcfg = self.GetCfgParser(cfgtype, cfgstr)

        top = tcfg.CfgToTopology(False)
        if top is None:
            self.k8s.GeneratePodEvent(pobj, podname, ns, 'FailedCfgParse', K8SEventType.EVENT_TYPE_WARNING, \
                    f'Error while processing config for pod {podname}')
            return False

        # Filter out nodes not belonging to the node group requested by this pod
        filt_nodes = self.InitialNodeFilter(podname, ns)
        self.logger.info(f"Nodes matching first filter: {filt_nodes}")
        
        match = self.matcher.FindNode(filt_nodes, top)
        nodename = match[0]

        if nodename == None:
            self.k8s.GeneratePodEvent(pobj, podname, ns, 'FailedScheduling', K8SEventType.EVENT_TYPE_WARNING, \
                    f'No valid candidate nodes found for scheduling pod {podname}')
            self.failed_schedule_count += 1
            return False

        self.k8s.GeneratePodEvent(pobj, podname, ns, 'Scheduling', K8SEventType.EVENT_TYPE_NORMAL, \
                f'Node {nodename} selected for scheduling')

        self.nodes[nodename].SetBusy()

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
            self.logger.info(f"Getting info for node {k}")
            nodeinfo = {
                'name': k,
                'freegpu': v.GetFreeGpuCount(),
                'totalgpu': v.GetTotalGPUs(),
                'freecpu': v.GetFreeCpuCoreCount(),
                'totalcpu': v.GetTotalCPUs(),
                'freehuge_gb': v.GetFreeHugepages(),
                'totalhuge_gb': v.GetTotalHugepages(),
                'totalpods': v.GetTotalPods(),
                'active': v.GetNodeActive(),
                'nicstats': v.GetNICUsedSpeeds()
            }

            nodes.append(nodeinfo)

        self.logger.info("Done getting node info")
        return nodes

    def GetPodStats(self):
        """
        Returns statistics about all pods running
        """
        pinfo = []
        for k,v in self.nodes.items():
            self.logger.info(f'Processing node {k} with {len(v.pod_info)} pods')
            for pname,pval in v.pod_info.items():
                annots = self.k8s.GetPodAnnotations(pname[1], pname[0])
                if annots == None:
                    self.logger.error(f'Couldn\'t get pod annotations for pod {pname[1]}.{pname[0]}')
                    continue

                tmp = {
                     'namespace':   pname[1],
                     'podname':     pname[0],
                     'node':        k,
                     'annotations': annots,
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
            q.put(rsp)
        elif msgid == RpcMsgType.TYPE_POD_INFO:
            rsp = self.GetPodStats()
            q.put(rsp)

    def AddNodeToNodelist(self,node):
        """
        Adds a new Node object to the node list. 
        Node is initially in non-active state (disabled for scheduling)
        """        
        self.logger.info(f"Adding new node: {node}") 
        self.nodes[node] = Node(node, active=False)

    def ScanAndInitNode(self, node):
        """
        Performs a scan of the new node and updates info in the node object.
        Scan is similar to BuildInitialNodeList function but for a single node.
        Returns False if any of the scans fails.
        """

        try:
            self.nodes[node].SetNodeAddr(self.k8s.GetNodeAddr(node))

            if not self.nodes[node].ParseLabels(self.k8s.GetNodeLabels(node)):
                self.logger.error(f'Error while parsing labels for node {node}, deactivating node')
                return False

            (alloc, free) = self.k8s.GetNodeHugepageResources(node) 
            if alloc == 0 or not self.nodes[node].SetHugepages(alloc, free):
                self.logger.error(f'Error while parsing allocatable resources for node {node}, deactivating node')
                return False

        except Exception as e:
            self.logger.error(f'Caught exception while setting up node {node}:\n {e}')
            return False

        # Return True if all initialization went successful
        return True

    def CheckPendingPods(self):
        podlist = self.k8s.ServicePods(self.sched_name)        
        for k, p in podlist.items():
            podkey = (k[0], k[1])
            if p[0] == 'Pending' and p[1] == None and ((podkey not in self.pod_state) or self.pod_state[podkey]['state'] != PodStatus.POD_STATUS_SCHEDULED):  
                self.logger.info(f'Found new pending pod {k[0]}.{k[1]} [{k[2]}]')
                # Normal pod that needs to be scheduled
                if self.AttemptScheduling(k[1], k[0]):
                    self.pod_state[podkey] = {'state': PodStatus.POD_STATUS_SCHEDULED, 'time': time.time(), 'uid': k[2]}
                else:
                    self.logger.error(f'Failed scheduling pod {k[0]}.{k[1]} [{k[2]}]')
                    self.pod_state[podkey] = {'state': PodStatus.POD_STATUS_FAILED, 'time': time.time(), 'uid': '0'}

            elif (p[0] == 'Failed') and (podkey in self.pod_state) and (self.pod_state[podkey] == PodStatus.POD_STATUS_SCHEDULED):
                self.logger.info(f'Pod {k[0]}.{k[1]} [{k[2]}] failed to schedule. Removing consumed resources')
                self.ReleasePodResources(k[1], k[0])
                self.pod_state[podkey] = {'state': PodStatus.POD_STATUS_FAILED, 'time': time.time(), 'uid': '0'}

    def run(self):
        """ 
        Main entry point for NHD. Initialization pulls node information and
        sets up all data structures needed for scheduling a pod.
        """
        self.BuildInitialNodeList()
        self.LoadDeployedConfigs()
        self.PrintAllNodeResources()

        # Do one pass where we look for pods that are pending while NHD wasn't running, and attempt scheduling on this
        self.logger.info('Scheduling any pods stuck in pending since we last ran')
        self.CheckPendingPods()   

        # Flush queue from any events that may have come in while we were servicing the existing pods on startup
        self.logger.info('Flushing controller queue')
        cnt = 0
        try:
            while True:
                item = self.nqueue.get(block = False, timeout = 0)
                cnt += 1
        except Empty as e:
            self.logger.info(f'Queue flushed with {cnt} messages in it - {e}')
        
        self.logger.warning("Starting main scheduler loop")

        idle_cnt = 0

        while True:
            # Start watching for pods that want to be scheduled or are waiting to be freed
            try:
                item = self.nqueue.get(block = False)
                self.logger.info(f"Got new pod notification: {item['type']}")
            except Empty as e:
                try:
                    item = self.rpcq.get(True, Q_BLOCK_TIME_SEC)
                    self.ParseRPCReq(item[0], item[1])
                    
                except Empty as e:
                    self.logger.debug('Empty gRPC queue')    

                    # If we've been completely idle for a while, check for any pending pods to be scheduled
                    idle_cnt += 1
                    if idle_cnt >= IDLE_CNT_THRESH:
                        idle_cnt = 0
                        self.CheckPendingPods()

                continue

            # Pod creation/deletion events
            if item["type"] in (NHDWatchTypes.NHD_WATCH_TYPE_TRIAD_POD_DELETE,
                                NHDWatchTypes.NHD_WATCH_TYPE_TRIAD_POD_CREATE):
                ns = item["pod"]["ns"]
                pn = item["pod"]["name"]
                uid = item["pod"]["uid"]

                if item["type"] == NHDWatchTypes.NHD_WATCH_TYPE_TRIAD_POD_DELETE:
                    self.logger.info(f'Pod {ns}.{pn} [{uid}] deleted. Freeing resources')
                    self.ReleasePodResources(pn, ns)
                    try:
                        del self.pod_state[(ns, pn)]
                    except KeyError as e:
                        self.logger.error(f'Failed to find pod {ns}.{pn} [{uid}] in podstats!')


                elif item["type"] == NHDWatchTypes.NHD_WATCH_TYPE_TRIAD_POD_CREATE:
                    # Note that the controller may have been generating events asynchronously about pods we already know about.
                    # Check if it's in our list, and do nothing if it is.
                    if ((ns, pn) in self.pod_state) and (self.pod_state[(ns, pn)]['state'] == PodStatus.POD_STATUS_SCHEDULED):
                        if self.pod_state[(ns, pn)]['uid'] == uid:
                            self.logger.info(f'Pod {ns}.{pn} [{uid}] appears to be scheduled already. Ignoring controller message')
                            continue
                        else:
                            self.logger.info(f'Pod {ns}.{pn} [{self.pod_state[(ns, pn)]["uid"]}] errantly reported as scheduled. Releasing resources and re-syncing internal record')
                            self.ReleasePodResources(pn, ns)
                            try:
                                del self.pod_state[(ns, pn)]
                            except KeyError:
                                self.logger.error(f'Failed to find pod {ns}.{pn} [{uid}] in podstats!')

                    # Schedule any new pods
                    self.logger.info(f'Found new pending pod {ns}.{pn} [{uid}]')
                    # Normal pod that needs to be scheduled
                    if self.AttemptScheduling(pn, ns):
                        self.pod_state[(ns, pn)] = {'state': PodStatus.POD_STATUS_SCHEDULED, 'time': time.time(), 'uid': uid}
                    else:
                        self.logger.error(f'Failed scheduling pod {ns}.{pn} [{uid}]')
                        self.pod_state[(ns, pn)] = {'state': PodStatus.POD_STATUS_FAILED, 'time': 0, 'uid': '0'}

                self.logger.debug(f'Done processing {ns}.{pn} [{uid}]. {len(self.pod_state)} pods in cache')
            # Node scheduleable/unscheduleable
            elif item["type"] in (NHDWatchTypes.NHD_WATCH_TYPE_NODE_CORDON,
                                  NHDWatchTypes.NHD_WATCH_TYPE_NODE_UNCORDON):
                self.logger.info(f'{"Cordoning" if item["type"] == NHDWatchTypes.NHD_WATCH_TYPE_NODE_CORDON else "Trying to uncordon"} node {item["node"]}')
                for n, v in self.nodes.items():
                    if n == item["node"]:
                        if item["type"] == NHDWatchTypes.NHD_WATCH_TYPE_NODE_CORDON:
                            if v.active:
                                self.logger.info(f'Removing node {item["node"]} from scheduling')
                                v.active = False
                            else:
                                self.logger.info(f'Node {item["node"]} already deactivated from scheduling')
                        elif item["type"] == NHDWatchTypes.NHD_WATCH_TYPE_NODE_UNCORDON:
                            if v.active:
                                self.logger.info(f'Node {item["node"]} already activated for scheduling')
                            else:
                                active = (self.k8s.IsNodeActive(v.name) and self.ScanAndInitNode(v.name))
                                if active:
                                    self.logger.info(f'Adding node {item["node"]} to schedulable list')
                                    v.active = active
                                else:
                                    self.logger.info(f'Node {item["node"]} failed to pass NHD Scheduler readiness checks. Deactivating')
                        break

            elif item["type"] == NHDWatchTypes.NHD_WATCH_TYPE_NODE_MAINT_START:
                for n, v in self.nodes.items():
                    if n == item["node"]:
                        if not v.maintenance:
                            self.logger.info(f'Node {item["node"]} entering maintenance')
                            v.maintenance = True
                        break
            elif item["type"] == NHDWatchTypes.NHD_WATCH_TYPE_NODE_MAINT_END:
                for n, v in self.nodes.items():
                    if n == item["node"]:
                        if v.maintenance:
                            self.logger.info(f'Node {item["node"]} leaving maintenance')
                            v.maintenance = False
                        break
            elif item["type"] == NHDWatchTypes.NHD_WATCH_TYPE_GROUP_UPDATE:
                self.logger.info(f'Updating NHD group to {item["groups"]} for node {item["node"]}')
                for n, v in self.nodes.items():
                    if n == item["node"]:
                        v.SetGroups(item["groups"])

           # Node addition event
            elif item["type"] == NHDWatchTypes.NHD_WATCH_TYPE_TRIAD_NODE_CREATE:
                self.logger.info(f'Trying to add a new node to the cluster')
                self.AddNodeToNodelist(item["node"])

            # Node deletion event
            elif item["type"] == NHDWatchTypes.NHD_WATCH_TYPE_TRIAD_NODE_DELETE:
                for n, v in self.nodes.items():
                    if n == item["node"]:
                        self.logger.info(f'Deleting Node {n} from NHD node list')
                        try:
                            del self.nodes[n]
                        except KeyError as e:
                            self.logger.error(f'Failed to delete node {n} from NHD node list!')
                        break