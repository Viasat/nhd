import logging
import os
from nhd.NHDCommon import NHDCommon
from colorlog import ColoredFormatter
from nhd.CfgTopology import SMTSetting
from nhd.CfgTopology import GpuType
from nhd.CfgTopology import NICCoreDirection
from nhd.CfgTopology import TopologyMapType
from nhd.CfgTopology import CfgTopology
from pprint import pprint
from typing import Dict, List, Tuple
from itertools import chain
from collections import defaultdict

NIC_BW_AVAIL_PERCENT                = 0.9 # Only allow NICs to be scheduled up to this much of their total capacity
SCHEDULABLE_NIC_SPEED_THRESH_MBPS   = 11000 # Don't include NICs for scheduling that are below this speed
ENABLE_SHARING                      = False # Allow pods to share a NIC

"""
Properties of a core inside of a node
"""
class NodeCore:
    def __init__(self, core, socket, sib):
        self.core: int = core
        self.sibling: int = sib
        self.socket: int = socket
        self.used: bool = False

    def SetSibling(self, sib):
        self.sibling = sib

"""
Properties of a NIC inside of a node
"""
class NodeNic:
    def __init__(self, ifname: str, mac: str, vendor: str, speed: int, numa_node: int, pciesw: int, card: int, port: int):
        self.ifname = ifname
        self.vendor = vendor
        self.speed = speed
        self.numa_node = numa_node
        self.speed_used = [0,0] # (rx, tx) set by scheduler
        self.pods_used = 0
        self.pciesw = pciesw
        self.card = card
        self.port = port

        # The MAC is in a weird format from NFD, so fix it here
        self.mac    = self.FormatMac(mac)

    def SetNodeIndex(self, idx):
        self.idx = idx

    def FormatMac(self, mac):
        return ':'.join(a+b for a,b in zip(mac[::2],mac[1::2])).upper()


"""
Properties of memory inside of a node
"""
class NodeMemory:
    def __init__(self):
        self.ttl_hugepages_gb = 0
        self.free_hugepages_gb = 0
        self.ttl_mem_gb = 0
        self.free_mem_gb = 0 


"""
Properties of a GPU inside of a node
"""
class NodeGpu:
    def __init__(self, gtype: str, device_id: int, numa_node: int, pciesw: int):
        self.gtype = self.GetType(gtype)
        self.device_id = device_id
        self.numa_node = numa_node
        self.used = False
        self.pciesw = pciesw

    def GetType(self, gtype: str):
        if '1080Ti' in gtype:
            return GpuType.GPU_TYPE_GTX_1080TI
        if '1080' in gtype:
            return GpuType.GPU_TYPE_GTX_1080
        if '2080Ti' in gtype:
            return GpuType.GPU_TYPE_GTX_2080TI
        if '2080' in gtype:
            return GpuType.GPU_TYPE_GTX_2080
        if 'V100' in gtype:
            return GpuType.GPU_TYPE_V100

        return GpuType.GPU_TYPE_NOT_SUPPORTED

"""
The Node class holds properties about a node's resources, as well as which resources have been used.
Current resource types in a node are CPUs, GPU, and NICs.
"""
class Node:

    NHD_MAINT_LABEL = 'sigproc.viasat.io/maintenance'

    def __init__(self, name, active = True):
        self.logger = NHDCommon.GetLogger(__name__)

        self.name = name
        self.active = active
        self.cores: List[NodeCore] = []
        self.gpus = []
        self.nics = []

        self.sockets = 0
        self.numa_nodes = 0 
        self.smt_enabled = False
        self.maintenance = False
        self.cores_per_proc = 0
        self.pod_info = {}
        self.data_vlan = 0
        self.groups: List[str] = ['default']
        self.gwip : str = '0.0.0.0/32'
        self.mem: NodeMemory = NodeMemory()
        self.reserved_cores = [] # Reserved CPU cores

    def GetMaintenance(labels):
        maintenance = False
        if (Node.NHD_MAINT_LABEL in labels):
            value = labels[Node.NHD_MAINT_LABEL].lower()
            if (value != 'not_scheduled'):
                maintenance = True

        return maintenance


    def ResetResources(self):
        """ Resets all resources back to initial values """
        self.logger.info(f'Node {self.name} resetting resources')

        for c in self.cores:
            if c.core not in self.reserved_cores:
                c.used = False

        for g in self.gpus:
            g.used = False

        for n in self.nics:
            n.pods_used = 0
            n.speed_used = [0,0]

        self.mem.free_hugepages_gb = self.mem.ttl_hugepages_gb

        self.pod_info.clear()

    def GetNodeActive(self):
        """ Gets the active state for a node """
        return self.active

    def GetTotalHugepages(self):
        """ Gets the total hugepages for a node """
        return self.mem.ttl_hugepages_gb

    def GetFreeHugepages(self):
        """ Gets the free hugepages for a node """
        return self.mem.free_hugepages_gb        

    def GetNIC(self, mac):
        """ Gets a NIC by MAC address """
        for n in self.nics:
            if n.mac == mac:
                return n
        return None

    def GetNICUsedSpeeds(self):
        """ Gets the RX and TX speeds used on each NIC """
        nicres = []
        for n in self.nics:
            nicres.append(n.speed_used)

        return nicres

    def GetNICFromIfName(self, ifname):
        """ Gets a NIC object from the interface name """
        for n in self.nics:
            if n.ifname == ifname:
                return n
        return None

    def GetTotalPods(self):
        """ Gets the total pods scheduled on the node """
        return len(self.pod_info)

    def PodPresent(self, pod, ns):
        """ Finds if a pod is present on the node """
        return (pod, ns) in self.pod_info

    def AddScheduledPod(self, pod, ns, top):
        """ Add a scheduled pod to the node """
        self.logger.info(f'Adding pod {(pod,ns)} to node {self.name}')
        self.pod_info[(pod,ns)] = top
    
    def RemoveScheduledPod(self, pod, ns):
        """ Remove a scheduled pod from the node """
        self.logger.info(f'Removing pod {(pod,ns)} from node {self.name}')
        try: 
            del self.pod_info[(pod,ns)]
        except KeyError as e:
            self.logger.error(f'Couldn\'t find pod {(pod,ns)} in pod list!')

    def GetGPU(self, di):
        """ Gets a GPU by device ID """
        for g in self.gpus:
            if g.device_id == di:
                return g
        return None

    def SMTEnabled(self) -> bool:
        """ Determine if SMT is enabled for this node """
        return self.smt_enabled

    def GetFreeCpuCoreCount(self) -> int:
        """ Gets the number of free CPU cores available. If SMT is enabled we only count cores where both
            siblings are unused.
        """
        if self.smt_enabled:
            return len([x for x in self.cores if not x.used and not self.cores[x.sibling].used])
        else:        
            return len([x for x in self.cores if not x.used])

    def GetFreeGpuCount(self) -> int:
        """ Gets the number of free GPUs """
        return len([x for x in self.gpus if not x.used])

    def GetTotalGPUs(self) -> int:
        """ Gets the total number of GPUs in the node """
        return len(self.gpus)

    def GetTotalCPUs(self) -> int:
        """ Gets the total number of CPUs on the node """
        return len(self.cores)

    def GetFreeCpuCores(self) -> int:
        """ Returns a list containing a list for each socket specifying how many cores + siblings are free. Note that
            we do not allow any multi-tenancy on cores which are already partially used (SMT on and 1/2 logical cores
            are used). """
        self.logger.info(f'Searching for free CPU cores on node {self.name}')

        fl = [0] * self.numa_nodes
        for c in range(self.cores_per_proc*self.sockets):
            if not self.cores[c].used:
                if not self.smt_enabled:
                    fl[self.cores[c].socket] += 1 # Fix in future if we use AMD or other weirdos
                elif not self.cores[self.cores[c].sibling].used:
                    fl[self.cores[c].socket] += 1
    
        return fl

    def GetFreeGPUPCICount(self):
        """ Returns list of free GPUs on each PCIe switch """
        ginfo = defaultdict(lambda: 0)
        for g in self.gpus:
            if not g.used:
                ginfo[g.pciesw] += 1

        return ginfo

    def GetNumaNICPCIResources(self):
        """ Returns list of PCI switch addresses for each NIC on the node """
        ninfo = [{} for _ in range(self.numa_nodes)]
        for n in self.nics:
            ninfo[n.numa_node][n.idx] = n.pciesw     

        return ninfo      
                   

    def GetFreeNumaNicResources(self) -> List[int]:
        """ Return the amount of free NIC resources per NUMA node """
        ninfo = [[] for _ in range(self.numa_nodes)]

        for n in self.nics:
            try:
                if ENABLE_SHARING:
                    ninfo[n.numa_node].append([n.speed*NIC_BW_AVAIL_PERCENT - n.speed_used[x] for x in range(2)])
                else:
                    ninfo[n.numa_node].append([0 if (n.pods_used > 0) else n.speed*NIC_BW_AVAIL_PERCENT for x in range(2)])
            except IndexError:
                self.logger.warning(f'Node {self.name}, NIC {n.mac} has unexpected NUMA node {n.numa_node} of {self.numa_nodes}')  

        return ninfo


    @staticmethod
    def ParseRangeList(rl: str):
        """ Parses a list of numbers separated by commas and hyphens. This is the typical cpuset format
            used by Linux in many boot parameters.
        """
        def ParseRange(r):
            parts = r.split("-")
            return range(int(parts[0]), int(parts[-1])+1)
        return sorted(set(chain.from_iterable(map(ParseRange, rl.split(",")))))   

    def SetGroups(self, groups:str):
        """ Sets the node groups from a label string """
        self.groups = groups.split('.')

    def InitGroups(self, labels):
        """ Initialize the node groups. If a node does not have this label, it's put into the default group """   
        if not ('NHD_GROUP' in labels):
            self.logger.warning(f'Couldn\'t find node NHD group in labels for node {self.name}. Using default')
            self.groups = ['default']
        else:
            self.groups = labels['NHD_GROUP'].split('.')
            self.logger.warning(f'NHD group set to {self.groups} for node {self.name}')  

        return True    

    def InitMaintenance(self, labels):
        self.maintenance = Node.GetMaintenance(labels)
        return True    

    def InitCores(self, labels):
        """ Initialize the CPU resouces based on the node labels """
        if not ('feature.node.kubernetes.io/nfd-extras-cpu.num_cores' in labels and 'feature.node.kubernetes.io/nfd-extras-cpu.numSockets' in labels):
            self.logger.error('Couldn\'t find node CPU labels. Ignoring node')
            return False

        self.sockets        = int(labels['feature.node.kubernetes.io/nfd-extras-cpu.numSockets'])
        cores               = int(labels['feature.node.kubernetes.io/nfd-extras-cpu.num_cores'])
        self.smt_enabled    = 'feature.node.kubernetes.io/cpu-hardware_multithreading' in labels
        self.numa_nodes     = self.sockets # Fix this if we move to something other than Intel with more NUMA nodes than sockets (AMD)

        self.cores_per_proc = cores // self.sockets

        self.logger.info(f'Initializing CPUs for node {self.name} with procs={self.sockets}, cores={cores}, smt={self.smt_enabled}')
        self.cores = [None]*cores if not self.smt_enabled else [None]*cores*2

        for c in range(len(self.cores)):
            proc = int(int(c % cores) // (cores/self.sockets))
            if self.smt_enabled:
                sib = (c + cores) if c < cores else (c - cores)
            else:
                sib = -1

            self.cores[c] = NodeCore(c, proc, sib)

        if 'feature.node.kubernetes.io/nfd-extras-cpu.isolcpus' not in labels:
            self.logger.info(f'No isolated CPU information found for node {self.name}')
        else:
            # Underscores split the range
            isolrange = labels['feature.node.kubernetes.io/nfd-extras-cpu.isolcpus'].split('_')
            isolcores = []
            for r in isolrange:
                isolcores.extend(Node.ParseRangeList(r))
            self.logger.info(f'Isolated cores in node {self.name} read as {isolrange}')
            ttlcores   = list(range(0, len(self.cores)))

            nonisol = list(set(ttlcores) - set(isolcores))

            # Mark all OS reserved cores as in use
            self.logger.info(f'Marking cores {nonisol} as used')
            for c in range(len(self.cores)):
                if c in nonisol:
                    self.cores[c].used = True
                    self.reserved_cores.append(c)


        self.logger.info(f'Finished setting up cores for node {self.name}')
        return True

    def InitNics(self, labels):
        self.logger.info(f'Initializing NICs for node {self.name}')
        pfs = []
        # Build list of any PFs
        for l,v in labels.items():
            if 'feature.node.kubernetes.io/nfd-extras-sriov' in l:
                p = l.split('.')
                pfs.append(p[5])

        if len(pfs):
            self.logger.info(f'SR-IOV enabled on NICs: {pfs}')

        for l,v in labels.items():
            if 'feature.node.kubernetes.io/nfd-extras-nic' in l:
                p = l.split('.')

                (ifname, vendor, mac, speed, numa_node, pciesw, card, port) = (p[4], p[5], p[6], p[7], int(p[8]), int(p[9],16), int(p[10],16), int(p[11]))

                if ifname in pfs:
                    continue

                if 'Mbs' in speed:
                    speed = int(speed[:speed.index('Mbs')])
                else:
                    self.logger.info(f'Not adding NIC {ifname} since speed is 0. Interface may be down')
                    continue

                if speed < SCHEDULABLE_NIC_SPEED_THRESH_MBPS:
                    self.logger.info(f'NIC {ifname} has speed lower than required ({speed} found, '
                                    f'{SCHEDULABLE_NIC_SPEED_THRESH_MBPS} required. Excluding from schedulable list')
                    continue

                self.nics.append(NodeNic(ifname, mac, vendor, speed/1e3, numa_node, pciesw, card, port))
                self.logger.info(f'Updated NIC with ifname={ifname} vendor={vendor}, mac={mac}, speed={speed}Mbps, numa_node={numa_node}, '\
                                    f'PCIe switch={pciesw}, card={card}, port={port} to node {self.name}')

        # Set all the node indices
        if len(self.nics):
            nidx = [0] * (max([x.numa_node for x in self.nics])+1)
            for n in self.nics:
                self.logger.info(f'Setting NIC node index to {nidx[n.numa_node]} on ifname {n.ifname}')
                n.SetNodeIndex(nidx[n.numa_node])
                nidx[n.numa_node] += 1

        return True

    def InitGpus(self, labels):
        self.logger.info(f'Initializing GPUs for node {self.name}')
        for l,v in labels.items():
            if 'feature.node.kubernetes.io/nfd-extras-gpu' in l:
                p = l.split('.')
                (device_id, gtype, numa_node, pciesw) = (int(p[4]), p[5], int(p[6]), int(p[7], 16))

                self.gpus.append(NodeGpu(gtype, device_id, numa_node, pciesw))
                self.logger.info(f'Added GPU with type={gtype}, device_id={device_id}, numa_node={numa_node}, pciesw={pciesw} to node {self.name}')

        return True

    def InitMisc(self, labels):
        self.logger.info(f'Initializing miscellaneous labels for node {self.name}')
        if 'DATA_PLANE_VLAN' not in labels:
            self.logger.error(f'Couldn\'t find data plane VLAN label for node {self.name}. Skipping node')
            return False
        
        self.data_vlan = int(labels['DATA_PLANE_VLAN'])
        self.logger.info(f'Read data plane VLAN as {self.data_vlan}')

        if 'DATA_DEFAULT_GW' not in labels:
            self.logger.error(f'Couldn\'t find data plane default GW label for node {self.name}. Skipping node')
            return False

        self.gwip = labels['DATA_DEFAULT_GW']
        self.logger.info(f'Read data plane default GW as {self.gwip}')

        return True

    def GetFreeNumaGPUs(self):
        gfree = [0] * self.numa_nodes
        for g in self.gpus:
            if not g.used:
                gfree[g.numa_node] += 1
        
        return gfree

    def SetNodeAddr(self, addr):
        self.logger.info(f'Setting node {self.name} address to {addr}')
        self.addr = addr

    def ParseLabels(self, labels):
        if not self.InitGroups(labels):
            return False

        if not self.InitMaintenance(labels):
            return False

        if not self.InitCores(labels):
            return False

        if not self.InitNics(labels):
            return False

        if not self.InitGpus(labels):
            return False

        if not self.InitMisc(labels):
            return False

        return True

    def SetHugepages(self, alloc: int, free: int) -> bool: 
        self.mem.ttl_hugepages_gb  = alloc
        self.mem.free_hugepages_gb = free
        self.logger.info(f'Found {self.mem.free_hugepages_gb}/{self.mem.ttl_hugepages_gb}GB of hugepages allocatable/capacity on node {self.name}')
        return True

    def GetNextGpuFree(self, numa):
        for g in self.gpus:
            if g.numa_node == numa and not g.used:
                return g

        return None                    
     
    def GetFreeCpuBatch(self, numa: int, num: int, smt: SMTSetting) -> List[int]:
        cpus = []
        for ci,c in enumerate(self.cores):
            if num == 0:
                break
            if c.socket == numa and not c.used:
                if self.smt_enabled:
                    if not self.cores[c.sibling].used: # Switch to numa instead of socket later
                        if smt == SMTSetting.SMT_ENABLED and num >= 2:
                            cpus.extend([c.core, c.sibling])
                            num -= 2
                        else:
                            cpus.append(c.core)
                            num -= 1
                else:
                    cpus.append(c.core)
                    num -= 1
        return cpus  

    def PrintResourceStats(self):
        self.logger.info(f'Node {self.name} resource stats:')
        self.logger.info(f'   {self.GetFreeCpuCoreCount()} free CPU cores')
        self.logger.info(f'   {self.GetFreeGpuCount()} free GPU devices')
        self.logger.info(f'   {self.mem.free_hugepages_gb}/{self.mem.ttl_hugepages_gb} hugepages free')
        self.logger.info(f'   NICs:')
        for n in self.nics:
            self.logger.info(f'        {n.mac}: {n.speed_used[0]}/{n.speed_used[1]} Gbps used on {n.speed} Gbps interface with {n.pods_used} pods using interface')
        


    def RemoveResourcesFromTopology(self, top) -> bool:
        """ Remove resources from a node that are present in a topology structure. """

        for pv in top.proc_groups:
            for m in pv.misc_cores:
                if self.cores[m.core].used:
                    self.logger.error(f'Processing group misc core {m.core} was already in use!')
                self.cores[m.core].used = True

            for m in pv.proc_cores:
                try:
                    if self.cores[m.core].used:
                        self.logger.error(f'Processing group core {m.core} was already in use!')
                    self.cores[m.core].used = True
                except IndexError as e:
                    self.logger.error(f"Failed to get core at index {m.core} when size is {len(self.cores)}")
                    return False

            for g in pv.group_gpus:
                dev = self.GetGPU(g.device_id)
                if dev is None:
                    self.logger.error(f'Cannot find GPU device ID {g.device_id}')
                else:
                    if dev.used:
                        self.logger.error(f'GPU {dev.device_id} was already in use!')

                    self.logger.info(f'Taking GPU device ID {g.device_id}')
                    dev.used = True

                for c in g.cpu_cores:
                    if self.cores[c.core].used:
                        self.logger.error(f'GPU core {c.core} was already in use!')
                    self.cores[c.core].used = True

        for m in top.misc_cores:
            if self.cores[m.core].used:
                self.logger.error(f'Miscellaneous core {m.core} was already in use!')
            self.cores[m.core].used = True

        for p in top.nic_core_pairing:
            nic = self.GetNIC(p.mac)
            if nic is None:
                self.logger.error(f'Cannot find NIC {p.mac} on node!')
                continue
            
            nic.speed_used[0] += p.rx_core.nic_speed
            nic.speed_used[1] += p.tx_core.nic_speed
            self.logger.info(f'Speeds used on NIC after removing resources = {nic.speed_used[0]}/{nic.speed_used[1]}')

            nic.pods_used += 1

        if top.hugepages_gb > 0:
            self.mem.free_hugepages_gb -= top.hugepages_gb    
            self.logger.info(f'Taking {top.hugepages_gb} 1GB hugepages from node. {self.mem.free_hugepages_gb} remaining')                  

        return True

    def AddResourcesFromTopology(self, top):
        """ Add resources from a node that are present in a topology structure. """

        for pv in top.proc_groups:
            for m in pv.misc_cores:
                if not self.cores[m.core].used:
                    self.logger.error(f'Processing misc core {m.core} was not in use!')
                self.cores[m.core].used = False

            for m in pv.proc_cores:
                if not self.cores[m.core].used:
                    self.logger.error(f'Processing core {m.core} was not in use!')
                self.cores[m.core].used = False

            for g in pv.group_gpus:
                dev = self.GetGPU(g.device_id)
                if dev is None:
                    self.logger.error(f'Cannot find GPU device ID {g.device_id}')
                else:
                    if not dev.used:
                        self.logger.error(f'GPU {dev.device_id} was not in use!')

                    dev.used = False

                for c in g.cpu_cores:
                    if not self.cores[c.core].used:
                        self.logger.error(f'GPU core {c.core} was not in use!')
                    self.cores[c.core].used = False

        for m in top.misc_cores:
            if not self.cores[m.core].used:
                self.logger.error(f'Misc core {m.core} was not in use!')
            self.cores[m.core].used = False

        for p in top.nic_core_pairing:
            nic = self.GetNIC(p.mac)
            if nic is None:
                self.logger.error(f'Cannot find NIC {p.mac} on node!')
                continue
            
            nic.speed_used[0] -= p.rx_core.nic_speed
            nic.speed_used[1] -= p.tx_core.nic_speed
            self.logger.info(f'Speeds used on NIC after adding resources = {nic.speed_used[0]}/{nic.speed_used[1]}')

            nic.pods_used -= 1

        # Hugepages requests
        if top.hugepages_gb > 0:
            self.mem.free_hugepages_gb += top.hugepages_gb    
            self.logger.info(f'Adding {top.hugepages_gb} 1GB hugepages to node. {self.mem.free_hugepages_gb} remaining')                    
    
    def GetNADListFromIndices(self, ilist: List[int]):
        """ Get the NAD list from the NIC indices """
        names = [self.nics[i].ifname for i in ilist]
        self.logger.info(f'Built NetworkAttachmentDefinition of {names}')
        return names


    def ClaimPodNICResources(self, nidx):
        for ni in nidx: # Mark as pod using the interface
            self.nics[ni].pods_used += 1


    def GetFreePciGpuFromNic(self, nic):
        # Search free GPUs
        for g in self.gpus:
            if g.pciesw == nic.pciesw and not g.used:
                self.logger.info(f'Found GPU {g.device_id} matching NIC PCI switch {nic.pciesw}')
                return g

        return None

    def GetNicObjFromIndex(self, numa_node, nic_idx):
        for ni, nic in enumerate(self.nics):
            if nic_idx == nic.idx and nic.numa_node == numa_node:
                return nic
        return None     


    def SetPhysicalIdsFromMapping(self, mapping, top: CfgTopology):
        """ Maps the indices after the mapping function is done into physical node resources based on what's free. Uses
            the previously-defined topology to pull the actual groups out """
        
        # Set up the "used" resources.
        used_cpus = []
        used_gpus = []
        used_nics = []
        
        try:
            # Go through each of the processing groups and map resources
            for pi,pv in enumerate(top.proc_groups):
                # Set VLAN
                self.logger.info(f'Setting VLAN to {self.data_vlan}')
                pv.vlan.vlan = self.data_vlan

                group_numa_node = mapping['gpu'][pi]
                cidx = 0 # Keep track of which CPU cores we've given out
                gcpu_req = len(pv.proc_cores) + sum([len(gpu.cpu_cores) for gpu in pv.group_gpus])
                group_cpus = self.GetFreeCpuBatch(group_numa_node, gcpu_req, pv.proc_smt)
                self.logger.info(f'Got {group_cpus} processing group cores for group {pi}')

                if len(group_cpus) != gcpu_req:
                    self.logger.error(f'Asked for {gcpu_req} free CPUs, but only got {len(group_cpus)} back!')
                    raise IndexError

                # Always try to pair up GPUs and NICs on the same PCI switch even if we're not using GPUDirect in this
                # pod, so that future GPUDirect pods have a higher chance of getting their resources. Build a dict of
                # all GPUs and NICs on the same switch
                # if top.map_type == TopologyMapType.TOPOLOGY_MAP_PCI: 
                #     if len(mapping['nic'][pi]) != len(pv.group_gpus):
                #         self.logger.error(f'When using PCI mode the number of GPUs and NICs must match for now ({len(mapping["nic"][pi])} != {len(pv.group_gpus)})')
                #         return None

                nic_gpu_map = []
                # Grab GPUs and NICs that are on the same switch using the physical NIC index. This will need to be updated when we support one NIC
                # to multiple GPUs. For now assume there's one NIC per processing group. This will be expanded later if needed.
                nicinfo = mapping['nic'][pi]
                nobj    = self.GetNicObjFromIndex(mapping['nic'][pi][0], mapping['nic'][pi][1])
                if nobj == None:
                    self.logger.error(f'Couldn\'t find NIC object from index {nicinfo}')
                    raise IndexError

                for gi,gv in enumerate(pv.group_gpus):
                    gdev = self.GetFreePciGpuFromNic(nobj)                    
                    if gdev == None:
                        # If we can't find a free GPU for this NIC, and we're in PCI mode, then something went wrong. We need to bail out.
                        if top.map_type == TopologyMapType.TOPOLOGY_MAP_PCI: 
                            self.logger.error(f'Couldn\'t find a free GPU for NIC PCI switch in PCI mode for switch {nobj.pciesw}! Bailing out: {nicinfo}')
                            raise IndexError
                        else:
                            # Just get the next free GPU we can find since there's not one free on the same PCI switch
                            gdev = self.GetNextGpuFree(group_numa_node)
                                            
                    if gdev == None:
                        self.logger.error(f'No free GPUs available on node {self.name} even though mapping found one!')
                        raise IndexError

                    self.logger.info(f'Got GPU with device ID {gdev.device_id}')                        
                    
                    gv.device_id = gdev.device_id
                    gdev.used = True
                    used_gpus.append(gdev.device_id)

                    for gpu_cpu in gv.cpu_cores:
                        gpu_cpu.core = group_cpus[cidx]
                        self.cores[gpu_cpu.core].used = True
                        used_cpus.append(gpu_cpu.core)
                        cidx += 1

                # Assign processing cores
                for groupi, groupc in enumerate(pv.proc_cores):
                    groupc.core = group_cpus[cidx]
                    self.cores[groupc.core].used = True
                    used_cpus.append(groupc.core)
                    cidx += 1

                    # Check if this core is using NIC resources
                    if groupc.nic_dir in (NICCoreDirection.NIC_CORE_DIRECTION_RX, NICCoreDirection.NIC_CORE_DIRECTION_TX):
                        idx = -1
                        for ni, nic in enumerate(self.nics):
                            if nic == nobj:
                                idx = ni
                                break

                        if idx == -1:
                            self.logger.error(f'Couldn\'t find NIC index for NUMA node {group_numa_node} on node')
                            raise IndexError
                        else:
                            sidx = 0 if groupc.nic_dir == NICCoreDirection.NIC_CORE_DIRECTION_RX else 1
                            self.nics[idx].speed_used[sidx] += groupc.nic_speed
                            used_nics.append((idx, groupc.nic_speed, groupc.nic_dir))

                        # Set physical NIC resources
                        ng = top.GetNICGroup(groupc)
                        if ng is None:
                            self.logger.error(f'Couldn\'t find core {groupc.name} in nic group list!')
                            raise IndexError
                        
                        self.logger.info(f'Adding interface {self.nics[idx].ifname} with mac {self.nics[idx].mac} to core {groupc.core}')
                        ng.AddInterface(self.nics[idx].mac)

                # Check that we used all the CPU cores
                if cidx != len(group_cpus):
                    self.logger.info('Still have {len(group_cpus) - cidx} leftover CPUs in request list!')
                    raise IndexError
                

                # Assign the helper cores
                helper_req = self.GetFreeCpuBatch(group_numa_node, len(pv.misc_cores), pv.helper_smt)
                self.logger.info(f'Got {helper_req} helper cores for group {pi}')
                cidx = 0
                if len(pv.misc_cores) != len(helper_req):
                    self.logger.error(f'Asked for {len(pv.misc_cores)} free helper CPUs, but only got {len(helper_req)} back!')
                    raise IndexError

                for hc in pv.misc_cores:
                    hc.core = helper_req[cidx]
                    self.cores[hc.core].used = True
                    used_cpus.append(hc.core)
                    cidx += 1

                if cidx != len(helper_req):
                    self.logger.info('Still have {len(helper_req) - cidx} leftover helper CPUs in request list!')
                    raise IndexError
                    
            # Set data plane default GWs
            top.SetDataDefaultGw(self.gwip)

            # Hugepages requests
            if top.hugepages_gb > 0:
                self.mem.free_hugepages_gb -= top.hugepages_gb    
                self.logger.info(f'Taking {top.hugepages_gb} 1GB hugepages from node. {self.mem.free_hugepages_gb} remaining')                    

            # Last, we assign the top-level miscellaneous cores. Miscellaneous cores are the last element in the CPU list
            misc_cpus = self.GetFreeCpuBatch(mapping['cpu'][-1], len(top.misc_cores), top.misc_cores_smt)
            self.logger.info(f'Got {misc_cpus} top-level miscellaneous cores')

            cidx = 0
            if len(top.misc_cores) != len(misc_cpus):
                self.logger.error(f'Asked for {top.misc_cores} free helper CPUs, but only got {len(misc_cpus)} back!')
                raise IndexError

            for mc in top.misc_cores:
                mc.core = misc_cpus[cidx]
                self.cores[mc.core].used = True
                used_cpus.append(mc.core)
                cidx += 1

            if cidx != len(misc_cpus):
                self.logger.info('Still have {len(misc_cpus) - cidx} leftover helper CPUs in request list!')
                raise IndexError

            self.logger.info(f'Setting control VLAN to {self.data_vlan}')
            top.ctrl_vlan.vlan = self.data_vlan

            self.logger.info('All assignments completed successfully')
            self.logger.info(f'CPU assignments: {used_cpus}')
            self.logger.info(f'GPU assignments: {used_gpus}')
            self.logger.info(f'NIC assignments: {used_nics}')

        except IndexError:
            self.logger.info('One or more failures assigning resources to node. Unwinding mapping and returning...')
            for c in used_cpus:
                self.cores[c].used = False
            for g in used_gpus:
                self.gpus[g].used = False
            for n in used_nics:
                if n[2] == NICCoreDirection.NIC_CORE_DIRECTION_RX:
                    self.nics[n[0]].speed_used[0] -= self.nics[n[1]]
                else:
                    self.nics[n[0]].speed_used[1] -= self.nics[n[1]]
            
            raise
        
        self.logger.info(f'Node {self.name} has {self.GetFreeCpuCoreCount()} CPU cores and {self.GetFreeGpuCount()} free GPUs left')

        return used_nics # The NIC list is used to populate the network attachment definitions externally

        
