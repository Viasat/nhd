from enum import Enum
from nhd.NHDCommon import NHDCommon
import logging
import os
from colorlog import ColoredFormatter
from typing import Dict, List

class GpuType(Enum):
    GPU_TYPE_ALL = 0
    GPU_TYPE_V100 = 1
    GPU_TYPE_GTX_1080 = 2
    GPU_TYPE_GTX_1080TI = 3
    GPU_TYPE_GTX_2080 = 4
    GPU_TYPE_GTX_2080TI = 5
    GPU_TYPE_NOT_SUPPORTED = 6

class CpuType(Enum):
    CPU_TYPE_ALL = 0
    CPU_TYPE_HASWELL = 1
    CPU_TYPE_BROADWELL = 2
    CPU_TYPE_SKYLAKE = 3
    CPU_TYPE_COOPER_LAKE = 4
    CPU_TYPE_ICE_LAKE = 5

class NICCoreDirection(Enum):
    NIC_CORE_DIRECTION_NONE = 0
    NIC_CORE_DIRECTION_RX = 1
    NIC_CORE_DIRECTION_TX = 2

class SMTSetting(Enum):
    SMT_DISABLED = 0
    SMT_ENABLED = 1

class NUMASetting(Enum):
    LOGICAL_NUMA_DONT_CARE = -1
    LOGICAL_NUMA_0 = 0
    LOGICAL_NUMA_1 = 1
    LOGICAL_NUMA_GROUP = 2

class TopologyMapType(Enum):
    TOPOLOGY_MAP_INVALID = 0
    TOPOLOGY_MAP_NUMA = 1
    TOPOLOGY_MAP_PCI = 2


class Core:
    def __init__(self, name, nic_speed, nic_dir, numa, core):
        self.nic_speed = nic_speed
        self.nic_dir = nic_dir
        self.name = name
        self.numa = numa

        self.core = core # Filled in by scheduler

class NICGroup:
    def __init__(self, rx_core, tx_core):
        self.rx_core = rx_core
        self.tx_core = tx_core
        self.mac = ""
        self.rx_ring_size = 4096 # Come back and fix this later 

    def AddInterface(self, mac):
        self.mac = mac

    def SetRxRingSize(self, ring_size: int):
        self.rx_ring_size = ring_size

class GPU:
    def __init__(self, cpu_cores: List[Core], dev_id_names: List[str], gtype: GpuType, dev_id: int):
        self.dev_id_names  = dev_id_names
        self.cpu_cores = cpu_cores
        self.gtype = gtype
        self.device_id = dev_id

class VLANInfo:
    def __init__(self, name: str, vlan: int):
        self.name = name
        self.vlan = vlan

class ProcGroup:
    def __init__(self):
        self.misc_cores:  List[Core] = []
        self.proc_cores:  List[Core] = []
        self.group_gpus:  List[GPU]  = []
        self.proc_smt = SMTSetting.SMT_DISABLED
        self.helper_smt = SMTSetting.SMT_DISABLED
        self.vlan = None

    def AddMiscCore(self, c: Core):
        self.misc_cores.append(c)

    def AddGroupCore(self, c: Core):
        self.proc_cores.append(c)

    def AddGroupGPU(self, g: GPU):
        self.group_gpus.append(g)

    def SetGpuType(self, t: GpuType):
        self.gpu_type = t

    def SetProcSmt(self, smt: SMTSetting):
        self.proc_smt = smt

    def SetHelperSmt(self, smt: SMTSetting):
        self.helper_smt = smt

    def SetDataVlan(self, vlan: VLANInfo):
        self.vlan = vlan

    def GetGpuType(self, gpu_type: str) -> GpuType:
        mapping = { 'ANY' : GpuType.GPU_TYPE_ALL,
                    'V100': GpuType.GPU_TYPE_V100,
                    '1080': GpuType.GPU_TYPE_GTX_1080,
                    '1080Ti': GpuType.GPU_TYPE_GTX_1080TI,
                    '2080': GpuType.GPU_TYPE_GTX_2080,
                    '2080Ti': GpuType.GPU_TYPE_GTX_2080TI}

        if gpu_type not in mapping:
            return None

        return mapping[gpu_type]


class CfgTopology:
    """ Serves as a generic class to store topology-level information in that is used
    by the scheduler to make scheduling decisions. There should be no config-specific
    items in this file, but rather generic information about the hardware in the machine
    to aide the scheduler. """
    def __init__(self):
        self.arch: CpuType = CpuType.CPU_TYPE_ALL
        self.misc_cores: List[Core] = []
        self.proc_groups: List[ProcGroup] = []
        self.nic_core_pairing: List[NICGroup] = []
        self.misc_cores_smt: SMTSetting = SMTSetting.SMT_DISABLED
        self.map_type: TopologyMapType = TopologyMapType.TOPOLOGY_MAP_INVALID
        self.ctrl_vlan: VLANInfo = None
        self.data_default_gw: str = ''
        self.hugepages_gb = 0

        self.logger = NHDCommon.GetLogger(__name__)

        self.logger.info('Initializing matcher')

    def AddPodReservations(self, res: Dict[str, int]):
        if 'hugepages-1Gi' in res:
            self.hugepages_gb = res['hugepages-1Gi']
            self.logger.info(f'Pod requesting {self.hugepages_gb} 1GB hugepages')

    def AddNicPairing(self, rx_core: Core, tx_core: Core):
        self.nic_core_pairing.append(NICGroup(rx_core, tx_core))

    def SetCtrlVlan(self, vlan: VLANInfo):
        self.ctrl_vlan = vlan

    def SetDataDefaultGw(self, gw: str):
        self.data_default_gw = gw

    def GetNICGroup(self, coreobj: Core):
        for c in self.nic_core_pairing:
            if (coreobj.nic_dir == NICCoreDirection.NIC_CORE_DIRECTION_RX and c.rx_core == coreobj) or \
               (coreobj.nic_dir == NICCoreDirection.NIC_CORE_DIRECTION_TX and c.tx_core == coreobj):
                return c

        return None

    def GetNICGroupFromCoreNumbers(self, rxcore: int, txcore: int):
        for c in self.nic_core_pairing:
            if (c.rx_core.core == rxcore and c.tx_core.core == txcore):
                return c

        return None


    def SetCpuArch(self, arch: str):
        mapping = { 'ANY' :         CpuType.CPU_TYPE_ALL,
                    'HASWELL':      CpuType.CPU_TYPE_HASWELL,
                    'BROADWELL':    CpuType.CPU_TYPE_BROADWELL,
                    'SKYLAKE':      CpuType.CPU_TYPE_SKYLAKE,
                    'COOPER_LAKE':  CpuType.CPU_TYPE_COOPER_LAKE,
                    'ICE_LAKE':     CpuType.CPU_TYPE_ICE_LAKE}
        if arch not in mapping:
            return None

        self.logger.debug(f'CPU architecture set to {arch}')
        return mapping[arch]

    def SetMiscCoreSmt(self, smt: SMTSetting):
        self.misc_cores_smt = smt

    def AddMiscCore(self, core: Core):
        self.misc_cores.append(core)
        self.logger.info(f'Created Core: num: {core.core} speed: {core.nic_speed}, nic_dir: {core.nic_dir}, numa: {core.numa}')

    def AddProcGroup(self, pg: ProcGroup):
        self.proc_groups.append(pg)

    def GetTotalGpusRequested(self) -> List[int]:
        return [len(p.group_gpus) for p in self.proc_groups]

    def GetTotalCpusRequested(self) -> List[List[int]]:
        """ Returns a tuple containing information about which CPUs were requested. There are several
            parameters when describing how a core is to be mapped, so all of that information must be
            captured in the tuple.
        """
        groups = {'proc': []}
        for g in self.proc_groups:
            cores = [] # proc, misc
            cores.append((len(g.proc_cores) + sum([len(gpu.cpu_cores) for gpu in g.group_gpus]), g.proc_smt))
            cores.append((len(g.misc_cores), g.helper_smt))

            groups['proc'].append(cores)

        groups['misc'] = [len(self.misc_cores), self.misc_cores_smt]

        return groups

    def GetTotalNICsRequested(self) -> List[List[int]]:
        """ Returns the total NIC bandwidth requests per group """
        groups = []
        for g in self.proc_groups:
            speeds = [0,0] # (rx,tx)
            for p in g.proc_cores:
                if p.nic_dir == NICCoreDirection.NIC_CORE_DIRECTION_RX:
                    speeds[0] += p.nic_speed
                elif p.nic_dir == NICCoreDirection.NIC_CORE_DIRECTION_TX:
                    speeds[1] += p.nic_speed

            groups.append(speeds)

        return groups

    def SetTopMapType(self, t: str) -> None:
        if t == "NUMA":
            self.map_type = TopologyMapType.TOPOLOGY_MAP_NUMA
            self.logger.info(f'Using topology map type of NUMA')
        elif t == "PCI":
            self.map_type = TopologyMapType.TOPOLOGY_MAP_PCI
            self.logger.info(f'Using topology map type of PCI')
        else:
            self.logger.error(f'Invalid topology mapping type of {t}')


