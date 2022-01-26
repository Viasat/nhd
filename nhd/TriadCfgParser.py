import logging
import os
import libconf
from colorlog import ColoredFormatter
from nhd.CfgTopology import CfgTopology
from nhd.CfgTopology import NICCoreDirection
from nhd.CfgTopology import NUMASetting
from nhd.CfgTopology import SMTSetting
from nhd.CfgTopology import VLANInfo
from nhd.CfgTopology import Core
from nhd.CfgTopology import GPU
from nhd.CfgTopology import ProcGroup
from nhd.CfgParser import CfgParser
from nhd.NHDCommon import NHDCommon
import functools
import collections
import magicattr


""" A configuration format to parser libconfig-style files. Used by the Triad software internal to ViaSat. Many of the tricks that
    are pulled here are because libconfig doesn't have first-class python support, so getting and setting values are hackish
    compared to mainstream formats like JSON.
"""
class TriadCfgParser(CfgParser):
    def __init__(self, dat, isFile):
        self.logger = NHDCommon.GetLogger(__name__)
        self.cfg = None
        self.top : CfgTopology = CfgTopology()

        if not isFile:
            self.LoadCfgStr(dat)
        else:
            self.LoadCfgFile(dat)


    def LoadCfgStr(self, dat):
        """
        Load a configuration string into our config structure
        """
        self.cfg = libconf.loads(dat)

    def LoadCfgFile(self, dat):
        """
        Load a configuration file into our config structure
        """        
        self.cfg = libconf.load(dat)


    def CheckMandatoryFields(self):
        """
        Checks whether the mandatory fields required by the config parser are present
        """        
        def check_field(f, lvl):
            if f not in lvl:
                self.logger.error(f'Failed to find {f} section in config file')
                return False
            else:
                self.logger.debug(f'Found {f} section in config')
            
            return True

        if not check_field('cpu_arch', self.cfg.TopologyCfg):
            return False

        if not check_field('ext_cores', self.cfg.TopologyCfg):
            return False

        if not check_field('kni_vlan', self.cfg.TopologyCfg):
            return False            

        return True

    def GetValFromAttrName(self, obj, attr, *args):
        """
        Gets a value back from a nested libconfig attribute
        """             
        def _getattr(obj, attr):
            return getattr(obj, attr, *args)
        return functools.reduce(_getattr, [obj] + attr.split('.'))

    def ParseKniDataVlan(self):
        """
        Find the KNI VLAN (data plane VLAN)
        """             
        if 'kni_vlan' not in self.cfg.TopologyCfg:
            self.logger.error('kni_vlan not in top level config!')
            return False

        self.logger.info(f'Setting KNI VLAN name to {self.cfg.TopologyCfg.kni_vlan}')
        self.top.SetCtrlVlan(VLANInfo(self.cfg.TopologyCfg.kni_vlan, 0)) # Don't need to set the VLAN from the config since it doesn't matter    
        
        return True

    def ParseHugePages(self):
        """
        Parse the hugepage information from the config
        """
        if 'Hugepages_GB' not in self.cfg:
            self.logger.error('Couldn\'t find hugepage information in config!')
            return False

        self.logger.info(f'Setting hugepages (GB) to {self.cfg.Hugepages_GB}')
        self.top.hugepages_gb = int(self.cfg.Hugepages_GB)

        return True

    def ParseMiscCores(self):
        """ 
        Sets up the miscellaneous cores for management and control tasks 
        """
        if 'ext_cores' not in self.cfg.TopologyCfg:
            self.logger.error('Couldn\'t find ext_cores section in topology config')
            return False
        
        if 'ext_cores_smt' not in self.cfg.TopologyCfg:
            self.logger.error('Couldn\'t find ext_cores_smt section in topology config')
            return False

        smt = SMTSetting.SMT_ENABLED if self.cfg.TopologyCfg.ext_cores_smt else SMTSetting.SMT_DISABLED
        self.top.SetMiscCoreSmt(smt)

        # Miscellaneous cores are in the format (name, numa_node, hyperthreading)
        self.logger.info('Parsing top-level miscellaneous cores')
        for i in self.cfg.TopologyCfg.ext_cores:
            try:
                c = int(magicattr.get(self.cfg, i))
                self.top.AddMiscCore(Core(i, 0, NICCoreDirection.NIC_CORE_DIRECTION_NONE, NUMASetting.LOGICAL_NUMA_DONT_CARE, c))
            except AttributeError as e:
                self.logger.error(f'Failed to parse field {i} from config file:\n    {e}')
                return False

        return True

    def ParseModGroups(self) -> bool:
        """ 
        Sets all the module groups in the topology structure 
        """
        if 'mod_defs' not in self.cfg.TopologyCfg:
            self.logger.debug('No module definitions found in topology config')
            return False

        if 'map_type' not in self.cfg.TopologyCfg:
            self.logger.error('Couldn\'t find map_type in topology config')
            return False
        
        self.top.SetTopMapType(self.cfg.TopologyCfg.map_type)

        for m in self.cfg.TopologyCfg.mod_defs:
            self.logger.debug(f'Searching config for modules with pattern {m.module}')
            if m.module in self.cfg:
                self.logger.info(f'Found module {m.module} at top level in config. Parsing topology...')

                for idx,mi in enumerate(self.cfg[m.module]):
                    self.logger.info(f'Processing module {mi.module} of type {m.module}')
                    clist = []
                    pg = ProcGroup()

                    mattr = f'{m.module}[{idx}]'
                    if 'helper_cores' in m:
                        if 'helper_cores_smt' not in m:
                            self.logger.error('helper_cores_smt not defined in module!')
                            return False

                        smt = SMTSetting.SMT_ENABLED if m.helper_cores_smt else SMTSetting.SMT_DISABLED
                        pg.SetHelperSmt(smt)

                        for hc in m.helper_cores:
                            name = f'{mattr}.{hc}'
                            attr = magicattr.get(self.cfg, name)
                            if type(attr) == list:
                                for idx,c in enumerate(attr):
                                    nname = f'{name}[{idx}]'
                                    self.logger.info(f'Adding helper core {nname}')
                                    c = int(magicattr.get(self.cfg, nname))
                                    pg.AddMiscCore(Core(nname, 0, NICCoreDirection.NIC_CORE_DIRECTION_NONE, NUMASetting.LOGICAL_NUMA_GROUP, c))
                            else:
                                self.logger.info(f'Adding helper core {name}')
                                c = int(magicattr.get(self.cfg, name))
                                pg.AddMiscCore(Core(name, 0, NICCoreDirection.NIC_CORE_DIRECTION_NONE, NUMASetting.LOGICAL_NUMA_GROUP, c))
                    else:
                        self.logger.debug(f'Helper cores not found in {mi.module}')

                    if 'data_vlan' in m:
                        name = f'{mattr}.{m.data_vlan}'
                        self.logger.info(f'Setting data plane VLAN to {name}')
                        pg.SetDataVlan(VLANInfo(name, 0)) # Don't need to set the VLAN from the config since it doesn't matter

                    # Parse data path groups. 
                    if 'dp_group' in m:
                        self.logger.info(f'Processing data path group for module {mi.module}')

                        try: 
                            attr = magicattr.get(self.cfg, f'{mattr}.{m.dp_group.name}')
                        except:
                            self.logger.error(f'Could not find attribute {m.dp_group.name} in {mattr}')
                            return False

                        if len(attr) != 1:
                            self.logger.error('DP groups of multiple NUMA nodes not supported yet!')
                            return False

                        if len(attr[0].rx_cores) != len(attr[0].tx_cores) != len(attr[0].rx_speeds) != len(attr[0].tx_speeds):
                            self.logger.error('Error in module {mattr} Must have same number of cores in speeds and core list in a DP group!')
                            return False

                        smtp = SMTSetting.SMT_ENABLED if m.dp_group.proc_cores_smt else SMTSetting.SMT_DISABLED
                        pg.SetProcSmt(smtp)

                        self.logger.info(f'Found {len(attr[0].rx_cores)*2} NIC cores in GPU map')
                        try:
                            for gidx in range(len(attr[0].rx_cores)):
                                name = f'{mattr}.{m.dp_group.name}[0].rx_cores[{gidx}]'
                                rx_speed = magicattr.get(self.cfg, f'{mattr}.{m.dp_group.name}[0].rx_speeds[{gidx}]')
                                c = int(magicattr.get(self.cfg, name))
                                rx_core = Core(name, rx_speed, NICCoreDirection.NIC_CORE_DIRECTION_RX, NUMASetting.LOGICAL_NUMA_GROUP, c)
                                pg.AddGroupCore(rx_core)

                                name = f'{mattr}.{m.dp_group.name}[0].tx_cores[{gidx}]'
                                tx_speed = magicattr.get(self.cfg, f'{mattr}.{m.dp_group.name}[0].tx_speeds[{gidx}]')
                                c = int(magicattr.get(self.cfg, name))
                                tx_core = Core(name, tx_speed, NICCoreDirection.NIC_CORE_DIRECTION_TX, NUMASetting.LOGICAL_NUMA_GROUP, c)
                                pg.AddGroupCore(tx_core)
    

                                #if self.cfg.dual_port: - use this to move dual_port to top level of the triad cfg
                                try:
                                    # See if dual_port is set to true/false in the config file
                                    dual_port = self.cfg.TopologyCfg.dual_port
                                except:
                                    # Detect missing dual_port parameter in config and set it to false
                                    self.logger.warn(f'dual_port parameter NOT detected in topology config - setting to False')
                                    dual_port = False

                                if dual_port:
                                # if dual port is detected - add backup cores
                                    self.logger.warn(f'dual_port  setting detected - will configure failover NIC')

                                self.top.AddNicPairing(rx_core, tx_core, dual_port)

                        except:
                            self.logger.error('Error when parsing NIC fields. Maybe forgot rx/tx_speeds?')
                            return False

                        try:
                            # CPU workers (no GPUs)
                            self.logger.info(f'Found {len(attr[0].cpu_workers)} CPU worker cores')
                            for cidx in range(len(attr[0].cpu_workers)):
                                cpuname = f'{mattr}.{m.dp_group.name}[0].cpu_workers[{cidx}]'
                                c = int(magicattr.get(self.cfg, cpuname))
                                cpucore = Core(cpuname, 0, NICCoreDirection.NIC_CORE_DIRECTION_NONE, NUMASetting.LOGICAL_NUMA_GROUP, c)
                                pg.AddGroupCore(cpucore)                                
                        except:
                            self.logger.info('No CPU workers found, or using the wrong format. Moving on...')                          

                        self.logger.info(f'Found {len(attr[0].gpu_map)} GPU cores in GPU map')
                        gpumap = collections.defaultdict(list)

                        for gidx in range(len(attr[0].gpu_map)):
                            cpuname = f'{mattr}.{m.dp_group.name}[0].gpu_map[{gidx}][0]'
                            gpu_dev_id = f'{mattr}.{m.dp_group.name}[0].gpu_map[{gidx}][1]'

                            gpumap[attr[0].gpu_map[gidx][1]].append((gpu_dev_id, cpuname))

                        if 'gpu_type' in m.dp_group:
                            gputype = pg.GetGpuType(m.dp_group.gpu_type)
                        else:
                            self.logger.info('No GPU type specified in dp_groups. Allowing any type')
                            gputype = pg.GetGpuType("ANY")

                        for gkey, gval in gpumap.items():
                            clist = [Core(x[1], 0, NICCoreDirection.NIC_CORE_DIRECTION_NONE, NUMASetting.LOGICAL_NUMA_GROUP, int(magicattr.get(self.cfg, x[1]))) for x in gval]
                            gdevlist = [x[0] for x in gval]
                            self.logger.info(f'Adding {len(clist)} CPU cores for GPU device')

                            pg.SetGpuType(gputype)
                            pg.AddGroupGPU(GPU(clist, gdevlist, gputype, gkey))

                    if 'nic_cores' in m:
                        self.logger.info(f'Processing NIC cores (non-data path) for module {mi.module}')
                        if len(m.nic_cores) != 5:
                            self.logger.error(f'Wrong number of parameters for NIC cores specification in {m.module}')
                            return False

                        try: 
                            rx_cores  = magicattr.get(self.cfg, f'{mattr}.{m.nic_cores[0]}')
                            rx_speeds = magicattr.get(self.cfg, f'{mattr}.{m.nic_cores[1]}')
                            tx_cores  = magicattr.get(self.cfg, f'{mattr}.{m.nic_cores[2]}')
                            tx_speeds = magicattr.get(self.cfg, f'{mattr}.{m.nic_cores[3]}')
                        except:
                            self.logger.error(f'Could not find NIC attributes for {m.module} in {mattr}')
                            return False                            
                    
                        if len(rx_cores) != len(rx_speeds) != len(tx_cores) != len(tx_speeds):
                            self.logger.error(f'All speed and core lengths must be the same for {mattr}')
                            return False

                        smtc = SMTSetting.SMT_ENABLED if m.nic_cores[4] else SMTSetting.SMT_DISABLED
                        pg.SetProcSmt(smtc)
                        for gidx in range(len(rx_cores)):
                            name = f'{mattr}.{m.nic_cores[0]}[{gidx}]'
                            rx_speed = magicattr.get(self.cfg, f'{mattr}.{m.nic_cores[1]}[{gidx}]')
                            self.logger.info(f'Adding core {name} with speed {rx_speed}')
                            c = int(magicattr.get(self.cfg, name))
                            rx_core = Core(name, rx_speed, NICCoreDirection.NIC_CORE_DIRECTION_RX, NUMASetting.LOGICAL_NUMA_GROUP, c)
                            pg.AddGroupCore(rx_core)

                            name = f'{mattr}.{m.nic_cores[2]}[{gidx}]'
                            tx_speed = magicattr.get(self.cfg, f'{mattr}.{m.nic_cores[3]}[{gidx}]')
                            self.logger.info(f'Adding core {name} with speed {tx_speed}')
                            c = int(magicattr.get(self.cfg, name))
                            tx_core = Core(name, tx_speed, NICCoreDirection.NIC_CORE_DIRECTION_TX, NUMASetting.LOGICAL_NUMA_GROUP, c)
                            pg.AddGroupCore(tx_core)

                            self.top.AddNicPairing(rx_core, tx_core)

                    self.top.proc_groups.append(pg)
            else:
                self.logger.error(f'Module {m.module} not found in config. Make sure each topology module has a real module mapping to it')
                return False

        return True

    def ParseNet(self):
        """
        Parse the network configuration section of a Triad config file
        """
        if 'Network_Config' not in self.cfg:
            self.logger.error('No network config found!')
            return False

        for net in self.cfg.Network_Config:
            for rxi,_ in enumerate(net.rxCores):
                ng = self.top.GetNICGroupFromCoreNumbers(int(net.rxCores[rxi]), int(net.txCores[rxi]))

                if ng is None:
                    self.logger.error(f'Failed to get NIC group for cores {net.rxCores[rxi]}, {net.txCores[rxi]}')
                    return False             
                
                ng.AddInterface(net.mac)


                try:
                    rs = int(net.rx_mbufs[rxi])
                    ng.SetRxRingSize(rs)
                except AttributeError as e:
                    self.logger.info(f'rx_mbufs field not found. Most likely an old config: {e}')                       
        
        return True

    def CfgToTopology(self, parseNet: bool):
        """
        Converts a configuration format into a topology format.
        """

        if 'TopologyCfg' not in self.cfg:
            self.logger.error('Topology configuration section not found in triad config! Bailing...')
            return None

        self.logger.info('Found topology section in config')

        if not self.CheckMandatoryFields():
            self.logger.error('Not all mandatory fields in config present. Bailing')
            return None

        if self.top.SetCpuArch(self.cfg.TopologyCfg.cpu_arch) is None:
            self.logger.error('Failed to set CPU architecture!')
            return None

        if not self.ParseMiscCores():
            self.logger.error('Failed to set miscellaneous cores!')
            return None

        if not self.ParseKniDataVlan():
            self.logger.error('Failed to set KNI data VLAN!')
            return None

        if not self.ParseModGroups():
            self.logger.error('Failed to parse data path groups!')
            return None
        else:
            self.logger.info(f'Finished parsing config file, and found {len(self.top.proc_groups)} processing groups')

        if not self.ParseHugePages():
            self.logger.fatal('Could not parse hugepage information from config!')
            return None

        # Note that the network config is not strictly necessary when scheduling a pod. It's only used when reading
        # configs in from pods that have been deployed to load their resources.
        if parseNet and not self.ParseNet():
            self.logger.error('Failed to parse network config!')
            return None

        return self.top
   
    def SetLibConfigValue(self, name, value):
        """ libconf wrote its own version of AttrDict, which doesn't have the propper setters for certain types.
            The code here is mostly due to trial and error of what works on each type. """
        if name[-1] == ']':
            magicattr.set(self.cfg, name, value)
            return

        pos = name.rfind('.')
        if pos >= 0:
            pos = name.rfind('.')
            prop = magicattr.get(self.cfg, name[:pos])
            prop[name[pos+1:]] = value
        else:
            self.cfg[name] = value

    def TopologyToGpuMap(self):
        """ Create a list of pod GPU mappings in a string form  from the proc_groups topology object
        """
        gpu_annotations = {}
        for c in self.top.proc_groups:
            if len(c.group_gpus) > 0:
                index = 0
                for g in c.group_gpus:
                    for gidx in range(len(g.dev_id_names)):
                        devname="nvidia"+str(index)
                        gpu_annotations[devname] = g.device_id
                        index += 1
 
        return gpu_annotations


    def TopologyToCfg(self) -> str:
        """ Translates the topology mapping with physical resources back into the triad configuration that previously held
            fake (placeholder) resources. This function assumes the topology structure in self.top has already been populated
            with appropriate values externally. Note that because libconfig doesn't support nested lists (only tuples), we
            can't just do a straightforward monkey patch to the libconfig structure. We must first build up what we have for
            each dp_group, and write it as a one-shot nested tuple. """

        # Top-level cores
        for c in self.top.misc_cores:
            self.SetLibConfigValue(c.name, c.core)

        # Ctrl VLAN
        self.SetLibConfigValue(self.top.ctrl_vlan.name, self.top.ctrl_vlan.vlan)

        for c in self.top.proc_groups:
            self.SetLibConfigValue(c.vlan.name, c.vlan.vlan)

            for pc in c.proc_cores:
                self.SetLibConfigValue(pc.name, pc.core)               

            for mc in c.misc_cores:
                self.SetLibConfigValue(mc.name, mc.core)

            if len(c.group_gpus) > 0:
                gpu_map = []
                for g in c.group_gpus:
                    # Build the nested tuple, if needed. See comment above for why
                    for gidx in range(len(g.dev_id_names)):
                        gpu_map.append((g.cpu_cores[gidx].core, g.device_id))

                # Now patch the entire tuple in. We need to know the name of the tuple, which we can figure out as a hack
                # by just looking at the last separator on the core mapping, which marks the groups.
                gpu_start = c.group_gpus[0].dev_id_names[0][:c.group_gpus[0].dev_id_names[0].rfind('.')]

                # Do not try to get tricky here and set gpu_map in the attribute format! The author of libconf overwrote the
                # AttrDict class and made their own. That version does not have a proper way to set attributes, so calling
                # magicattr.set on it will not work! It won't error, but it will continue on without writing any value. It
                # MUST be set using traditional dict indexing.
                prop = magicattr.get(self.cfg, gpu_start)
                prop['gpu_map'] = tuple(gpu_map)
        
        # Top-level networking
        ncfg = self.PopulateNetCfg()

        self.SetLibConfigValue('Network_Config', ncfg)

        return libconf.dumps(self.cfg)


    def PopulateNetCfg(self):
        """ 
        Populates the network config section given the cores that have been filled out 
        """
        maccfgs = collections.defaultdict(list)
        for c in self.top.nic_core_pairing:
            maccfgs[c.mac].append((c.rx_core.core, c.tx_core.core, c.rx_ring_size))

        # Now that we've populated all resources per MAC, make the actual libconfig structure
        netconf = []
        ifcnt = 0

        # For now we use the same gateway IP on every interface.

        for k,v in maccfgs.items():
            rxcores,txcores,rs = zip(*v)
            gwips = [self.top.data_default_gw] * len(rxcores)
            ips = [f'10.0.0.{x+ifcnt}' for x in range(len(rxcores))]
            ifconf = {
                'module'    : f'fake_{ifcnt}',
                'ifname'    : f'fake_if_{ifcnt}',
                'mac'       : k,
                'rxCores'   : list(rxcores),
                'txCores'   : list(txcores),
                'rx_mbufs'  : list(rs),
                'gwIps'     : gwips,
                'txIps'     : ips,
                'rxIps'     : ips,
                'ts_group' : True
            }

            netconf.append(ifconf)
            ifcnt += len(rxcores)

        return tuple(netconf)


                
