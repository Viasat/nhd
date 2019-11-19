from abc import ABC, abstractmethod
from enum import Enum

class CfgType(Enum):
    CFG_TYPE_TRIAD = 1



class CfgParser(ABC):
    def __init__(self, cfgtype: CfgType, cfg: str, isfile: bool):
        self.cfgtype = cfgtype
        super().__init__()

    @abstractmethod
    def TopologyToCfg(self):
        """
        Converts an existing topology structure back into the config format needed by the parser.
        """
        pass

    @abstractmethod
    def CfgToTopology(self):
        """
        Converts a configuration format into a topology format.
        """        
        pass

    @abstractmethod
    def CheckMandatoryFields(self):
        """
        Checks whether mandatory fields are present in the config structure
        """        
        pass        