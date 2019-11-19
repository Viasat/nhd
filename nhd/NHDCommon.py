import logging
from colorlog import ColoredFormatter
import os
from enum import Enum

"""
Common class for all NHD functions.
"""
class NHDCommon:
    NHD_LOGFMT = '%(asctime)s.%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'
    NHD_DATEFMT = '%Y-%m-%d:%H:%M:%S'
    NHD_LOGCOLOR = { 'DEBUG'   : 'bold_orange',       
                    'INFO' : 'reset',
                    'WARNING' : 'bold_yellow', 
                    'ERROR': 'bold_red',
                    'CRITICAL': 'bold_red' }             

    @staticmethod
    def GetLogger(name):
        l = logging.getLogger(name)

        if not l.hasHandlers():
            l.setLevel(logging.INFO)
            ch = logging.StreamHandler()
            logformat = NHDCommon.NHD_LOGFMT
            date_format = NHDCommon.NHD_DATEFMT
            if os.isatty(2):
                cformat = '%(log_color)s' + logformat
                f = ColoredFormatter(cformat, date_format,
                    log_colors = NHDCommon.NHD_LOGCOLOR)
            else:
                f = logging.Formatter(logformat, date_format)

            ch.setFormatter(f)
            l.addHandler(ch)

        return l         

class RpcMsgType(Enum):
    TYPE_NODE_INFO = 1     
