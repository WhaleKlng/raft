# coding: utf-8

__author__ = 'zhenhang.sun@gmail.com'
__version__ = '1.0.0'

import sys

from raft.Minix import ServerNode
from test.my_node import MNode

sys.path.append("..")

if __name__ == '__main__':
    conf = {'id': 'node_3',
            'addr': ('localhost', 10003),
            'peers': {'node_1': ('localhost', 10001),
                      'node_2': ('localhost', 10002)
                      }
            }

    node = ServerNode(conf)

