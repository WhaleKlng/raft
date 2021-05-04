# coding: utf-8

__author__ = 'zhenhang.sun@gmail.com'
__version__ = '1.0.0'

import sys

from raft.my_gui import GUI
from raft.node2 import Node2
from test.my_node import MNode

sys.path.append("..")

if __name__ == '__main__':
    conf = {'id': 'node_1',
            'addr': ('localhost', 10001),
            'peers': {'node_2': ('localhost', 10002),
                      'node_3': ('localhost', 10003)
                      }
            }
    server1 = GUI()

    node = Node2(conf, gui=server1)

    # node.run()
