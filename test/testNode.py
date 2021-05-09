# coding: utf-8

__author__ = 'zhenhang.sun@gmail.com'
__version__ = '1.0.0'

import sys

from raft.Minix import ServerNode

sys.path.append("..")

if __name__ == '__main__':
    conf = {'id': 'node_1',
            'addr': ('localhost', 10001),
            'peers': {'node_2': ('localhost', 10002),
                      'node_3': ('localhost', 10003)
                      }
            }
    #  实例化了一个对象node
    # 永远先执行__init__方法
    node = ServerNode(conf)

