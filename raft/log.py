# coding: utf-8

__author__ = 'zhenhang.sun@gmail.com'
__version__ = '1.0.0'

import os
import json
from typing import List


class Log(object):

    def __init__(self, path):
        self.empowered_data = {}  # 赋能场景:已经隐私保护的隐私池子
        self.empower_pool = [634601, 633572, 633572, 621906, 665903]  # 赋能场景：所提供的短接号池子
        self.file_path = path + '/log.json'

        if os.path.exists(self.file_path):
            with open(self.file_path, 'r') as f:
                self.entries = json.load(f)
        else:
            self.entries = []

    @property
    def last_log_index(self):
        # print('last_log_index')
        # if len(self.entries)==0:
        #     return 0
        return len(self.entries) - 1

    @property
    def last_log_term(self):
        return self.get_log_term(self.last_log_index)

    def get_log_term(self, log_index):
        '''
        leader do 
        follower
        '''
        if log_index >= len(self.entries):
            return -1
        elif log_index < 0:
            return -1
        else:
            return self.entries[log_index]['term']

    def get_entries(self, next_index):
        '''
        leader do
        '''
        # print('get_entries')
        return self.entries[max(0, next_index):]

    def delete_entries(self, prev_log_index):
        # print('delete_entries')

        self.entries = self.entries[:max(0, prev_log_index)]
        self.save()

    def append_entries(self, prev_log_index, entries: List[dict]):
        # print('append_entries')
        # 美团赋能场景
        self.entries = self.entries[:max(0, prev_log_index + 1)] + entries

        self.save()

    def save(self):
        with open(self.file_path, 'w') as f:
            json.dump(self.entries, f, indent=4)
