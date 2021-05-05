import os
import json
import threading
import time
import socket
import random
import logging
from typing import List

import rsa

from .log import Log
from tkinter import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

LEADER = "leader"
FOLLOWER = "follower"
CANDIDATE = "candidate"


class ServerNode:

    def __init__(self, conf):
        self.role = 'follower'
        self.id = conf['id']
        self.addr = conf['addr']

        self.peers = conf['peers']
        self.pubkey, self.privkey = rsa.newkeys(1024)  # 加密处理

        self.init_window = Tk()
        self.set_init_window()

        # persistent state
        self.current_term = 0
        self.voted_for = None

        if not os.path.exists(self.id):
            os.mkdir(self.id)

        # init persistent state
        self.load()
        self.log = Log(self.id)

        # volatile state
        # rule 1, 2
        self.commit_index = 0
        self.last_applied = 0

        # volatile state on leaders
        # rule 1, 2
        self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers}
        self.match_index = {_id: -1 for _id in self.peers}

        # append entries
        self.leader_id = None

        # request vote
        self.vote_ids = {_id: 0 for _id in self.peers}

        # client request
        self.client_addr = None

        # tick
        self.wait_ms = (10, 20)
        self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)
        self.next_heartbeat_time = 0

        self.key_pool = {}
        # msg send and recv
        self.ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ss.bind(self.addr)
        self.ss.settimeout(2)

        self.cs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.init_window.mainloop()

    def load(self):
        self.public_key_Text.insert(END, self.pubkey.save_pkcs1())
        self.sec_key_Text.insert(END, self.privkey.save_pkcs1())
        file_path = self.id + '/key.json'
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                data = json.load(f)

            self.current_term = data['current_term']
            self.voted_for = data['voted_for']

        else:
            self.save()

    def save(self):
        data = {'current_term': self.current_term,
                'voted_for': self.voted_for,
                }

        file_path = self.id + '/key.json'
        with open(file_path, 'w') as f:
            json.dump(data, f)

    def get_pub_key_by_addr(self, addr):
        for k in self.peers:
            if self.peers[k] == addr:
                return rsa.PublicKey.load_pkcs1(self.key_pool[k])

    def send(self, msg: dict, addr):
        msg_type = msg['type']
        if msg_type == 'request_vote':  # 第一步
            public_key_str = self.pubkey.save_pkcs1().decode()
            # print("{}成为候选者并且发送公钥:{}".format(self.id, public_key_str))
            msg['p_key'] = public_key_str
            rst = json.dumps(msg).encode()
        elif msg_type == "request_vote_response":  # 第三步： 响应征票请求  需要加密消息并拼接一下
            rst = json.dumps(msg)
            # print("中间件拦截到了更随着的征票响应数据：{}".format(rst))
            crypto_text = rsa.encrypt(rst.encode(), self.get_pub_key_by_addr(addr))
            rst = b"votes_response " + crypto_text
            # print("中间件后续处理消息为：{}".format(rst))
            # self.cs.sendto(rst, addr)
        else:
            rst = json.dumps(msg).encode()
        self.cs.sendto(rst, addr)

    def recv(self):
        msg, addr = self.ss.recvfrom(65535)
        # 如果收到的消息前缀中包含：vote_response的话，就切片处理之
        # msg 现在是一个字符串
        # print(msg)
        # print(type(msg))
        if b"votes_response " in msg:  # 收到的是征票响应类信息
            # print("进来了")
            data = msg[15:]  # 得到密文
            rst = json.loads(rsa.decrypt(data, self.privkey).decode())  # 解密之
            # print(rst)
            return rst, addr
        else:  # 普通信息
            rst = json.loads(msg)
            if rst['type'] == "request_vote":  # 第二步：是征票请求
                # print("{}节点收到候选者{}的征票请求，且密码是{}".format(self.id, rst['src_id'], rst['p_key']))
                self.key_pool[rst['src_id']] = rst['p_key']
                # print("存入节点公钥成功,现在的车子是{}".format(self.key_pool))
            return rst, addr

    def redirect(self, data, addr):
        if data is None:
            return None

        if data['type'] == 'client_append_entries':
            if self.role != 'leader':
                if self.leader_id:
                    self.my_log('redirect: client_append_entries to leader')
                    self.send(data, self.peers[self.leader_id])
                return None
            else:
                self.client_addr = addr
                return data

        if data['dst_id'] != self.id:
            self.my_log('redirect: to ' + data['dst_id'])
            # self.my_log('redirec to leader')
            self.send(data, self.peers[data['dst_id']])
            return None
        else:
            return data

    def append_entries(self, data):
        """
        追加条目
        :param data:
        :return:
        """
        response = {'type': 'append_entries_response',
                    'src_id': self.id,
                    'dst_id': data['src_id'],
                    'term': self.current_term,
                    'success': False
                    }

        # append_entries: rule 1
        if data['term'] < self.current_term:
            self.my_log('          2. smaller term')
            self.my_log('          3. success = False: smaller term')
            self.my_log('          4. send append_entries_response to leader ' + data['src_id'])
            response['success'] = False
            self.send(response, self.peers[data['src_id']])
            return

        self.leader_id = data['leader_id']

        # heartbeat
        if not data['entries']:
            self.my_log('          4. heartbeat')
            return

        prev_log_index = data['prev_log_index']
        prev_log_term = data['prev_log_term']

        tmp_prev_log_term = self.log.get_log_term(prev_log_index)

        # append_entries: rule 2, 3
        # append_entries: rule 3
        if tmp_prev_log_term != prev_log_term:
            self.my_log('          4. success = False: index not match or term not match')
            self.my_log('          5. send append_entries_response to leader ' + data['src_id'])
            self.my_log('          6. log delete_entries')
            self.my_log('          6. log save')

            response['success'] = False
            self.send(response, self.peers[data['src_id']])
            self.log.delete_entries(prev_log_index)

        # append_entries rule 4
        else:
            self.my_log('          4. success = True')
            self.my_log('          5. send append_entries_response to leader ' + data['src_id'])
            self.my_log('          6. log append_entries')
            self.my_log('          7. log save')

            response['success'] = True
            self.send(response, self.peers[data['src_id']])
            entries = data['entries']
            # 在Minix中挂载log组件中的虚拟号码
            self.sankuai_helper(entries)
            self.log.append_entries(prev_log_index, entries)

            # append_entries rule 5
            leader_commit = data['leader_commit']
            if leader_commit > self.commit_index:
                commit_index = min(leader_commit, self.log.last_log_index)
                self.commit_index = commit_index
                self.my_log('          8. commit_index = ' + str(commit_index))

        return

    def request_vote(self, data):
        '''
        request vote rpc
        only used in follower state
        '''

        response = {'type': 'request_vote_response',
                    'src_id': self.id,
                    'dst_id': data['src_id'],
                    'term': self.current_term,
                    'vote_granted': False
                    }

        # request vote: rule 1
        if data['term'] < self.current_term:
            self.my_log('          2. smaller term')
            self.my_log('          3. success = False')
            self.my_log('          4. send request_vote_response to candidate ' + data['src_id'])
            response['vote_granted'] = False
            self.send(response, self.peers[data['src_id']])
            return

        self.my_log('          2. same term')
        candidate_id = data['candidate_id']
        last_log_index = data['last_log_index']
        last_log_term = data['last_log_term']

        if self.voted_for is None or self.voted_for == candidate_id:
            if last_log_index >= self.log.last_log_index and last_log_term >= self.log.last_log_term:
                self.voted_for = data['src_id']
                self.save()
                response['vote_granted'] = True
                self.send(response, self.peers[data['src_id']])
                self.my_log('          3. success = True: candidate log is newer')
                self.my_log('          4. send request_vote_response to candidate ' + data['src_id'])
            else:
                self.voted_for = None
                self.save()
                response['vote_granted'] = False
                self.send(response, self.peers[data['src_id']])
                self.my_log('          3. success = False: candidate log is older')
                self.my_log('          4. send request_vote_response to candidate ' + data['src_id'])
        else:
            response['vote_granted'] = False
            self.send(response, self.peers[data['src_id']])
            self.my_log('          3. success = False: has vated for ' + self.voted_for)
            self.my_log('          4. send request_vote_response to candidate ' + data['src_id'])

    def all_do(self, data):
        '''
        all servers: rule 1, 2
        '''

        # self.my_log('----------------------------all----------------------------------')

        if self.commit_index > self.last_applied:
            self.last_applied = self.commit_index
            self.my_log('all: 1. last_applied = ' + str(self.last_applied))

        if data is None:
            return

        if data['type'] == 'client_append_entries':
            return

        if data['term'] > self.current_term:
            self.my_log('all: 1. bigger term')
            self.my_log('     2. become follower')
            self.role = 'follower'
            self.current_term = data['term']
            self.voted_for = None
            self.save()

        return

    def follower_do(self, data):

        '''
        rules for servers: follower
        '''
        self.my_log('--------------------------follower--------------------------------')

        t = time.time()
        # follower rules: rule 1
        if data:

            if data['type'] == 'append_entries':
                self.my_log('follower: 1. recv append_entries from leader ' + data['src_id'])

                if data['term'] == self.current_term:
                    self.my_log('          2. same term')
                    self.my_log('          3. reset next_leader_election_time')
                    self.next_leader_election_time = t + random.randint(*self.wait_ms)
                self.append_entries(data)

            elif data['type'] == 'request_vote':
                self.my_log('follower: 1. recv request_vote from candidate ' + data['src_id'])
                self.request_vote(data)

        # follower rules: rule 2
        if t > self.next_leader_election_time:
            self.my_log('follower：1. become candidate')
            self.next_leader_election_time = t + random.randint(*self.wait_ms)
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.id
            self.save()
            self.vote_ids = {_id: 0 for _id in self.peers}

        return

    def candidate_do(self, data):
        '''
        rules for fervers: candidate
        '''
        self.my_log('--------------------------candidate--------------------------------')

        t = time.time()
        # candidate rules: rule 1
        for dst_id in self.peers:
            if self.vote_ids[dst_id] == 0:
                self.my_log('candidate: 1. send request_vote to peer ' + dst_id)
                request = {
                    'type': 'request_vote',
                    'src_id': self.id,
                    'dst_id': dst_id,
                    'term': self.current_term,
                    'candidate_id': self.id,
                    'last_log_index': self.log.last_log_index,
                    'last_log_term': self.log.last_log_term,
                    'p_key': self.pubkey  # 候选者同时广播个人公钥
                }
                # logging.info(request)

                self.send(request, self.peers[dst_id])

        # if data != None and data['term'] < self.current_term:
        #     self.my_log('candidate: 1. smaller term from ' + data['src_id'])
        #     self.my_log('           2. ignore')
        # return

        if data and data['term'] == self.current_term:
            # candidate rules: rule 2
            if data['type'] == 'request_vote_response':
                self.my_log('candidate: 1. recv request_vote_response from follower ' + data['src_id'])

                self.vote_ids[data['src_id']] = data['vote_granted']
                vote_count = sum(list(self.vote_ids.values()))

                if vote_count >= len(self.peers) // 2:
                    self.my_log('           2. become leader')
                    self.role = 'leader'
                    self.voted_for = None
                    self.save()
                    self.next_heartbeat_time = 0
                    self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers}
                    self.match_index = {_id: 0 for _id in self.peers}
                    return

            # candidate rules: rule 3
            elif data['type'] == 'append_entries':
                self.my_log('candidate: 1. recv append_entries from leader ' + data['src_id'])
                self.my_log('           2. become follower')
                self.next_leader_election_time = t + random.randint(*self.wait_ms)
                self.role = 'follower'
                self.voted_for = None
                self.save()
                return

        # candidate rules: rule 4
        if t > self.next_leader_election_time:
            self.my_log('candidate: 1. leader_election timeout')
            self.my_log('           2. become candidate')
            self.next_leader_election_time = t + random.randint(*self.wait_ms)
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.id
            self.save()
            self.vote_ids = {_id: 0 for _id in self.peers}
            return

    def leader_do(self, data):
        '''
        rules for fervers: leader
        '''
        self.my_log('--------------------------leader--------------------------------')

        # leader rules: rule 1, 3
        t = time.time()
        if t > self.next_heartbeat_time:
            self.next_heartbeat_time = t + random.randint(0, 5)

            for dst_id in self.peers:
                self.my_log('leader：1. send append_entries to peer ' + dst_id)

                request = {'type': 'append_entries',
                           'src_id': self.id,
                           'dst_id': dst_id,
                           'term': self.current_term,
                           'leader_id': self.id,
                           'prev_log_index': self.next_index[dst_id] - 1,
                           'prev_log_term': self.log.get_log_term(self.next_index[dst_id] - 1),
                           'entries': self.log.get_entries(self.next_index[dst_id]),
                           'leader_commit': self.commit_index
                           }
                # leader先设置自己的日志
                # 等待下一轮再转发给自己的跟随者们
                self.send(request, self.peers[dst_id])

        # leader rules: rule 2
        if data and data['type'] == 'client_append_entries':
            data['term'] = self.current_term
            self.sankuai_helper([data])
            self.log.append_entries(self.log.last_log_index, [data])

            self.my_log('leader：1. recv append_entries from client')
            self.my_log('        2. log append_entries')
            self.my_log('        3. log save')

            return

        # leader rules: rule 3.1, 3.2
        if data and data['term'] == self.current_term:
            if data['type'] == 'append_entries_response':
                self.my_log('leader：1. recv append_entries_response from follower ' + data['src_id'])
                if data['success'] is False:
                    self.next_index[data['src_id']] -= 1
                    self.my_log('        2. success = False')
                    self.my_log('        3. next_index - 1')
                else:
                    self.match_index[data['src_id']] = self.next_index[data['src_id']]
                    self.next_index[data['src_id']] = self.log.last_log_index + 1
                    self.my_log('        2. success = True')
                    logging.info(
                        '        3. match_index = ' + str(self.match_index[data['src_id']]) + ' next_index = ' + str(
                            self.next_index[data['src_id']]))

        # leader rules: rule 4
        while True:
            N = self.commit_index + 1

            count = 0
            for _id in self.match_index:
                if self.match_index[_id] >= N:
                    count += 1
                if count >= len(self.peers) // 2:
                    self.commit_index = N
                    self.my_log('leader：1. commit + 1')
                    if self.client_addr:
                        response = {'type': 'append_entries_ok', 'index': self.commit_index,
                                    "empowered_data": self.log.empowered_data}
                        self.send(response, (self.client_addr[0], 10000))

                    break
            else:
                self.my_log('leader：2. commit = ' + str(self.commit_index))
                break

    def run(self):
        while True:
            self.append_entry_pool_text(self.log.entries)
            # print(self.log.entries)
            self.change_window_title("{}:{}".format(self.id, self.role))
            if self.role == LEADER:
                self.set_node_leader(self.id)
            else:
                self.set_node_leader(self.leader_id if self.leader_id else "等待选举")
            try:
                try:
                    data, addr = self.recv()
                except Exception as e:
                    logging.info(e)
                    data, addr = None, None

                data = self.redirect(data, addr)

                self.all_do(data)

                if self.role == 'follower':
                    self.follower_do(data)

                if self.role == 'candidate':
                    self.candidate_do(data)

                if self.role == 'leader':
                    self.leader_do(data)

            except Exception as e:
                logging.info(e)

    def generate_4_random_number(self):
        return "{}{}{}{}".format(random.randint(0, 9), random.randint(0, 9), random.randint(0, 9), random.randint(0, 9))

    def sankuai_helper(self, entries: List):
        for entry in entries:
            real_number = entry.get("real_number", "")
            if real_number not in self.log.empowered_data.values():
                while True:
                    short_num = random.choice(self.log.empower_pool)
                    random_num = self.generate_4_random_number()
                    cur_key = "{}#{}".format(short_num, random_num)
                    if cur_key not in self.log.empowered_data:
                        self.log.empowered_data[cur_key] = real_number
                        break

    # GUI 设置窗口

    def my_log(self, data):
        logging.info(data)
        self.ins_log(data)

    def set_init_window(self):
        self.init_window.title(self.id)  # 窗口名
        # 290 160为窗口大小，+10 +10 定义窗口弹出时的默认展示位置
        self.init_window.geometry('1068x681+10+10')
        # 节点Leader
        self.node_leader_label = Label(self.init_window, text="节点Leader", justify=LEFT)
        self.node_leader_label.grid(row=0, column=0)
        # 节点公钥
        self.public_key_label = Label(self.init_window, text="节点公钥", justify=LEFT)
        self.public_key_label.grid(row=2, column=0)
        # 节点私钥
        self.sec_key_label = Label(self.init_window, text="节点私钥", justify=LEFT)
        self.sec_key_label.grid(row=4, column=0)
        # # 收取的消息
        # self.recv_msg_label = Label(self.init_window, text="收取的消息")
        # self.recv_msg_label.grid(row=5, column=0)
        # # 内容池
        self.data_pool_label = Label(self.init_window, text="内容池")
        self.data_pool_label.grid(row=6, column=0)
        self.log_data_label = Label(self.init_window, text="日志", justify=LEFT)
        self.log_data_label.grid(row=0, column=12)
        # 标签
        self.node_leader_Text = Text(self.init_window, width=66, height=1.5)
        self.node_leader_Text.grid(row=1, column=0, columnspan=10)
        self.public_key_Text = Text(self.init_window, width=66, height=3)  # 公钥
        self.public_key_Text.grid(row=3, column=0, columnspan=10)
        self.sec_key_Text = Text(self.init_window, width=66, height=3)  # 私钥
        self.sec_key_Text.grid(row=5, column=0, columnspan=10)
        self.entry_pool_Text = Text(self.init_window, width=66, height=30)  # 内容池子
        self.entry_pool_Text.grid(row=7, column=0, columnspan=10)
        # 文本框
        self.log_data_Text = Text(self.init_window, width=70, height=49)  # 日志框
        self.log_data_Text.grid(row=1, column=12, rowspan=15, columnspan=10)

        # 按钮
        self.str_trans_to_md5_button = Button(self.init_window, text="开启节点", bg="lightblue", width=10, height=2,
                                              command=self.run_node)  # 调用内部方法  加()为直接调用
        self.str_trans_to_md5_button.grid(row=8, column=11)

    def run_node(self):
        thread = threading.Thread(target=self.run)
        thread.setDaemon(True)
        thread.start()

    def ins_log(self, data):
        self.log_data_Text.insert(END, data + "\n")
        self.log_data_Text.see(END)

    def change_window_title(self, name):
        self.init_window.title(name)

    def set_node_leader(self, name):
        self.node_leader_Text.delete(1.0, END)
        self.node_leader_Text.insert(END, name)

    def append_entry_pool_text(self, data):
        self.entry_pool_Text.delete(1.0, END)
        self.entry_pool_Text.insert(END, json.dumps(data, indent=2))
