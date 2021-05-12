import os
import json
import threading
import time
import socket
import random
import logging
from typing import List

import rsa

from raft.utils import check_is_phone
from .log import Log
from tkinter import *

# 控制台日志的格式化
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 全局变量
LEADER = "leader"
FOLLOWER = "follower"
CANDIDATE = "candidate"


class ServerNode:

    def __init__(self, conf):
        self.role = 'follower'  # 每个节点都初始化为跟随者
        self.id = conf['id']  # 还有一个ID name名字
        self.addr = conf['addr']  # 监听地址，这个节点以后会接收（10001）收到数据 MSG {}

        self.peers = conf['peers']  # 初始化集群中的其他节点,记录一下 其他的地址
        self.pubkey, self.privkey = rsa.newkeys(1024)  # 加密处理 rsa秘钥   rsa.newkeys(1024) 返回两个值 非对称加密
        # 128字  1024  ++++ 520 4048长度

        self.init_window = Tk()  # 初始化一个GUI界面
        self.set_init_window()  # 业务代码了

        # 选举任期相关：统治期
        self.current_term = 0
        self.voted_for = None

        if not os.path.exists(self.id):  # 日志同步： 创建文件夹
            os.mkdir(self.id)

        # init persistent state
        # 初始化任期：优先去读取过去的日志
        self.load()
        self.log = Log(self.id)  # 是另外一个组件 日志同步的主要实现

        # volatile state
        # rule 1, 2
        # 节点是不是也会自己保存消息，我
        self.commit_index = 0  # 现在处理到那个了
        self.last_applied = 0  # 我上一个是谁  某个节点  后面加进来的时候

        # volatile state on leaders
        # rule 1, 2
        self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers}
        self.match_index = {_id: -1 for _id in self.peers}

        # append entries
        self.leader_id = None  # 节点的Leader是谁

        # request vote
        self.vote_ids = {_id: 0 for _id in self.peers}  # 给那些节点征票 向别人 0就是标志这个人有没有给咱投赞成票

        # client request
        self.client_addr = None  # 客户端的地址

        # tick
        self.wait_ms = (10, 20)  # 初始化一个随机数
        self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)  # 随机计时器
        self.next_heartbeat_time = 0  # 下一次心跳时间,成为Leader后立马发

        self.key_pool = {}  # 美团的赋能
        # msg send and recv
        self.ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # 初始化了一个server socket  接收的
        self.ss.bind(self.addr)
        self.ss.settimeout(2)

        self.cs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # 初始化了一个发送套接字

        # self.init_window.mainloop()  # GUI窗口持久化的

    def load(self):
        self.public_key_Text.insert(END, self.pubkey.save_pkcs1())  # 插入一下秘钥们
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
        msg, addr = self.ss.recvfrom(65535)  # 收到了来自addr的msg消息
        # 如果收到的消息前缀中包含：vote_response的话，就切片处理之
        # msg 现在是一个字符串
        # print(msg)
        # print(type(msg))
        if b"votes_response " in msg:  # 收到的是征票响应类信息
            # print("进来了")
            self.ins_log("密文:{}".format(msg))
            data = msg[15:]  # 得到密文
            rst = json.loads(rsa.decrypt(data, self.privkey).decode())  # 解密之
            # print(rst)
            self.ins_log("解密得到的密文为:{}".format(rst))
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

        if data['type'] == 'client_append_entries' or data['type'] == "empower_staff_heart":
            # 试想有人说客户端追加条目了，
            if self.role != LEADER:  # 尝试转发
                if self.leader_id:  # 且选出来了
                    self.my_log('redirect: client_append_entries to leader')
                    self.send(data, self.peers[self.leader_id])
                return None
            else:
                self.client_addr = addr
                return data

        if data['dst_id'] != self.id:  # 如果这条消息不是给我看的，因为一个Leader可能会失去该身份
            self.my_log('redirect: to ' + data['dst_id'])
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
            # self.sankuai_helper(entries)
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

        self.my_log('----------------------------all----------------------------------')

        if self.commit_index > self.last_applied:
            self.last_applied = self.commit_index
            self.my_log('all: 1. last_applied = ' + str(self.last_applied))

        if data is None:
            return

        if data['type'] == 'client_append_entries' or data['type'] == "empower_staff_heart":
            return

        if data['term'] > self.current_term:  # 如果消息中的任期大于自己的了，自己是井底之蛙
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

        t = time.time()  # 获取当前时间
        # follower rules: rule 1
        if data:  # 如果命令来了或者消息来了

            if data['type'] == 'append_entries':  # 听命令：日志追加
                self.my_log('follower: 1. recv append_entries from leader ' + data['src_id'])

                if data['term'] == self.current_term:  # 我没有被落下
                    self.my_log('          2. same term')
                    self.my_log('          3. reset next_leader_election_time')
                    self.next_leader_election_time = t + random.randint(*self.wait_ms)
                self.append_entries(data)

            elif data['type'] == 'request_vote':  # 响应征票
                self.my_log('follower: 1. recv request_vote from candidate ' + data['src_id'])
                self.request_vote(data)

        # follower rules: rule 2
        if t > self.next_leader_election_time:  # 计时器已经结束
            self.my_log('follower：1. become candidate')
            self.next_leader_election_time = t + random.randint(*self.wait_ms)
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.id  # 立马给自己投票
            self.save()
            self.vote_ids = {_id: 0 for _id in self.peers}  # 初始化征票列表且认为他们都没投过票呢 是0

        return

    def candidate_do(self, data):
        '''
        rules for fervers: candidate
        '''
        self.my_log('--------------------------candidate--------------------------------')

        t = time.time()
        # candidate rules: rule 1
        for dst_id in self.peers:
            if self.vote_ids[dst_id] == 0:  # 这个零
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
            if data['type'] == 'request_vote_response':  # 要响应收到的 选票们
                self.my_log('candidate: 1. recv request_vote_response from follower ' + data['src_id'])

                self.vote_ids[data['src_id']] = data['vote_granted']
                vote_count = sum(list(self.vote_ids.values()))

                if vote_count >= len(self.peers) // 2:
                    self.my_log('           2. become leader')
                    self.role = LEADER
                    self.voted_for = None
                    self.save()
                    self.next_heartbeat_time = 0
                    self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers}
                    self.match_index = {_id: 0 for _id in self.peers}
                    return

            # candidate rules: rule 3
            elif data['type'] == 'append_entries':  # 听命令
                self.my_log('candidate: 1. recv append_entries from leader ' + data['src_id'])
                self.my_log('           2. become follower')
                self.next_leader_election_time = t + random.randint(*self.wait_ms)
                self.role = 'follower'
                self.voted_for = None
                self.save()
                return

        # candidate rules: rule 4
        if t > self.next_leader_election_time:  # 重新成为候选者
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
        print("Leader收到了{}".format(data))
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
            cur_entries = self.sankuai_helper([data])
            self.log.append_entries(self.log.last_log_index, cur_entries)

            self.my_log('leader：1. recv append_entries from client')
            self.my_log('        2. log append_entries')
            self.my_log('        3. log save')

            return
        # 强打补丁
        if data and data['type'] == "empower_staff_heart":
            self.my_log("leader: recv Client init connection")
            response = {'index': self.commit_index, "type": "empower_staff_heart_response",
                        "empowered_data": self.log.empowered_data}
            data['term'] = self.current_term
            self.log.append_entries(self.log.last_log_index, [data])

            self.send(response, (self.client_addr[0], 10000))
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
        while True:  # 一直在干一件事，导致不会结束
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
                    logging.error(e)
                    data, addr = None, None

                data = self.redirect(data, addr)  # 转发消息

                self.all_do(data)  # 所有节点都做的事情

                if self.role == 'follower':
                    self.follower_do(data)

                if self.role == 'candidate':
                    self.candidate_do(data)

                if self.role == LEADER:
                    self.leader_do(data)

            except Exception as e:
                logging.error(e)

    def generate_4_random_number(self):
        return "{}{}{}{}".format(random.randint(0, 9), random.randint(0, 9), random.randint(0, 9), random.randint(0, 9))

    def sankuai_helper(self, entries: List):
        for entry in entries:
            real_number = entry.get("real_number", "")
            if check_is_phone(real_number):
                if real_number not in self.log.empowered_data.values():
                    while True:
                        short_num = random.choice(self.log.empower_pool)
                        random_num = self.generate_4_random_number()
                        cur_key = "{}#{}".format(short_num, random_num)
                        if cur_key not in self.log.empowered_data:
                            self.log.empowered_data[cur_key] = real_number
                            entry['virtual_number'] = cur_key
                            break
                else:
                    entry['virtual_number'] = self.log.empowered_data['real_number']
        return entries

    # GUI 设置窗口

    def my_log(self, data):
        logging.info(data)
        self.ins_log(data)

    def set_init_window(self):
        """
        初始化页面的相关操作
        :return:
        """
        self.init_window.title(self.id)  # 窗口名
        # 290 160为窗口大小，+10 +10 定义窗口弹出时的默认展示位置
        self.init_window.geometry('1050x900+10+10')
        # 节点Leader
        canvas = Canvas(self.init_window, width=1068, height=100, )
        image_file = PhotoImage(file="../raft/hdu.png")
        canvas.create_image(534, 0, anchor='n', image=image_file)
        canvas.pack(side='top')
        Label(self.init_window, text='一种具备隐私保护的分布式共识算法设计与仿真', font=('楷体', 32)).pack()

        # Leader
        self.node_leader_label = Label(self.init_window, text="节点Leader", justify=LEFT)
        self.node_leader_label.place(x=20, y=155)
        self.node_leader_Text = Text(self.init_window, width=66, height=1.5)
        self.node_leader_Text.place(x=20, y=175)

        # 节点公钥
        self.public_key_label = Label(self.init_window, text="节点公钥", justify=LEFT)
        self.public_key_label.place(x=20, y=205)
        self.public_key_Text = Text(self.init_window, width=66, height=5)  # 公钥
        self.public_key_Text.place(x=20, y=225)

        # 节点私钥
        self.sec_key_label = Label(self.init_window, text="节点私钥", justify=LEFT)
        self.sec_key_label.place(x=20, y=291)
        self.sec_key_Text = Text(self.init_window, width=66, height=7)  # 私钥
        self.sec_key_Text.place(x=20, y=311)

        #  内容池
        self.data_pool_label = Label(self.init_window, text="内容池")
        self.data_pool_label.place(x=20, y=407)
        self.entry_pool_Text = Text(self.init_window, width=66, height=30)  # 内容池子
        self.entry_pool_Text.place(x=20, y=427)

        # 日志
        self.log_data_label = Label(self.init_window, text="日志", justify=LEFT)
        self.log_data_label.place(x=525, y=155)
        self.log_data_Text = Text(self.init_window, width=70, height=49.4)  # 日志框
        self.log_data_Text.place(x=525, y=175)

        # 按钮
        self.str_trans_to_md5_button = Button(self.init_window, text="开启节点", bg="lightblue", width=10, height=2,
                                              command=self.run_node)  # 调用内部方法  加()为直接调用 点了按钮开发跑raft的代码
        self.str_trans_to_md5_button.place(x=440, y=835)

        Label(self.init_window, text='朱雅鑫 17272109', font=('楷体', 16)).place(x=880, y=850)

        self.init_window.mainloop()  # GUI窗口持久化的

    def run_node(self):
        thread = threading.Thread(target=self.run)  # 开了一个县城去做self,run方法
        thread.setDaemon(True)  # 设置成为守护线程
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
        self.log_data_Text.see(END)
