import json
import os
import random
import socket
import time
import logging

from raft.log import Log

LEADER = "leader"
FOLLOWER = "follower"
CANDIDATE = "candidate"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def my_log(data):
    print(data)


class MNode:
    def __init__(self, config: dict):
        """
        节点构造方法
        :param config:初始化配置信息
        """
        self.role = 'follower'  # 初始化角色

        self.id = config['id']
        self.addr = config['addr']
        self.peers = config['peers']

        self.current_term = 0  # 当前的全局任期，现在的领导人是第几届
        self.voted_for = None  # 给谁投了票

        if not os.path.exists(self.id):
            os.mkdir(self.id)

        self.log = Log(self.id)
        self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers}
        self.match_index = {_id: -1 for _id in self.peers}

        self.commit_index = 0  # 日志提交索引，当前提交到了几次消息了，同步了几次消息了
        self.last_applied = 0  # 上次的日志提交索引

        self.leader_id = None  # 选举出来的Leader，当前节点认可的Leader

        self.vote_ids = {_id: 0 for _id in self.peers}

        self.client_addr = None  # 客户端地址

        self.wait_ms = (10, 20)
        self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)  # 初始化的领导人选举等待时间
        self.next_heartbeat_time = 0

        self.recv_s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # 接收套接字
        self.recv_s.bind(self.addr)
        self.recv_s.settimeout(2)

        self.send_s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # 发送套接字

    def load(self):
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

    def send(self, msg, dest_addr):
        """
        发送数据之方法
        :param msg: 等待发送的数据
        :param dest_addr: 目标地址
        :return:
        """
        msg = json.dumps(msg).encode('utf-8')
        self.send_s.sendto(msg, dest_addr)

    def recv(self):
        """ 自当前节点的地址接收而来的数据 """
        msg, sour_addr = self.recv_s.recvfrom(65535)
        return json.loads(msg), sour_addr

    def redirect(self, data, addr):
        """
        重定向消息方法，如果需要转发且能够则转发，然后返回空；否则就做一些业务，然后返回data本身
        :param data: 收取到的消息
        :param addr: 从哪个地址收取的消息
        :return:
        """
        if not data:
            return None
        data: dict
        msg_type = data.get("type")  # 当前消息的消息类型
        msg_dst_id = data.get("dst_id")  # 当前消息的目的发送ID
        if msg_type == "client_append_entries":  # 收到客户端发开的"追加消息"
            if self.role != LEADER:  # 当前节点不是Leader，无权处理之，需要将消息转发给Leader
                if self.leader_id:  # 存在Leader，已经选举完成了
                    my_log("redirect: 客户端追加条目转发至Leader")
                    self.send(data, self.peers[self.leader_id])
                # 没Leader就不做任何事，且返回空，当做无法处理当前消息且不知道给谁转发；因为客户端发来的消息"出现"地太早了
                return
            else:
                self.client_addr = addr  # 更新节点对应的客户端地址,以后就点对点了
                return data

        if msg_dst_id != self.id:  # 如果当前消息不是发给自己的，那就转发消息给对应的人
            my_log('redirect: to ' + data['dst_id'])
            self.send(data, self.peers[msg_dst_id])  # 转发之，并返回空
            return
        else:
            return data

    def append_entries(self, data):
        """
        append entries rpc
        only used in follower state
        追加日志条目，进行集群内的信息复制，并写入到当前节点工作
        :param data: 收到的消息
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
            logging.info('          2. smaller term')
            logging.info('          3. success = False: smaller term')
            logging.info('          4. send append_entries_response to leader ' + data['src_id'])
            response['success'] = False
            self.send(response, self.peers[data['src_id']])
            return

        self.leader_id = data['leader_id']

        # heartbeat
        if data['entries'] == []:
            logging.info('          4. heartbeat')
            return

        prev_log_index = data['prev_log_index']
        prev_log_term = data['prev_log_term']

        tmp_prev_log_term = self.log.get_log_term(prev_log_index)

        # append_entries: rule 2, 3
        # append_entries: rule 3
        if tmp_prev_log_term != prev_log_term:
            logging.info('          4. success = False: index not match or term not match')
            logging.info('          5. send append_entries_response to leader ' + data['src_id'])
            logging.info('          6. log delete_entries')
            logging.info('          6. log save')

            response['success'] = False
            self.send(response, self.peers[data['src_id']])
            self.log.delete_entries(prev_log_index)

        # append_entries rule 4
        else:
            logging.info('          4. success = True')
            logging.info('          5. send append_entries_response to leader ' + data['src_id'])
            logging.info('          6. log append_entries')
            logging.info('          7. log save')

            response['success'] = True
            self.send(response, self.peers[data['src_id']])
            self.log.append_entries(prev_log_index, data['entries'])

            # append_entries rule 5
            leader_commit = data['leader_commit']
            if leader_commit > self.commit_index:
                commit_index = min(leader_commit, self.log.last_log_index)
                self.commit_index = commit_index
                logging.info('          8. commit_index = ' + str(commit_index))

        return

    def process_req_vote(self, data):
        response = {
            'type': 'request_vote_response',
            'src_id': self.id,
            'dst_id': data['src_id'],
            'term': self.current_term,
            'vote_granted': True
        }
        if data['term'] < self.current_term:  # 小于当前统治期，不同意其参选
            response['vote_granted'] = False

        candidate_id = data['candidate_id']
        self.send(response, self.peers[data['src_id']])

    def all_do(self, data):
        """
        在一轮消息通信过程中，任何角色都需要做的事情
        :param data: 自节点的接收套接字接收而来的消息
        :return:
        """
        if self.commit_index > self.last_applied:  # 尝试更新上次日记索引位置
            self.last_applied = self.commit_index
        if not data:
            return
        if data["type"] == "client_append_entries":
            return
        if data["term"] > self.current_term:  # 消息的任期大于节点任期，说明产生了新一轮选举了
            my_log("{}被告知更高统治期，被迫变为跟随者".format(self.id))
            self.role = FOLLOWER
            self.current_term = data["term"]
            self.voted_for = None
            self.save()

    def follower_do(self, data):
        """
        在一轮消息通信过程中，跟随者需要做的事情
        :param data: 自节点的接收套接字接收而来的消息
        :return:
        """
        cur_time = time.time()  # 当前时间戳
        # 跟随者策略1
        if data:
            data: dict
            msg_type = data.get("type")  # 当前消息的消息类型
            if msg_type == "append_entries":  # 跟随者收到追加日志信息，注意这个类型和客户端的那个类型可不一样client_append_entries
                my_log('跟随者： 1. recv append_entries from leader ' + data['src_id'])
                if data['term'] == self.current_term:
                    self.next_leader_election_time = cur_time + random.randint(*self.wait_ms)
                self.append_entries(data)  # 追加日志
            if msg_type == "request_vote":
                my_log('follower: 1. recv request_vote from candidate ' + data['src_id'])
                self.process_req_vote(data)

        # 跟随者策略2
        if cur_time > self.next_leader_election_time:  # 当前时间超过了初始化成为候选人的等待时间
            my_log("成为候选者")
            self.next_leader_election_time = cur_time + random.randint(*self.wait_ms)  # 重置候选人计时器
            self.role = CANDIDATE  # 成为候选人了，之后做候选人的事情
            self.current_term += 1  # 任期后置加一
            self.voted_for = self.id  # 给自己投
            self.save()
            self.vote_ids = {_id: 0 for _id in self.peers}

    def candidate_do(self, data):
        cur_time = time.time()
        # 候选者策略1：征求选票
        for dst_id in self.peers:
            if self.vote_ids[dst_id] == 0:
                req_msg = {
                    'type': 'request_vote',
                    'src_id': self.id,
                    'dst_id': dst_id,
                    'term': self.current_term,
                    'candidate_id': self.id,
                    # 'last_log_index': self.log.last_log_index,
                    # 'last_log_term': self.log.last_log_term
                }
                self.send(req_msg, self.peers[dst_id])

        if data and data["term"] == self.current_term:
            msg_type = data['type']
            # 候选者策略2:处理所获得的选票
            if msg_type == "request_vote_response":
                self.vote_ids[data['src_id']] = data['vote_granted']
                vote_count = sum(list(self.vote_ids.values()))
                if vote_count >= len(self.peers) // 2:
                    self.role = LEADER
                    self.voted_for = None
                    # self.save()
                    self.next_heartbeat_time = 0
                    # self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers}
                    # self.match_index = {_id: 0 for _id in self.peers}

            # 候选者策略3:虽然当前是候选者，但是也要服从Leader的信息同步命令(可能其他人已经成为了Leader),属于被通知的
            elif msg_type == "append_entries":
                self.next_leader_election_time = cur_time + random.randint(*self.wait_ms)
                self.role = FOLLOWER
                self.voted_for = None  # 属于被通知的
                # self.save()

        # 候选者策略4:候选者参选时间超时,重新变为候选者，统治期加1
        if cur_time > self.next_leader_election_time:
            self.next_leader_election_time = cur_time + random.randint(*self.wait_ms)
            self.role = CANDIDATE
            self.current_term += 1
            self.voted_for = self.id
            # self.save()
            self.vote_ids = {_id: 0 for _id in self.peers}

    def leader_do(self, data):
        cur_time = time.time()

        # 领导者策略1：维持心跳
        if cur_time > self.next_heartbeat_time:
            self.next_heartbeat_time = cur_time + random.randint(0, 5)

            for dst_id in self.peers:
                request = {'type': 'append_entries',
                           'src_id': self.id,
                           'dst_id': dst_id,
                           'term': self.current_term,
                           'leader_id': self.id,
                           # 'prev_log_index': self.next_index[dst_id] - 1,
                           # 'prev_log_term': self.log.get_log_term(self.next_index[dst_id] - 1),
                           # 'entries': self.log.get_entries(self.next_index[dst_id]),
                           'leader_commit': self.commit_index
                           }

                self.send(request, self.peers[dst_id])

        # 领导者策略2： 处理客户端的追加条目 类型的消息
        if data and data["type"] == "client_append_entries":
            data['term'] = self.current_term

        # 领导者策略3： 处理集群内的同步情况
        if data and data["term"] == self.current_term:
            return

        # 领导者策略4： 准备相应客户端的追加相应情况
        while True:
            return

    def clean_sockets(self):
        self.send_s.close()
        self.recv_s.close()

    def run(self):
        while True:
            try:
                try:
                    data, addr = self.recv()
                except Exception as e:
                    # logging.info(e)
                    data, addr = None, None

                data = self.redirect(data, addr)
                print("{}收到data为：{}".format(self.id, (data, addr)))
                self.all_do(data)

                if self.role == 'follower':
                    self.follower_do(data)

                if self.role == 'candidate':
                    self.candidate_do(data)

                if self.role == 'leader':
                    self.leader_do(data)

            except Exception as e:
                print(e)


if __name__ == '__main__':
    conf = {'id': 'node_1',
            'addr': ('localhost', 10001),
            'peers': {'node_2': ('localhost', 10002),
                      'node_3': ('localhost', 10003)
                      }
            }

    node = MNode(conf)

    node.run()
