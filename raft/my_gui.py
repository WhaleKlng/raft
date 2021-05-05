#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import socket
import threading
from tkinter import *
import hashlib
import time
from typing import List
import random

LOG_LINE_NUM = 0


class Empower:
    def __init__(self, server_list: List):
        self.cs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ss.bind(('localhost', 10000))
        self.servers = server_list
        self.init_window = Tk()
        self.set_init_window()

    # 设置窗口
    def set_init_window(self):
        self.init_window.title("隐私保护赋能客户端")  # 窗口名
        self.init_window.geometry('1068x681+10+10')
        # 标签
        self.init_data_label = Label(self.init_window, text="待处理数据:用户真实号码", justify=LEFT)
        self.init_data_label.grid(row=0, column=0)
        self.result_data_label = Label(self.init_window, text="输出结果", justify=LEFT)
        self.result_data_label.grid(row=2, column=0)
        self.log_label = Label(self.init_window, text="输出结果日志", justify=LEFT)
        self.log_label.grid(row=0, column=12)

        # 文本框
        self.init_data_Text = Text(self.init_window, width=67, height=3)  # 原始数据录入框
        self.init_data_Text.grid(row=1, column=0, columnspan=10)
        self.result_data_Text = Text(self.init_window, width=66, height=43)  # 处理结果展示
        self.result_data_Text.grid(row=3, column=0, columnspan=10)

        self.log_data_Text = Text(self.init_window, width=70, height=49)  # 日志框
        self.log_data_Text.grid(row=0, column=12, rowspan=15, columnspan=10)
        # 按钮

        self.str_trans_to_md5_button = Button(self.init_window, text="获取短接号", bg="lightblue", width=10,
                                              command=self.c_send_real_phone_number)  # 调用内部方法  加()为直接调用
        self.str_trans_to_md5_button.grid(row=1, column=11)
        self.run_recv()
        self.init_window.mainloop()

    # 功能函数
    def str_trans_to_md5(self):
        src = self.init_data_Text.get(1.0, END).strip().replace("\n", "").encode()
        # print("src =",src)
        if src:
            try:
                myMd5 = hashlib.md5()
                myMd5.update(src)
                myMd5_Digest = myMd5.hexdigest()
                # print(myMd5_Digest)
                # 输出到界面
                self.result_data_Text.delete(1.0, END)
                self.result_data_Text.insert(1.0, myMd5_Digest)
                self.write_log_to_Text("INFO:str_trans_to_md5 success")
            except:
                self.result_data_Text.delete(1.0, END)
                self.result_data_Text.insert(1.0, "字符串转MD5失败")
        else:
            self.write_log_to_Text("ERROR:str_trans_to_md5 failed")

    # 获取当前时间
    def get_current_time(self):
        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        return current_time

    # 日志动态打印
    def write_log_to_Text(self, logmsg):
        global LOG_LINE_NUM
        current_time = self.get_current_time()
        logmsg_in = str(current_time) + " " + str(logmsg) + "\n"  # 换行
        if LOG_LINE_NUM <= 7:
            self.log_data_Text.insert(END, logmsg_in)
            LOG_LINE_NUM = LOG_LINE_NUM + 1
        else:
            self.log_data_Text.delete(1.0, 2.0)
            self.log_data_Text.insert(END, logmsg_in)

    # def get_virtual_number(self, real_number) -> str:
    def c_send_real_phone_number(self, ):
        real_number = self.init_data_Text.get(1.0, END).strip().replace("\n", "")
        random_addr = random.choice(self.servers)
        msg = {'type': 'client_append_entries', "real_number": real_number}
        self.cs.sendto(json.dumps(msg).encode('utf-8'), random_addr)

    def write_recv_msg_to_log(self):
        while True:
            msg, addr = self.ss.recvfrom(65535)
            data = json.loads(msg)
            self.log_data_Text.insert(END, json.dumps(data, indent=2) + "\n")
            self.log_data_Text.see(END)
            self.result_data_Text.delete(1.0, END)
            self.result_data_Text.insert(END, json.dumps(data['empowered_data'], indent=2))

    def run_recv(self):
        self.thread = threading.Thread(target=self.write_recv_msg_to_log)
        self.thread.start()


def gui_start():
    Empower([
        ('localhost', 10001),
        ('localhost', 10002),
        ('localhost', 10003)
    ])


gui_start()
