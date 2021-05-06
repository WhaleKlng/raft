import json
import socket
import threading
from tkinter import *
from typing import List
import random

LOG_LINE_NUM = 0


class EmpowerStaff:
    """
    运营人员客户端
    """

    def __init__(self, server_list: List):
        self.cs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ss.bind(('localhost', 10000))
        self.servers = server_list
        self.empowered_data = None
        self.init_window = Tk()
        self.set_init_window()

    # 设置窗口
    def set_init_window(self):
        self.init_window.title("隐私保护赋能客户端-骑手端")  # 窗口名
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
            self.empowered_data = data['empowered_data']
            self.result_data_Text.delete(1.0, END)
            self.result_data_Text.insert(END, json.dumps(data['empowered_data'], indent=2))

    def run_recv(self):
        thread = threading.Thread(target=self.write_recv_msg_to_log)
        thread.setDaemon(True)
        thread.start()
