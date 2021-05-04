from tkinter import *

from test.my_node import MNode

LOG_LINE_NUM = 0


class GUI:
    def __init__(self):
        self.init_window = Tk()
        self.set_init_window()
        self.init_window.mainloop()

    # 设置窗口
    def set_init_window(self):
        self.init_window.title("节点身份")  # 窗口名
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
        self.log_data_Text = Text(self.init_window, width=66, height=30)  # 内容池子
        self.log_data_Text.grid(row=7, column=0, columnspan=10)
        # 文本框
        self.log_data_Text = Text(self.init_window, width=70, height=49)  # 日志框
        self.log_data_Text.grid(row=1, column=12, rowspan=15, columnspan=10)

        # 按钮
        self.str_trans_to_md5_button = Button(self.init_window, text="字符串转MD5", bg="lightblue", width=10, height=2,
                                              command=self.run_node)  # 调用内部方法  加()为直接调用
        self.str_trans_to_md5_button.grid(row=8, column=11)

    def run_node(self):
        self.log_data_Text.insert(END, "sadasda\n")

    def ins_log(self, data):
        self.log_data_Text.insert(END, data)

    def change_window_title(self, name):
        self.init_window.title(name)

    def set_node_leader(self, name):
        self.node_leader_Text.delete(1, END)
        self.node_leader_Text.insert(END, name)


# def gui_start(node: MNode):
#     init_window = Tk()  # 实例化出一个父窗口
#     ZMJ_PORTAL = GUI(init_window)
#     # 设置根窗口默认属性
#     ZMJ_PORTAL.set_init_window()
#
#     init_window.mainloop()  # 父窗口进入事件循环，可以理解为保持窗口运行，否则界面不展示


if __name__ == '__main__':
    serverNode = MNode({'id': 'node_1',
                        'addr': ('localhost', 10001),
                        'peers': {'node_2': ('localhost', 10002),
                                  'node_3': ('localhost', 10003)
                                  }
                        })
    server1 = GUI()
