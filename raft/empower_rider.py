import json
import socket
import threading
from tkinter import *
from tkinter import messagebox


class EmpowerRider:
    """
    骑手客户端
    """

    def __init__(self, staff_addr):
        self.cs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.ss.bind(('localhost', 9999))
        self.staff_addr = staff_addr
        self.empowered_data = None
        self.init_window = Tk()
        self.set_init_window()

    # 设置窗口
    def set_init_window(self):
        self.init_window.title("隐私保护赋能客户端-骑手端")  # 窗口名
        self.init_window.geometry('320x130+10+10')
        # 标签
        self.init_data_label = Label(self.init_window, text="待处理数据:短接号码", justify=LEFT)
        self.init_data_label.grid(row=0, column=0)

        # 文本框
        self.init_data_Text = Text(self.init_window, width=43, height=3)  # 原始数据录入框
        self.init_data_Text.grid(row=1, column=0, columnspan=10)

        # 按钮
        self.str_trans_to_md5_button = Button(self.init_window, text="获取真实号码", bg="lightblue", width=10, height=2,
                                              command=self.run_recv)  # 调用内部方法  加()为直接调用
        self.str_trans_to_md5_button.grid(row=4, column=3)
        self.init_window.mainloop()

    def c_send_real_phone_number(self):
        virtual_number = self.init_data_Text.get(1.0, END).strip().replace("\n", "")
        msg = {'type': 'ask_real_phone', "virtual_number": virtual_number}
        self.cs.sendto(json.dumps(msg).encode('utf-8'), self.staff_addr)
        while True:
            msg, addr = self.ss.recvfrom(65535)
            data = json.loads(msg)
            if virtual_number in data:
                messagebox.showinfo(title="成功啦", message=data[virtual_number])
                return
            else:
                messagebox.showerror(title="失败啦", message=data["errMsg"])

    def run_recv(self):
        thread = threading.Thread(target=self.c_send_real_phone_number)
        thread.setDaemon(True)
        thread.start()


