import argparse
import sys
import json
import re

sys.path.append("C:\\ModuleR")
from MBase import MBase
from Wrvslm import CWeatherCommute


class Wcdc(MBase):
    def __init__(self) -> None:
        super().__init__()
        self.weather = CWeatherCommute()

    def moduleFunction(self):
        # 工作模式
        if self.wMode == '1' and self.wMode != '':

            # 路段桩号对应的限速信息
            stubs_info = self.get_value_from_data_items('Stubs')
            # 发现异常情况 路段 对应的设备号
            device_no = self.get_value_from_data_items('DeviceNo')
            # 最后一次检测到拥堵 的桩号
            last_stub = self.get_value_from_data_items('Location')
            stub_array = []
            speed_array = []

            pos = -1
            i = 0
            for substr in stubs_info.split(';'):
                # 按竖线 "|" 拆分子串为 stub 和 speed 部分
                parts = substr.split("|")
                stub = parts[0]
                speed = parts[1]

                # 将各部分添加到相应的数组中
                stub_array.append(stub)
                speed_array.append(int(speed))
                i += 1
                if stub == last_stub:
                    pos = i

            # 上游的速度减缓
            for i in range(pos):
                speed_array[i] = int(speed_array[i] * 0.8) // 10 * 10

            # 下游的速度增加
            for i in range(pos, len(speed_array)):
                speed_array[i] *= int(speed_array[i] * 1.2) // 10 * 10

            # 所有摄像机桩号，用于之后查找每个情报板距离最近的摄像头
            all_cameras_stubs = stub_array

            # 查询所有情报板的桩号，基于mysql视图，预先创建好的视图(将所有可变情报板的数据，组织为一个视图)
            sql = "select operating_pile from gulai where device_name = '情报板' and left_right_side = '左幅'"
            all_message_board_stubs = self.weather.query(sql)
            all_message_board_stubs = [item[0] for item in all_message_board_stubs]

            # 将所有的桩号转换为数字形式
            all_cameras_stubs_num = [self.weather.transfer_stub_to_num(item) for item in all_cameras_stubs]
            all_message_board_stubs_num = [self.weather.transfer_stub_to_num(item) for item in all_message_board_stubs]

            # 求每个情报板，距离最近的摄像头的桩号的下标
            nearest_index_list = self.weather.find_nearest_camera(all_cameras_stubs_num, all_message_board_stubs_num)

            for i in range(len(all_cameras_stubs)):
                sql = "SELECT DeviceNo FROM H_DEVICE WHERE location = '{}' and DeviceName like '%隧道%' ".format(
                    all_cameras_stubs[i])
                res = self.weather.query(sql, sql_server=True)
                if len(res):
                    speed_array[i] = TUNNEL_SPEED_LIMIT

            # 将情报板桩号与设备的id做映射，因为存在有的桩号上，查不到对应情报板的设备id
            msg_stub_to_device = dict()

            # 查询每个情报板桩号对应的设备id
            for stub in all_message_board_stubs:
                sql = "select DeviceNo from H_DEVICE where SubCategory = '情报板' and location like '%{}'".format(
                    stub[:-1])
                msg = self.weather.query(sql, fetchall=False, sql_server=True)
                if msg:
                    msg_stub_to_device[stub] = msg[0]

            # 将距离当前情报板最近的摄像头的限速值,进行映射
            message_board_limit = []
            for index in nearest_index_list:
                message_board_limit.append(speed_array[index])

            # 更新全局变量
            self.update_stubs_info(all_message_board_stubs, message_board_limit, msg_stub_to_device)

        # 测试模式
        if self.wMode == '0' and self.wMode != '':
            pass

        self.change('1')
        self.sendBack()

    def update_stubs_info(self, message_board_id, limits, msg_stub_to_device):
        """
        更新全局变量stubs的信息，将其给出情报板的设备号与限速值
        :param msg_stub_to_device: 桩号上对应的情报板的id
        :param message_board_id: 情报板的桩号
        :param limits: 情报板的限速
        :return:
        """
        info = ""
        for i in range(len(message_board_id)):
            # 从数据库中查到的情报板的桩号，后面会多一个空格，所以需要[:-1]
            # 获取对应桩号的情报板的设备号
            device_id = msg_stub_to_device.get(message_board_id[i])
            if device_id:
                temp = str(device_id) + '|' + str(limits[i] // 10 * 10) + ';'
                info += temp
        # 去除最尾部的‘;’
        info = info[:-1]
        for item in self.data_Items:
            if item['relevantData_Name'] == 'Stubs':
                item['relevantData_Value'] = info
                break


if __name__ == '__main__':
    test = True
    if test:
        data = {
            "msgNode": {
                "tag": "3",
                "topic": "637939643"
            },
            "paras": {
                "aDef": "3",
                "aIns": "89040",
                "data_Items": [
                    {
                        "argName": "_DeviceNo",
                        "relevantData_Index": "1",
                        "relevantData_Name": "DeviceNo",
                        "relevantData_Type": "String",
                        "relevantData_Value": "dcf2c7952a414b099273854fc9dcc2af"
                    },
                    {
                        "argName": "_Location",
                        "relevantData_Index": "2",
                        "relevantData_Name": "Location",
                        "relevantData_Type": "String",
                        "relevantData_Value": "139+660"
                    },
                    {
                        "argName": "_ETime",
                        "relevantData_Index": "3",
                        "relevantData_Name": "ETime",
                        "relevantData_Type": "String",
                        "relevantData_Value": "test"
                    },
                    {
                        "argName": "_Address",
                        "relevantData_Index": "4",
                        "relevantData_Name": "Address",
                        "relevantData_Type": "String",
                        "relevantData_Value": "http://189d0591.r1.cpolar.top"
                    },
                    {
                        "argName": "_LisenerPort",
                        "relevantData_Index": "5",
                        "relevantData_Name": "LisenerPort",
                        "relevantData_Type": "String",
                        "relevantData_Value": "80"
                    },
                    {
                        "argName": "_ServerName",
                        "relevantData_Index": "6",
                        "relevantData_Name": "ServerName",
                        "relevantData_Type": "String",
                        "relevantData_Value": ""
                    },
                    {
                        "argName": "_Stubs",
                        "relevantData_Index": "7",
                        "relevantData_Name": "Stubs",
                        "relevantData_Type": "String",
                        "relevantData_Value": "137+180|40;138+160|100;138+510|100;139+020|100;139+310|100"
                                              ";139+660|100"
                    }
                ],
                "module_Deal": "0",
                "module_ID": "38",
                "module_Name": "Wrvslm.py",
                "pDef": "637939643",
                "pIns": "26633",
                "wItem": "146075",
                "wMode": "1"
            }
        }
        # 转换为json字符串
        data = json.dumps(data)

        # 去掉双引号
        data = str(data).replace('"', "")
    else:
        data = sys.argv[1]

    # 创建实例对象
    cdc = Wcdc()

    # 解析数据
    cdc.jsonParse(data)
    cdc.working()

    # 隧道内固定限速
    TUNNEL_SPEED_LIMIT = 60

    # 调用实现逻辑
    cdc.moduleFunction()
