import json
import re
import sys
import os
from sys import argv

sys.path.append("C:\\ModuleR")

from Module.MBase import MBase
from utils.poolRvslm import POOL_RVSLM
from utils.poolDevice import POOL_DEVICE


class CWeatherCommute(MBase):
    def __init__(self):
        super(CWeatherCommute, self).__init__()
        self.connRVSLM = POOL_RVSLM.connection()
        self.connDevice = POOL_DEVICE.connection()

    def moduleFunction(self):
        # 工作模式
        if self.wMode == '1' and self.wMode != '':
            # 路段桩号对应的限速信息
            stubs_info = self.get_value_from_data_items('Stubs')
            # 发现异常情况 路段 对应的设备号
            device_no = self.get_value_from_data_items('DeviceNo')
            # 最后一次检测到状况（雨雾雪等） 发生设备（摄像头）的桩号
            last_stub = self.get_value_from_data_items('Location')
            stub_array = []
            lspeed_array = []
            mspeed_array = []

            for substr in stubs_info.split(';'):
                # 按竖线 "|" 拆分子串为 stub 和 speed 部分
                parts = substr.split("|")
                stub = parts[0]
                speed = parts[1]

                # 按星号 "*" 拆分 speed 部分为 lspeed 和 mspeed
                speeds = speed.split("*")
                lspeed = int(speeds[0])
                mspeed = int(speeds[1])

                # 将各部分添加到相应的数组中
                stub_array.append(stub)
                lspeed_array.append(lspeed)
                mspeed_array.append(mspeed)

            # 查询发生设备（设置头）桩号，在全局变量中的索引
            begin_stub_index = -1
            for i in range(len(stub_array)):
                if stub_array[i] == last_stub:
                    begin_stub_index = i
                    break

            # 限速平滑处理, 得到最终的每个路段的限速值
            last_limits = self.speed_limit_coordinate(lspeed_array, mspeed_array, begin_stub_index)

            # 所有摄像机桩号，用于之后查找每个情报板距离最近的摄像头
            all_cameras_stubs = stub_array

            # 查询所有情报板的桩号，基于mysql视图，预先创建好的视图(将所有可变情报板的数据，组织为一个视图)
            sql = "select operating_pile from gulai where device_name = '情报板' and left_right_side = '左幅'"
            all_message_board_stubs = self.query(sql)
            all_message_board_stubs = [item[0] for item in all_message_board_stubs]

            # 将所有的桩号转换为数字形式
            all_cameras_stubs_num = [self.transfer_stub_to_num(item) for item in all_cameras_stubs]
            all_message_board_stubs_num = [self.transfer_stub_to_num(item) for item in all_message_board_stubs]

            # 将情报板桩号与其纯数字形式做映射
            msg_stub_to_num_stub = dict()
            for stub, stubs_num in zip(all_message_board_stubs, all_message_board_stubs_num):
                msg_stub_to_num_stub[stub] = stubs_num

            # 计算间隔桩号之间的距离
            interval_distance = [all_message_board_stubs_num[i + 1] - all_message_board_stubs_num[i]
                                 for i in range(len(all_message_board_stubs_num) - 1)]

            # 求每个情报板，距离最近的摄像头的桩号的下标
            nearest_index_list = self.find_nearest_camera(all_cameras_stubs_num, all_message_board_stubs_num)

            # 将情报板桩号与设备的id做映射，因为存在有的桩号上，查不到对应情报板的设备id
            msg_stub_to_device = dict()

            # 查询每个情报板桩号对应的设备id
            for stub in all_message_board_stubs:
                sql = "select DeviceNo from H_DEVICE where SubCategory = '情报板' and location like '%" + stub[:-1] + "'"
                msg = self.query(sql, fetchall=False, sql_server=True)
                if msg:
                    msg_stub_to_device[stub] = msg[0]

            # 将距离当前情报板最近的摄像头的限速值,进行映射
            message_board_limit = []
            for index in nearest_index_list:
                message_board_limit.append(last_limits[index])

            # 更新全局变量
            self.update_stubs_info(all_message_board_stubs, message_board_limit, msg_stub_to_device)

        # 测试模式
        if self.wMode == '0' and self.wMode != '':
            pass

        self.change('1')
        self.sendBack()

    def query(self, sql, fetchall=True, sql_server=False):
        print('---sql----', sql)
        try:
            if sql_server:
                with self.connDevice.cursor() as cursor:
                    cursor.execute(sql)
                    result = cursor.fetchall() if fetchall else cursor.fetchone()
            else:
                with self.connRVSLM.cursor() as cursor:
                    cursor.execute(sql)
                    result = cursor.fetchall() if fetchall else cursor.fetchone()
        finally:
            pass
        print('sql-result', result)
        return result

    def query_Device(self, sql, fetchall=True):
        try:
            with self.connDevice.cursor() as cursor:
                print(sql)
                cursor.execute(sql)
                result = cursor.fetchall() if fetchall else cursor.fetchone()
        finally:
            pass
        return result

    def find_nearest_camera(self, camera_num_list, message_boards_num_list):
        """

        :param camera_num_list: 摄像机的桩号列表
        :param message_boards_num_list: 情报板的桩号列表
        :return: 每个情报板距离最近的摄像机的数组下标
        """
        nearest_index_list = []
        for message_board in message_boards_num_list:
            min_distance = float('inf')
            nearest_index = None
            i = 0
            for camera in camera_num_list:
                distance = abs(camera - message_board)
                if distance < min_distance:
                    min_distance = distance
                    nearest_index = i
                i += 1
            nearest_index_list.append(nearest_index)
        return nearest_index_list

    def transfer_stub_to_num(self, stub):
        """
        提取桩号中的物理数字
        :param stub: 物理真实桩号
        :return: 桩号中的数字
        """
        return int(''.join(re.findall(r'\d+', stub)))

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

    def speed_limit_coordinate(self, lspeed, mspeed, begin_stub_index):
        """
        平滑整个路段的限速值
        :param begin_stub_index: 最下游检测到异常情况的摄像头桩号的下标
        :param lspeed: 给定的最大安全速度
        :param mspeed: 平均速度
        :return: 平滑完后每个个路段的速度
        """
        last_speed = lspeed
        # 从异常检测点，从下游到上游进行遍历
        for i in range(begin_stub_index, 0, -1):
            # 新的限速值 要小于给定的最大安全速度，最低不能低于40
            for v in range(lspeed[i], 40, -1):
                # 当前路段新的限速值，要和下个路段的限速值差值在合理范围内
                # 新的限速值，要和原有的平均速度差值也不能太大
                if i + 1 < len(lspeed) and abs(v - last_speed[i + 1]) <= 2 and abs(v - mspeed[i]) <= 2:
                    last_speed[i] = v
                    break
        return last_speed

    def get_value_from_data_items(self, key):
        for item in self.data_Items:
            if item['relevantData_Name'] == key:
                return item['relevantData_Value']
        raise ValueError("Don't have this key：", key)

    def float_string_to_int(self, num):
        return round(float(num))


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
                        "relevantData_Value": "137+180|40*23;138+160|100*22;138+510|100*21;139+020|100*30;139+310|100"
                                              "*26;139+660|100*29 "
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
    weather = CWeatherCommute()

    # 解析数据
    weather.jsonParse(data)
    weather.working()

    # 调用实现逻辑
    weather.moduleFunction()
