import json
import re

from Module.MBase import MBase
from utils.poolRvslm import POOL_RVSLM


class CWeatherCommute(MBase):
    def __init__(self):
        super(CWeatherCommute, self).__init__()
        self.connRVSLM = POOL_RVSLM.connection()

    def moduleFunction(self):
        # 工作模式
        if self.wMode == '1' and self.wMode != '':
            stubs_info = self.get_value_from_data_items('stubs')
            device_no = self.get_value_from_data_items('DeviceNo')

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

            # 查询最后一次检测到状况（雨雾雪等） 发生设备（摄像头）的桩号
            sql = "select zhuanghao from devc_info where devc_id = '%s'" % (device_no,)
            begin_stub_id = self.query(sql)[0]

            # 查询发生设备（设置头）桩号，在全局变量中的索引
            begin_stub_index = -1
            for i in range(len(stub_array)):
                if stub_array[i] == begin_stub_id:
                    begin_stub_index = i
                    break

            # 限速平滑处理, 得到最终的每个路段的限速值
            last_limits = self.speed_limit_coordinate(lspeed_array, mspeed_array, begin_stub_index)

            # 所有摄像机桩号，用于之后查找每个情报板距离最近的摄像头
            all_cameras_stubs = stub_array

            # 查询所有情报板的桩号，基于mysql视图，预先创建好的视图(将所有可变情报板的数据，组织为一个视图)
            sql = "select operating_pile  from gulai where device_name  = '情报板' and left_right_side = '右幅'"
            all_message_board_stubs = self.query(sql)
            all_message_board_stubs = [item[0] for item in all_message_board_stubs]

            # 将所有的桩号转换为数字形式
            all_cameras_stubs_num = [self.transfer_stub_to_num(item) for item in all_cameras_stubs]
            all_message_board_stubs_num = [self.transfer_stub_to_num(item) for item in all_message_board_stubs]

            # 求每个情报板，距离最近的摄像头的桩号的下标
            nearest_index_list = self.find_nearest_camera(all_cameras_stubs_num, all_message_board_stubs_num)

            # 将距离当前情报板最近的摄像头的限速值， 进行映射
            message_board_limit = []
            for index in nearest_index_list:
                message_board_limit.append(last_limits[index])

            # 更新全局变量
            self.update_limit_info(all_message_board_stubs, message_board_limit)

        # 测试模式
        if self.wMode == '0' and self.wMode != '':
            self.change('1')
            self.sendBack()

        self.change('1')
        self.sendBack()

    def query(self, sql):
        try:
            with self.connRVSLM.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchall()
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
                i += 1
                distance = abs(camera - message_board)
                if distance < min_distance:
                    min_distance = distance
                    nearest_index = i
            nearest_index_list.append(nearest_index)
        return nearest_index_list

    def transfer_stub_to_num(self, stub):
        """
        提取桩号中的物理数字
        :param stub: 物理真实桩号
        :return: 桩号中的数字
        """
        return int(''.join(re.findall(r'\d+', stub)))

    def update_limit_info(self, message_board_id, limits):
        """
        更新全局变量的信息，可以考虑封装为基类的方法
        :param message_board_id: 情报板的桩号
        :param limits: 情报板的限速
        :return:
        """
        info = []
        for i in range(len(message_board_id)):
            # 从数据库中查到的情报板的桩号，后面会多一个空格，所以需要[:-1]
            temp = str(message_board_id[i][:-1]) + '|' + str(limits[i] // 10 * 10)
            info.append(temp)
        for item in self.data_Items:
            if item['relevantData_Name'] == 'stubs':
                item['relevantData_Value'] = info
                break

    def speed_limit_coordinate(self, lspeed, mspeed, begin_stub_index):
        """
        平滑整个路段的限速值
        :param begin_stub_index: 最下游检测到异常情况的摄像头桩号的下标
        :param lspeed: 给定的安全速度
        :param mspeed: 平均速度
        :return: 平滑完后每个个路段的速度
        """
        last_speed = lspeed
        # 从异常检测点，从下游到上游进行遍历
        for i in range(begin_stub_index, 0, -1):
            # 新的限速值 要小于给定的最大安全速度
            for v in range(lspeed[i], 60, -1):
                # 当前路段新的限速值，要和下个路段的限速值差值在合理范围内
                # 新的限速值，要和原有的平均速度差值也不能太大
                if abs(v - last_speed[i + 1]) <= 2 and abs(v - mspeed[i]) <= 2:
                    last_speed[i] = v
                    break
        return last_speed


if __name__ == '__main__':
    data = {
        "msgNode": {
            "topic": "302910591649516816",
            "tag": "1905"
        },
        "paras": {
            "pDef": "0",
            "pIns": "1",
            "wMode": "1",
            "module_Deal": 0,
            "data_Items": [
                {
                    "relevantData_Name": "stubs",
                    "relevantData_Value": "YK158+910|100*130;YK176+035|101*80",
                    "relevantData_Index": "1",
                    "relevantData_Type": "String",
                },
                {
                    "relevantData_Name": "DeviceNo",
                    "relevantData_Value": "DEV0133060402503",
                    "relevantData_Index": "2",
                    "relevantData_Type": "String",
                },

            ]
        }
    }

    weather = CWeatherCommute()

    # 转换为json字符串
    data = json.dumps(data)

    # 去掉双引号
    output = str(data).replace('"', "")

    # 解析数据
    weather.jsonParse(output)
    weather.working()

    # 调用实现逻辑
    weather.moduleFunction()
