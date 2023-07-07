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
            stubs = ''
            device_no = ''
            for item in weather.data_Items:
                if item['relevantData_Name'] == 'data':
                    stubs = item['relevantData_Value']['stubs']

            for item in weather.data_Items:
                if item['relevantData_Name'] == 'body':
                    device_no = item['relevantData_Value']['DeviceNo']

            road = []
            for item in stubs:
                temp = []
                parts = item.split('|')
                temp.append(parts[0])
                speed_parts = parts[1].split('*')
                temp.append(speed_parts[0])
                temp.append(speed_parts[1])
                road.append(temp)

            # 查询最后一次检测到状况（雨雾雪等） 发生设备的桩号
            sql = "select zhuanghao from devc_info where devc_id = '%s'" % (device_no,)
            begin_stub_id = self.query(sql)[0]

            # 查询到所有摄像机的桩号， 实际上应该从报文中解析出来
            sql = "select zhuanghao from devc_info where type = '摄像机'"
            all_cameras_stubs = self.query(sql)
            all_cameras_stubs = [item[0] for item in all_cameras_stubs]

            # 查询所有情报板的桩号，基于mysql视图，预先创建好的视图(将所有可变情报板的数据，组织为一个视图)
            sql = "select zhuanghao from message_board"
            all_message_board_stubs = self.query(sql)
            all_message_board_stubs = [item[0] for item in all_message_board_stubs]

            # 将所有的桩号转换为数字形式
            all_cameras_stubs_num = [self.transfer_stub_to_num(item) for item in all_cameras_stubs]
            all_message_board_stubs_num = [self.transfer_stub_to_num(item) for item in all_message_board_stubs]

            # 求每个情报板，距离最近的摄像头的桩号
            nearest_index_list = self.find_nearest_camera(all_cameras_stubs_num, all_message_board_stubs_num)
            limits = [1] * len(all_cameras_stubs_num)

            # 将距离当前情报板最近的摄像头的限速值， 进行映射
            message_board_limit = []
            for index in nearest_index_list:
                message_board_limit.append(limits[index])

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

        :param message_board_id:
        :param limits:
        :return:
        """
        info = []
        for id, limit in message_board_id, limits:
            temp = str(id) + '|' + str(limit)
            info.append(temp)
        for item in self.data_Items:
            if item['relevantData_Name'] == 'data':
                item['relevantData_Value']['stubs'] = info


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
                    "relevantData_Name": "data",
                    "relevantData_Value": {
                        "stubs": ["stub1|50*50", "stub2|80*90"],
                    },
                    "relevantData_Index": "1",
                    "relevantData_Type": "String",
                },
                {
                    "relevantData_Name": "body",
                    "relevantData_Value": {
                        "DeviceNo": "x2312312"
                    },
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
    #
    weather.jsonParse(output)
    weather.working()
    #
    weather.moduleFunction()