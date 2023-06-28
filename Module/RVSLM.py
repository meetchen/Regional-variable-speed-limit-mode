from RVSLM_UNTIS import *


class RVSLM:

    def __init__(self):
        self.config_dict = read_config("../config/RVSLM_config.ini")
        self.rain_visi = self.config_dict['rain_visi']
        self.fog_visi = self.config_dict['fog_visi']
        self.snow_visi = self.config_dict['snow_visi']
        self.rain_d_share = self.config_dict['rain_d_share']
        self.rain_c_share = self.config_dict['rain_c_share']
        self.fog_d_share = self.config_dict['fog_d_share']
        self.fog_c_share = self.config_dict['fog_c_share']
        self.snow_d_share = self.config_dict['snow_d_share']
        self.snow_c_share = self.config_dict['snow_c_share']
        self.length = self.config_dict['length']
        self.data = LinkNode(1)

    # 异常判断（根据不同的天气，进行不同的异常判断条件）
    def judge_by_weather(self, node):
        if node.weather == 0:
            return node.visibility <= self.rain_visi
        elif node.weather == 1:
            return node.visibility <= self.fog_visi
        elif node.weather == 2:
            return node.visibility <= self.snow_visi
        else:
            return node.last_speed_limit != node.speed_limit

    def judge_by_service_level(self, node):
        if node.service_level == 3:
            return 30 <= node.speed <= 40
        elif node.service_level == 2:
            return 40 <= node.speed <= 60
        elif node.service_level == 1:
            return 60 <= node.speed <= 80
        else:
            return False

    # 占有率判断
    def judge(self, node):
        if node.weather == 0:
            if node.type == 0:
                return node.share >= self.rain_d_share
            elif node.type == 1:
                return node.share >= self.rain_c_share

        elif node.weather == 1:
            if node.type == 0:
                return node.share >= self.fog_d_share
            elif node.type == 1:
                return node.share >= self.fog_c_share

        elif node.weather == 2:
            if node.type == 0:
                return node.share >= self.snow_d_share
            elif node.type == 1:
                return node.share >= self.snow_c_share

    # 车检器类型
    def process_car_checker(self, i):
        current = self.data.get_position(i)

        if self.judge(current):
            self.data.update_next_speed_limit(i, min(1.2 * current.speed, current.maximum_safety_speed))
            if i:
                self.data.update_next_speed_limit(i - 1, min(0.8 * current.speed, current.prior.maximum_safety_speed))
        else:
            flag = False
            for vlim in range(current.maximum_safety_speed, 60, -1):
                if (((current.prior is None) or abs(vlim - current.prior.next_speed_limit) <= 4) and abs(
                        vlim - current.speed_limit) <= 4) or (
                        ((current.prior is None) or abs(vlim - current.prior.next_speed_limit) <= 4) and (
                        current.speed_limit - vlim >= 4)):
                    self.data.update_next_speed_limit(i, vlim)
                    flag = True
                    break
            if not flag:
                self.data.update_next_speed_limit(i, current.maximum_safety_speed)
                if i:
                    self.data.update_next_speed_limit(i - 1, (current.maximum_safety_speed + 2))
                    if current.prior.next_speed_limit > current.prior.maximum_safety_speed:
                        self.data.update_next_speed_limit(i - 1, current.prior.maximum_safety_speed)

    # 卡扣摄像机类型
    def process_camera(self, i):
        current = self.data.get_position(i)

        if self.judge(current):
            self.data.update_next_speed_limit(i, min(1.2 * current.speed, current.maximum_safety_speed))
            if i:
                self.data.update_next_speed_limit(i - 1, min(0.8 * current.speed, current.prior.maximum_safety_speed))
        else:
            flag = False
            for vlim in range(current.maximum_safety_speed, 60, -1):
                if (((current.prior is None) or abs(vlim - current.prior.next_speed_limit) <= 2) and abs(
                        vlim - current.speed_limit) <= 2) or (
                        ((current.prior is None) or abs(vlim - current.prior.next_speed_limit) <= 2) and (
                        current.speed_limit - vlim >= 2)):
                    self.data.update_next_speed_limit(i, vlim)
                    flag = True
                    break
            if not flag:
                self.data.update_next_speed_limit(i, current.maximum_safety_speed)
                if i:
                    self.data.update_next_speed_limit(i - 1, (current.maximum_safety_speed + 0.8))
                    if current.prior.next_speed_limit > current.prior.maximum_safety_speed:
                        self.data.update_next_speed_limit(i - 1, current.prior.maximum_safety_speed)

    # 合并邻近路段
    def gap_merge(self, a, b, c):
        gap_list = [a[i + 1] - a[i] for i in range(len(a) - 1)]

        # 如果两个端点距离较近 则进行合并
        for i in range(len(a) - 1):
            if i < len(a) - 1:
                if gap_list[i] <= self.length:
                    b.append([a[i], a[i + 1]])

        for i in range(len(a)):
            if a[i] not in sum(b, []):
                c.append(a[i])

    # 合并路段处理
    def sub_process(self, b, c):
        right_index = []
        right = 0
        for item in b:
            min_maximum_safety_speed = 0x3f3f3f3f  # 合并路段最小安全速度
            for i in range(item[0], item[1] + 1):
                min_maximum_safety_speed = min(self.data.get_position(i).maximum_safety_speed, min_maximum_safety_speed)
            for i in range(item[0], item[1] + 1):
                self.data.update_next_speed_limit(i, min_maximum_safety_speed)
            # 合并路段上游处理
            for j in range(item[0] - 1, right, -1):
                if self.data.get_position(j).type == 0:
                    self.process_car_checker(j)
                elif self.data.get_position(j).type == 1:
                    self.process_camera(j)
            right = item[1]
            right_index.append(right)

        # 非合并路段的处理
        if c is not None:
            for i in c:
                # 获取距离当前节点左侧 最近的区间右端点
                max_right = 0
                for u in range(len(right_index)):
                    if right_index[u] > i:
                        break
                    max_right = right_index[u]
                for j in range(i, max_right, -1):
                    if self.data.get_position(j).type == 0:
                        self.process_car_checker(j)
                    elif self.data.get_position(j).type == 1:
                        self.process_camera(j)

    def get_min_speed(self, index):
        min_speed = 0x3f3f3f3f
        count = 0
        for i in range(index, self.data.get_length()):
            min_speed = min(self.data.get_position(i).next_speed_limit, min_speed)
            count += 1
            if count == 10:
                break
        return min_speed

    def model(self, path, congestion_dissipation_control=False):
        """

        :param congestion_dissipation_control: 是否为拥堵消散模块， 否则为全天候通勤
        :param path: 传入json的列表文件， 或者传入json列表对象
        :return: 处理后的下一个时刻的json列表对象
        """

        # 直接使用json列表对象
        self.data = transfer_json_to_list(path)

        anomaly = []
        sub_anomaly = []
        ab_anomaly = []

        current = self.data.head
        while current is not None:
            if (not congestion_dissipation_control and self.judge_by_weather(current)) or (
                    congestion_dissipation_control and self.judge_by_service_level(current)):
                # 如果有异常，就添加到异常列表中，为下一步区间合并做准备
                anomaly.append(self.data.getIndex(current.stake_id))
            else:
                # 如果没有异常就使用路段固定限速
                self.data.update_next_speed_limit(self.data.getIndex(current.stake_id), current.fixed_speed_limit)
            current = current.next

        self.gap_merge(anomaly, sub_anomaly, ab_anomaly)
        self.sub_process(sub_anomaly, ab_anomaly)

        current = self.data.head
        while current is not None:
            if current.vms == 1:
                min_speed = self.get_min_speed(self.data.getIndex(current.stake_id))
                self.data.update_vms_speed_limit(self.data.getIndex(current.stake_id), min_speed)

            current = current.next

        v = []
        now_v = []
        current = self.data.head
        while current is not None:
            v.append(current.next_speed_limit // 10 * 10)
            now_v.append(current.speed_limit // 10 * 10)
            current = current.next
        # print(v)
        # print(now_v)
        # 保存或传递处理结果
        return list_to_json_str(self.data)


# if __name__ == '__main__':
#     model = RVSLM()
#     data = get_info(flow_id=1, table_name="before_process_rv", local=False)
#     result = model.model(data)
#     print(result)
