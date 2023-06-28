import json
import random
import configparser
import mysql.connector
from utils.poolHZ import POOL_HZ


class LinkNode:
    def __init__(self, stake_id, visibility=0, share=1, type=0, weather=0, vms=0, vms_speed_limit=0, speed=80,
                 fixed_speed_limit=0, maximum_safety_speed=1, last_speed_limit=0,
                 speed_limit=0, next_speed_limit=0, service_level=0, next=None, prior=None):
        self.stake_id = stake_id
        self.visibility = visibility  # 能见度
        self.share = share  # 占有率
        self.type = type  # 0表示车检器，1表示摄像头
        self.weather = weather  # 0表示雨天(350)，1表示雾天(250)，2表示雪天(300)，3表示晴天
        self.vms = vms  # 0表示无情报板，1表示有情报板
        self.vms_speed_limit = vms_speed_limit  # 情报板 限速
        self.speed = speed  # 当前路段速度
        self.fixed_speed_limit = fixed_speed_limit  # 当前路段的固定限速
        self.maximum_safety_speed = maximum_safety_speed  # 当前路段的最大安全速度
        self.last_speed_limit = last_speed_limit  # 上次路段限速
        self.speed_limit = speed_limit  # 当前路段限速
        self.next_speed_limit = next_speed_limit  # xxxx
        self.service_level = service_level  # 服务水平 1,自由流  2, xx流  3 xx流
        self.next = next
        self.prior = prior

    def __str__(self):
        return (
                "stake_id : %d,  visibility : %d, share : %.2f, type : %d, weather : %d, vms : %d, "
                "vms_speed_limit : %d, speed : %d, fixed_speed_limit : %d, maximum_safety_speed : %d, last_speed_limit "
                ": %d, speed_limit : %d, next_speed_limit : %d , service_level : %d" % (
                    self.stake_id, self.visibility, self.share, self.type, self.weather, self.vms, self.vms_speed_limit,
                    self.speed,
                    self.fixed_speed_limit, self.maximum_safety_speed, self.last_speed_limit, self.speed_limit,
                    self.next_speed_limit, self.service_level))

    def random_data(self):
        self.visibility = random.randint(100, 500)
        self.share = random.randint(0, 30) / 100
        self.type = random.choice([0, 1])  # 0表示车检器，1表示摄像头
        self.weather = random.choice([0, 1, 2, 3])  # 0表示雨天(350)，1表示雾天(250)，2表示雪天(300)，3表示正常
        my_list = [0] * 90 + [1] * 10
        self.vms = random.choice(my_list)  # 0表示无情报板，1表示有情报板
        self.vms_speed_limit = 0
        self.share = random.randint(0, 30) / 100
        self.speed = random.randint(60, 120)
        self.fixed_speed_limit = random.choice([120, 80])
        self.maximum_safety_speed = random.randint(60, 100)
        self.last_speed_limit = random.randint(40, 120)
        self.service_level = random.choice([1, 2, 3])
        self.speed_limit = random.randint(40, 120)
        self.next_speed_limit = 0


class LinkList:
    def __init__(self, ):
        self.head = None

    # 判断队列是否为空
    def is_empty(self):
        if self.head is None:
            return True
        else:
            return False

    # #链表长度
    def get_length(self):
        count = 0
        if self.is_empty():
            return count
        else:
            current = self.head
            while current is not None:
                count += 1
                current = current.next
        return count

    # 从队头添加节点
    def add_node_from_head(self, node):
        if self.head is None:
            self.head = node
        else:
            node.next = self.head
            self.head.prior = node
            self.head = node
        return self

    # 从队尾添加节点
    def add_node_from_behind(self, node):
        current_node = self.head
        if self.is_empty():
            self.add_node_from_head(node)
        else:
            # 一直遍历到尾部节点
            while current_node.next is not None:
                current_node = current_node.next
            current_node.next = node
            node.prior = current_node
        return self

    # 在某个位置中插入元素
    def add_node_index(self, node, index):
        # 特殊位置，在第一个位置的时候，头插法
        if index <= 0:
            self.add_node_from_head(node)

        # 在尾部的时候，使用尾插法
        elif index > self.get_length() - 1:
            self.add_node_from_behind(node)

        # 中间位置
        else:
            current = self.head
            count = 0
            while count < index - 1:
                current = current.next
                count += 1

            # 找到了要插入位置的前驱之后，进行如下操作
            node.prior = current
            node.next = current.next
            current.next.prior = node
            current.next = node

    # 删除节点
    def remove(self, item):
        current = self.head
        if self.is_empty():
            return
        elif current.stake_id == item:
            # 第一个节点就是目标节点，那么需要将下一个节点的前驱改为None 然后再将head指向下一个节点
            current.next.prior = None
            self.head = current.next
        else:
            # 找到要删除的元素节点
            while current is not None and current.stake_id != item:
                current = current.next
            if current is None:
                print("not found {0}".format(item))

            # 如果尾节点是目标节点，让前驱节点指向None
            elif current.next is None:
                current.prior.next = None

            # 中间位置，因为是双链表，可以用前驱指针操作
            else:
                current.prior.next = current.next
                current.next.prior = current.prior

    # 输出队列
    def traversing_list(self):
        current_node = self.head
        while current_node:
            print(current_node)
            current_node = current_node.next

    # 按索引取节点
    def get_position(self, index):
        if self.is_empty() or index < 0 or index >= self.get_length():
            print("error: out of index")
            return
        j = 0
        node = self.head
        while node.next and j < index:
            node = node.next
            j += 1
        return node

    # 修改一个节点
    def update_next_speed_limit(self, index, newdata):
        if self.is_empty() or index < 0 or index >= self.get_length():
            print('error: out of index')
            return
        j = 0
        node = self.head
        while node.next and j < index:
            node = node.next
            j += 1
        if j == index:
            node.next_speed_limit = newdata

    def update_vms_speed_limit(self, index, newdata):
        if self.is_empty() or index < 0 or index >= self.get_length():
            print('error: out of index')
            return
        j = 0
        node = self.head
        while node.next and j < index:
            node = node.next
            j += 1
        if j == index:
            node.vms_speed_limit = newdata

    # 查找一个节点的索引
    def getIndex(self, value):
        j = 0
        if self.is_empty():
            print("this chain table is empty")
            return
        node = self.head
        while node:
            if node.stake_id == value:
                return j
            node = node.next
            j += 1

        if j == self.length:
            print("%s not found" % str(value))
            return


def list_to_json_file(my_list, path):
    json_data = list_to_json_str(my_list)
    with open(path, 'w') as f:
        json.dump(json_data, f)
    print("list saved")


def json_file_to_list(path):
    # 从文件中读取JSON数据
    with open(path, 'r') as f:
        json_data = f.read()

    lst = LinkList()
    # 将JSON数组拆分为单个JSON对象
    json_objects = json_data.splitlines()
    for idx, obj in enumerate(json_objects):
        data_list = json.loads(obj)
        for data in data_list:
            node = LinkNode(idx)
            for key, value in data.items():
                node.__dict__[key] = value
            lst.add_node_from_behind(node)

    return lst


def get_json_from_file(path):
    with open(path, 'r') as f:
        json_data = f.read()
    return json_data


def transfer_json_to_list(json_data):
    lst = LinkList()
    # 将JSON数组拆分为单个JSON对象
    json_objects = json_data.splitlines()
    for idx, obj in enumerate(json_objects):
        data_list = json.loads(obj)
        for data in data_list:
            node = LinkNode(idx)
            for key, value in data.items():
                node.__dict__[key] = value
            lst.add_node_from_behind(node)
    return lst


def read_config(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)

    config_dict = {}
    for section in config.sections():
        for option in config.options(section):
            value = config.get(section, option)
            # 根据需要进行类型转换
            if option in ['rain_visi', 'fog_visi', 'snow_visi', 'length']:
                value = int(value)
            elif option in ['rain_d_share', 'rain_c_share', 'fog_d_share', 'fog_c_share', 'snow_d_share',
                            'snow_c_share']:
                value = float(value)
            config_dict[option] = value

    return config_dict


def list_to_json_str(my_list):
    json_data = []
    current = my_list.head
    while current is not None:
        node_dict = vars(current)
        real_dict = {}
        for key, value in node_dict.items():
            if key not in ['prior', 'next']:
                real_dict[key] = value
        json_data.append(real_dict)
        current = current.next
    return json.dumps(json_data)


def insert_data_database(table_name, json_data, flow_id, local=True):
    """
    将需要算法处理的路段数据（json） 存到对应的数据库
    :param local: 默认使用本地数据库
    :param table_name: 需要插入的表名，如果插入的是准全天候通勤则使用 before_process_rv 如果是拥堵消散 使用的 before_process_cdc
    :param json_data: 传入json字符串，是一组上面的LinkNode(去除next 与 prior)
    :param flow_id: 流程号 用于全局整个流程的传递
    :return: 无
    """
    # 连接到MySQL数据库
    conn = get_database_connection(local)

    # 创建游标对象
    cursor = conn.cursor()

    # 构建插入语句

    insert_query = f"INSERT INTO {table_name} (flow_id, stake_id, visibility, share, type, weather, vms," \
                   " vms_speed_limit, speed, fixed_speed_limit, maximum_safety_speed, last_speed_limit, " \
                   "speed_limit,next_speed_limit, service_level) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    values = generate_value_tuples(json_data, flow_id)

    # 执行插入操作
    cursor.executemany(insert_query, values)

    # 提交事务
    conn.commit()

    # 关闭游标和连接
    cursor.close()
    conn.close()


# 获取数据库链接
def get_database_connection(local=True):
    """

    :param local: 默认使用本地数据库， 传入False使用云端数据库
    :return: 数据库连接，记得关闭
    """
    host = '127.0.0.1'
    password = 'root'
    if not local:
        #     host = '8.142.73.253'
        #     password = 'xxd12315'
        config_dict = read_config("../config/RVSLM_config.ini")
        return mysql.connector.connect(
            host=config_dict['host'],
            password=config_dict['password'],
            port=config_dict['port'],
            database=config_dict['database_name'],
            user=['user']
        )
    return mysql.connector.connect(
        host=host,
        user='root',
        password=password,
        database='rvslm'
    )


#
def generate_value_tuples(json_data, flow_id):
    """
        解析 JSON 数据并生成元组列表, 便于数据库的插入等
    :param json_data:
    :param flow_id:
    :return:
    """
    data = json.loads(json_data)
    keys = list(data[0].keys())
    keys.insert(0, 'flow_id')
    values = [(flow_id,) + tuple(item[key] for key in data[0].keys()) for item in data]
    return values


def random_list_data(size):
    """
    生成给定长度的随机节点链表
    :param size: 给定长度
    :return: 链表
    """
    data = LinkList()
    for i in range(size):
        node = LinkNode(i)
        node.random_data()
        data.add_node_from_behind(node)
    # data.traversing_list()
    return data


# 获取处理前的数据
def get_info(flow_id, table_name, local=True):
    """
    给定流程号 读取路段数据，并将结果转化为json对象
    :param flow_id: 流程号
    :param table_name: 需要读取的表名
    :return: 链表的json化
    """
    conn = get_database_connection(local)
    cursor = conn.cursor()

    # 将列名连接成字符串，用于构建SQL查询语句
    columns_str = "stake_id, visibility, share, type, weather, vms," \
                  " vms_speed_limit, speed, fixed_speed_limit, maximum_safety_speed, last_speed_limit, " \
                  "speed_limit,next_speed_limit,service_level"

    # 构建SQL查询语句
    query = f"SELECT {columns_str} FROM {table_name} where flow_id = {flow_id}"

    try:
        # 执行查询
        cursor.execute(query)
        rows = cursor.fetchall()

        # 构建结果字典
        results = []
        for row in rows:
            result = {
                "stake_id": row[0],
                "visibility": row[1],
                "share": row[2],
                "type": row[3],
                "weather": row[4],
                "vms": row[5],
                "vms_speed_limit": row[6],
                "speed": row[7],
                "fixed_speed_limit": row[8],
                "maximum_safety_speed": row[9],
                "last_speed_limit": row[10],
                "speed_limit": row[11],
                "next_speed_limit": row[12],
                "service_level": row[13]
            }
            results.append(result)

        # 将结果转换为 JSON 字符串
        json_data = json.dumps(results)

        return json_data

    except mysql.connector.Error as e:
        print(f"数据库错误：{e}")

    finally:
        # 关闭数据库连接
        cursor.close()
        conn.close()


if 0:
    data = random_list_data(100)
    data = list_to_json_str(data)
    # list_to_json_file(data, path='./data/my_list.json')
    insert_data_database(table_name='before_process_rv', json_data=data, flow_id=1)
