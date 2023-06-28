import argparse
import sys

sys.path.append("C:\\ModuleR")
from Module.MBase import MBase
from RVSLM_UNTIS import *
from RVSLM import RVSLM


class AllDayCommute(MBase):
    def __init__(self) -> None:
        super().__init__()
        # self.config = ZBase()
        self.count_dict = []
        self.json_data = []

    def moduleFunction(self):
        # 工作模式
        if self.wMode == '1' and self.wMode != '':
            flow_id = self.instance_id
            data = get_info(flow_id=flow_id, table_name="before_process_rv")
            model = RVSLM()
            result = model.model(data, congestion_dissipation_control=False)
            insert_data_database(table_name='after_process_rv', json_data=result, flow_id=flow_id)
            print("AllDayCommute for flow_id", flow_id, 'finish')

        # 测试模式
        if self.wMode == '0' and self.wMode != '':
            self.change('1')
            self.sendBack()

        self.change('1')
        self.sendBack()


if __name__ == '__main__':
    data = '{msgNode:{tag:456,topic:15},paras:{pDef:0,pIns:1,module_Deal:0,wMode:1,data_Items:[{' \
           'relevantData_Value:23,test2:1},{relevantData_Value:32,test2:1}{relevantData_Value:21,test2:1}]}} '
    model = AllDayCommute()
    model.jsonParse(data)
    model.working()
    model.moduleFunction()
    print("yes")
