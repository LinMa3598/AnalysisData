# -*-coding:utf-8-*-
import copy

from clickhouse_driver import Client, connect


class CKServer:
    def __init__(self, host, port, user, password, database, settings={}):
        self.client = Client(host=host, port=port, user=user, password=password, database=database, settings=settings)
        self.connect = connect(host=host, port=port, user=user, password=password, database=database)
        self.cursor = self.connect.cursor()

    def execute(self, sql: object, params: list) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params)
        return result

    def execute_use_setting(self, sql: object, params: list, settings) -> object:
        # self.cursor.execute(sql)
        # result = self.cursor.fetchall()
        result = self.client.execute(sql, params, settings=settings)
        return result

    def execute_no_params(self, sql: object):
        result = self.client.execute(sql)
        return result

    def fetchall(self, sql):
        result = self.client.execute(sql)
        return result

    def close(self):
        self.client.disconnect()


with open('linux_maintainer', 'r') as f:
    all_lines = f.readlines()
    maintainer_json = {}
    module = ''
    maintainer_list = []
    for line in all_lines:
        if not (line.startswith('M:') or
                line.startswith('L:') or
                line.startswith('S:') or
                line.startswith('F:') or
                line.startswith('W:') or
                line.startswith('T:') or
                line.startswith('R:') or
                line.startswith('B:') or
                line.startswith('N:') or
                line.startswith('K:') or
                line.startswith('Q:') or
                line.startswith('X:') or
                line.startswith('P:') or
                line.startswith('C:') or
                line == '\n'):
            module = line[:-1]
            print(line)
        if line.startswith('M:') or line.startswith('R:'):
            if line.find('<') != -1:
                a = line.split('<')[1]
                if a.endswith('\n'):
                    a = a[0:-2]
                    # print(a)
            email = a

            maintainer_list.append(email)
        if line == '\n':
            m_list = copy.deepcopy(maintainer_list)
            maintainer_json[module] = m_list
            maintainer_list.clear()
bulk_data = []
for module in maintainer_json:
    for email in maintainer_json[module]:
        bulk_data.append({
            "module": module,
            "email": email
        })
ck = CKServer(host='', port=0, user='',password='',database='')
ck.execute('insert into table linux_maintainer_1 values', bulk_data)
"""
--sql查询
select uniqIf(email,endsWith(email, 'huawei.com')) as all_huawei_maintainer_count,
       uniqIf(email,endsWith(email, 'google.com')) as all_google_maintainer_count,
       uniqIf(email,endsWith(email,'huawei.com') and lower(module) not like '%driver%') as huawei_maintainer_not_at_driver_module,
       uniqIf(email,endsWith(email,'google.com') and lower(module) not like '%driver%') as google_maintainer_not_at_driver_module
--        sum(endsWith(email, 'huawei.com') and lower(module) not like '%driver%')
from linux_maintainer_1
"""