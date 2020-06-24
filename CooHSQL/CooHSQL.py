import datetime
from tempfile import TemporaryFile
from CooHSQL_Utils import is_number, merge_cond, clear_event_type, reverse_delete
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)


class CooHSQL(object):
    def __init__(self, connection_settings, max_filtration=2, start_file=None, start_pos=None, event_schemas=None,
                 event_tables=None):
        self.max_filtration_len = max_filtration  # 匹配 where 允许字段最大字符长度
        self.coon_settings = connection_settings  # MySQL 配置信息
        self.log_file = start_file  # binlog 日志文件
        self.log_pos = start_pos  # binlog 位置
        self.only_schemas = event_schemas
        self.only_tables = event_tables
        self.resume_stream = True if start_file and start_pos else False

    def process_binlog(self):
        """
        伪装 slave 获取数据库中的 binlog
        :return:
        """
        stream = BinLogStreamReader(
            connection_settings=self.coon_settings,
            server_id=100,
            log_file=self.log_file,
            log_pos=self.log_pos,
            only_schemas=self.only_schemas,
            only_tables=self.only_tables,
            # blocking=True,
            resume_stream=self.resume_stream,
            only_events=[
                DeleteRowsEvent,
                UpdateRowsEvent,
                WriteRowsEvent
            ],
        )

        tem_file = TemporaryFile(mode='w+')
        for binlog_event in stream:
            for row in binlog_event.rows:
                # print(binlog_event.extra_data_length)
                # print(binlog_event.packet.log_pos)
                try:
                    event_time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
                except OSError:
                    event_time = datetime.datetime(1980, 1, 1, 0, 0)

                event = {"schema": binlog_event.schema, "table": binlog_event.table,
                         "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S")}

                if isinstance(binlog_event, DeleteRowsEvent):
                    event["action"] = "delete"
                    event["data"] = clear_event_type(row["values"])

                elif isinstance(binlog_event, UpdateRowsEvent):
                    event["action"] = "update"
                    event["before_data"] = clear_event_type(row["before_values"])
                    event["after_data"] = clear_event_type(row["after_values"])

                elif isinstance(binlog_event, WriteRowsEvent):
                    event["action"] = "insert"
                    event["data"] = clear_event_type(row["values"])

                tem_file.write(self.reverse_sql(event)+'\n')

        tem_file.seek(0)
        for x in tem_file.readlines()[::-1]:
            print(x)

    def reverse_sql(self, event):
        """
        逆向 SQL
        :param event:
        :return:
        """
        if event["action"] == 'delete':
            # print(reverse_delete(event))
            return reverse_delete(event)
            # pass

        elif event["action"] == 'insert':
            # print(self.reverse_insert(event))
            return self.reverse_insert(event)
            # pass

        elif event["action"] == 'update':
            # print(self.reverse_update(event))
            return self.reverse_update(event)
            # pass

    def reverse_insert(self, event):
        """
        逆向 insert 语句 DELETE
        :param event:
        :return:
        """
        template = "DELETE FROM `{0}`.`{1}` WHERE {2};".format(event["schema"], event['table'],
                                                               self.filter_where(event['data']))
        return template

    def reverse_update(self, event):
        """
        逆向 update 语句 update
        :param event:
        :return:
        """
        template = "UPDATE `{0}`.`{1}` SET {2} WHERE {3};".format(event["schema"], event['table'],
                                                                  merge_cond(event['before_data']),
                                                                  self.filter_where(event['after_data']))
        return template

    def filter_where(self, event_df):
        """
        筛选组合 where 条件
        如果 where 字段条件太多或者复杂的话
        会影响执行效率
        :return:
        """
        option_list = list()
        for k, v in event_df.items():
            if len(str(v)) <= self.max_filtration_len:
                tem_equ = "`" + str(k) + "`" + "=" + (
                    str(v) if is_number(v) else "'" + str(v) + "'")
                option_list.append(tem_equ)
        return ' and '.join(option_list)


if __name__ == '__main__':
    mysql_settings = {
        'host': '',
        'port': 3306,
        'user': '',
        'password': ''
    }
    CooHSQL = CooHSQL(mysql_settings, start_file='mysql-bin.000001', start_pos=143228)
    CooHSQL.process_binlog()
