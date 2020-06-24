import datetime
import decimal


def is_number(num):
    try:
        if str == 'NaN':
            return False
        float(num)
        return True
    except ValueError:
        return False


def merge_cond(tem_cond):
    """
        合并 UPDATE 语句中的 SET 部分
        :param tem_cond:
        :return:
        """
    option_list = list()
    for k, v in tem_cond.items():
        tem_equ = "`" + str(k) + "`" + "=" + (
            str(v) if is_number(v) else "'" + str(v) + "'")
        option_list.append(tem_equ)
    return ','.join(option_list)


def seg_dict(event_dict):
    """
    :param event_dict:
    :return:
    """
    keys, values = list(), list()
    for k, v in event_dict.items():
        keys.append(k)

        if str(v).isdigit():
            values.append(str(v))
        else:
            values.append("'" + str(v) + "'")

    return ','.join(keys), ','.join(values)


def clear_event_type(event):
    """
    加载进来的 binlog 时间类型也被转换为 datetime
    此方法是将 datetime 类型转换为 str 方便后续操作
    :param event:
    :return:
    """
    for k, v in event.items():
        if isinstance(v, datetime.datetime):
            event[k] = v.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(v, decimal.Decimal):
            event[k] = str(v)
    return event


def reverse_delete(event):
    """
    逆向 delete 语句 insert
    :param event:
    :return:
    """
    tem_key, tem_value = seg_dict(event['data'])
    template = "INSERT INTO `{0}`.`{1}`({2}) VALUE ({3});".format(event['schema'], event['table'],
                                                                  tem_key, tem_value)
    return template
