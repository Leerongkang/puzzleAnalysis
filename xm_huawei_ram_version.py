#! python3


# 统计小米和华为两个渠道在RAM, Android API Level 的 图片导入成功率 和 图片导入分辨率
#
# 输入： ./puzzle_data/
#
# 输出： ./output/xm_ram.csv             小米以RAM分类的图片导入成功率和图片导入分辨率
#       ./output/xm_version.csv         小米以Android API Level分类的图片导入成功率和图片导入分辨率
#       ./output/huawei_ram.csv         华为以RAM分类的图片导入成功率和图片导入分辨率
#       ./output/huawei_version.csv     华为以Android API Level分类的图片导入成功率和图片导入分辨率



import json
import os
import csv

xm_ram = {
    0: 0,
    1: 0,
    2: 0,
    3: 0,
    4: 0,
    5: 0,
    6: 0,
    7: 0,
    8: 0,
    9: 0,
    10: 0
}

xm_ram_success = {
    0: 0,
    1: 0,
    2: 0,
    3: 0,
    4: 0,
    5: 0,
    6: 0,
    7: 0,
    8: 0,
    9: 0,
    10: 0
}

huawei_ram = {
    0: 0,
    1: 0,
    2: 0,
    3: 0,
    4: 0,
    5: 0,
    6: 0,
    7: 0,
    8: 0,
    9: 0,
    10: 0
}

huawei_ram_success = {
    0: 0,
    1: 0,
    2: 0,
    3: 0,
    4: 0,
    5: 0,
    6: 0,
    7: 0,
    8: 0,
    9: 0,
    10: 0
}

xm_ram_input_size = {
    0: 0,
    1: 0,
    2: 0,
    3: 0,
    4: 0,
    5: 0,
    6: 0,
    7: 0,
    8: 0,
    9: 0,
    10: 0
}

huawei_ram_input_size = {
    0: 0,
    1: 0,
    2: 0,
    3: 0,
    4: 0,
    5: 0,
    6: 0,
    7: 0,
    8: 0,
    9: 0,
    10: 0
}

xm_ram_input_size_count = {
    0: 0,
    1: 0,
    2: 0,
    3: 0,
    4: 0,
    5: 0,
    6: 0,
    7: 0,
    8: 0,
    9: 0,
    10: 0
}

huawei_ram_input_size_count = {
    0: 0,
    1: 0,
    2: 0,
    3: 0,
    4: 0,
    5: 0,
    6: 0,
    7: 0,
    8: 0,
    9: 0,
    10: 0
}

xm_version = {}

xm_version_success = {}

huawei_version = {}

huawei_version_success = {}

xm_version_input_size = {}

huawei_version_input_size = {}

xm_version_input_size_count = {}

huawei_version_input_size_count = {}

xm_total = 0

huawei_total = 0


def parse_data(file_name):
    global xm_ram, xm_version, huawei_ram, huawei_version, xm_total, huawei_total, \
        xm_version_input_size, xm_version_input_size_count, xm_ram_input_size, xm_ram_input_size_count, \
        huawei_version_input_size, huawei_version_input_size_count, \
        huawei_ram_input_size, huawei_ram_input_size_count

    with open(file_name) as data_file:
        text = data_file.read()
        json_data = json.loads(text)
        items = json_data['hits']['hits']
        for item in items:
            row = item['_source']
            input_success = row.get('bodyInfo.metric.input_suc', '-')
            if input_success == '-':
                continue
            input_origin_sizes = row.get('bodyInfo.metric.input_origin_sizes', '[]')
            user_import_count = input_origin_sizes.count('[') - 1
            if user_import_count <= 0:
                continue
            channel = row.get('clientInfo.channel', 'other')[0:2]
            # xm 渠道
            if channel == 'xm':
                # 总数 +1
                xm_total += 1
                # ram 区间 +1
                ram = row.get('bodyInfo.baggage.ram', '-')
                if ram == '-':
                    continue
                ram_index = int(int(ram) / 1024)
                if ram_index > 10:
                    ram_index = 10
                xm_ram[ram_index] += 1
                # version
                version = row.get('bodyInfo.baggage.android_sdk_int', 0)
                xm_version[version] += 1
                # 成功率统计
                if input_success == 1:
                    xm_ram_success[ram_index] += 1
                    xm_version_success[version] += 1
                average_size = average_input_size(input_origin_sizes)
                xm_ram_input_size[ram_index] += average_size
                xm_ram_input_size_count[ram_index] += 1
                xm_version_input_size[version] += average_size
                xm_version_input_size_count[version] += 1
            # 华为渠道
            elif channel == 'zh':
                # 总数 +1
                huawei_total += 1
                # ram 区间 +1
                ram = row.get('bodyInfo.baggage.ram', '-')
                if ram == '-':
                    continue
                ram_index = int(int(ram) / 1024)
                if ram_index > 10:
                    ram_index = 10
                huawei_ram[ram_index] += 1
                # version
                version = row.get('bodyInfo.baggage.android_sdk_int', 0)
                huawei_version[version] += 1
                # 成功率统计
                if input_success == 1:
                    huawei_ram_success[ram_index] += 1
                    huawei_version_success[version] += 1
                average_size = average_input_size(input_origin_sizes)
                huawei_ram_input_size[ram_index] += average_size
                huawei_ram_input_size_count[ram_index] += 1
                huawei_version_input_size[version] += average_size
                huawei_version_input_size_count[version] += 1


def average_input_size(origin):
    import_count = 0
    total_size = 0
    origin_wh = origin[1:-1].split('],')
    for i_wh in origin_wh:
        s = i_wh.replace('[', '').replace(']', '').split(',')
        h = -1
        for j in s:
            if j == '':
                break
            k = int(j)
            if h == -1:
                h = k
            else:
                total_size += (h * k)
                import_count += 1
    average = 0
    if import_count != 0:
        average = total_size / import_count
    return int(average)


def write_csv_data(file_name, data, mode):
    with open(file_name, mode) as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(data)


def write_csv_head(file_name, head, mode):
    with open(file_name, mode) as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(head)


def walk_file(base_dir):
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            yield os.path.join(root, file)


def file_counter(b_dir):
    for root, dirs, files in os.walk(b_dir):
        return len(files)


def init_version():
    for version in range(21, 31):
        xm_version[version] = 0
        huawei_version[version] = 0
        xm_version_success[version] = 0
        huawei_version_success[version] = 0
        xm_version_input_size[version] = 0
        huawei_version_input_size[version] = 0
        xm_version_input_size_count[version] = 0
        huawei_version_input_size_count[version] = 0


if __name__ == '__main__':
    fs = walk_file('./puzzle_data')
    pos = 0
    file_count = file_counter('./puzzle_data')
    init_version()
    for f in fs:
        pos += 1
        print('\r 正在解析： %2d / %2d : %s' % (pos, file_count, f), end="")
        parse_data(f)
    print('\r 解析完成', end="")
    ##################################################
    print('\r xm ram', end="")
    xm_ram_file_name = 'output/xm_ram.csv'
    xm_ram_file_csv_head = ['ram_GB', '总数', 'ram占比(%)', '保存成功数量', '保存成功率 (%)', '平均输入分辨率']
    write_csv_head(xm_ram_file_name, xm_ram_file_csv_head, 'w')
    xm_ram_data = []
    for key, value in xm_ram.items():
        row_data = [key + 1, value,
                    str(round(value / xm_total, 5) * 100)[0:6],
                    xm_ram_success[key]]
        per = 0.0
        if value != 0:
            per = str(round(xm_ram_success[key] / value, 5) * 100)[0:6]
        row_data.append(per)
        if xm_ram_input_size_count[key] != 0:
            row_data.append(int(xm_ram_input_size[key] / xm_ram_input_size_count[key]))
        xm_ram_data.append(row_data)
    write_csv_data(xm_ram_file_name, xm_ram_data, 'a+')
    print('\r xm ram 数据写入完成', end="")
    #################################################
    print('\r xm version', end="")
    xm_version_file_name = 'output/xm_version.csv'
    xm_version_file_csv_head = ['sdk', '总数', '占比(%)', '保存成功数量', '保存成功率 (%)', '平均输入分辨率']
    write_csv_head(xm_version_file_name, xm_version_file_csv_head, 'w')
    xm_version_data = []
    for key, value in xm_version.items():
        row_data = [key, value,
                    str(round(value / xm_total, 5) * 100)[0:6],
                    xm_version_success[key]]
        per = 0.0
        if value != 0:
            per = str(round(xm_version_success[key] / value, 5) * 100)[0:6]
        row_data.append(per)
        xm_version_data.append(row_data)
        if xm_version_input_size_count[key] != 0:
            row_data.append(int(xm_version_input_size[key] / xm_version_input_size_count[key]))
    write_csv_data(xm_version_file_name, xm_version_data, 'a+')
    print('\r xm version 数据写入完成', end="")
    ##################################################
    print('\r huawei ram', end="")
    huawei_ram_file_name = 'output/huawei_ram.csv'
    huawei_ram_file_csv_head = ['ram_GB', '总数', 'ram占比(%)', '保存成功数量', '保存成功率 (%)', '平均输入分辨率']
    write_csv_head(huawei_ram_file_name, huawei_ram_file_csv_head, 'w')
    huawei_ram_data = []
    for key, value in huawei_ram.items():
        row_data = [key + 1, value,
                    str(round(value / huawei_total, 5) * 100)[0:6],
                    huawei_ram_success[key]]
        per = 0.0
        if value != 0:
            per = str(round(huawei_ram_success[key] / value, 5) * 100)[0:6]
        row_data.append(per)
        huawei_ram_data.append(row_data)
        if huawei_ram_input_size_count[key] != 0:
            row_data.append(int(huawei_ram_input_size[key] / huawei_ram_input_size_count[key]))
    write_csv_data(huawei_ram_file_name, huawei_ram_data, 'a+')
    print('\r huawei ram 数据写入完成', end="")
    #################################################
    print('\r huawei version', end="")
    huawei_version_file_name = 'output/huawei_version.csv'
    huawei_version_file_csv_head = ['sdk', '总数', '占比(%)', '保存成功数量', '保存成功率 (%)', '平均输入分辨率']
    write_csv_head(huawei_version_file_name, huawei_version_file_csv_head, 'w')
    huawei_version_data = []
    for key, value in huawei_version.items():
        row_data = [key, value,
                    str(round(value / huawei_total, 5) * 100)[0:6],
                    huawei_version_success[key]]
        per = 0.0
        if value != 0:
            per = str(round(huawei_version_success[key] / value, 5) * 100)[0:6]
        row_data.append(per)
        if huawei_version_input_size_count[key] != 0:
            row_data.append(int(huawei_version_input_size[key] / huawei_version_input_size_count[key]))
        huawei_version_data.append(row_data)
    write_csv_data(huawei_version_file_name, huawei_version_data, 'a+')
    print('\r xm version 数据写入完成', end="")

    print('\r 分析完成：\n %s \n %s \n %s \n %s ' % (xm_ram_file_name, xm_version_file_name, huawei_ram_file_name,
                                                huawei_version_file_name), end="")

