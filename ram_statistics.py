#! python3


# 统计统计拼图成功率
#
# 小米，华为，OPPO，vivo四个渠道下的图片导入成功率，图片处理成功率，以及图片输出成功率
# 统计ram大小的区间分布，区间为(0,1], (1,2], (2,3], (3,4], (4,5], (5,6], (6,7], (7,8], (8,9], (9,10], (10,+∞)，单位为GB
# 统计各ram大小区间下的图片导入成功率
#
# 输入： ./puzzle_data/
#
# 输出： ./output/ram_channel_detail.csv       每个输入文件的分析的RAM以及渠道的详细数据
#       ./output/ram_total.csv                汇总RAM大小的区间分布，以及区间下的图片导入成功率
#       ./output/channel_total.csv            小米，华为，OPPO，vivo四个渠道下的图片导入成功率，图片处理成功率，以及图片输出成功率


import json
import os
import csv

ram_distribution = {
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

import_success_ram_distribution = {
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

channel_success = {
    'xm': {
        'total': 0,
        'input': 0,
        'process': 0,
        'output': 0
    },
    'op': {
        'total': 0,
        'input': 0,
        'process': 0,
        'output': 0
    },
    'vi': {
        'total': 0,
        'input': 0,
        'process': 0,
        'output': 0
    },
    'zh': {
        'total': 0,
        'input': 0,
        'process': 0,
        'output': 0
    }
}


def parse_data(file_name):
    with open(file_name) as data_file:
        text = data_file.read()
        data = []
        json_data = json.loads(text)
        items = json_data['hits']['hits']

        file_ram_distribution = {
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
        file_import_success_ram_distribution = {
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
        file_channel_success = {
            'xm': {
                'total': 0,
                'input': 0,
                'process': 0,
                'output': 0
            },
            'op': {
                'total': 0,
                'input': 0,
                'process': 0,
                'output': 0
            },
            'vi': {
                'total': 0,
                'input': 0,
                'process': 0,
                'output': 0
            },
            'zh': {
                'total': 0,
                'input': 0,
                'process': 0,
                'output': 0
            }
        }

        for item in items:
            row = item['_source']
            ram = row.get('bodyInfo.baggage.ram', '-')
            if ram == '-':
                continue
            index = int(int(ram) / 1024)
            if index > 10:
                index = 10
            # else:
            #     index = str(index)

            input_success = row.get('bodyInfo.metric.input_suc', '-')
            process_success = row.get('bodyInfo.metric.process_suc', '-')
            output_success = row.get('bodyInfo.metric.output_suc', '-')
            if input_success == '-' and process_success == '-' and output_success == '-':
                continue
            file_ram_distribution[index] += 1
            channel = row.get('clientInfo.channel', 'other')[0:2]
            if input_success == 1:
                file_import_success_ram_distribution[index] += 1
            if channel == 'xm' or channel == 'op' or channel == 'zh' or channel == 'vi':
                file_channel_success[channel]['total'] += 1
                if input_success == 1:
                    file_channel_success[channel]['input'] += 1
                if process_success == 1:
                    file_channel_success[channel]['process'] += 1
                if output_success == 1:
                    file_channel_success[channel]['output'] += 1
    global ram_distribution, import_success_ram_distribution, channel_success
    data.append(file_name[-24:])
    total = 0
    for i in range(11):
        total += file_ram_distribution[i]
    data.append(total)
    for i in range(11):
        data.append(file_ram_distribution[i])
        data.append(str(round(file_ram_distribution[i] / total, 5) * 100)[0:6])
        ram_distribution[i] += file_ram_distribution[i]
        data.append(file_import_success_ram_distribution[i])
        import_success_ram_distribution[i] += file_import_success_ram_distribution[i]
        pre = 0
        if file_ram_distribution[i] != 0:
            pre = str(round(file_import_success_ram_distribution[i] / file_ram_distribution[i], 5) * 100)[0:6]
        data.append(pre)
        data.append(' ')
    for key, value in file_channel_success.items():
        for k, v in value.items():
            channel_success[key][k] += v
            data.append(v)
            if k != 'total':
                data.append(str(round(v / value['total'], 5) * 100)[0:6])
        data.append(' ')
    return data


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


if __name__ == '__main__':
    fs = walk_file('./puzzle_data')
    pos = 0
    file_count = file_counter('./puzzle_data')
    all_data = []
    ram_file_name = 'output/ram_channel_detail.csv'

    for f in fs:
        pos += 1
        print('\r 正在解析： %2d / %2d : %s' % (pos, file_count, f), end="")
        all_data.append(parse_data(f))
    print('\r 解析完成', end="")
    print(' 开始写入单个文件数据')
    file_csv_head = ['文件名', '总数',
                     '1GB', 'ram占比(%)', '成功数量', '成功率 (%)', ' ', '2GB', 'ram占比(%)', '成功数量', '成功率 (%)', ' ',
                     '3GB', 'ram占比(%)', '成功数量', '成功率 (%)', ' ', '4GB', 'ram占比(%)', '成功数量', '成功率 (%)', ' ',
                     '5GB', 'ram占比(%)', '成功数量', '成功率 (%)', ' ', '6GB', 'ram占比(%)', '成功数量', '成功率 (%)', ' ',
                     '7GB', 'ram占比(%)', '成功数量', '成功率 (%)', ' ', '8GB', 'ram占比(%)', '成功数量', '成功率 (%)', ' ',
                     '9GB', 'ram占比(%)', '成功数量', '成功率 (%)', ' ', '10GB', 'ram占比(%)', '成功数量', '成功率 (%)', ' ',
                     '10GB以上', 'ram占比(%)', '成功数量', '成功率 (%)', ' ',
                     '小米', '导入成功', '导入成功率 (%)', '处理成功', '处理成功率 (%)',  '保存成功', '保存成功率 (%)', ' ',
                     'oppo', '导入成功', '导入成功率 (%)', '处理成功', '处理成功率 (%)',  '保存成功', '保存成功率 (%)', ' ',
                     'vivo', '导入成功', '导入成功率 (%)', '处理成功', '处理成功率 (%)',  '保存成功', '保存成功率 (%)', ' ',
                     '华为', '导入成功', '导入成功率 (%)', '处理成功', '处理成功率 (%)',  '保存成功', '保存成功率 (%)', ' '
                     ]
    write_csv_head(ram_file_name, file_csv_head, 'w')
    write_csv_data(ram_file_name, all_data, 'a+')
    print('\r 单个文件数据写入完成', end="")

    #################################################
    print(' 正在汇总ram数据', end="")
    ram_total_file_name = 'output/ram_total.csv'
    global_ram_head = ['ram_GB', '数量', '占比 (%)', '导入成功数', '导入成功率 (%)']
    global_data_ram = []
    total = 0
    total_import_success = 0
    for i in range(11):
        total += ram_distribution[i]
        total_import_success += import_success_ram_distribution[i]
    for i in range(11):
        ram_size = i + 1
        if ram_size > 10:
            ram_size = '大于 10'
        ram_item_list = [ram_size, ram_distribution[i], str(round(ram_distribution[i] / total, 5) * 100)[0:6],
                         import_success_ram_distribution[i]]
        pre = 0
        if ram_distribution[i] != 0:
            pre = str(round(import_success_ram_distribution[i] / ram_distribution[i], 5) * 100)[0:6]
        ram_item_list.append(pre)
        global_data_ram.append(ram_item_list)
    import_pre = 0
    if total != 0:
        import_pre = str(round(total_import_success / total, 5) * 100)[0:6]
    global_data_ram.append(['汇总', total, '100', total_import_success, import_pre])
    print('\r 写入ram数据', end="")
    write_csv_head(ram_total_file_name, global_ram_head, 'w')
    write_csv_data(ram_total_file_name, global_data_ram, 'a+')
    print('\r ram数据写入完成', end="")

    print('\r 正在汇总channel数据', end="")
    channel_total_file_name = 'output/channel_total.csv'
    global_channel_head = ['渠道', '总数量', '导入数量', '导入成功率', '处理数量', '处理成功率', '保存数量', '保存成功率']
    global_data_channel = []
    global_channel_total = {
        'total': 0,
        'input': 0,
        'process': 0,
        'output': 0
    }
    for key, value in channel_success.items():
        channel_item_list = [key]
        for k, v in value.items():
            channel_item_list.append(v)
            global_channel_total[k] += v
            if k != 'total':
                channel_item_list.append(str(round(v / value['total'], 5) * 100)[0:6])
        global_data_channel.append(channel_item_list)
    global_data_channel.append(['汇总',
                                global_channel_total['total'],
                                global_channel_total['input'],
                                str(round(global_channel_total['input'] / global_channel_total['total'], 5) * 100)[0:6],
                                global_channel_total['process'],
                                str(round(global_channel_total['process'] / global_channel_total['total'], 5) * 100)[0:6],
                                global_channel_total['output'],
                                str(round(global_channel_total['output'] / global_channel_total['total'], 5) * 100)[0:6]
                                ])
    print('\r 写入channel数据', end="")
    write_csv_head(channel_total_file_name, global_channel_head, 'w')
    write_csv_data(channel_total_file_name, global_data_channel, 'a+')
    print('\r channel数据写入完成', end="")
    print('\r 分析完成：\n %s \n %s \n %s ' % (ram_file_name, ram_total_file_name, channel_total_file_name), end="")
