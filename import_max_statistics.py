#! python3


# 统计拼图输入图片最大边长的分布情况
#
# 使用分档次进行统计：(0, 1920]，(1920,2160]，(2160,3264]，(3264,4608]统计单位为像素
# 下一个档次为上一个档次的√2‾倍
#
# 1. 统计全体数据的输入图片最大边长的分布情况
# 2. 统计各渠道下的输入图片最大边长的分布情况，渠道包括 xm, zhy, oppo, vivo, google
#
# 输入： ./puzzle_data/
#
# 输出：  ./output/import_max_channel.csv      按渠道分类的导入图片最长边分布情况
#        ./output/import_max_version.csv      按安卓版本分类的导入图片最长边分布情况



import json
import os
import csv

global_import_max_count = {
    'global': {
        1920: 0,
        2160: 0,
        3264: 0,
        4608: 0
    },
    'xm': {
        1920: 0,
        2160: 0,
        3264: 0,
        4608: 0
    },
    'zh': {
        1920: 0,
        2160: 0,
        3264: 0,
        4608: 0
    },
    'op': {
        1920: 0,
        2160: 0,
        3264: 0,
        4608: 0
    },
    'vi': {
        1920: 0,
        2160: 0,
        3264: 0,
        4608: 0
    },
    'go': {
        1920: 0,
        2160: 0,
        3264: 0,
        4608: 0
    },
}

global_import_max_count_version = {}


def parse_data(file_name):
    global global_import_max_count
    with open(file_name) as data_file:
        text = data_file.read()
        json_data = json.loads(text)
        items = json_data['hits']['hits']
        file_import_count = 0
        for item in items:
            row = item['_source']
            input_origin_sizes = row.get('bodyInfo.metric.input_origin_sizes', '[]')  # ss
            user_import_count = input_origin_sizes.count('[') - 1
            channel = row.get('clientInfo.channel', 'other')[0:2]
            version = row.get('bodyInfo.baggage.android_sdk_int', 0)
            if user_import_count < 0:
                user_import_count = 0
            file_import_count += user_import_count
            max_size_list = max_length(input_origin_sizes)
            for max_ in max_size_list:
                # 统计全局
                if max_ > max(global_import_max_count['global'].keys()):
                    create_range(max_, global_import_max_count, 'global')
                for k, v in global_import_max_count['global'].items():
                    if max_ <= k:
                        global_import_max_count['global'][k] += 1
                        break
                # 按版本分类
                if max_ > max(global_import_max_count_version[version].keys()):
                    create_range(max_, global_import_max_count_version, version)
                for k, v in global_import_max_count_version[version].items():
                    if max_ <= k:
                        global_import_max_count_version[version][k] += 1
                        break
                # 按特定渠道分类
                if channel == 'xm' or channel == 'op' or channel == 'zh' or channel == 'vi' or channel == 'go':
                    if max_ > max(global_import_max_count[channel].keys()):
                        create_range(max_, global_import_max_count, channel)
                    for k, v in global_import_max_count[channel].items():
                        if max_ <= k:
                            global_import_max_count[channel][k] += 1
                            break


def max_length(size):
    origin_wh = size[1:-1].split('],')
    max_size = []
    for i_wh in origin_wh:
        s = i_wh.replace('[', '').replace(']', '').split(',')
        m_max = 0
        for j in s:
            if j == '':
                continue
            k = int(j)
            if k > m_max:
                m_max = k
        max_size.append(m_max)
    return max_size


def create_range(max_, collections, channel):
    current_max = max(collections[channel].keys())
    while max_ > current_max:
        current_max = int(current_max * 1.414)
        if current_max % 2 == 1:
            current_max += 1
        else:
            current_max += 2
        collections[channel][current_max] = 0
    return current_max


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


def init_version_import_max_count():
    for i in range(21, 31):
        global_import_max_count_version[i] = {
            1920: 0,
            2160: 0,
            3264: 0,
            4608: 0
        }


if __name__ == '__main__':
    fs = walk_file('./puzzle_data')
    pos = 0
    file_count = file_counter('./puzzle_data')
    all_data = []
    import_file_name = 'output/import_max_channel.csv'
    init_version_import_max_count()
    global_total = 0
    for f in fs:
        pos += 1
        print('\r 正在解析： %2d / %2d : %s' % (pos, file_count, f), end="")
        parse_data(f)
    print('\r 解析完成', end="")
    # 渠道
    print('\r 正在汇总渠道分类数据', end="")
    global_csv_head = ['渠道', '总数量']
    for k, v in global_import_max_count['global'].items():
        global_csv_head.append('%dpx 数量/张' % k)
    write_csv_head(import_file_name, global_csv_head, 'w')

    for key, value in global_import_max_count.items():
        row = [key]
        total = 0
        for k, v in value.items():
            total += v
        row.append(total)
        if key == 'global':
            global_total = total
        for k, v in value.items():
            row.append(v)
        all_data.append(row)

    all_data.append([])
    global_csv_head_per = ['渠道', '总占比']
    for k, v in global_import_max_count['global'].items():
        global_csv_head_per.append('%dpx 占比 %s' % (k, ' (%)'))
    all_data.append(global_csv_head_per)
    for key, value in global_import_max_count.items():
        total = 0
        for k, v in value.items():
            total += v
        row_per = [key, str(round(total / global_total, 5) * 100)[0:6]]
        for k, v in value.items():
            row_per.append(str(round(v / total, 5) * 100)[0:6])
        all_data.append(row_per)
    print('\r 写入渠道分类数据', end="")
    write_csv_data(import_file_name, all_data, 'a+')
    print('\r 渠道分类数据写入完成 %s' % import_file_name, end="")

    # 安卓版本
    import_file_name_version = 'output/import_max_version.csv'
    print('\r 正在汇总安卓版本数据', end="")
    global_csv_head_version = ['Android api', '总数量']
    for k, v in global_import_max_count['global'].items():
        global_csv_head_version.append('%dpx 数量/张' % k)
    write_csv_head(import_file_name_version, global_csv_head_version, 'w')
    all_data.clear()
    for key, value in global_import_max_count_version.items():
        row = [key]
        total = 0
        for k, v in value.items():
            total += v
        row.append(total)
        if key == 'global':
            global_total = total
        for k, v in value.items():
            row.append(v)
        all_data.append(row)

    all_data.append([])
    global_csv_head_version_per = ['Android api', '总占比']
    for k, v in global_import_max_count['global'].items():
        global_csv_head_version_per.append('%dpx 占比 %s' % (k, ' (%)'))
    all_data.append(global_csv_head_version_per)
    for key, value in global_import_max_count_version.items():
        total = 0
        for k, v in value.items():
            total += v
        row_per = [key, str(round(total / global_total, 5) * 100)[0:6]]
        for k, v in value.items():
            row_per.append(str(round(v / total, 5) * 100)[0:6])
        all_data.append(row_per)
    print('\r 写入安卓版本数据', end="")
    write_csv_data(import_file_name_version, all_data, 'a+')
    print('\r 安卓版本数据写入完成 %s' % import_file_name_version, end="")
    print('\r 任务完成 \n %s \n %s ' % (import_file_name, import_file_name_version))
