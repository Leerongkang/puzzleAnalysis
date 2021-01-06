#! python3


# 统计 1920 2160 3260 的导入成功率
# 统计将 3264 提升到 4680 后的原图导入率
#
# 输入： ./puzzle_data/
#
# 输出： ./output/import_4608.csv      汇总的原图导入率以及各输入文件的详细数据

import json
import os
import csv

all_origin_import_count_1920 = 0
all_total_import_count_1920 = 0

all_origin_import_count_2160 = 0
all_total_import_count_2160 = 0

all_origin_import_count_3264 = 0
all_total_import_count_3264 = 0

all_origin_import_count_other = 0
all_total_import_count_other = 0

all_fix_to_4608_count = 0


def parse_data(file_name):
    with open(file_name) as data_file:
        text = data_file.read()
        data = []
        json_data = json.loads(text)
        items = json_data['hits']['hits']
        origin_import_count_1920 = 0
        total_import_count_1920 = 0

        origin_import_count_2160 = 0
        total_import_count_2160 = 0

        origin_import_count_3264 = 0
        total_import_count_3264 = 0

        origin_import_count_other = 0
        total_import_count_other = 0

        fix_to_4608_count = 0

        for item in items:
            row = item['_source']
            input_origin_sizes = row.get('bodyInfo.metric.input_origin_sizes', '[]')  # ss
            user_import_count = input_origin_sizes.count('[') - 1
            if user_import_count < 0:
                user_import_count = 0
            puzzle_import_size = int(float(row.get('bodyInfo.label.puzzle_import_size', 0)))
            original_wh_count = int(row.get('bodyInfo.label.original_wh_count', 0))
            if puzzle_import_size == 1920:
                origin_import_count_1920 += original_wh_count
                total_import_count_1920 += user_import_count
            elif puzzle_import_size == 2160:
                origin_import_count_2160 += original_wh_count
                total_import_count_2160 += user_import_count
            elif puzzle_import_size == 3264:
                origin_import_count_3264 += original_wh_count
                total_import_count_3264 += user_import_count
                fix_to_4608_count += count_size_smaller_than_4608(input_origin_sizes, user_import_count)
            else:
                origin_import_count_other += original_wh_count
                total_import_count_other += user_import_count

        total_import = total_import_count_1920 + total_import_count_2160 + total_import_count_3264 + total_import_count_other
        origin_import = origin_import_count_1920 + origin_import_count_2160 + origin_import_count_3264 + origin_import_count_other

        digist = 5
        per_total = 0
        if total_import != 0:
            per_total = round(origin_import / total_import, digist) * 100
        per_1920 = 0
        if total_import_count_1920 != 0:
            per_1920 = round(origin_import_count_1920 / total_import_count_1920, digist) * 100
        per_2160 = 0
        if total_import_count_2160 != 0:
            per_2160 = round(origin_import_count_2160 / total_import_count_2160, digist) * 100
        per_3264 = 0
        per_4608 = 0
        if total_import_count_3264 != 0:
            per_3264 = round(origin_import_count_3264 / total_import_count_3264, digist) * 100
            per_4608 = round(fix_to_4608_count / total_import_count_3264, digist) * 100
        per_other = 0
        if total_import_count_other != 0:
            per_other = round(origin_import_count_other / total_import_count_other, digist) * 100

        data.append(file_name[-24:])
        data.append(origin_import)
        data.append(total_import)
        data.append(str(per_total)[0:6])
        data.append(origin_import_count_1920)
        data.append(total_import_count_1920)
        data.append(str(per_1920)[0:6])
        data.append(origin_import_count_2160)
        data.append(total_import_count_2160)
        data.append(str(per_2160)[0:6])
        data.append(origin_import_count_3264)
        data.append(total_import_count_3264)
        data.append(str(per_3264)[0:6])
        data.append(fix_to_4608_count)
        data.append(str(per_4608)[0:6])
        data.append(origin_import_count_other)
        data.append(total_import_count_other)
        data.append(str(per_other)[0:6])

        global all_fix_to_4608_count, all_origin_import_count_1920, all_total_import_count_1920, all_origin_import_count_2160, all_total_import_count_2160, all_origin_import_count_3264, all_total_import_count_3264, all_origin_import_count_other, all_total_import_count_other

        all_origin_import_count_1920 += origin_import_count_1920
        all_total_import_count_1920 += total_import_count_1920
        all_origin_import_count_2160 += origin_import_count_2160
        all_total_import_count_2160 += total_import_count_2160
        all_origin_import_count_3264 += origin_import_count_3264
        all_total_import_count_3264 += total_import_count_3264
        all_origin_import_count_other += origin_import_count_other
        all_total_import_count_other += total_import_count_other
        all_fix_to_4608_count += fix_to_4608_count
    return data


def count_size_smaller_than_4608(size, input_count):
    count = input_count
    origin_wh = size[1:-1].split('],')
    for i_wh in origin_wh:
        s = i_wh.replace('[', '').replace(']', '').split(',')
        for j in s:
            if j == '':
                continue
            k = int(j)
            if k > 4608:
                count -= 1
                break
    return count


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
    import_file_name = 'output/import_4608.csv'

    for f in fs:
        pos += 1
        print('\r 正在解析： %2d / %2d : %s' % (pos, file_count, f), end="")
        all_data.append(parse_data(f))
    print('\r 解析完成', end="")
    print('\r 正在汇总数据', end="")
    global_csv_head = ['类别', '原图导入数量', '图片导入数量', '原图导入率 (%)']
    write_csv_head(import_file_name, global_csv_head, 'w')
    global_data = []

    global_import_count = all_total_import_count_other + all_total_import_count_1920 + all_total_import_count_2160 + all_total_import_count_3264
    global_origin_count_3264 = all_origin_import_count_other + all_origin_import_count_1920 + all_origin_import_count_2160 + all_origin_import_count_3264
    global_origin_count_4608 = all_origin_import_count_other + all_origin_import_count_1920 + all_origin_import_count_2160 + all_fix_to_4608_count
    global_per_3264 = 0
    global_per_4608 = 0
    if global_import_count != 0:
        global_per_3264 = round(global_origin_count_3264 / global_import_count, 5) * 100
        global_per_4608 = round(global_origin_count_4608 / global_import_count, 5) * 100
    global_data.append(['汇总3264', global_origin_count_3264, global_import_count, str(global_per_3264)[0:6]])
    global_data.append(['汇总4608', global_origin_count_4608, global_import_count, str(global_per_4608)[0:6]])
    global_data.append([])

    global_per_1920 = 0
    if all_total_import_count_1920 != 0:
        global_per_1920 = round(all_origin_import_count_1920 / all_total_import_count_1920, 5) * 100
    global_data.append(['1920', all_origin_import_count_1920, all_total_import_count_1920, str(global_per_1920)[0:6]])

    global_per_2160 = 0
    if all_total_import_count_2160 != 0:
        global_per_2160 = round(all_origin_import_count_2160 / all_total_import_count_2160, 5) * 100
    global_data.append(['2160', all_origin_import_count_2160, all_total_import_count_2160, str(global_per_2160)[0:6]])

    global_per_3264 = 0
    global_per_4608 = 0
    if all_total_import_count_3264 != 0:
        global_per_3264 = round(all_origin_import_count_3264 / all_total_import_count_3264, 5) * 100
        global_per_4608 = round(all_fix_to_4608_count / all_total_import_count_3264, 5) * 100
    global_data.append(['3264', all_origin_import_count_3264, all_total_import_count_3264, str(global_per_3264)[0:6]])
    global_data.append(['4608', all_fix_to_4608_count, all_total_import_count_3264, str(global_per_4608)[0:6]])

    global_per_other = 0
    if all_total_import_count_other != 0:
        global_per_other = round(all_origin_import_count_other / all_total_import_count_other, 5) * 100
    global_data.append(
        ['other', all_origin_import_count_other, all_total_import_count_other, str(global_per_other)[0:6]])
    global_data.append([])
    print('\r 写入汇总数据', end="")
    write_csv_data(import_file_name, global_data, 'a+')
    print('\r 开始写入单个文件数据', end="")
    file_csv_head = ['文件名', '原图导入总数量', '导入图片总数量', '总原图导入率(%)',
                     '1920原图导入数量', '1920图片导入数量', '1920原图导入率(%)',
                     '2160原图导入数量', '2160图片导入数量', '2160原图导入率(%)',
                     '3264原图导入总数量', '3264图片导入数量', '3264原图导入率(%)',
                     '4608原图导入总数量', '4608原图导入率(%)',
                     '其他尺寸原图导入数量', '其他尺寸图片导入数量', '其他尺寸原图导入率(%)']

    write_csv_head(import_file_name, file_csv_head, 'a+')
    write_csv_data(import_file_name, all_data, 'a+')
    print('\r 写入完成 %s' % import_file_name, end="")
