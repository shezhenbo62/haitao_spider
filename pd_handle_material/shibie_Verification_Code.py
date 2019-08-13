# coding:utf-8

import os
from PIL import Image
from aip import AipOcr
import cv2

# # 将不能识别的图片类型gif转化为可识别图片类型png
# for filename in filenames:
#     # 将路径与文件名结合起来就是每个文件的完整路径
#     info = os.path.join(file_path, filename)
#     print(info)
#     if 'gif' in info:
#         im = Image.open(info)
#         im.save(info+'.png')
# # 删除原gif图片
# for path in filenames:
#     info1 = os.path.join(file_path, path)
#     if info1.endswith('gif'):
#         os.remove(info1)


def shibie_img(filenames, file_path):
    for filename2 in filenames:
        name = filename2.split('.')[0]
        save_path = os.path.join('D://figures', filename2)
        img_path = os.path.join(file_path, filename2)
        im = cv2.imread(img_path)  # 打开图片文件
        im = cv2.cvtColor(im, cv2.COLOR_BGR2GRAY)  #灰值化
        # 二值化
        th1 = cv2.adaptiveThreshold(im, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 21, 1)
        cv2.imwrite(save_path, th1)
        return th1


def interference_line(img, img_name):
    filename = './out_img/' + img_name.split('.')[0] + '-interferenceline.jpg'
    h, w = img.shape[:2]
    for y in range(1, w - 1):
        for x in range(1, h - 1):
            count = 0
            if img[x, y - 1] > 245:
                count = count + 1
            if img[x, y + 1] > 245:
                count = count + 1
            if img[x - 1, y] > 245:
                count = count + 1
            if img[x + 1, y] > 245:
                count = count + 1
            if count > 2:
                img[x, y] = 255
    cv2.imwrite(filename, img)
    return img


def baidu_shibie():
    APP_ID = '14734850'
    API_KEY = 'TmKdrcRh6lu9AyMsOi13kwau'
    SECRET_KEY = 'mbhD3UT5OWwzqAhzF6MY9u4by9dnc6D5'

    client = AipOcr(APP_ID, API_KEY, SECRET_KEY)
    file_path = 'D:/figures'
    filenames = os.listdir(file_path)
    for filename in filenames:
        img_path = os.path.join(file_path, filename)
        name = filename.split('.')[0]
        with open(img_path, 'rb') as fp:
            # 获取文件夹的路径
            image = fp.read()
            # 定义参数变量
            options = {'detect_direction': 'true',
                       # 'language_type': 'CHN_ENG',
                       'language_type': 'ENG'
                       }
            content_list = []
            # 调用通用文字识别接口
            result = client.basicGeneral(image, options)
            if result['words_result_num'] == 0:
                print(filename + ':' + '----')
            else:
                for i in range(len(result['words_result'])):
                    content_list.append(result['words_result'][i]['words'])
                content_list = ','.join(content_list)
                print(name, content_list)


def main():
    # 读取图片
    file_path = 'D:/yanzhengma'
    filenames = os.listdir(file_path)
    shibie_img(filenames, file_path)
    baidu_shibie()


if __name__ == '__main__':
    main()
