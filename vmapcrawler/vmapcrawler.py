import os 
import string 
import json
import requests
import pandas as pd
import numpy as np
import multiprocessing as mp
from geopy import distance, Point
import csv

class VmapCrawler:
    @staticmethod
    def geturl(points):
        '''     Get latitude and longitude and form a new url
                Arg:        point(latitude, longitude)        
                Return:     list of urls
        '''
        urls = []

        for point in points:
            url = 'https://vmap.vn/geocode2/reverse?lat={0}&lon={1}'.format( str(point[0]) , str(point[1]))
            urls.append(url)
        return urls

    @staticmethod
    def getlinks(url):
        '''     Get json of url response
                Arg:    url
                Return: {'features': [{'geometry': {'coordinates': [105.7532999, 21.0653662], 'type': 'Point'}, 'type': 'Feature', 'properties': {'osm_id': 602021270, 'osm_type': 'W', 'extent': [105.7532999, 21.0655984, 105.7533661, 21.0653662], 'country': 'Vietnam', 'osm_key': 'highway', 'city': 'Minh Khai', 'osm_value': 'secondary', 'postcode': '04', 'name': 'Văn Tiến Dũng', 'state': 'Thành Phố Hà Nội'}}], 'type': 'FeatureCollection'}
        '''
        try:
            resp = requests.get(url)

            if (resp.ok):
                return json.loads(resp.content)
        
        except Exception as e:
            print(str(e))

    @staticmethod
    def getpoints(min_hoanh, max_hoanh, min_tung, max_tung, scope):
        '''     Get list of points
                Args:   min_hoanh, max_hoanh, min_tung, max_tung
                        scope: 2 = 20m btw each point , 4 = 40 m ....
                Return: list of crawl points
        '''
        hoanh_dist_lst  = []
        tung_dist_lst   = []
        points          = []
        _points         = []
        _sub_lst        = []
        _count          = 0

        _p1 = Point("{0} {1}".format(str(min_hoanh), str(min_tung)))
        _p2 = Point("{0} {1}".format(str(max_hoanh), str(min_tung)))
        _p3 = Point("{0} {1}".format(str(max_hoanh), str(max_tung)))
        _p4 = Point("{0} {1}".format(str(min_hoanh), str(max_tung)))
        
        #number of points in hoanh_dist_lst
        _num_hoanh  =   distance.distance(_p1, _p2).kilometers * 1000  / scope 
        #number of points in tung_dist_lst
        _num_tung   =   distance.distance(_p1, _p4).kilometers * 1000 / scope

        # scope to degree (10 m -> gps distance)
        min_hoanh_dist  = (max_hoanh - min_hoanh) / _num_hoanh
        min_tung_dist   = (max_tung - min_tung ) / _num_tung

        #get list 
        for _ in range(int(_num_hoanh)):
            hoanh_dist_lst.append(min_hoanh + _ * min_hoanh_dist )
        for _ in range(int(_num_tung)):
            tung_dist_lst.append(min_tung + _*min_tung_dist )

        #get list of points with each sub_lst contains 100 points -> easy to append file
        for i in hoanh_dist_lst:
            for j in tung_dist_lst:
                if _count == 100:
                    points.append(_sub_lst)
                    _sub_lst    = []
                    _count      = 0
                else:
                    _sub_lst.append((i, j))
                    _count += 1

        return points

    @staticmethod
    def removeduplicate(data):
        ''' Find repeated json data in list and remove 
        '''
        seen = []
        for x in data:
            if x not in seen:
                yield x
                seen.append(x)

    @staticmethod
    def matchKey(value, dict_quan):
        ''' tim quan/huyen tuong ung voi xa/phuong
        '''
        for key in dict_quan.keys():
            if value in dict_quan[key]:
                return key
        return ''


    @staticmethod
    def writecsv(filename, _data, dict_quan=None):
        ''' Return csv file in list with format: hoanh, tung, sonha, pho, phuong, quan, tpho
            Args:   filename(csv dir)
                    _data: json data
                    dict_quan: dictionary in format: {quan1: [ huyen1, huyen2, huyen 3]}
        '''
        res_data = []
        for json in _data:
            try:
                data = json['features'][-1]
                tung = data['geometry']['coordinates'][0]
                hoanh = data['geometry']['coordinates'][1]

                try:
                    osm_key = data['properties']['osm_key']
                except:
                    osm_key = ''
                    
                try:
                    osm_value = data['properties']['osm_value']
                except:
                    osm_value = ''

                try:
                    name = data['properties']['name']
                except:
                    name = ''


                try:
                    sonha = data['properties']['housenumber']
                except:
                    sonha = ''
                
                try:
                    pho  = data['properties']['street']
                except:
                    pho = ''
                
                try:
                    phuong = data['properties']['city']
                except:
                    phuong = ''
                
                try:
                    if 'Thành phó'  in  str(data['properties']['state']):
                        tpho =  ' '.join(s for s in str(data['properties']['state']).split()[2:]).strip()
                    else:
                        tpho = str(data['properties']['state'])
                except:
                    tpho = ''
                
                if dict_quan is not  None:
                    quan = [ VmapCrawler.matchKey(phuong, dict_quan) if VmapCrawler.matchKey(phuong, dict_quan) else '' ][-1]
                else:
                    quan = ''
                res_data.append((hoanh, tung, osm_key, osm_value, name, sonha, pho, phuong, quan, tpho ))
            except:
                pass
            
        df_crawl = pd.DataFrame(data= res_data, columns=['hoanh', 'tung', 'osm_key', 'osm_value', 'name', 'sonha', 'pho', 'phuong', 'quan', 'tp']).drop_duplicates()
        df_crawl.to_csv(filename, mode='a', index=None, header= False)

    @staticmethod
    def writejson(filename, _data):
        with open(filename, 'a', encoding='utf-8') as file:
            for jitem in _data:
                json.dump(jitem, file, ensure_ascii=False)
                file.write('\n')


def getvmapcsv(min_hoanh, max_hoanh, min_tung, max_tung, scope, file_path, textfile , continue_num = 0,  dict_quan = None):
    '''     Crawl data and write to csv file
        min_hoanh, max_hoanh, min_tung, max_tung
        scope: 2 = 20m, 3 = 30m, 4 = 40m: smaller scope = more points crawled -> longer
        file_path: file_path to csv file
        continue_num: back to the a specific loop where you left last time to continue crawling,default = 0
        dict_quan: dictionary in format { quan1: { phuong1, phuong2, phuong3 } }, default = None
    '''
    points = VmapCrawler.getpoints(min_hoanh, max_hoanh, min_tung, max_tung, scope)

    if continue_num != 0:
        points = points[continue_num:]
    for i in range(len(points)):
        urls = VmapCrawler.geturl(points[i])
        p = mp.Pool(processes= len(urls))

        data = p.map(VmapCrawler.getlinks, urls)
        data_remove_dup = list(VmapCrawler.removeduplicate(data))
        p.close()

        VmapCrawler.writecsv(file_path, data_remove_dup, dict_quan)
        # VmapCrawler.writejson(file_path, data_remove_dup)
        file = open(textfile, 'w')
        file.write(str(i))
        file.close()

def getvmapjson(min_hoanh, max_hoanh, min_tung, max_tung, scope, file_path, textfile , continue_num = 0,  dict_quan = None):
    '''     Crawl data and write to csv file
        min_hoanh, max_hoanh, min_tung, max_tung
        scope: 2 = 20m, 3 = 30m, 4 = 40m: smaller scope = more points crawled -> longer
        file_path: file_path to csv file
        continue_num: back to the a specific loop where you left last time to continue crawling,default = 0
        dict_quan: dictionary in format { quan1: { phuong1, phuong2, phuong3 } }, default = None
    '''
    points = VmapCrawler.getpoints(min_hoanh, max_hoanh, min_tung, max_tung, scope)

    if continue_num != 0:
        points = points[continue_num:]
    for i in range(len(points)):
        urls = VmapCrawler.geturl(points[i])
        p = mp.Pool(processes= len(urls))

        data = p.map(VmapCrawler.getlinks, urls)
        data_remove_dup = list(VmapCrawler.removeduplicate(data))
        p.close()

        # VmapCrawler.writecsv(file_path, data_remove_dup, dict_quan)
        VmapCrawler.writejson(file_path, data_remove_dup)
        file = open(textfile, 'w')
        file.write(str( continue_num +  i))
        file.close()

# if __name__ == '__main__':
#     dict_quan = {
#         'Ba Đình' : [ 'Cống Vị', 'Điện Biên', 'Đội Cấn', 'Giảng Võ', 'Kim Mã', 'Liễu Giai', 'Ngọc Hà', 'Ngọc Khánh', 'Nguyễn Trung Trực', 'Phúc Xá', 'Quán Thánh', 'Thành Công', 'Trúc Bạch', 'Vĩnh Phúc'],
#         'Hoàn Kiếm' : ['Phúc Tân', 'Đồng Xuân', 'Hàng Mã', 'Hàng Buồm' , 'Hàng Đào' , 'Hàng Bồ', 'Cửa Đông', 'Lý Thái Tổ' ,'Hàng Bạc' , 'Hàng Gai', 'Chương Dương Độ', 'Hàng Trống' ,'Cửa Nam' , 'Hàng Bông', 'Tràng Tiền', 'Trần Hưng Đạo' ,'Phan Chu Trinh' , 'Hàng Bài' ],
#         'Tây Hồ' : ['Bưởi', 'Yên Phụ', 'Thuỵ Khuê', 'Tứ Liên', 'Quảng An', 'Nhật Tân', 'Xuân La', 'Phú Thượng'],
#         'Long Biên' : ['Bồ Đề','Cự Khối','Đức Giang','Gia Thụy','Giang Biên','Long Biên','Ngọc Lâm','Ngọc Thụy','Phúc Đồng','Phúc Lợi','Sài Đồng','Thạch Bàn','Thượng Thanh','Việt Hưng' ],
#         'Cầu Giấy' : ['Dịch Vọng', 'Dịch Vọng Hậu', 'Mai Dịch', 'Nghĩa Đô', 'Nghĩa Tân', 'Quan Hoa', 'Trung Hòa', 'Yên Hòa' ],
#         'Đống Đa' : ['Cát Linh', 'Hàng Bột', 'Khâm Thiên', 'Khương Thượng', 'Kim Liên', 'Láng Hạ', 'Láng Thượng', 'Nam Đồng', 'Ngã Tư Sở', 'Ô Chợ Dừa', 'Phương Liên', 'Phương Mai', 'Quang Trung', 'Quốc Tử Giám', 'Thịnh Quang', 'Thổ Quan', 'Trung Liệt', 'Trung Phụng', 'Trung Tự', 'Văn Chương', 'Văn Miếu'],
#         'Hai Bà Trưng' : ['Bạch Đằng', 'Bách Khoa', 'Bạch Mai', 'Bùi Thị Xuân', 'Cầu Dền', 'Đống Mác', 'Đồng Nhân', 'Đồng Tâm', 'Giáp Bát', 'Lê Đại Hành', 'Minh Khai', 'Ngô Thì Nhậm', 'Nguyễn Du', 'Phạm Đình Hổ', 'Phố Huế', 'Quỳnh Lôi', 'Quỳnh Mai', 'Thanh Lương', 'Thanh Nhàn', 'Trương Định', 'Tương Mai', 'Vĩnh Tuy'],
#         'Hoàng Mai' : ['Đại Kim', 'Định Công', 'Giáp Bát', 'Hoàng Liệt', 'Hoàng Văn Thụ', 'Lĩnh Nam', 'Mai Động', 'Tân Mai', 'Thanh Trì', 'Thịnh Liệt', 'Trần Phú', 'Tương Mai', 'Vĩnh Hưng', 'Yên Sở'],
#         'Thanh Xuân' : ['Hạ Đình', 'Khương Đình', 'Khương Mai', 'Khương Trung', 'Kim Giang', 'Nhân Chính', 'Phương Liệt', 'Thanh Xuân Bắc', 'Thanh Xuân Nam', 'Thanh Xuân Trung', 'Thượng Đình'],
#         'Sóc Sơn' : ['Bắc Phú', 'Bắc Sơn', 'Đông Xuân', 'Đức Hòa', 'Hiền Ninh', 'Hồng Kỳ', 'Kim Lũ', 'Mai Đình', 'Minh Phú', 'Minh Trí', 'Nam Sơn', 'Phú Cường', 'Phù Linh', 'Phù Lỗ', 'Phú Minh', 'Quang Tiến', 'Tân Dân', 'Tân Hưng', 'Tân Minh', 'Thanh Xuân', 'Tiên Dược', 'Trung Giã', 'Việt Long', 'Xuân Giang', 'Xuân Thu' ],
#         'Đông Anh' : [  'An Dương Vương' , 'Bắc Hồng', 'Cổ Loa', 'Đại Mạch', 'Đông Hội', 'Dục Tú', 'Hải Bối', 'Kim Chung', 'Kim Nỗ', 'Liên Hà', 'Mai Lâm', 'Nam Hồng', 'Nguyên Khê', 'Tàm Xá', 'Thụy Lâm', 'Tiên Dương', 'Uy Nỗ', 'Vân Hà', 'Vân Nội', 'Việt Hùng', 'Vĩnh Ngọc', 'Võng La', 'Xuân Canh', 'Xuân Nộn'  ],
#         'Gia Lâm' : [ 'Bát Tràng', 'Cổ Bi', 'Đa Tốn', 'Đặng Xá', 'Đình Xuyên', 'Đông Dư', 'Dương Hà', 'Dương Quang', 'Dương Xá', 'Kiêu Kỵ', 'Kim Lan', 'Kim Sơn', 'Lệ Chi', 'Ninh Hiệp', 'Phù Đổng', 'Phú Thị', 'Trâu Quỳ', 'Trung Mầu', 'Văn Đức', 'Yên Thường', 'Yên Viên' ],
#         'Nam Từ Liêm' : [ 'Cầu Diễn', 'Đại Mỗ', 'Mễ Trì','Mỹ Đình' ,'Mỹ Đình 1', 'Mỹ Đình 2', 'Phú Đô', 'Tây Mỗ', 'Phương Canh', 'Trung Văn', 'Xuân Phương' ],
#         'Thanh Trì' : [ 'Đại Áng', 'Đại Kim', 'Định Công', 'Đông Mỹ', 'Duyên Hà', 'Hoàng Liệt', 'Hoàng Văn Thụ', 'Hữu Hòa', 'Khương Đình', 'Liên Ninh', 'Lĩnh Nam', 'Ngọc Hồi', 'Ngũ Hiệp', 'Tả Thanh Oai', 'Tam Hiệp', 'Tân Triều', 'Thanh Liệt', 'Thanh Trì', 'Thịnh Liệt', 'Trần Phú', 'Tứ Hiệp', 'Vạn Phúc', 'Vĩnh Quỳnh', 'Vĩnh Tuy', 'Yên Mỹ', 'Yên Sở' ],
#         'Bắc Từ Liêm' : [ 'Cổ Nhuế 1', 'Cổ Nhuế 2', 'Đông Ngạc', 'Đức Thắng', 'Liên Mạc', 'Minh Khai', 'Phú Diễn', 'Phúc Diễn', 'Tây Tựu', 'Thượng Cát', 'Thụy Phương', 'Xuân Đỉnh', 'Xuân Tảo' ],
#         'Mê Linh' : [ 'Chi Đông', 'Chu Phan' , 'Đại Thịnh', 'Hoàng Kim' , 'Kim Hoa' ,'Liên Mạc', 'Mê Linh', 'Quang Minh' ,'Tam Đồng' , 'Thạch Đà', 'Thanh Lâm', 'Tiền Phong', 'Tiến Thắng', 'Tiến Thịnh', 'Tráng Việt', 'Tự Lập', 'Vạn Yên' , 'Mê Linh' , 'Văn Khê' ],
#         'Hà Đông' : [ 'Hà Cầu', 'La Khê', 'Mộ Lao', 'Nguyễn Trãi', 'Phú La', 'Phúc La', 'Quang Trung', 'Vạn Phúc', 'Văn Quán', 'Yết Kiêu', 'Biên Giang', 'Đồng Mai', 'Dương Nội', 'Kiến Hưng', 'Phú Lãm', 'Phú Lương', 'Yên Nghĩa' ],
#         'Sơn Tây' : ['Cổ Đông', 'Đường Lâm' , 'Kim Sơn', 'Lê Lợi', 'Ngô Quyền', 'Phú Thịnh', 'Quang Trung', 'Sơn Đông', 'Sơn Lộc' ,'Thanh Mỹ', 'Trung Hưng' , 'Trung Sơn Trầm' , 'Viên Sơn', 'Xuân Khanh', 'Xuân Sơn'],
#         'Ba Vì' : ['Tây Đằng' , 'Ba Trại', 'Ba Vì', 'Cẩm Lĩnh', 'Cam Thượng', 'Châu Sơn', 'Chu Minh', 'Cổ Đô', 'Đông Quang', 'Đồng Thái', 'Khánh Thượng', 'Minh Châu', 'Minh Quang', 'Phong Vân', 'Phú Châu', 'Phú Cường', 'Phú Đông', 'Phú Phương', 'Phú Sơn', 'Sơn Đà', 'Tản Hồng', 'Tản Lĩnh', 'Thái Hòa', 'Thuần Mỹ', 'Thụy An', 'Tiên Phong', 'Tòng Bạt', 'Vân Hòa', 'Vạn Thắng', 'Vật Lại', 'Yên Bài'],
#         'Phúc Thọ' : ['Phúc Thọ', 'Cẩm Đình', 'Hát Môn', 'Hiệp Thuận', 'Liên Hiệp', 'Long Xuyên', 'Ngọc Tảo', 'Phúc Hòa', 'Phụng Thượng', 'Phương Độ', 'Sen Chiểu', 'Tam Hiệp', 'Tam Thuấn', 'Thanh Đa', 'Thọ Lộc', 'Thượng Cốc', 'Tích Giang', 'Trạch Mỹ Lộc', 'Vân Hà', 'Vân Nam', 'Vân Phúc', 'Võng Xuyên', 'Xuân Phú'],
#         'Đan Phượng' : [ 'Đan Phượng', 'Đồng Tháp', 'Hạ Mỗ', 'Hồng Hà', 'Liên Hà', 'Liên Hồng', 'Liên Trung', 'Phương Đình', 'Song Phượng', 'Tân Hội', 'Tân Lập', 'Thọ An', 'Thọ Xuân', 'Thượng Mỗ', 'Trung Châu'],
#         'Hoài Đức' : [ 'Trạm Trôi', 'An Khánh', 'An Thượng', 'Cát Quế', 'Đắc Sở', 'Di Trạch', 'Đông La', 'Đức Giang', 'Đức Thượng', 'Dương Liễu', 'Dương Nội', 'Kim Chung', 'La Phù', 'Lại Yên', 'Minh Khai', 'Sơn Đồng', 'Song Phương', 'Tiền Yên', 'Vân Canh', 'Vân Côn', 'Yên Nghĩa', 'Yên Sở' ],
#         'Quốc Oai' : [ 'Quốc Oai', 'Cấn Hữu', 'Cộng Hòa', 'Đại Thành', 'Đồng Quang', 'Đông Yên', 'Hòa Thạch', 'Liệp Tuyết', 'Nghĩa Hương', 'Ngọc Liệp', 'Ngọc Mỹ', 'Phú Cát', 'Phú Mãn', 'Phượng Cách', 'Sài Sơn', 'Tân Hòa', 'Tân Phú', 'Thạch Thán', 'Tuyết Nghĩa', 'Yên Sơn' ],
#         'Thạch Thất' : ['Liên Quan', 'Bình Phú', 'Bình Yên', 'Cẩm Yên', 'Cần Kiệm', 'Canh Nậu', 'Chàng Sơn', 'Đại Đồng', 'Dị Nậu', 'Đồng Trúc', 'Hạ Bằng', 'Hương Ngải', 'Hữu Bằng', 'Kim Quan', 'Lại Thượng', 'Phú Kim', 'Phùng Xá', 'Tân Xã', 'Thạch Hòa', 'Thạch Xá', 'Tiến Xuân', 'Yên Bình', 'Yên Trung'],
#         'Chương Mỹ' : [ 'Chúc Sơn', 'Xuân Mai', 'Đại Yên', 'Đông Phương Yên', 'Đông Sơn', 'Đồng Lạc', 'Đồng Phú', 'Hòa Chính', 'Hoàng Diệu', 'Hoàng Văn Thụ', 'Hồng Phong', 'Hợp Đồng', 'Hữu Văn', 'Lam Điền', 'Mỹ Lương', 'Nam Phương Tiến', 'Ngọc Hòa', 'Phú Nam An', 'Phú Nghĩa', 'Phụng Châu', 'Quảng Bị', 'Tân Tiến', 'Tiên Phương', 'Tốt Động', 'Thanh Bình', 'Thủy Xuân Tiên', 'Thụy Hương', 'Thượng Vực', 'Trần Phú', 'Trung Hòa', 'Trường Yên', 'Văn Võ' ],
#         'Thanh Oai' : [ 'Kim Bài', 'Bích Hòa', 'Bình Minh', 'Cao Dương', 'Cao Viên', 'Cự Khê', 'Dân Hòa', 'Đỗ Động', 'Hồng Dương', 'Kim An', 'Kim Thư', 'Liên Châu', 'Mỹ Hưng', 'Phương Trung', 'Tam Hưng', 'Tân Ước', 'Thanh Cao', 'Thanh Mai', 'Thanh Thùy', 'Thanh Văn', 'Xuân Dương' ],
#         'Thường Tín' : [ 'Thường Tín' , 'Chương Dương', 'Dũng Tiến', 'Duyên Thái', 'Hà Hồi', 'Hiền Giang', 'Hòa Bình', 'Khánh Hà', 'Hồng Vân', 'Lê Lợi', 'Liên Phương', 'Minh Cường', 'Nghiêm Xuyên', 'Nguyễn Trãi', 'Nhị Khê', 'Ninh Sở', 'Quất Động', 'Tân Minh', 'Thắng Lợi', 'Thống Nhất', 'Thư Phú', 'Tiền Phong', 'Tô Hiệu', 'Tự Nhiên', 'Vạn Điểm', 'Văn Bình', 'Văn Phú', 'Văn Tự', 'Vân Tảo' ],
#         'Phú Xuyên' : [ 'Phú Xuyên', 'Phú Minh', 'Bạch Hạ', 'Châu Can', 'Chuyên Mỹ', 'Đại Thắng', 'Đại Xuyên', 'Hoàng Long', 'Hồng Minh', 'Hồng Thái', 'Khai Thái', 'Minh Tân', 'Nam Phong', 'Nam Triều', 'Phú Túc', 'Phú Yên', 'Phúc Tiến', 'Phượng Dực', 'Quang Lãng', 'Quang Trung', 'Sơn Hà', 'Tân Dân', 'Thụy Phú', 'Tri Thủy', 'Tri Trung', 'Văn Hoàng', 'Văn Nhân', 'Vân Từ' ],
#         'Ứng Hòa' : [ 'Vân Đình', 'Cao Thành', 'Đại Cường', 'Đại Hùng', 'Đội Bình', 'Đông Lỗ', 'Đồng Tiến', 'Đồng Tân', 'Hoa Sơn', 'Hòa Lâm', 'Hòa Nam', 'Hòa Phú', 'Hòa Xá', 'Hồng Quang', 'Ứng Hòa', 'Kim Đường', 'Liên Bạt', 'Lưu Hoàng', 'Minh Đức', 'Phù Lưu', 'Phương Tú', 'Quảng Phú Cầu', 'Sơn Công', 'Tảo Dương Văn', 'Trầm Lộng', 'Trung Tú', 'Trường Thịnh', 'Vạn Thái', 'Viên An', 'Viên Nội' ],
#         'Mỹ Đức' : [ 'Đại Nghĩa', 'An Mỹ', 'An Phú', 'An Tiến', 'Bột Xuyên', 'Đại Hưng', 'Đốc Tín', 'Đồng Tâm', 'Hồng Sơn', 'Hợp Thanh', 'Hợp Tiến', 'Hùng Tiến', 'Hương Sơn', 'Lê Thanh', 'Mỹ Thành', 'Phù Lưu Tế', 'Phúc Lâm', 'Phùng Xá', 'Thượng Lâm', 'Tuy Lai', 'Vạn Kim', 'Xuy Xá' ]
#     }
#     getvmapcsv(21.1959357, 21.280875, 105.7317325, 105.829239, 100, '/home/vuviethung/Desktop/craaaas.csv' , continue_num=-1)



if __name__ == "__main__":
    getvmapjson(16.01186500000000,  16.10589305555600, 108.15683083333000, 108.25513055556000, 100, '../danang.json' , '../count.txt')