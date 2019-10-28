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
        
        except:
            # print(str(e))
            pass

    @staticmethod
    def getpoints(min_hoanh, max_hoanh, min_tung, max_tung, scope):
        '''     Get list of points
                Args:   min_hoanh, max_hoanh, min_tung, max_tung
                        scope: 2 = 20m btw each point , 4 = 40 m ....
                Return: list of crawl points
        '''
        points          = []
        sub_lst        = []
        count          = 0

        _p1 = Point("{0} {1}".format(str(min_hoanh), str(min_tung)))
        _p2 = Point("{0} {1}".format(str(max_hoanh), str(min_tung)))
        _p3 = Point("{0} {1}".format(str(max_hoanh), str(max_tung)))
        _p4 = Point("{0} {1}".format(str(min_hoanh), str(max_tung)))
        
        #number of points in hoanh_dist_lst
        num_hoanh  =   distance.distance(_p1, _p2).kilometers * 1000  / scope 
        #number of points in tung_dist_lst
        num_tung   =   distance.distance(_p1, _p4).kilometers * 1000 / scope

        # scope to degree (10 m -> gps distance)
        min_hoanh_dist  = (max_hoanh - min_hoanh) / num_hoanh
        min_tung_dist   = (max_tung - min_tung ) / num_tung

        #get list of points with each sub_lst contains 100 points -> easy to append file
        for lat in np.arange(min_hoanh, max_hoanh + min_hoanh_dist, min_hoanh_dist ):
            for lon in np.arange(min_tung, max_tung + min_tung_dist, min_tung_dist):
                if count == 100:
                    points.append(sub_lst)
                    sub_lst    = []
                    sub_lst.append((lat,lon))
                    count      = 1
                else:
                    sub_lst.append((lat, lon))
                    count += 1

        return points
        
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
                    # if 'Thành phó'  in  str(data['properties']['state']):
                    #     tpho =  ' '.join(s for s in str(data['properties']['state']).split()[2:]).strip()
                    # else:
                    #     tpho = str(data['properties']['state'])
                    tpho = str(data['properties']['state']).replace('Thành Phố ', '').strip()
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
    def writejson(file, data):
        for jitem in data:
            try:
                if jitem is not None:
                    json.dump(jitem, file, ensure_ascii=False)
                    file.write('\n')
            except:
                pass
        del data

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
        p.close()

        VmapCrawler.writecsv(file_path, data, dict_quan)

        del data
        file = open(textfile, 'w')
        file.write(str(continue_num + i))
        file.close()

def getvmapjson(min_hoanh, max_hoanh, min_tung, max_tung, scope, jsonfile, countfile , continue_num = 0,  dict_quan = None):
    '''     Crawl data and write to csv file
        min_hoanh, max_hoanh, min_tung, max_tung
        scope: 2 = 20m, 3 = 30m, 4 = 40m: smaller scope = more points crawled -> longer
        file_path: file_path to csv file
        continue_num: back to the a specific loop where you left last time to continue crawling,default = 0
        dict_quan: dictionary in format { quan1: { phuong1, phuong2, phuong3 } }, default = None
    '''
    file_json = open(jsonfile, 'a')

    points = VmapCrawler.getpoints(min_hoanh, max_hoanh, min_tung, max_tung, scope)

    if continue_num != 0:
        points = points[continue_num:]     
    for i in range(len(points)):
        urls = VmapCrawler.geturl(points[i])
        p = mp.Pool(processes= len(urls))
        data = p.map(VmapCrawler.getlinks, urls)
        p.close()

        VmapCrawler.writejson(file_json, data)

        file_count = open(countfile, 'w')
        file_count.write(str( continue_num +  i))
        file_count.close()    
    file_json.close()

if __name__ == "__main__":
    getvmapjson(16.01186500000000,  16.10589305555600, 108.15683083333000, 108.25513055556000, 100, '../danang.json' , '../count.txt')