import csv
import operator
from collections import OrderedDict

import redis
import urllib2 as url2
import zipfile as zf

from bs4 import BeautifulSoup

redis_db = redis.StrictRedis(host='localhost', port=6379, db=0)
redis_db1 = redis.StrictRedis(host='localhost', port=6379, db=1)
data = redis_db.keys("*")
values = [redis_db.hgetall(item) for item in data]


class GetCsvFromZip(object):
    """
    Using urllib2 and BeautifulsSoup to get the link of the file form the webpage and getting the href tag value of
           that <a></a> tag
           Since the provided link also includes some ajax content so normal BeautifulSoap methods wont work.
           So I am directly using the link from where the content is coming..
    """

    # page_url = 'http://www.bseindia.com/markets/equity/EQReports/BhavCopyDebt.aspx?expandable=3'
    page_url = 'http://www.bseindia.com/markets/equity/EQReports/Equitydebcopy.aspx'

    def __init__(self):
        """__init__ method when class will instantiate it will automatically start fetching url and extracting data
        from url"""
        self.get_link()

    def get_link(self):
        """
        Tihs methos will find href attribute of  <a></a> whose id is 'btnhylZip"
        :return: the link of zip file to get_zip_file() method.
        """

        header = {'user-agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                'Chrome/63.0.3239.108 Safari/537.36'}
        req = url2.Request(self.page_url, headers=header)
        resp = url2.urlopen(req)
        resp_data = resp.read()
        parsed_html = BeautifulSoup(resp_data, 'html.parser')
        zip_file_link = parsed_html.find('a', attrs={'id': 'btnhylZip'})['href']  # link of the zip file

        return self.get_zip_file(zip_file_link)

    def get_zip_file(self, link_of_anchor_tag):
        """ This method accepts link of the zip file from the get_zip() method and will download that zip file """

        # Initiating Downloading of zip file
        try:
            zip_file_data = url2.urlopen(link_of_anchor_tag)
            zip_file = zip_file_data.read()

            # setting zip file name and writing the zip file
            zip_name = zip_file_data.url.split('/')[-1]
            print('attempting to download file %s' % zip_name)

            with open(zip_name, 'wb') as data:
                data.write(zip_file)
                print('File %s Download complete' % zip_name)
        except Exception as e:
            zip_name = ''
            print(e)
        return self.extract_csv(zip_name)

    def extract_csv(self, zip_name):
        """ This method will receive the name of the zip file and will extract the CSV file from the zip file """
        # Extracting CSV file from Zip file

        try:
            print('Extracting data from zip file')
            with zf.ZipFile(zip_name, 'r') as exract_csv:
                exract_csv.printdir()
                exract_csv.extractall()
                csv_file_name = exract_csv.namelist()[0]
                print('Data extraction complete')
            columns = [0, 1, 4, 5, 6, 7]
            data = get_csv_data(csv_file_name, columns=columns)
            return self.store_data(data)
        except Exception as e:
            print(e)

    def store_data(self, data):
        """
        This function will get the data from get_csv_data function and will try to write that data into the Redis database.
        The datatype for insertion will be hmset as it support mass input.
        :param data: received data that is returned by get_csv_data() function.
        :return:
        """

        print('inserting data into database  by SC_CODE ')
        del data[0]
        try:
            [redis_db.hmset('entry:' + str(item[0]).strip(),
                            {'SC_CODE': item[0], 'SC_NAME': str(item[1]), 'OPEN': float(item[2]), 'HIGH': float(item[3]),
                             'LOW': float(item[4]), 'CLOSE': float(item[5])}) for item in data for i in range(len(item))]
            print('Success')
        except Exception as e:
            print(e)
        return self.store_data_by_name(data)

    def store_data_by_name(self, data):

        """
        This function will get the data from get_csv_data function and will try to write that data into the Redis database.
        The datatype for insertion will be hmset as it support mass input.
        :param data: received data that is returned by get_csv_data() function.
        :return:
        """

        print('inserting data into database by SC_NAME')
        try:
            [redis_db1.hmset('entry:' + str(item[1]).strip(),
                             {'SC_CODE': item[0], 'SC_NAME': item[1], 'OPEN': item[2], 'HIGH': item[3],
                              'LOW': item[4], 'CLOSE': item[5]}) for item in data for i in range(len(item))]
            print('Success')
        except Exception as e:
            print(e)
        return


def get_csv_data(csv_file, **columns):
    """
    This function will read the complete CSV file and return the selected columns  i.e.  columns = [0, 1, 4, 5, 6, 7].
    These columns numbers stands for [code, name, open, high, low, close] from the existing CSV. After reading the
    particular columns, this will return the result to the caller.
    """

    col1, col2, col3, col4, col5, col6 = columns['columns']
    with open(csv_file) as csvf:
        csv_data = csv.reader(csvf)
        return [[row[col1], row[col2], row[col3], row[col4], row[col5], row[col6]] for row in csv_data]


def get_type(value):
    try:
        value = int(value)
        return value
    except Exception as e:
        msg = e
    try:
        value = str(value).upper()
        return value
    except Exception as e:
        msg = e
        return e


def fetch_data(field_id, reverse=True):
    print('trying')
    field_value = get_value_by_id(field_id)
    newlist = sorted(values, key=operator.itemgetter(field_value), reverse=reverse)
    # abc = newlist.sort(key=lambda val: float(val[5]))
    return newlist[0:10]


def get_value_by_id(field_id):
    if field_id == 1:
        return 'SC_CODE'
    elif field_id == 2:
        return 'SC_NAME'
    elif field_id == 3:
        return 'OPEN'
    elif field_id == 4:
        return 'HIGH'
    elif field_id == 5:
        return 'LOW'
    elif field_id == 6:
        return 'CLOSE'
    else:
        return ''


def search_data_by_code(val):
    try:
        result = redis_db.hgetall('entry:' + str(val))
        return result
    except KeyError:
        return "Couldn't find key %s " % val


def search_data_by_name(val):
    try:
        result = redis_db1.hgetall('entry:' + str(val))
        return result
    except KeyError:
        return "Couldn't find key %s " % val


GetCsvFromZip()
