import pybase
from pybase.filters import *
from urllib import unquote
import hashlib
import time, datetime

# Please note that all below unit tests require the existence of a table
# to play with. Table must contain two column families specified below as well.

zkquorum="hbase01-zk-cl1-d.private\,hbase02-zk-cl1-d.private\,hbase03-zk-cl1-d.private\,hbase04-zk-cl1-d.private\,hbase05-zk-cl1-d.private"
zkquorum = zkquorum.replace("\\", "")
client1 = pybase.NewClient(zkquorum)

def test():
    i='flipboard/curator%2Fmagazine%2F8RjjrmAjQ6e4kHPOYQHSBQ%3Am%3A52304625'
    mag_service_id = unquote(i).split('/')[3]
    magazine = mag_service_id.split(':')
    mag_uuid = magazine[0]
    mag_uid = "flipboard:" + magazine[2]
    print mag_service_id, mag_uid, mag_uuid

    collection_families = {
        "article": [
        ]
    }

    #pFilter = filters.ColumnPaginationFilter(0, 10, '')
    pFilter = ColumnCountGetFilter(10)
    lastday_ts = time.mktime((datetime.date.today() - datetime.timedelta(days=1)).timetuple()) * 1000
    today_ts = time.mktime((datetime.date.today()).timetuple()) * 1000
    time_range = [long(lastday_ts),long(today_ts)]
    print time_range[0]
    print time_range[1]
    #time_range=[1464586026127,]

    rsp_col = client1.get("collection", hashlib.sha1(mag_uid).hexdigest(), families=collection_families, filters=pFilter, time_range=time_range)

    for cell in rsp_col.cells:
        print cell

if __name__ == '__main__':
    test()