from ..pb.Client_pb2 import GetRequest, MutateRequest, ScanRequest, Column, MutationProto
from ..filters import _to_filter, _to_time_range
from ..exceptions import MalformedFamilies, MalformedValues, MalformedTimeRange
from ..pb.HBase_pb2 import TimeRange

# Table + Family used when requesting meta information from the
# MetaRegionServer
metaTableName = "hbase:meta,,1"
metaInfoFamily = {"info": []}


class Request:

    def __init__(self, type, pb):
        self.type = type
        self.pb = pb


def master_request(meta_key):
    rq = GetRequest()
    rq.get.row = meta_key
    rq.get.column.extend(families_to_columns(metaInfoFamily))
    rq.get.closest_row_before = True
    rq.region.type = 1
    rq.region.value = metaTableName
    return Request("Get", rq)


def get_request(region, key, families, filters, time_range):
    pbFilter = _to_filter(filters)
    rq = GetRequest()
    rq.get.row = key
    rq.get.column.extend(families_to_columns(families))
    if time_range is not None:
        rq.get.time_range.CopyFrom(_to_time_range(time_range))
    rq.region.type = 1
    rq.region.value = region.region_name
    if pbFilter is not None:
        rq.get.filter.CopyFrom(pbFilter)
    return Request("Get", rq)


def put_request(region, key, values):
    rq = MutateRequest()
    rq.region.type = 1
    rq.region.value = region.region_name
    rq.mutation.row = key
    rq.mutation.mutate_type = 2
    rq.mutation.column_value.extend(values_to_column_values(values))
    return Request("Mutate", rq)


def delete_request(region, key, values):
    rq = MutateRequest()
    rq.region.type = 1
    rq.region.value = region.region_name
    rq.mutation.row = key
    rq.mutation.mutate_type = 3
    rq.mutation.column_value.extend(
        values_to_column_values(values, delete=True))
    return Request("Mutate", rq)


def append_request(region, key, values):
    rq = MutateRequest()
    rq.region.type = 1
    rq.region.value = region.region_name
    rq.mutation.row = key
    rq.mutation.mutate_type = 0
    rq.mutation.column_value.extend(values_to_column_values(values))
    return Request("Mutate", rq)


def increment_request(region, key, values):
    rq = MutateRequest()
    rq.region.type = 1
    rq.region.value = region.region_name
    rq.mutation.row = key
    rq.mutation.mutate_type = 1
    rq.mutation.column_value.extend(values_to_column_values(values))
    return Request("Mutate", rq)


def scan_request(region, start_key, stop_key, families, filters, close, scanner_id):
    rq = ScanRequest()
    rq.region.type = 1
    rq.region.value = region.region_name
    rq.number_of_rows = 128
    if close:
        rq.close_scanner = close
    if scanner_id is not None:
        rq.scanner_id = int(scanner_id)
        return Request("Scan", rq)
    rq.scan.column.extend(families_to_columns(families))
    rq.scan.start_row = start_key
    if stop_key is not None:
        rq.scan.stop_row = stop_key
    if filters is not None:
        rq.scan.filter.CopyFrom(filters)
    return Request("Scan", rq)


#  Converts a dictionary specifying ColumnFamilys -> Qualifiers into the Column pb type.
#
#    Families should look like
#    {
#        "columnFamily1": [
#            "qual1",
#            "qual2"
#        ],
#        "columnFamily2": [
#            "qual3"
#        ]
#    }
#  Also support single qualifier
#    {
#        "columnFamily1": "qual",
#        "columnFamily2": [
#            "qual3"
#        ]
#    }

def families_to_columns(fam):
    try:
        cols = []
        for key in fam.keys():
            c = Column()
            c.family = key
            val = fam[key]
            if type(val) is list:
                c.qualifier.extend(val)
            else:
                c.qualifier.append(val)
            cols.append(c)
        return cols
    except Exception:
        raise MalformedFamilies()


# Converts a dictionary specifying ColumnFamilys -> Qualifiers -> Values into the protobuf type.
#
#   {
#      "cf1": {
#           "mycol": "hodor",
#           "mycol2": "alsohodor"
#      },
#      "cf2": {
#           "mycolumn7": 24
#      }
#   }
def values_to_column_values(val, delete=False):
    try:
        col_vals = []
        for cf in val.keys():
            cv = MutationProto.ColumnValue()
            cv.family = cf
            qual_vals = []
            for qual in val[cf].keys():
                qv = MutationProto.ColumnValue.QualifierValue()
                qv.qualifier = qual
                qv.value = val[cf][qual]
                if delete:
                    qv.delete_type = 1
                qual_vals.append(qv)
            cv.qualifier_value.extend(qual_vals)
            col_vals.append(cv)
        return col_vals
    except Exception:
        raise MalformedValues()