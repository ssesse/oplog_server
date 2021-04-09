import time
import sys
import os
import re
import logging as log

import pymongo
from bson import BSON
from bson.json_util import dumps

try:
    from .rotate_file import RotatingFile
except Exception:
    from rotate_file import RotatingFile


def mongo_connect_replset(connect_instance_list, username='mongodb_backup', auth_source='admin', password="",
                          read_preference='secondaryPreferred'):
    mongo_connect = pymongo.MongoClient(host=connect_instance_list, username=username, password=password,
                                        authSource=auth_source, readPreference=read_preference)
    return mongo_connect


class OplogGet(object):
    def __init__(self, **kwargs):
        """

        :param kwargs:
        """
        self.connect_instance_list = kwargs.get('connect_instance_list')
        self.username = kwargs.get('username')
        self.password = kwargs.get('password')
        self.read_preference = kwargs.get('read_preference', 'secondaryPreferred')
        self.mongo_connect = kwargs.get('mongo_connect')
        if kwargs.get('mongo_connect') is not None:
            self.mongo_connect = kwargs.get('mongo_connect')
        else:
            self.mongo_connect = mongo_connect_replset(connect_instance_list=self.connect_instance_list,
                                                       username=self.username, password=self.password,
                                                       read_preference=self.read_preference)

        self.db_colls = kwargs.get('db_colls')
        self.oplog_directory = kwargs.get('oplog_directory', './')
        if not os.path.exists(self.oplog_directory):
            os.makedirs(self.oplog_directory)

        max_file_size = kwargs.get('max_file_size', 128 * 1024 * 1024)
        rotate_by = kwargs.get('rotate_by', 'size')
        compress_method = kwargs.get('compress_method', 'gzip')
        flush_rt = kwargs.get('flush_rt', '0')
        write_mod = kwargs.get('write_mod')
        filename = kwargs.get('filename', 'oplog.bson')
        self.rotate_file = RotatingFile(directory=self.oplog_directory, max_file_size=max_file_size,
                                        write_mod=write_mod, flush_rt=flush_rt,
                                        filename=filename, rotate_by=rotate_by, compress_method=compress_method)

    def oplog_get(self, ts_begging_position=1):
        """
        :param ts_begging_position:
        :param append_or_rewrite: 1: append , 2: rewrite
        :return:
        """

        oplog = self.mongo_connect.local.oplog.rs
        if ts_begging_position == 1:
            first = oplog.find().sort('$natural', pymongo.DESCENDING).limit(-1).next()
            ts = first['ts']
        elif ts_begging_position == -1:
            first = oplog.find().sort('$natural', pymongo.ASCENDING).limit(-1).next()
            ts = first['ts']
        else:
            first = oplog.find().sort('$natural', pymongo.ASCENDING).limit(-1).next()
            ts = first['ts']
        default_ns_filter = {"$and": [{"ns": {"$not": re.compile("admin\\.")}},
                                      {"ns": {"$not": re.compile("local\\.")}},
                                      {"ns": {"$not": re.compile("config\\.")}}]}
        ts_filter = {'ts': {'$gt': ts}}
        op_filter = {"$or": [{"op": "i"}, {"op": "u"}, {"op": "d"}], }
        if self.db_colls is not None:
            ns_filter_list = []
            for db_coll in self.db_colls:
                database = db_coll.get('database')
                colls = db_coll.get('collections')
                if colls is None:
                    db_coll_filter = {"ns": re.compile("{database}\\.".format(database=database))}
                    ns_filter_list.append(db_coll_filter)
                else:
                    for coll in colls:
                        db_coll_filter = {"ns": "{database}.{coll}".format(database=database, coll=coll)}
                        ns_filter_list.append(db_coll_filter)
            ns_filter = {"$or": ns_filter_list}
            oplog_filter = {"$and": [ts_filter, op_filter, default_ns_filter, ns_filter]}
        else:
            oplog_filter = {"$and": [ts_filter, op_filter, default_ns_filter]}
        while True:
            # For a regular capped collection CursorType.TAILABLE_AWAIT is the
            # only option required to create a tailable cursor. When querying the
            # oplog, the oplog_replay option enables an optimization to quickly
            # find the 'ts' value we're looking for. The oplog_replay option
            # can only be used when querying the oplog. Starting in MongoDB 4.4
            # this option is ignored by the server as queries against the oplog
            # are optimized automatically by the MongoDB query engine.

            # cursor = oplog.find({'ts': {'$gt': ts}, "ns": {"$not": {"$regex": "admin"}}},

            cursor = oplog.find(oplog_filter,
                                cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                                oplog_replay=True)
            while cursor.alive:
                for doc in cursor:
                    try:
                        del doc['lsid']
                    except KeyError:
                        pass
                    self.rotate_file.write(BSON.encode(doc))

                time.sleep(1)

def main():
    host = ["10.1.1.232:35001", "10.1.1.232:35002"]
    username = 'mongodb_backup'
    password = 'xxxxx'
    db_colls = [
        {
            "database": "ubi",
            "collections": [
                "driver_insurance_income_daily",
                "driver_insurance_income_month"
            ]
        }
    ]
    write_mod = 'ab'
    oplog_directory = "./"
    flush_rt = 1
    oplog_get = OplogGet(connect_instance_list=host, password=password, username=username,
                         oplog_directory=oplog_directory, write_mod=write_mod, flush_rt=flush_rt, db_colls=db_colls)
    oplog_get.oplog_get(ts_begging_position=1)


if __name__ == "__main__":
    sys.exit(main())
