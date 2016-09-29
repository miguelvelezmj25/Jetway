#!/usr/bin/python
import mysql.connector as connector
import ConfigParser
import sys

configParser = ConfigParser.RawConfigParser()   
configFilePath = r'.dbconfig'
configParser.read(configFilePath)

db = connector.connect(host="feature.isri.cmu.edu", # your host, usually localhost
                       user=configParser.get('db','user'), # your username
                       passwd=configParser.get('db','passwd'), # your password
                       db="measurement") # name of the data base
cur = db.cursor()


seriesIdCache = {}
nfpIdCache = {}


def exec_sql_one(sql):
    cur.execute(sql)
    r = cur.fetchone()

    if r is None:
        return None

    return r[0]


def get_series_id(series_name):
    """
    looks up the series. fails if series does not exist
    """
    if series_name not in seriesIdCache:
        cur.execute('select SeriesId from Series where name="' + series_name + '"')
        r = cur.fetchone()

        if r is None:
            print "Series " + series_name + " not found in measurement database. Quitting."
            sys.exit(1)

        seriesIdCache[series_name] = r[0]

    return seriesIdCache[series_name]


def get_NFP_id(nfp):
    """
    looks up an NFP, creates that NFP if it does not exist
    """
    if nfp not in nfpIdCache:
        cur.execute('select ID from NFP where name="'+nfp+'"')
        r = cur.fetchone()

        if r is None:
            print "creating new NFP: "+nfp
            cur.execute('insert into NFP (Name) values ("'+nfp+'")')
            cur.execute('select ID from NFP where name="'+nfp+'"')
            db.commit()
            r = cur.fetchone()

        nfpIdCache[nfp] = r[0]

    return nfpIdCache[nfp]


def store_measurements(series_name, config_id, result_map):
    """
    resultmap is a map from NFP-names to string values representing results
    """
    global cur
    assert len(result_map) > 0
    sql = 'insert into MResults (ConfigurationID, SeriesID, NFPID, Value) values '

    for k in result_map:
        sql += '({0}, {1}, {2}, "{3}"), '.format(config_id, get_series_id(series_name), get_NFP_id(k), result_map[k])

    cur.execute(sql[:-2])
    db.commit()


def claim_next_measurement(series_name):
    """
    finds the next available measurement in the todo table
    returns the configurationId or None if there is no remaining configuration (or if there is a concurrency issue)
    deletes the entry from the todo table, so that it's not claimed again
    """
    db.commit()

    try:
        sid = get_series_id(series_name)
        next_config = exec_sql_one("select ConfigurationID from Todos where SeriesId={0} order by priority, ConfigurationID Limit 1".format(sid))

        if next_config is not None:
            cur.execute("delete from Todos where SeriesId={0} and ConfigurationId={1}".format(sid,next_config))

        db.commit();
        return next_config
    except Exception, e:
        print e
        db.rollback()
        return None


def count_remaining_measurements(series_names):
    return exec_sql_one("select count(*) from Todos, Series where " +
                        " or ".join(map((lambda x: '(Series.Name="'+x+'")'), series_names)))


def get_config_params(config_id):
    return exec_sql_one("select CompilerOptions from Configurations where ID=" + str(config_id))
