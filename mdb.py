#!/usr/bin/python
import mysql.connector as connector
import ConfigParser
import sys

cursor = None
connection = None
series_id_cache = {}
nfp_id_cache = {}


def startup():
    global cursor, connection

    config_parser = ConfigParser.RawConfigParser()
    config_file_path = r'.dbconfig'
    config_parser.read(config_file_path)

    connection = connector.connect(host=config_parser.get('db', 'hostname'),  # your host, usually localhost
                                   user=config_parser.get('db', 'user'),  # your username
                                   passwd=config_parser.get('db', 'password'),  # your password
                                   db=config_parser.get('db', 'database'))  # name of the data base
    cursor = connection.cursor()


def shutdown():
    if cursor:
        cursor.close()

    if connection:
        connection.close()


def exec_sql_one(sql):
    cursor.execute(sql)
    r = cursor.fetchone()

    if r is None:
        return None

    return r[0]


def insert(table, columns, values):
    statement = 'insert into ' + table + ' (' + columns + ') values ' + '({0})'.format(values)
    cursor.execute(statement)
    connection.commit()


def get_series_id(series_name):
    """
    looks up the series. fails if series does not exist
    """
    if series_name not in series_id_cache:
        cursor.execute('select SeriesId from Series where name="' + series_name + '"')
        r = cursor.fetchone()

        if r is None:
            print "Series " + series_name + " not found in measurement database. Quitting."
            sys.exit(1)

        series_id_cache[series_name] = r[0]

    return series_id_cache[series_name]


def get_NFP_id(nfp):
    """
    looks up an NFP, creates that NFP if it does not exist
    """
    if nfp not in nfp_id_cache:
        cursor.execute('select ID from NFP where name="' + nfp + '"')
        r = cursor.fetchone()

        if r is None:
            print "creating new NFP: "+nfp
            cursor.execute('insert into NFP (Name) values ("' + nfp + '")')
            cursor.execute('select ID from NFP where name="' + nfp + '"')
            connection.commit()
            r = cursor.fetchone()

        nfp_id_cache[nfp] = r[0]

    return nfp_id_cache[nfp]


def store_measurements(series_name, config_id, result_map):
    """
    resultmap is a map from NFP-names to string values representing results
    """
    global cursor
    assert len(result_map) > 0
    sql = 'insert into MResults (ConfigurationID, SeriesID, NFPID, Value) values '

    for k in result_map:
        sql += '({0}, {1}, {2}, "{3}"), '.format(config_id, get_series_id(series_name), get_NFP_id(k), result_map[k])

    cursor.execute(sql[:-2])
    connection.commit()


def claim_next_measurement(series_name):
    """
    finds the next available measurement in the todo table
    returns the configurationId or None if there is no remaining configuration (or if there is a concurrency issue)
    deletes the entry from the todo table, so that it's not claimed again
    """
    connection.commit()

    try:
        sid = get_series_id(series_name)
        next_config = exec_sql_one("select ConfigurationID from Todos where SeriesId={0} order by priority, ConfigurationID Limit 1".format(sid))

        if next_config is not None:
            cursor.execute("delete from Todos where SeriesId={0} and ConfigurationId={1}".format(sid, next_config))

        connection.commit();
        return next_config
    except Exception, e:
        print e
        connection.rollback()
        return None


def count_remaining_measurements(series_names):
    return exec_sql_one("select count(*) from Todos, Series where " +
                        " or ".join(map((lambda x: '(Series.Name="'+x+'")'), series_names)))


def get_config_params(config_id):
    return exec_sql_one("select CompilerOptions from Configurations where ID=" + str(config_id))
