#!/usr/bin/python
import mysql.connector as connector
import ConfigParser
import sys

connection = None
cursor = None
series_id_cache = {}
nfp_id_cache = {}


def startup(db):
    config_parser = ConfigParser.RawConfigParser()
    config_file_path = r'.dbconfig'
    config_parser.read(config_file_path)

    global connection, cursor
    connection = connector.connect(host=config_parser.get(db, 'hostname'),  # your host, usually localhost
                                   user=config_parser.get(db, 'user'),  # your username
                                   passwd=config_parser.get(db, 'password'),  # your password
                                   db=config_parser.get(db, 'database'))  # name of the data base
    cursor = connection.cursor()


def shutdown():
    if cursor:
        cursor.close()

    if connection:
        connection.close()


def exec_sql_one(sql):
    print sql
    cursor.execute(sql)
    r = cursor.fetchone()

    if r is None:
        return None

    return r[0]


def exec_sql(sql):
    print sql
    cursor.execute(sql)
    r = cursor.fetchall()

    if r is None:
        return None

    return r


def select_ids(sql):
    statement = 'select id ' + sql
    print statement

    ids = []
    for element in exec_sql(statement):
        ids.append(str(element[0]))

    return ids


def insert(table, columns, values):
    statement = 'insert into ' + table + ' (' + columns + ') values ({0})'.format(values)
    print statement

    cursor.execute(statement)
    connection.commit()


def get_nfp_id(nfp):
    """
    looks up an nfp, creates that nfp if it does not exist
    """
    if nfp not in nfp_id_cache:
        statement = 'select id from nfps where name="{0}"'.format(nfp)
        print statement
        cursor.execute(statement)
        r = cursor.fetchone()

        if r is None:
            print "creating new nfp: "+nfp
            statement = 'insert into nfps (Name) values ("{0}")'.format(nfp)
            print statement

            cursor.execute(statement)
            connection.commit()
            statement = 'select id from nfps where name="{0}"'.format(nfp)
            print statement

            cursor.execute(statement)
            r = cursor.fetchone()

        nfp_id_cache[nfp] = r[0]

    return nfp_id_cache[nfp]


def add_configuration(option):
    statement = 'insert into configurations (options) values ("{0}")'.format(option)
    print statement

    cursor.execute(statement)
    connection.commit()


def get_configurations_like_option(option):
    statement = 'select * from configurations where options like "{0}"'.format(option)
    print statement

    configurations = []
    for element in exec_sql(statement):
        configurations.append(element)

    return configurations


def get_next_todo():
    statement = 'select * from todos order by priority limit 1'
    print statement

    cursor.execute(statement)
    result = cursor.fetchall()
    id = result[0][0]
    iterations = result[0][1]

    if id:
        if iterations > 1:
            statement = 'update todos set iterations = iterations - 1 where configuration_id = "{0}"'.format(id)
        else:
            statement = 'delete from todos where configuration_id = {0}'.format(id)

        print statement
        cursor.execute(statement)
        connection.commit()

    return id


def add_todo(id, iterations, priority=None):
    if priority is not None:
        statement = 'insert into todos (configuration_id, iterations, priority) ' \
                    'values ("{0}", {1}, {2})'.format(id, iterations, priority)
    else:
        statement = 'insert into todos (configuration_id, iterations) ' \
                    'values ("{0}", {1})'.format(id, iterations)

    print statement
    cursor.execute(statement)
    connection.commit()


############### NOT TESTED ###############

def get_series_id(series_name):
    """
    looks up the series. fails if series does not exist
    """
    if series_name not in series_id_cache:
        statement = 'select SeriesId from Series where name="' + series_name + '"'
        print statement

        cursor.execute(statement)
        r = cursor.fetchone()

        if r is None:
            print "Series " + series_name + " not found in measurement database. Quitting."
            sys.exit(1)

        series_id_cache[series_name] = r[0]

    return series_id_cache[series_name]


def store_measurements(series_name, config_id, result_map):
    """
    resultmap is a map from nfp-names to string values representing results
    """
    global cursor
    assert len(result_map) > 0
    sql = 'insert into MResults (ConfigurationID, SeriesID, nfpid, Value) values '

    for k in result_map:
        sql += '({0}, {1}, {2}, "{3}"), '.format(config_id, get_series_id(series_name), get_nfp_id(k), result_map[k])

    print sql[:-2]
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
            statement = "delete from Todos where SeriesId={0} and ConfigurationId={1}".format(sid, next_config)
            print statement

            cursor.execute(statement)

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
