###########################################################################
# SITE           : OPM
# PROJECT        : IVOA Services Validator
# FILE           : db.py
# AUTHOR         : Renaud.Savalle@obspm.fr
# LANGUAGE       : Python
# DESCRIPTION    : Module for my sqlite3 functions
# NOTE           : 
###########################################################################
# HISTORY        : 
#                : Version 1.2
#                :    - do a commit() only when sql query starts with INSERT or UPDATE or CREATE
#                : Version 1.1
#                :    - added lock_timeout=30 secs while connecting to avoid "EXCEPTION database is locked while executing query"
#                :    - added display "Query executed OK" to help debugging
#                : Version 1.0 
#                :    - Created: 2017-12-21 to factorize these functions for val.py and query-rr.py
###########################################################################


import sqlite3
import os
import sys
import logging


def open_db(db_file):
    '''
    open a sqlite3 file and return connection
    :param db: name of sqlite3 db file
    :return: sqlite3 connection object
    '''
    #global logger
    
    #if os.path.isfile(db):
    logging.info("Opening or creating db=%s",db_file)
    try:
        lock_timeout=30; # default timeout is 5 secs, increase it to 30 to avoid "EXCEPTION database is locked while executing query"
        conn = sqlite3.connect(db_file,lock_timeout)
        #conn.row_factory = sqlite3.Row # not used because we need a regular array to split it later
    except: 
        logging.error("Could not open db %s",db_file)
    #else: 
    #   logging.error("db_file %s is not an existing readable file. Aborting",db)
    #    sys.exit(10)
    
    # could do that in Python3 ? 
    #conn.set_trace_callback(logging.debug)
    
    return conn


def execute_db(conn, sql, values=[], stop=False):
    '''
    execute a SQL query, print query before executing, handle exceptions
    :param conn: sqlite3 connection object
    :param sql: sql string with ? placeholders
    :param values: values for the placeholders
    :param stop: True => exit program in case of exception 
    :return: the cursor created (for the caller to get the results)
    '''
    
    # Prepare sql string for display - from https://stackoverflow.com/questions/5266430/how-to-see-the-real-sql-query-in-python-cursor-execute
    sqld = sql
    unique = "%PARAMETER%"
    sqld = sqld.replace("?", unique)
    for v in values: sqld = sqld.replace(unique, repr(v).lstrip("u"), 1)

    # Display query to be executed
    logging.debug("Executing query: %s",sqld)

    # Execute query
    try:
        cur = conn.cursor()
        cur.execute(sql,values) 
        
        logging.debug("Query executed OK")
        
        sqls = sql.split()
        sql_command=sqls[0]
        #logging.debug("SQL command was %s",sql_command)
        
        if(sql_command in ("INSERT","UPDATE","CREATE")):
            conn.commit()
            logging.debug("commit() called after %s",sql_command)
        
        
    except Exception as e:
        logging.error("EXCEPTION %s while executing query: %s",e,sqld)
        if(stop): 
            logging.error("Aborting after last exception per caller request")
            sys.exit(10)
    
    
    return cur 

