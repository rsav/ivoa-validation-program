###########################################################################
# SITE           : OPM
# PROJECT        : IVOA Services Validator
# FILE           : val.py
# AUTHOR         : Renaud.Savalle@obspm.fr
# LANGUAGE       : Python
# DESCRIPTION    : Collect results of validation of IVOA services
# NOTE           : 
###########################################################################
# TODO           :
#                : [] 2017-10-05 update services.params in SQL db (currently only set once by db-import-vop.php/query-vop.py)
# HISTORY        : 
#                : Version 1.9 2018-04-18 
#                :     - consider only services updated today, not today -2 days ago
#                : Version 1.8 2018-04-09 
#                :     - do not copy all errors for TAP services. errors will only be in the DB for the first service found of a TAP url
#                :       (before the script was spending too much time upserting thosee errors)
#                : Version 1.7 2018-04-05
#                :     - added maxtable=1 to tapvalidator options to reduce time taken by validation
#                : Version 1.6 2018-03-15 
#                :     - using db.py v 1.1
#                : Version 1.5 2017-12-28 
#                :     - use functions db.open_db and db.execute_db in db.pyvs declared locally
#                : Version 1.4 2017-10-05 
#                :     - if call to VO-Paris validator times out, set the services.nb_* cols of the service to value (-1)
#                :     - if that calls fails with a *socket* timeout, set the services.nb_* cols of the service to value (-2)
#                : Version 1.3 2017-10-04 
#                :     - for CS, SIA, SSA services default param is now SR=0.1 instead of 1.0
#                : Version 1.2 2017-07-17 
#                :     - for TAP services, also update the date of the service
#                :       when copying data from already validated URL
#                : Version 1.1 2017-07-17  
#                :    - Changed processing of timeout for reading results.
#                : Version 1.0 
#                :    - Created: 2017-05 to replace old registry-search2.php script
###########################################################################
#
#
# TODO: 
#
#
#

import logging
#import colorlog
#import coloredlogs
import multiprocessing
import time
import sqlite3
import db
import socket
import sys
#import traceback
import getopt
import os.path
import numpy # for array_split
import datetime
import urllib2
import urllib
import xml.etree.cElementTree as ET
import json
#from threading import Timer

# Global variables


# URLs of validators for each spec
validatorBaseURLs={
     "Simple Cone Search"           : "http://voparis-validator.obspm.fr/validator.php?format=XML&"
    ,"Simple Image Access"          : "http://voparis-validator.obspm.fr/validator.php?format=XML&"
    ,"Simple Spectral Access"       : "http://voparis-validator.obspm.fr/validator.php?format=XML&"
    ,"Table Access Protocol"        : "http://voparis-validation.obspm.fr/tapvalidator.php?format=JSON&"
}


#logger=None


def upsert_error(conn, ivoid, url, date, type, num, name, msg="", section=""):
    '''
    insert or update an error in the errors table 
    :param conn: sqlite3 DB connection object
    :param ivoid: service id
    :param url: service url
    :param date: date
    :param type: error type (warning, error, fatal, failure)
    :param num: sequence number of error
    :param name: name of error
    :param msg: error msg
    :param section: section for error
    '''

    logging.info("Upserting error")
    
    # Check if the error already exists in the DB
    # NB: this query can take a long time if no index (id,url) has been defined on table errors !
    # => create index errors_idx on errors(id,url);



    query = """
           SELECT count(*) FROM errors
           WHERE id = ? and url = ? and date = ? and type = ? and num = ? and name = ? and msg = ? and section = ?
           """
           
    cur = db.execute_db(conn,query,(ivoid, url, date, type, num, name, msg, section))
    
    nb_errors = cur.fetchone()[0]
    
    #logging.info("nb_errors = %d",nb_errors)

    if(nb_errors==0): # if the error does not already exist
        # insert the error


        query = """
                INSERT INTO errors (id,url,date,type,num,name,msg,section) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
   
        cur = db.execute_db(conn,query,(ivoid, url, date, type, num, name, msg, section))
        conn.commit() # because INSERT
        
    return
    

def update_service(conn,ivoid,url,results):
    '''
    update the service in the sqlite3 DB
    :param conn: sqlite3 connection object
    :param ivoid: id of the service
    :param url: url of the service
    :param results: results object returned by parse_*_validator
    '''

    logging.info("Updating sqlite3 db for service ivoid=%s url=%s",ivoid,url) # : %s",data)
    
    #print results
    new_result_vot = results["result_vot"] 
    new_result_spec = results["result_spec"] 
    new_nb_warn = results["nb_warn"] 
    new_nb_err = results["nb_err"] 
    new_nb_fatal = results["nb_fatal"] 
    new_nb_fail = results["nb_fail"] 
    
    # get today's date in format 2017-05-18 
    date_today = datetime.datetime.today()
    date_today_s=date_today.strftime('%Y-%m-%d')
    
    # Get previous results
    
    query = """
            SELECT val_mode, result_vot, result_spec, nb_warn, nb_err, nb_fatal, nb_fail, date, days_same
            FROM services
            WHERE id=? AND url=?
            """
    logging.info("Getting previous results")
    
    cur = db.execute_db(conn, query, (ivoid, url))
    
    service = cur.fetchone()
    prev_val_mode = service[0]
    prev_result_vot = service[1]
    prev_result_vot = service[2]
    prev_nb_warn = service[3]
    prev_nb_err = service[4]
    prev_nb_fatal = service[5]
    prev_nb_fail = service[6]
    prev_date = service[7]
    prev_days_same = service[8]
    
    # Compute days same 
    # datetime when service was last updated
    
    if(prev_date==None): # if service was never validated
        date_prev = date_today # then nb_days will be 0
    else:
        date_prev = datetime.datetime.strptime(prev_date,'%Y-%m-%d')
    
    interval = date_today-date_prev
    
    nb_days = interval.days
    
    #logging.info(interval)
    logging.debug("The service was last updated %d days ago",nb_days)
    
    new_days_same = prev_days_same + nb_days
    
    logging.debug("Old days_same: %d New days_same: %d",prev_days_same,new_days_same)
    
    
    # create query to update the service
  
    query = """
            UPDATE services SET 
             date = ?
            ,val_mode='normal'
            ,result_vot=?
            ,result_spec=?
            ,nb_warn=?
            ,nb_err=?
            ,nb_fatal=?
            ,nb_fail=? 
            ,days_same=?
            WHERE id=? AND url=?
            """

    logging.info("Updating table services for service")

    cur = db.execute_db(conn,query,[date_today_s
               ,new_result_vot
               ,new_result_spec
               ,new_nb_warn
               ,new_nb_err
               ,new_nb_fatal
               ,new_nb_fail 
               ,new_days_same
               ,ivoid, url])
               
    conn.commit()
        
    # get today's date in format 2017-05-31 
    #date_today_s=datetime.date.today().strftime('%Y-%m-%d')
    
    
    # insert the warnings found
    num=0
    for warning in results["warnings"]:
        num=num+1
        logging.info("Upserting warning num=%d",num)
        upsert_error(conn, ivoid, url, date_today_s, "warning", num, warning["name"], warning["msg"], warning["section"])
    
    # insert the errors found
    num=0
    for error in results["errors"]:
        num=num+1
        logging.info("Upserting error num=%d",num)
        upsert_error(conn, ivoid, url, date_today_s, "error", num, error["name"], error["msg"], error["section"])
    
    # insert the fatals found
    num=0
    for fatal in results["fatals"]:
        num=num+1
        logging.info("Upserting fatal num=%d",num)
        upsert_error(conn, ivoid, url, date_today_s, "fatal", num, fatal["name"], fatal["msg"], fatal["section"])
    
    # insert the failures found
    num=0
    for fail in results["fails"]:
        num=num+1
        logging.info("Upserting failure num=%d",num)
        upsert_error(conn, ivoid, url, date_today_s, "failure", num, fail["name"], fail["msg"], fail["section"])
       
              
     
    return





def parse_tap_validator(data):
    '''
    parse JSON output from TAP validator taplint 
    :param data: JSON data returned by TAP validator taplint
    :return: structure containing information about the errors/warnings/fatals/fails
    '''
    # default values, they will be returned like that if the parsing failed with an exception
    result_vot=""
    result_spec=""
    nb_warn=(-1)
    nb_err=(-1)
    nb_fatal=(-1)
    nb_fail=(-1)
    warnings=[]
    errors=[]
    fatals=[]
    fails=[]
    
    #logging.debug(data)
    
    # Parse JSON 
    try:
        output = json.loads(data)
        #logging.debug(output)
        
        # Parse nb of warnings/errors/failures
        nb_warn = output['totals']['WARNING']
        nb_err = output['totals']['ERROR']
        nb_fail = output['totals']['FAILURE']
        nb_fatal = 0 # taplint does not issue any fatal error, so 0
        
        
        logging.info("nb_warn=%d nb_err=%d nb_fatal=%d nb_fail=%d",nb_warn,nb_err,nb_fatal,nb_fail)
        
    
        # Parse individual errors
        sections = output["sections"]
        for section in sections:
            section_code = section["code"]
            reports = section["reports"]
            for report in reports:
                
                level = report["level"]
                code = report["code"]
                
                if("text" in report):
                    text = report["text"]
                else:
                    text = "N/A"
                    
                    
                    
                if(level=="ERROR"):
                    error={}
                    error["name"] = code
                    error["msg"] = text 
                    error["section"] = section_code
                    errors.append(error)
                    
                if(level=="WARNING"):
                    warning={}
                    warning["name"] = code
                    warning["msg"] = text 
                    warning["section"] = section_code
                    warnings.append(warning)
                    
                if(level=="FAILURE"):
                    fail={}
                    fail["name"] = code
                    fail["msg"] = text 
                    fail["section"] = section_code
                    fails.append(fail)
                
        
    except Exception as e:
        # In case of timeout, we get here with "No JSON object could be decoded" or other JSON error, then res will contain the defaults value, indicating a timeout
        logging.error("EXCEPTION %s during JSON parsing of data=%s",e,data)    
        
    
    
    res = {
         "result_vot"       : result_vot
        ,"result_spec"      : result_spec
        ,"nb_warn"          : nb_warn
        ,"nb_err"           : nb_err
        ,"nb_fatal"         : nb_fatal
        ,"nb_fail"          : nb_fail
        ,"warnings"         : warnings
        ,"errors"           : errors
        ,"fatals"           : fatals
        ,"fails"            : fails 
    }
    return res

    
def extract_dal_errors(nodes):
    '''
    extract the errors attributes from an array of XML nodes, used by parse_dal_validator
    :param nodes: array of XML nodes
    :return: array containing the errors attributes
    '''

    # init the return array
    errors = []    
    
    for node in nodes: # iterate over all the (warnings/errors/fatals) nodes
        #print(n.attrib) # {'name': '2.3'}
        #logging.debug(node)

        
        error={} # create new error
        if("name" in node.attrib): # if the error has a name
            name = node.attrib["name"]
            logging.debug("FOUND NAME=%s",name)
            error["name"]=name
        
        # Try to parse the error msg - There is HTML embedded in XML which would render the parsing very difficult     
        #html=node.find("{http://www.w3.org/1999/xhtml}div") # {http://voparis-validator.obspm.fr/}
        #logging.debug(html)
        #sys.exit(0)
        
        
        error["msg"] = "" # we do not know to extract error msg from DAL validator output
        error["section"] = "" # idem for section
        
        errors.append(error)
    
    return errors
                
        
def parse_dal_validator(spec,data):
    '''
    parse output from VOParis DAL validator 
    :param spec: specification
    :param data: XML data returned by VOParis DAL validator
    :return: structure containing information about the errors/warnings/fatals
    '''
    logging.info("Parsing XML data") # : %s",data)
    logging.debug("XML data: %s",data)

    # default values, they will be returned like that if the parsing failed with an exception
    result_vot=""
    result_spec=""
    nb_warn=(-1)
    nb_err=(-1)
    nb_fatal=(-1)
    nb_fail=(-1)
    warnings=[]
    errors=[]
    fatals=[]
    fails=[]
    
    # Parse the XML result of DAL validator
    try:
        root = ET.fromstring(data) # initialize XML parsing
        
        # Useful to debug the tree
        #for node in root.iter():
        #    print node.tag, node.attrib
        
        
        
        #print(root.tag)
        node_result_vot = root.find("{http://voparis-validator.obspm.fr/}valid[@spec='VOTable']")
        if(node_result_vot!=None): # in case of fatal ERROR_RETRIEVING, that node is not found
            result_vot=node_result_vot.text
            #print(result_vot) 
        node_result_spec = root.find("{http://voparis-validator.obspm.fr/}valid[@spec='"+spec+"']")
        if(node_result_spec!=None): # in case of fatal ERROR_RETRIEVING, that node is not found
            result_spec=node_result_spec.text
            #print(result_spec) 
        
        # Retrieve warnings and count them
        nodes_warnings = root.findall("{http://voparis-validator.obspm.fr/}warning")
        #print(node_warnings)
        nb_warn=len(nodes_warnings)
        # extract the warnings infos
        warnings = extract_dal_errors(nodes_warnings)
        
        logging.debug("Warnings found: %s", warnings)
        
        # Retrieve errors and count them
        nodes_errors = root.findall("{http://voparis-validator.obspm.fr/}error")
        #print(node_errors)
        nb_err=len(nodes_errors)    
        # Extract the errors infos
        errors = extract_dal_errors(nodes_errors)

        logging.debug("Errors found: %s",errors)
            
        # Retrieve fatals and count them
        nodes_fatals = root.findall("{http://voparis-validator.obspm.fr/}fatal")
        #print(node_fatals)
        nb_fatal=len(nodes_fatals)    
        # extract the fatal names
        fatals = extract_dal_errors(nodes_fatals)
        
        logging.debug("Fatals found: %s",fatals)
        
        nb_fail = 0 # for DAL services always 0
    
    except Exception as e:
        logging.error("EXCEPTION %s during XML parsing of data=%s",e,data)
    
    logging.info("result_vot=%s result_spec=%s nb_warn=%d nb_err=%d nb_fatal=%d nb_fail=%d",result_vot,result_spec,nb_warn,nb_err,nb_fatal,nb_fail)
    
    res = {
         "result_vot"       : result_vot
        ,"result_spec"      : result_spec
        ,"nb_warn"          : nb_warn
        ,"nb_err"           : nb_err
        ,"nb_fatal"         : nb_fatal
        ,"nb_fail"          : nb_fail
        ,"warnings"         : warnings
        ,"errors"           : errors
        ,"fatals"           : fatals
        ,"fails"            : fails 
    }
    return res


def parse_validator(spec,data):
    '''
    parse results from a validator by calling the right parse_*_validator function
    :param spec: specification
    :param data: XML or JSON data returned by the validator
    :return: structure containing information about the errors/warnings/fatals
    '''
    
    if(spec=="Table Access Protocol"):
        return parse_tap_validator(data)
    else:
        return parse_dal_validator(spec,data)
    
    


def validate_service(conn,service,timeout):
    '''
    validate one service: calls validator and update the sqlite3 DB
    :param conn: sqlite3 connection object
    :param service: array containing attributes of the service per SQL query done 
    :param timeout: timeout for calling the validator
    '''

    # extract the service attributes, order is defined by SQL request done in main
    ivoid=service[0]
    url=service[1]
    
    spec=service[2]
    specv=service[3]
    params=service[4]
    
    logging.info("Processing service ivoid=%s url=%s spec=%s specv=%s",ivoid,url,spec,specv)
    
    # For TAP services, check if the url has already been validated today because
    # there are many TAP services which have a different IVOID but the same URL
    
    # get today's date in format 2017-05-18 
    date_today = datetime.datetime.today()
    date_today_s=date_today.strftime('%Y-%m-%d')
    
    
    # Check how many services with the same URL have been validated today
    
    logging.info("Checking how many services with same URL validated today")
    query = """
            SELECT COUNT(*)
            FROM services
            WHERE url = ? AND date = ?
            """
    cur = db.execute_db(conn, query, (url, date_today_s))
    nb_services = cur.fetchone()[0]
    logging.debug("nb_services with same URL validated today=%d",nb_services)
    
    if((spec=="Table Access Protocol") and (nb_services>0)): # At least one TAP service with same URL validated today
        
        # Get the first one
        logging.info("Getting first such service")
        query = """
            SELECT val_mode, result_vot, result_spec, nb_warn, nb_err, nb_fatal, nb_fail, days_same, id
            FROM services
            WHERE url = ? AND date = ?
            """
        cur = db.execute_db(conn, query, (url, date_today_s))
        service = cur.fetchone()
        prev_val_mode = service[0]
        prev_result_vot = service[1]
        prev_result_spec = service[2]
        prev_nb_warn = service[3]
        prev_nb_err = service[4]
        prev_nb_fatal = service[5]
        prev_nb_fail = service[6]
        prev_days_same = service[7]
        prev_ivoid = service[8]
        
        # Copy the previous results to the current service
        logging.info("Updating current service with service found")
        query = """
            UPDATE services
            SET date=?, val_mode=?, result_vot=?, result_spec=?, nb_warn=?, nb_err=?, nb_fatal=?, nb_fail=?, days_same=?
            WHERE id=? AND url=?
            """
        cur = db.execute_db(conn, query, (date_today_s, prev_val_mode, prev_result_vot, prev_result_spec, prev_nb_warn, prev_nb_err, prev_nb_fatal, prev_nb_fail, prev_days_same, ivoid, url))
        conn.commit()  # because of UPDATE

        if(False): # Copy the errors too - disabled 2018-04-09 to reduce time - because of all TAP VizieR services / webapp updated to take this into account
            logging.info("Copying errors")
            query="""
                SELECT type,num,name,msg,section
                FROM errors
                WHERE id=? AND url=? AND date=?
                """
            cur = db.execute_db(conn, query, (prev_ivoid,url,date_today_s))
            errors = cur.fetchall()
            for error in errors:
                type = error[0]
                num = error[1]
                name = error[2]
                msg = error[3]
                section = error[4]
                
                upsert_error(conn, ivoid, url, date_today_s, type, num, name, msg, section)
                
        return
    
    else: # Run validator
    
        # Construct validator url: validator base URL
        vurl = validatorBaseURLs[spec]
        # add validator params
        #vurl += validatorParams[spec]
        vurl += params
        # add spec and spec version
        vurl += "&"+urllib.urlencode({"spec":spec+" "+specv})
        # add service URL
        vurl += "&"+urllib.urlencode({"serviceURL":url})
        
        # Set TAP validator timeout
        tap_timeout = timeout -1; # try to make sure TAP validator timeouts before our socket timeout 
        if(tap_timeout<=0): tap_timeout=1
        
        if(spec=="Table Access Protocol"): # TAP validator also needs the timeout
            vurl += "&"+urllib.urlencode({"timeout":tap_timeout})
            vurl += "&"+urllib.urlencode({"maxtable":"1"}) # added 2018-04-05 to reduce time taken by TAP validation
        
        #vurl="https://www.test123456.com/" # debug - test timeout
        
        logging.info("Calling validator URL: %s (timeout is %d secs)",vurl,timeout)
       
        # set the timeout for call to urllib2.urlopen - not necessary urllib2.urlopen has a timeout parameter
        # socket.setdefaulttimeout(float(timeout))
        
            
        request = urllib2.Request(vurl)
        
        try:
            
            # NB: The optional timeout parameter specifies a timeout in seconds for blocking operations like the connection attempt 
            # It seems that the timeout is useless here for timeouting the TAP validator : https://www.daniweb.com/programming/software-development/threads/182555/how-to-set-timeout-for-reading-from-urls-in-urllib
            # => the timeout there it is only for opening url. It wont give exception while reading.
            
            
            response = urllib2.urlopen(request,timeout=timeout)   
            http_status = response.getcode()
            logging.debug("HTTP status: %d",http_status) 
        except Exception as e: 
            logging.error("EXCEPTION %s while calling URL=%s. Using default results (-1).",e,vurl)
            
            # These are the results to use in case of timeout:
            results = {
                         "result_vot"       : ""
                        ,"result_spec"      : ""
                        ,"nb_warn"          : -1
                        ,"nb_err"           : -1
                        ,"nb_fatal"         : -1
                        ,"nb_fail"          : -1
                        ,"warnings"         : []
                        ,"errors"           : []
                        ,"fatals"           : []
                        ,"fails"            : [] 
            }
            # Update the service with the results
            update_service(conn,ivoid,url,results)  
                
        except socket.timeout as e:
            logging.error("EXCEPTION: socket timeout: %s while calling URL=%s. Using defaults results (-2).",e,vurl)
            
            # These are the results to use in case of *socket* timeout:
            results = {
                         "result_vot"       : ""
                        ,"result_spec"      : ""
                        ,"nb_warn"          : -2
                        ,"nb_err"           : -2
                        ,"nb_fatal"         : -2
                        ,"nb_fail"          : -2
                        ,"warnings"         : []
                        ,"errors"           : []
                        ,"fatals"           : []
                        ,"fails"            : [] 
            }
            # Update the service with the results
            update_service(conn,ivoid,url,results)          
          
        else: # if no exception 
            if(http_status==200):
                try:
                    # set a timer here to have a timeout during reading of data from URL, but tapvalidator.php does not stop correctly
                    #t = Timer(timeout, response.close)
                    #t.start()
                    logging.debug("Reading data.") 
                    data = response.read() # get response in either XML or JSON format
                    #t.cancel()
                    logging.debug("Reading data done.") # we end up here in case tapvalidator.php times out. The unfinished JSON will make throw an exception in parse_tap_validator
                except Exception as e: # try to catch timeout exception... not sure it works
                    logging.error("EXCEPTION %s (timeout?) while reading data from URL=%s. Using default results (-1).",e,vurl)
                
                    # These are the results to use in case of timeout:
                    results = {
                         "result_vot"       : ""
                        ,"result_spec"      : ""
                        ,"nb_warn"          : -1
                        ,"nb_err"           : -1
                        ,"nb_fatal"         : -1
                        ,"nb_fail"          : -1
                        ,"warnings"         : []
                        ,"errors"           : []
                        ,"fatals"           : []
                        ,"fails"            : [] 
                    }
                
                
                else:
                    #logging.info(data)            
                    results = parse_validator(spec,data)
                
                # Update the service with the results
                update_service(conn,ivoid,url,results)  
                    
            else: # http_status!=200
                logging.error("HTTP status is not 200 but %d. Giving up.",http_status)

        return


def validate_services(services,timeout,db_file):
    '''
    worker function to validate several services
    :param services: array of services returned by SQL query
    :param timeout: timeout for calling the validator
    :param db_file: name of the sqlite3 DB file 
    '''
    
    # open our own connection to the DB (since we are running in parallel)
    
    logging.info("Opening DB file %s",db_file)  
    conn = db.open_db(db_file)
    
    
    no_service=0
    nb_services=len(services)
    
    logging.info("Worker starting to process %d services",nb_services)
    
    for service in services:
        no_service=no_service+1
        logging.info("Processing service %d/%d",no_service,nb_services)
        validate_service(conn,service,timeout)
        
        # retrieve individual columns - same order as query in main
        
    
    #time.sleep(2)

    # close DB connection before end of ps 
    logging.info('Closing DB connection')
    conn.close()
    logging.info("Worker finished with %d services processed",no_service)
    
    
    
    
    return




def usage():
    '''
    display this program's usage
    '''
    print("Usage: %s -h --db <db_file> --ps <nb_processes> --timeout <timeout> --log <log_file>" % sys.argv[0])
    return

    
def main(argv):
    '''
    main program
    :param argv: parameters
    '''
    
    
    
    program_version="1.9"
    #global logger
    
    # Read program arguments
    db_file=None # no default
    nb_ps = 1 # nb of processes to use
    timeout = 20  # timeout for validation of individual service, in secs
    log_file=None # no default
    
    try:
        opts, args = getopt.getopt(argv,"h",["db=","ps=","timeout=","log="])
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)
        
    for o, a in opts:
        if o in ("-h"):
            usage()
            sys.exit(0)
        elif o in ("--db"):
            db_file = a
        elif o in ("--ps"):
            nb_ps = int(a)
        elif o in ("--timeout"):
            timeout = int(a)
            if(timeout<1):
                logging.error("timeout must be greater or equal to 1")
                sys.exit(1)
        elif o in ("--log"):
            log_file = a
        else:
            assert False, "unhandled option"

        
        
    if(db_file==None):
        print('ERROR: No db_file')
        usage()
        exit(2)
        
    
    
    
    # Setup logging
    
    # Try to use coloredlogs but does not work well 
    # Create a logger object.
    #logger = logging.getLogger('val.py')
    # By default the install() function installs a handler on the root logger,
    # this means that log messages from your code and log messages from the
    # libraries that you use will all show up on the terminal.    
    #coloredlogs.install(level='DEBUG',fmt='%(asctime)s %(filename)s %(levelname)s %(lineno)d %(processName)s %(funcName)s: %(message)s')
    
    # Try to use colorlog - does not work
    #colorlog.basicConfig(format='%(asctime)s %(filename)s %(levelname)s %(lineno)d %(processName)s %(funcName)s: %(message)s', level=logging.DEBUG)    
    #colorlog.info("Starting argv=%s",argv)
    
    
    
    
    logging.basicConfig(format='%(asctime)s %(filename)s %(levelname)s %(lineno)d %(processName)s %(funcName)s: %(message)s'
                        , level=logging.DEBUG, filename=log_file)

    # Add colors - from https://stackoverflow.com/questions/384076/how-can-i-color-python-logging-output
    logging.addLevelName( logging.WARNING, "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.WARNING))
    logging.addLevelName( logging.ERROR, "\033[1;41m%s\033[1;0m" % logging.getLevelName(logging.ERROR))

    
    
    logging.info("This is val.py version %s. argv=%s",program_version,argv)
    
    
    
    # Configuration depending on hostname
    hostname = socket.gethostname()
    
    logging.info("Running on %s",hostname)
    

    
    conn = db.open_db(db_file)

    
    
    # Prepare where clause for extracting suitable services
    #min_update_date = datetime.date.today() - datetime.timedelta(2)  # today - 2 days
    min_update_date = datetime.date.today()   # 2018-04-18 changed to today => assume query-rr-*.py was run just before and the same day...
    min_update_date_s = min_update_date.strftime('%Y-%m-%d')
    
    logging.info("min_update_date_s is %s",min_update_date_s)
    
    
    where = "date_update >='"+min_update_date_s+"'"
    
    #where = where + " and id like '%vopdc%'"  # debug: 3 services 
    #where = where + " and url like '%.au%'" # debug: 7 services
    #where = where + " and id like '%irsa%'" # debug: 366 services
    #where = where + " and id='ivo://vopdc.obspm/imcce/skybot'" # debug: 1 service with 1 error
    #where = where + " and id='ivo://CDS.VizieR/J/AJ/127/1227'" # debug: 1 service with 2 errors
    #where = where + " and id='ivo://vopdc.obspm/imcce/dynastvo/epn' and url='http://voparis-tap-planeto.obspm.fr/__system__/tap/run/tap'" # debug: TAP query 
    #where = where +" and url='http://camelot.star.le.ac.uk:8080/dsa-catalog/SubmitCone?DSACAT=ledas&DSATAB=a2rtraw&'" # debug: service for which the VO-Paris validator times out
    
    
    # Count nb of suitable services in the db 
    query = "SELECT count(*) FROM services WHERE "+where
    
    cur = db.execute_db(conn, query, [], True)
    
    nb_services = cur.fetchone()[0]
    
    logging.info("nb_services = %d",nb_services)
    
    
    if (nb_services!=0): 
        
        if(nb_ps<=nb_services):
            
            # Get suitable services
            order = "id asc, url asc"
            # query : get those columns only
            query = "SELECT id,url,spec,specv,params FROM services WHERE "+where +" ORDER BY "+order
        
            cur = db.execute_db(conn, query, [], True)
            services = cur.fetchall()   
            
            conn.close()    
            #sys.exit(0)
        
            #print(services[0]["id"]) # debug
            #print(services[0])
            #sys.exit(0)
            
            #print(services[0])
            
            # Slice the services array into nb_ps arrays of same size
            sservices = numpy.array_split(services, nb_ps)
            
        
            for i in range(len(sservices)):
                logging.info("Slice # %d len=%d first ivoid=%s",i,len(sservices[i]),(sservices[i][0][0]))
        
        
            #sys.exit(0)
            
            
            jobs = []
            for i in range(nb_ps):
                p = multiprocessing.Process(target=validate_services,args=(sservices[i],timeout,db_file))
                jobs.append(p)
                p.start()
                #p.join()
                
        else:
            logging.error("nb_ps>nb_services. Try with a lower nb_ps")
            sys.exit(1)
            
    else: 
        logging.error("No suitable service found. Aborting.")
        sys.exit(10)

    
if __name__ == '__main__':
    main(sys.argv[1:])
    
    

 
#     try:
#         main(sys.argv[1:])
#     except:
#         print("========== Trigger Exception, traceback info forward to log file.==========")
#         traceback.print_exc()
#         sys.exit(1)
#         
#         
        

