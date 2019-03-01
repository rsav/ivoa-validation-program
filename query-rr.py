###########################################################################
# SITE           : OPM
# PROJECT        : IVOA Services Validator
# FILE           : query-rr.py
# AUTHOR         : Renaud.Savalle@obspm.fr
# LANGUAGE       : Python
# DESCRIPTION    : Query relational registry for services of a certain type and put the results into a SQLite DB
# NOTE           : 
###########################################################################
# HISTORY        : 
#                : Version 1.3 2018-04-18 
#                :     - identify SIAv2 services with the standardid pattern standard_id LIKE 'ivo://ivoa.net/std/sia#query-%2.%' (first % is to match a possible aux capability)
#                : Version 1.2 2018-04-16 
#                :     - support for SIAv2, redone query for SIA (v1), better heuristic for finding service version from standardid's fragment first
#                : Version 1.1 2018-03-15 
#                :     - using db.py v 1.1
#                : Version 1.0 
#                :    - Created: 2017-12-21 to replace query-vop.php + db-import-vop.php because voparis-registry is not maintained anymore and being deprecated
###########################################################################
# TODO           : [ ] Extract the spec version from the sdandard_id
#                :     NB (per Markus after discussion in Santiago 2017-10): the correct way is to check the end of standardid then if no version found, std_version
#
###########################################################################

import sys
import os
import getopt
import logging
import db # my db module
import val # my val module/program
import datetime


import pyvo as vo
from time import sleep


# correspondance between our service types (as in the RR's standard_ids) and the service type for searching the RR with search()
service_types = {
         "ConeSearch": "conesearch",
         "SIA" : "sia",
         "SSA" : "ssa",
         "TAP" : "tap",
         "SIAv2" : "siav2",
}



# Definition of the names of the specifications for each beginning of standardid
# NB: standardid are in lowercase as in the RR
spec_from_standardid = {
         "ivo://ivoa.net/std/conesearch" : "Simple Cone Search",
         "ivo://ivoa.net/std/sia" :"Simple Image Access",
         "ivo://ivoa.net/std/ssa" : "Simple Spectral Access",
         "ivo://ivoa.net/std/sla" : "Simple Line Access", 
         "ivo://ivoa.net/std/tap" : "Table Access Protocol",
}

# Definition of the default spec version to use for validation for each beginning of standardid
# NB: standardid are in lowercase as in the RR
default_specv_from_standardid = {
         "ivo://ivoa.net/std/conesearch" : "1.03",
         "ivo://ivoa.net/std/sia" : "1.0",
         "ivo://ivoa.net/std/ssa" : "1.03",
         "ivo://ivoa.net/std/sla" : "1.0",     
         "ivo://ivoa.net/std/tap" : "1.0",
}



# validator parameters for each spec

# Before 2017-10-04 SR=1.0 
# After 2017-10-04 SR=0.1 per PLS request

validatorParams={
     "Simple Cone Search"       : "RA=180.0&DEC=60.0&SR=0.1"
    ,"Simple Image Access"      : "POS=180.0,60.0&SIZE=0.1,0.1&FORMAT=ALL"
    ,"Simple Spectral Access"   : "REQUEST=queryData&POS=180.0,60.0&SIZE=0.1&TIME=&BAND=&FORMAT=ALL"
    ,"Table Access Protocol"    : ""
    # SIAv2: not sure of the mandatory args to use. Tried to use a sample from PADC validator's form:
    ,"Simple Image Access 2.0"  : "REQUEST=query&POS=CIRCLE+180.0+60.0+0.5&FOV=0.00017+%2BInf&BAND=-Inf+%2BInf&TIME=45000.0+%2BInf&POL=&SPATRES=-Inf+200.0&EXPTIME=-Inf+3600&ID=&COLLECTION=&FACILITY=&INSTRUMENT=&DTYPE=image&CALIB=0+%2BInf&TARGET=&TIMERES=-Inf+10000.0&SPECRP=-Inf+%2BInf&FORMAT=&MAXREC=5"
                                #"REQUEST=query&POS=CIRCLE+180.0+60.0+0.5&BAND=-Inf+%2BInf&FOV=-Inf+%2BInf&TIME=-Inf+%2BInf&SPATRES=-Inf+%2BInf&DTYPE=image&TARGET=&COLLECTION=&FACILITY=&INSTRUMENT=&CALIB=-Inf+%2BInf&TIMERES=-Inf+%2BInf&SPECRP=-Inf+%2BInf&FORMAT=&MAXREC=5"
}


def create_table_services(conn):
    """
    create the services table if it does not exist
    """
    
    # NB: the comments are kept by sqlite3 and can be accessed with command ".schema"
    query_create = """
        CREATE TABLE IF NOT EXISTS services (
             id TEXT NOT NULL                    /* resource ivoid */
            ,url TEXT NOT NULL                   /* access URL */
            ,title TEXT                          /* resource title */
            ,short_name TEXT                     /* resource short name */
            ,date_insert TEXT                    /* date row was inserted by query-rr.py = date when the service was first seen */
            ,date_update TEXT                    /* date row was updated by query-rr.py = last date when the service was seen */
            ,vor_status TEXT                     /* status of the resource, coming from the RR it will always be 'active' */
            ,vor_created TEXT                    /* date of creation of the resource */
            ,vor_updated TEXT                    /* date of update of the resource */
            ,contact_name TEXT                   /* 1st contact name for curation */
            ,contact_email TEXT                  /* 1st email for curation */
            ,provenance TEXT                     /* ivoid of registry where resource comes from */
            ,date TEXT                           /* validation date */
            ,standard_id TEXT                    /* standard_id of the capability */
            ,xsi_type TEXT                       /* type of the interface - should always be vs:paramhttp per the search() funtion implementation */
            ,spec TEXT                           /* specification of the service in natural language as in spec_from_standardid */
            ,specv TEXT                          /* version of the specification for the service */
            ,params TEXT                         /* parameters for the validator */
            ,val_mode TEXT                       /* validator mode ("not_run","normal","batch") */
            ,result_vot TEXT                     /* validator result for VOTable ("yes","no","") */
            ,result_spec TEXT                    /* validator result for spec ("yes","no","") */
            ,nb_warn INT                         /* validator nb of warnings */
            ,nb_err INT                          /* validator nb of errors */
            ,nb_fail INT                         /* validator nb of failures (for TAP validator taplint) */
            ,nb_fatal INT                        /* validator nb o f fatal errors */
            ,days_same INT DEFAULT 0             /* nb of days the result have been the same */
        )        
    """
         
    
    cur_create = db.execute_db(conn, query_create, [], True)
    #conn.commit() # no need, db.execute_db does it 
    
    query_create_index = """
        CREATE UNIQUE INDEX IF NOT EXISTS pk ON services (id,url)
    """
    
    cur_create_index = db.execute_db(conn, query_create_index, [], True)
    #conn.commit() # no need, db.execute_db does it 
        
        
  
def create_table_errors(conn):
    """
    create the errors table if it does not exist
    """
    
    # NB: the comments are kept by sqlite3 and can be accessed with command ".schema"
    query_create = """
        CREATE TABLE IF NOT EXISTS errors ( 
             id TEXT NOT NULL            /* resource ivoid */
            ,url TEXT NOT NULL           /* access URL */
            ,date TEXT                   /* validation date */
            ,type TEXT                   /* error type ex: "error" "warning" "fatal" */
            ,num INT                     /* error number (there can be several errors with the same name) */
            ,name TEXT                   /* error name ex: "4.3.2" */
            ,msg TEXT                    /* error msg - verbose - for taplint */
            ,section TEXT                /* error section - for taplint */
        )
    """
    
   
    cur_create = db.execute_db(conn, query_create, [], True)
    #conn.commit() # no need, db.execute_db does it 
    
    query_create_index = """
        CREATE UNIQUE INDEX IF NOT EXISTS pk ON errors (id,url,date,type,num,name)
    """
    
    cur_create_index = db.execute_db(conn, query_create_index, [], True)
    #conn.commit() # no need, db.execute_db does it 
            
         

# rewrite of pyvo.registry.regtap.search() for our purpose
def search(baseurl=None,servicetype=None):
    """
    execute a simple query to the RegTAP registry.
    Parameters
    ----------

    servicetype : str
       the service type to restrict results to.
       Allowed values include
       'conesearch', 
       'sia' ,
       'ssa',
       'slap',
       'tap'
       'siav2'

    Returns
    -------
    RegistryResults
       a container holding a table of matching resource (e.g. services)
    See Also
    --------
    RegistryResults
    """
    if (servicetype==None):
        raise dalq.DALQueryError(
            "No servicetype parameter passed to registry search")

    joins = set(["rr.interface", "rr.resource"])
    #joins.add("rr.interface") # is that necessary ? it was already put there above ?
    joins.add("rr.res_role") # for email
    
    wheres = list()
    
    
    
    if(servicetype=='siav2'): # identify SIAv2 services: ivo://ivoa.net/std/sia#query-[aux]-2.X
        wheres.append("standard_id LIKE 'ivo://ivoa.net/std/sia#query-%2.%'") # filter the SIAv2 services
    elif (servicetype=='sia'): # make sure we identify only SIA and not SIAv2 services
        wheres.append("standard_id LIKE 'ivo://ivoa.net/std/sia%'")
        wheres.append("standard_id NOT LIKE 'ivo://ivoa.net/std/sia#query-%2.%'") # filter out the SIAv2 services
    else:
        wheres.append("standard_id LIKE 'ivo://ivoa.net/std/{}%'".format(vo.tap.escape(servicetype)))
    
    wheres.append("base_role = 'contact'") # added 2018-04-16 => avoid duplicate lines because of several res_role, keep only the one with base_role='contact'
    wheres.append("intf_type = 'vs:paramhttp'")

    query = """SELECT DISTINCT rr.interface.*, rr.capability.*, rr.resource.*, rr.res_role.* 
    FROM rr.capability
    {}
    {}
    """.format(
        ''.join("NATURAL JOIN {} ".format(j) for j in joins),
        ("WHERE " if wheres else "") + " AND ".join(wheres)
    )

    service = vo.tap.TAPService(baseurl)
    
    logging.debug("Executing RR query=\n%s",query)
    
    
    query = vo.registry.regtap.RegistryQuery(service.baseurl, query, maxrec=service.hardlimit)
    return query.execute()


def usage():
    '''
    display this program's usage
    '''
    print("Usage: %s -h --type <service_type> --db <db_file> --log <log_file>" % sys.argv[0])
    return

def main(argv):
    '''
    main program
    :param argv: parameters
    '''
    
    
    
    program_version="1.3"
    #global logger
    
    # Read program arguments
    service_type=None # no default
    db_file=None # no default
    log_file=None # no default
    
    try:
        opts, args = getopt.getopt(argv,"h",["type=","db=","log="])
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)
        
    for o, a in opts:
        if o in ("-h"):
            usage()
            sys.exit(0)
        elif o in ("--type"):
            service_type = a
        elif o in ("--db"):
            db_file = a
        elif o in ("--log"):
            log_file = a
        else:
            assert False, "unhandled option"

        
        
    if(service_type==None):
        print('ERROR: No service_type')
        usage()
        exit(2)
        
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

    
    
    logging.info("This is query-rr.py version %s. argv=%s",program_version,argv)
    
    # Try to open the DB file,
    conn = db.open_db(db_file)
    # create the tables if they don't exist
    create_table_services(conn)
    create_table_errors(conn)
    
    
    # URL of RR to use
    url_rr="http://voparis-rr.obspm.fr/tap"
    
    # Look for services with type service_type in the RR
    logging.debug("Looking for services with type=%s in RR=%s",service_type,url_rr)
    services = search(baseurl=url_rr,servicetype=service_types[service_type])

    # Get nb of services found
    nb_services = len(services)

    logging.debug("Nb of services found=%d",nb_services)
    
    s=0
    for service in services:
        s=s+1
        logging.info("Processing service %d/%d ivoid=%s url=%s standardid=%s",s,nb_services,service.ivoid,service.access_url,service.standard_id)
        
        if(False): # display all service attributes
            for a in service:
                logging.debug("%s: %s",a,service[a])
        
        # get today's date in format 2017-12-21 
        date_today = datetime.datetime.today()
        date_today_s=date_today.strftime('%Y-%m-%d')
            
        # Check if the service exists in the DB
        query = """
        SELECT count(*) FROM services WHERE id=? and url=?
        """
        cur = db.execute_db(conn, query, (service.ivoid, service.access_url), True)
        nb_such_service = cur.fetchone()[0] 
        #logging.debug("Nb of such service found in the DB=%d",nb_such_service)
        
        
        
        if(nb_such_service==0): # service does not exist in the DB yet => insert it
                logging.debug("No such service found in the DB, inserting it")
                
                query_insert = """
                INSERT INTO services (id,url,date_insert) 
                VALUES (?, ?, ?)
                """
                cur_insert = db.execute_db(conn,query_insert,(service.ivoid, service.access_url, date_today_s))
                #conn.commit() # because INSERT # no need, db.execute_db does it 
        
        
    
        # Update the service with data from registry
        if(True):
            
            # Extract the part before the # in the standardid and the fragment (#something at the end of the standard id)
            standardid = service.standard_id # in lowercase in the RR
            index_pound = standardid.find('#') # index of char '#' in standardid
            #logging.debug("index_pound=%d",index_pound)
            fragment=None
            if(index_pound!=-1): # pound found, copy only the part before the pound
                standardid_substring = standardid[:index_pound]
                fragment = standardid[index_pound:]
            else: # no pound found, copy the entire string
                standardid_substring = standardid
                
            logging.debug("Found standardid_substring=%s",standardid_substring)
            logging.debug("Found fragment=%s",fragment)
            
            spec=spec_from_standardid[standardid_substring]
            logging.debug("Found spec=%s",spec)
            
            # Try to extract version of std used by service
            # NB (per Markus after discussion in Santiago 2017-10)
            # => the correct way is to check the end of standardid then if no version found, std_version
            
            
            logging.debug("Determining spec version")
            specv=None
            if(fragment!=None): # if a fragment was found, extract the spec from the fragment
                logging.debug("standardid's fragment found")
                specv_from_fragment = fragment[7:] # extract "2.0" from "#query-2.0" => skip "#query-"
                logging.debug("Found specv_from_fragment=%s",specv_from_fragment)
                if(specv_from_fragment.replace(".", "", 1).isdigit()): # https://stackoverflow.com/questions/4138202/using-isdigit-for-floats
                    logging.debug("specv_from_fragment can convert to float => using it")
                    specv = specv_from_fragment
                    
            if(specv==None): # version not found in fragment
                logging.debug("No standardid's fragment found or version not found in fragment, checking std_version=%s",service['std_version'])
                if(service['std_version']!=""):
                    logging.debug("std_version not empty, using it")
                    specv=service['std_version']
                else: # if not found, use default version for this standard
                    logging.debug("std_version empty, using default specv from standardid")
                    specv=default_specv_from_standardid[standardid_substring]
                
            logging.info("Found specv=%s",specv)
                
            # default params for validation are extracted from the array val.validatorParams (in vla.py)
            if((spec=="Simple Image Access") and (specv=="2.0")): # for SIAv2 we need this case
                params = validatorParams[spec+" "+specv]
            else:
                params = validatorParams[spec]
            
            # if some attributes are void string "" then set them to N/A
            role_name = service['role_name']
            if(role_name==""):
                role_name="N/A"
                
            email = service['email']
            if(email==""):
                email="N/A"
            
            query_update = """
                UPDATE services SET 
                 date_update = ?
                ,vor_created = ?
                ,vor_updated = ?
                ,vor_status = ?
                ,provenance = ?
                ,standard_id = ?
                ,title = ?
                ,short_name = ?
                ,contact_name = ?
                ,contact_email = ?
                ,xsi_type = ?
                ,spec = ?
                ,specv = ?
                ,params = ?
                WHERE id=? AND url=?
                """
            logging.info("Updating table services for service")
            
            cur_update = db.execute_db(conn,query_update,[date_today_s
                ,service['created'] # must be accessed like this and not by property because property not exposed in class RegistryResource
                ,service['updated'] # idem etc.
                ,'active' # we assume that if a service was found in the RR, it is active. Reason:
                # See http://ivoa.net/documents/RegTAP/20171206/WD-RegTAP-1.1-20171206.html
                # "The status attribute of vr:Resource is considered an implementation detail of the XML serialization and is not kept here. 
                # Neither inactive nor deleted records may be kept in the resource table. Since all other tables in the relational registry should keep 
                # a foreign key on the ivoid column, this implies that only metadata on active records is being kept in the relational registry. 
                # In other words, users can expect a resource to exist and work if they find it in a relational registry"
                ,service['harvested_from'] # the provenance registry's ivoid
                ,standardid # the standardid
                ,service.res_title
                ,service.short_name
                ,role_name # contact name
                ,email # contact email
                ,service['intf_type'] # per the search() function's implementation this should always be 'vs:paramhttp'
                ,spec
                ,specv
                ,params
                ,service.ivoid, service.access_url])
                   
            #conn.commit() # because UPDATE # no need, db.execute_db does it 
        # if True    

    # at the end, close the DB connection
    logging.info("Done. Closing connection")
    conn.close()
     
    
if __name__ == '__main__':
    main(sys.argv[1:])
    
    