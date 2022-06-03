#####################################################################################################
##                    Script to securely dump PostgreSQL Database:                                 ##
##                                                                                                 ##
## Security measures adopted:                                                                      ##
##  - No sensitive information, such as passwords are given to command line;                       ##
##  - No passwords are kept as plain text in the script;                                           ##
##  - No passwords or other sentive data are kept as plain text in auxiliary files                 ##
##    (pgpass.conf) after the execution.                                                           ##
##                                                                                                 ##
## Script calling example:                                                                         ##
##  - python postgresDump.py -t -h localhost -db DBName -u postgres -ev postgresDBPass             ##
##  - python postgresDump.py -h localhost -p port -s -sn serverName -u admin -ev postgresDBPass    ##                                                                             
## Arguments Explanation:                                                                          ##
##                                                                                                 ##
##  - (-t): Dump table by table of Database (Optional)                                             ##
##  - (-h): Hostname                                                                               ##
##  - (-db): Database name                                                                         ##
##  - (-u): Database user                                                                          ##
##  - (-ev): Name of environment variable set with Database password for the given user            ##
##  - (-s): Dump all databases found on the server                                                 ##
##  - (-sn): Server Name                                                                           ##
##                                                                                                 ##
##  Dump File(s) Location(s) and created folders:                                                  ##
##                                                                                                 ##
##  - When the script is executed, it will create and dump files to c:\pgDump. The                 ##
##    configuration file is created under %APPDATA%\postgresql\, if the folder doesn't             ##
##    exists, it will be created.                                                                  ##                                                                                   
##  - The script must run in a python environment provided with all the dependencies               ##
##    listed in the import section bellow.                                                         ##
##                                                                                                 ##
##  Final observations:                                                                            ##
##                                                                                                 ##
##  - The configuration file is overriden after the code execution, so no sensitive data           ##
##    will be stored in plain text after the dump process.                                         ##
##                                                                                                 ##
##  Any advices or suggestions:                                                                    ##
##                                                                                                 ##
##  - moises.morais.henriques@gmail.com                                                            ##
##  - https://github.com/moises-henriques                                                          ##
##                                                                                                 ##
#####################################################################################################


## Imports ##
from datetime import datetime
import sys
import pandas as pd
import psycopg2
import os

## Global Flags ##
compressFlag = False
environPassword = 'postgresDBPass'
host = 'localhost'
database="DBName"
port="5432"
serverDump = False
serverName = 'ServerName'

## Connect to Postgres DB
def DBConnect(**kwargs):
    conn = None
    if 'host' in kwargs:
        host = kwargs.get('host')
    else:
        host="localhost"
    
    if 'database' in kwargs:
        database = kwargs.get('database')
    else:
        database="DBName"

    if 'user' in kwargs:
        if 'password' in kwargs:
            user = kwargs.get('user')
            password = kwargs.get('password')
        else:
            if os.getenv('postgresDBUser'):
                password = os.getenv('postgresDBUser')
            else:
                raise RuntimeError('A password was not provided while connecting to DB using DBConnect, please review your code and provide a password to mitigate this error.')
    else:
        user='postgres'
        password= os.getenv('postgresDBUser')

    try:
        conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password= password)
    except (Exception, psycopg2.DatabaseError) as error:
        raise psycopg2.OperationalError(error.args[0])
    else:
        print('A connection was successfully established with server!')
        cursor = conn.cursor()
        return conn, cursor


## List all table names ##
def queryTableList(conn,cursor):
    query = ("SELECT table_name FROM information_schema.tables "
            "WHERE (table_schema = 'public') "
            "ORDER BY table_name")
    
    try:
        cursor.execute(query)
        try:
            tableList = pd.DataFrame(cursor.fetchall())
            tableList.columns = ['Table Name']
        except:
            tableList = None
    except:
        conn.rollback()
        return 'Error in SQL execution while quering tableList'
    else:
        conn.commit()
        return tableList


## Set/Create pgpass.conf ##
def setPGPass(DBName,user,password,**kwargs):
    if 'host' in kwargs:
        host = kwargs.get('host')
    else:
        host="localhost"
    
    if 'port' in kwargs:
        port = str(kwargs.get('port'))
    else:
        port="5432"
    

    try:
        appdataPath = os.getenv('APPDATA')
        if not os.path.isdir(appdataPath+'\\postgresql\\'):
            os.mkdir(appdataPath+'\\postgresql\\')
        pgPath =appdataPath+'\\postgresql\\pgpass.conf'

        pgpass = open(pgPath,'w+')
        pgpass.write(host+':'+port+':'+DBName+':'+user+':'+password)
        pgpass.truncate()
        pgpass.close()

        return True
    except:
        return None
    

## Clear created pgpass.conf ##
def clearPgpass():
    try:
        appdataPath = os.getenv('APPDATA')
        pgPath =appdataPath+'\\postgresql\\pgpass.conf'
        os.remove(pgPath)
        return True
    except:
        return None

    
## List all table names ##
def backupDB(host,port,user,password,**kwargs):
    if 'DBName' in kwargs:
        DBName = kwargs.get('DBName')
    elif 'serverName' in kwargs:
        serverName = kwargs.get('serverName')
        DBName='*'
    else:
        raise RuntimeError('ERROR: Database Name or Server Name must be provided to backupDB function.')
    if 'mode' in kwargs:
        mode = kwargs.get('mode')
        if mode == 'byTable':
            ## Connect ##
            conn, cursor = DBConnect(host=host,user= user,database=DBName,password=password)
            ## Get table list in DB ##
            tableList = queryTableList(conn, cursor)
            conn.close()
    else:
        mode="all"
    
    
    
    try:
        if setPGPass(DBName,user,password,host=host):
            if not os.path.isdir('c:\pgDump\\'):
                os.mkdir('c:\pgDump\\')
            global serverDump

            if not serverDump:
                if mode == 'all':
                    pg_dump = '"pg_dump -h '+host+' -p '+port+' -U '+user+' -F t '+DBName+' > c:\pgDump\\'+DBName+'.tar"'
                    os.system('"cmd.exe"' and pg_dump)

                elif mode ==  'byTable':
                    for idx in tableList.index:
                        pg_dump = '"pg_dump -h '+host+' -p '+port+' -U '+user+' -F t --table public.'+tableList.iloc[idx][0]+' '+DBName+' > c:\pgDump\\'+DBName+'_tb_'+tableList.iloc[idx][0]+'.tar"'
                        os.system('"cmd.exe"' and pg_dump)
                else:
                    raise ValueError('ERROR: Invalid mode provided in backupDB function.')
            else:
                pg_dumpall = '"pg_dumpall -h '+host+' -p '+port+' -U '+user+' > c:\pgDump\\'+serverName+'.dump"'
                os.system('"cmd.exe"' and pg_dumpall)
        else:
            raise RuntimeError('ERROR: We could not Set/Create the pgpass.conf, review the code.')
        if clearPgpass():
            return True
        else:
            raise RuntimeError('ERROR: We could not clear the pgpass.conf content, review the code.')
    except:
        raise RuntimeError('ERROR: We could not proced with the backup process, review the code.')


## Get initial arguments and set flags ##
def getArguments():
    # Server dump
    global serverDump
    global serverName
    if '-s' in sys.argv:
        serverDump = True
        if '-sn' in sys.argv:
            serverName = None
            idx = 0
            for entry in sys.argv:
                if entry == '-sn':
                    serverName = sys.argv[idx+1]
                    break
                else:
                    idx += 1    
    else:
        serverDump = False

    # mode
    if '-t' in sys.argv:
        mode='byTable'
    else:
        mode = 'all'

    # host
    global host
    host = None
    if '-h' in sys.argv:
        idx = 0
        for entry in sys.argv:
            if entry == '-h':
                host = sys.argv[idx+1]
                break
            else:
                idx += 1
                
    # Port
    global port
    if '-p' in sys.argv:
        idx = 0
        for entry in sys.argv:
            if entry == '-p':
                port = sys.argv[idx+1]
                break
            else:
                idx += 1

    # Data Base
    global DBName
    DBName = None
    if '-db' in sys.argv:
        idx = 0
        for entry in sys.argv:
            if entry == '-db':
                DBName = sys.argv[idx+1]
                break
            else:
                idx += 1

    # user
    user = None
    if '-u' in sys.argv:
        idx = 0
        for entry in sys.argv:
            if entry == '-u':
                user = sys.argv[idx+1]
                break
            else:
                idx += 1
    
    # envPassword
    global environPassword
    if '-ev' in sys.argv:
        idx = 0
        for entry in sys.argv:
            if entry == '-ev':
                environPassword = sys.argv[idx+1]
                break
            else:
                idx += 1

    if serverDump:
        if not (serverName or host or user):
            raise ValueError('Server Name and/or host and or user arguments missing.')
        else:
            return host, port, serverName, user, environPassword 
    else:
        if not (DBName or host or user):
            raise ValueError('Database Name and/or host arguments missing.')
        else:
            return mode, host, port, DBName, user, environPassword

### Main ###
if __name__ == '__main__':
    executionTime = datetime.now()
    ## get arguments ##
    if '-s' in sys.argv:
        host, port, serverName, user, environPassword = getArguments()
    else:
        mode, host, port, DBName, user, environPassword = getArguments()

    if serverDump:
        if backupDB(serverName=serverName,host=host,port=port,user=user,password=os.getenv(environPassword)):
            print('Backup finished, you can find your files on c:\pgDump directory.')
            executionTime = datetime.now()-executionTime
            print('Execution time: {}'.format(executionTime))
        else:
            print('The backup could not be done.')
            executionTime = datetime.now()-executionTime
            print('Execution time: {}'.format(executionTime))
    else:
        if backupDB(DBName=DBName,host=host,port=port,user=user,password=os.getenv(environPassword),mode = mode):
            print('Backup finished, you can find your files on c:\pgDump directory.')
            executionTime = datetime.now()-executionTime
            print('Execution time: {}'.format(executionTime))
        else:
            print('The backup could not be done.')
            executionTime = datetime.now()-executionTime
            print('Execution time: {}'.format(executionTime))
