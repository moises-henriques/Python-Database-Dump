############################################################################################
##                    Script to securely dump PostgreSQL Database:                        ##
##                                                                                        ##
## Security measures adopted:                                                             ##
##  - No sensitive information, such as passwords are given to command line;              ##
##  - No passwords are kept as plain text in the script;                                  ##
##  - No passwords or other sentive data are kept as plain text in auxiliary files        ##
##    (my.cnf) after the execution.                                                       ##
##                                                                                        ##
## Script calling example:                                                                ##
##  - python mysqlDump.py -t -c -h localhost -db DBName -u postgres -ev MySQLDBPass       ##
##                                                                                        ##
## Arguments Explanation:                                                                 ##
##                                                                                        ##
##  - (-t): Dump table by table of Database (Optional)                                    ##
##  - (-c): Compress the dump result (Optional, takes more time to complete)              ##
##  - (-h): Hostname                                                                      ##
##  - (-db): Database name                                                                ##
##  - (-u): Database user                                                                 ##
##  - (-ev): Name of environment variable set with Database password for the given user   ##
##                                                                                        ##
##  Dump File(s) Location(s) and created folders:                                         ##
##                                                                                        ##
##  - When the script is executed, it will create and dump files to c:\MySQLDump. The     ##
##    configuration file is created under the same folder where the script is executed.   ##
##                                                                                        ##
##  Final observations:                                                                   ##
##                                                                                        ##
##  - The configuration file is overriden after the code execution, so no sensitive data  ##
##    will be stored in plain text after the dump process.                                ##
##  - The script must run in a python environment provided with all the dependencies      ##
##    listed in the import section bellow.                                                ##
##                                                                                        ##
##  Any advices or suggestions:                                                           ##
##                                                                                        ##
##  - moises.morais.henriques@gmail.com                                                   ##
##  - https://github.com/moises-henriques                                                 ##
##                                                                                        ##
############################################################################################

## Imports ##
import sys
import pandas as pd
import mysql
from mysql.connector import (connection)
from mysql.connector import errorcode
import os
import gzip
import shutil
from datetime import datetime

## Global Flags ##
compressFlag = False
environPassword = 'MySQLDBPass'

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
            if os.getenv('mysqlDBUser'):
                password = os.getenv('mysqlDBUser')
            else:
                raise RuntimeError('A password was not provided while connecting to DB using DBConnect, please review your code and provide a password to mitigate this error.')
    else:
        user='postgres'
        password= os.getenv('mysqlDBUser')

    try:
        conn = connection.MySQLConnection(
        host=host,
        database=database,
        user=user,
        password= password)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            raise PermissionError("Wrong user name or password.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            raise ValueError("Database does not exist")
        else:
            raise RuntimeError('ERROR: '+err.msg)
    else:
        print('A connection was successfully established with the SEI server!')
        cursor = conn.cursor()
        return conn, cursor


## List all table names ##
def queryTableList(conn,cursor,DBName):
    query = ("SELECT table_name FROM information_schema.tables "
            "WHERE (table_schema = '"+DBName+"') "
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
def setMycnf(host,user,password,**kwargs):
    if 'port' in kwargs:
        port = str(kwargs.get('port'))
    else:
        port="3306"
    
    try:
        mycnf = open('my.cnf','w+')
        mycnf.write('[client]')
        mycnf.write('\nhost='+host)
        mycnf.write('\nuser='+user)
        mycnf.write('\npassword="'+password+'"')
        mycnf.truncate()
        mycnf.close()

        return True
    except:
        return None

## Clear created pgpass.conf ##
def clearMycnf():
    try:
        os.remove('my.cnf')
        return True
    except:
        return None


## Compress data ##
def compress(path,filename):
    compressedFile = filename+'.tar.gz'
    compressedFile = compressedFile.replace('.sql','')

    with open(path+filename, "rb") as fin, gzip.open(path+compressedFile, "wb") as fout:
        # Reads the file by chunks to avoid exhausting memory
        shutil.copyfileobj(fin, fout)
    
    ## File Sizes in MB
    uncompressedSize = os.stat(path+filename).st_size/(1024**2)
    compressedSize = os.stat(path+compressedFile).st_size/(1024**2)

    print("Uncompressed size: {0:.2f} Mb".format(uncompressedSize))
    print("Compressed size: {0:.2f} Mb".format(compressedSize))

## Backup MySQL DB ##
def backupDB(host,DBName,user,password,**kwargs):
    global compressFlag
    if 'mode' in kwargs:
        mode = kwargs.get('mode')
        if mode == 'byTable':
            ## Connect ##
            conn, cursor = DBConnect(user= user,database=DBName,password=password,host=host)
            ## Get table list in DB ##
            tableList = queryTableList(conn, cursor,DBName=DBName)
            conn.close()
    else:
        mode="all"
    
    try:
        if setMycnf(host,user,password):
            if not os.path.isdir('c:\MySQLDump\\'):
                os.mkdir('c:\MySQLDump\\')
            if mode == 'all':
                path=os.getcwd()
                mysqldump = 'mysqldump.exe --defaults-file="'+path+'\my.cnf" --default-character-set=utf8 --protocol=tcp --column-statistics=0 --skip-triggers "'+DBName+'" > c:\MySQLDump\\'+DBName+'.sql'
                os.system('"cmd.exe"' and mysqldump)

                if compressFlag:
                    compress(path='c:\MySQLDump\\',filename=DBName+'.sql')
            elif mode ==  'byTable':
                for idx in tableList.index:
                    path=os.getcwd()
                    mysqldump = 'mysqldump.exe --defaults-file="'+path+'\my.cnf" --default-character-set=utf8 --protocol=tcp --column-statistics=0 --skip-triggers "'+DBName+'" "'+tableList.iloc[idx][0]+'"> c:\MySQLDump\\'+DBName+'_tb_'+tableList.iloc[idx][0]+'.sql'
                    os.system('"cmd.exe"' and mysqldump)
                    if compressFlag:
                        compress(path='c:\MySQLDump\\',filename=DBName+'_tb_'+tableList.iloc[idx][0]+'.sql')
            else:
                raise ValueError('ERROR: Invalid mode provided in backupDB function.')
        else:
            raise RuntimeError('ERROR: We could not Set/Create the my.cnf, review the code.')
        if clearMycnf():
            return True
        else:
            raise RuntimeError('ERROR: We could not clear the my.cnf content, review the code.')
    except:
        raise RuntimeError('ERROR: We could not proced with the backup process, review the code.')

## Get initial arguments and set flags ##
def getArguments():
    # mode
    if '-t' in sys.argv:
        mode='byTable'
    else:
        mode = 'all'
    
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
    
    # Compress result
    global compressFlag
    if '-c' in sys.argv:
        compressFlag = True

    # host
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
    port = None
    if '-p' in sys.argv:
        idx = 0
        for entry in sys.argv:
            if entry == '-p':
                port = sys.argv[idx+1]
                break
            else:
                idx += 1
    if not port:
        port = 3306

    # Data Base
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
    
    if not (DBName or host or user):
        raise ValueError('Database Name and/or host arguments missing.')
    else:
        return mode, host, port, DBName, user, environPassword


### Main ###
if __name__=='__main__':
    executionTime = datetime.now()
    ## get arguments ##
    mode, host, port, DBName, user, environPassword = getArguments()

    ## Run Backup ##
    if backupDB(host=host,port=port,DBName=DBName,user=user,password=os.getenv(environPassword),mode=mode):
        print('Backup finished, you can find your files on c:\MySQLDump directory.')
        executionTime = datetime.now()-executionTime
        print('Execution time: {}'.format(executionTime))
    else:
        print('The backup could not be done.')
        executionTime = datetime.now()-executionTime
        print('Execution time: {}'.format(executionTime))

