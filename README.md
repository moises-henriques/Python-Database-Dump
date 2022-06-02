# Python Script to securely dump PostgreSQL and MySQL Databases on Windows
## Security measures adopted:                                                          
- No sensitive information, such as passwords are given to command line;           
- No passwords are kept as plain text in the script;                                 
- No passwords or other sentive data are kept as plain text in auxiliary files (pgpass.conf or my.cnf) after the execution.

## Script calling example:                                                           
- python mysqlDump.py -t -c -h localhost -db DBName -u postgres -ev MySQLDBPass      
                                                                                   
## Arguments Explanation:                                                            
                                                                                   
- (-t): Dump table by table of Database (Optional)                               
- (-c): Compress the dump result (Optional, takes more time to complete)         
- (-h): Hostname                                                                 
- (-db): Database name                                                           
- (-u): Database user                                                            
- (-ev): Name of environment variable set with Database password for the given user   
                                                                                  
##  Dump file(s) location(s) and created folders:                                    
                                                                                  
- When the script is executed, it will create and dump files to c:\MySQLDump. The configuration file is created under the same folder where the script is executed.  
                                                                                  
##  Final observations:                                                              
                                                                                 
- The configuration file is removed after the code execution, so no sensitive data will be stored in plain text after the dump process.                           
- The script must run in a python environment provided with all the dependencies listed in the import section bellow.
- Before first execution the user must set an environment variable for the database password. The name of this variable should be given to the script using the -ev argument.                                           
                                                                                  
##  Any advices or suggestions:                                                      
                                                                                  
- moises.morais.henriques@gmail.com                                                  
- https://github.com/moises-henriques                                                
