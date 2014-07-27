import MySQLdb
import sys

def getDbConnection(hostname,username,password):
    db_con = MySQLdb.connect(host=hostname,user=username,passwd=password)
    return db_con
    
def setupDB():
    db_con=getDbConnection("localhost","nishant","password")
    db_cursor=db_con.cursor()    
    db_cursor.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'PubMedRepository'");
    db_stat=db_cursor.fetchone()
 
    if (not db_stat):
        for mysql_stmt in open('setup_db.sql'):
            if mysql_stmt.strip():
                db_cursor.execute(mysql_stmt.strip())
        
    db_cursor.close()
    db_con.close()

def getLastId():
    db_con=getDbConnection("localhost","nishant","password")
    db_cursor=db_con.cursor() 
    db_cursor.execute("SELECT LAST_INSERT_ID()")
    last_id=db_cursor.fetchone()
    db_cursor.close()
    db_con.close()
    return last_id
    
setupDB()
