#encoding: utf-8
__author__ ='zhangyuanxiang'

import sys 
reload(sys) 
sys.setdefaultencoding('utf8') 
import chardet
import time
import os
from optparse import OptionParser
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.AL32UTF8'
#os.environ['NLS_LANG']='SIMPLIFIED CHINESE_CHINA.ZHS16GBK'

def get_cli_options():
    parser = OptionParser(usage="usage: python %prog [options]",
                          description=""" Data Transfer """)

    parser.add_option("--from", "--from_dsn",
                      dest="from_dsn",
                      default="local2",
                      help="host:port:user:pass:db:tablename"
                     )
    
    parser.add_option("--to", "--to_dsn",
                      dest="to_dsn",
                      default="local2",
                      help="host:port:user:pass:db:tablename")

    parser.add_option("--where", "--sql_where",
                      dest="where",
                      default="1=1",
                      help="where")
    
    parser.add_option("--sid", "--sid",
                      dest="sid",
                      default="orcl",
                      help="Oracle SID")

    parser.add_option("--type", "--type",
                      dest="dbtype",
                      default="NULL",
                      help="o2o o2m o2s m2m m2o m2s s2o s2m s2s"
                      )
    (options, args) = parser.parse_args()

 
    return options



class  excutemysqlstr():

    def __init__(self,str1,str2,sid):
        self.host=str1.strip().split(':')[0]
        self.port=str1.strip().split(':')[1]
        self.user=str1.strip().split(':')[2]
        self.passwd=str1.strip().split(':')[3]
        self.dbname=str1.strip().split(':')[4]
        self.table= str1.strip().split(':')[5]
        self.oracle_sid=sid
        self.dbtype=str2

    def gen_cursor(self):
       dbtype=self.dbtype.lower()
       if dbtype=="o2m" or dbtype=="s2m" or dbtype=="m2m":
         import pymysql as dbapi
         import pymysql.cursors
         from pymysql.constants import FIELD_TYPE 
         try:
           conn = dbapi.connect(host=self.host,port=int(self.port),user=self.user,passwd=self.passwd,db=self.dbname,charset="UTF8" \
                  ,cursorclass = dbapi.cursors.SSCursor)
         except Exception , e:
                print e
         return conn

       elif dbtype=="o2o" or dbtype=="s2o" or dbtype=="m2o":
         import cx_Oracle as dbapi
         try:
           dsn_tns =dbapi.makedsn(self.host,self.port,self.oracle_sid)
           conn = dbapi.connect(self.user,self.passwd,dsn_tns)
         except Exception , e:
                print e
         return conn
       elif dbtype=="o2s" or dbtype=="m2s" or dbtype=="s2s":
         import pymssql as dbapi
         try:
           conn = dbapi.connect(server=self.host,port=self.port,user=self.user,password=self.passwd,database=self.dbname,charset="cp936")
         except Exception , e:
                print e
         return conn
       else:
           print "input source database type"                 

    def gen_bluck (self):
        dbtype=self.dbtype.lower() 
        cursor=self.gen_cursor().cursor()
        sql_col="""SELECT * from  %s where 1=0 """ % (self.table)
        if dbtype=="o2s" or dbtype=="m2s" or dbtype=="s2s" or \
           dbtype=="o2m" or dbtype=="s2m" or dbtype=="m2m":
             row='('
             ncol='(%s'
             insert_sql="insert into %s" %(self.table)
             cursor.execute(sql_col)
             data = cursor.fetchall()
             for i in range(0, len(cursor.description)):
               if i==0:
                  row =row+cursor.description[i][0]
                  ncol=ncol
               else:
                  row +=','+cursor.description[i][0]
                  i=i+1
                  ncol +=',%s'
             insert_table=insert_sql+row+' '+')'+' '+'values'+' '+ncol+')'
             
        else:
           row='('
           ncol='(:1'
           sql_col="select * from %s.%s WHERE 1=2" %(self.dbname,self.table)
           insert="insert into %s.%s" %(self.dbname,self.table)
           cursor.execute(sql_col)
           data = cursor.fetchall()
           for i in range(0, len(cursor.description)):
              if i==0:
                 row =row+cursor.description[i][0]
                 ncol=ncol
              else:
                 row +=','+cursor.description[i][0]
                 i=i+1
                 ncol +=',:'+str(i)
          
           insert_table=insert+row+' '+')'+' '+'values'+' '+ncol+')'
        return insert_table

class query_data():

    def __init__(self,str1,str2,sid,where):

        self.host=str1.strip().split(':')[0]
        self.port=str1.strip().split(':')[1]
        self.user=str1.strip().split(':')[2]
        self.passwd=str1.strip().split(':')[3]
        self.dbname=str1.strip().split(':')[4]
        self.table= str1.strip().split(':')[5]
        self.oracle_sid=sid
        self.dbtype=str2
        self.where=where

    def gen_cursor(self):
         dbtype=self.dbtype.lower()  
         if dbtype=="m2o" or dbtype=="m2s" or dbtype=="m2m":
             import pymysql as dbapi
             import pymysql.cursors
             from pymysql.constants import FIELD_TYPE
             try:
               conn = dbapi.connect(host=self.host,port=int(self.port),user=self.user,passwd=self.passwd,db=self.dbname,charset='UTF8',\
                      cursorclass = dbapi.cursors.SSCursor)
             except Exception , e:
                print e
             return conn 
         elif dbtype=="o2o" or dbtype=="o2s" or dbtype=="o2m":
             import cx_Oracle as dbapi
             try:
               dsn_tns = dbapi.makedsn(self.host,self.port,self.oracle_sid)
               conn = dbapi.connect(self.user,self.passwd,dsn_tns)
             except Exception , e:
               print e
             return conn
         elif dbtype=="s2o" or dbtype=="s2m" or dbtype=="s2s":
              import pymssql as dbapi
              try:
                conn = dbapi.connect(server=self.host,port=self.port,user=self.user,password=self.passwd,database=self.dbname,charset='cp1251')
              except Exception , e:
                print e
              return conn
         else:
           print "input source database type"
     
    def gen_cnt(self):
        dbtype=self.dbtype.lower()
        start=0
        end=0
        sqlstr="select * from " 
        if dbtype=="s2o" or dbtype=="s2m" or dbtype=="s2s" or \
           dbtype=="m2o" or dbtype=="m2s" or dbtype=="m2m": 
           query_str=sqlstr+" "+self.table +" "+"where"+" "+self.where
        else:
           query_str=sqlstr+" "+self.dbname+'.'+self.table +" "+"where"+" "+self.where
        return query_str

    def ResultIter(Cursor, arraysize=1000):

        while True:
              results =Cursor.fetchmany(arraysize)
              if not results:
                 break
              for result in results:
                 yield result

    def query_db(self):
        mysql_cursor=self.gen_cursor().cursor()
        sqlstr=self.gen_cnt()
        return  sqlstr
    def Getdbtype(self):
        dbtype=self.dbtype.lower()
        return dbtype

def import_data():
    options = get_cli_options()
    excute_str=excutemysqlstr(options.to_dsn,options.dbtype,options.sid) 
    query_str=query_data(options.from_dsn,options.dbtype,options.sid,options.where)   
    count=0
    batch=10000
    cnt=5
    insertsql=excute_str.gen_bluck()
    dbtype=query_str.Getdbtype()
    print "start................"
    selectsql=query_str.query_db()
    tocursor =excute_str.gen_cursor().cursor()
    fromcursor = query_str.gen_cursor().cursor()
    fromcursor.execute(selectsql)
    num_fields = len(fromcursor.description)
    result = fromcursor.fetchmany(batch) 
    while result:
          lg=len(result)
          try:
             if dbtype=="m2o" or dbtype=="s2o" or dbtype=="o2o":
                tocursor.prepare(insertsql)
                tocursor.executemany(None,result)
                tocursor.execute('commit')
             elif dbtype=="m2s" or dbtype=="s2s" or dbtype=="o2s":
                tocursor.executemany(insertsql,result)
                tocursor.execute('commit')
                tocursor.execute('begin transaction')
             else:
                 tocursor.executemany(insertsql,result)
                 tocursor.execute('commit')
             count+=1
             dt=time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
             if (lg==batch and (count%cnt)==0):
                print "%s commited %s" %(count*batch,dt)
             elif(lg != batch):
                print "%s commited %s" %((count-1)*batch+lg,dt)
             else:
                pass
          except (KeyboardInterrupt, SystemExit):
                 raise
          except Exception,e:
               print Exception,":",e
          result=[]
          result = fromcursor.fetchmany(batch)
def main():
    import_data()

if __name__ == '__main__':
       main()
