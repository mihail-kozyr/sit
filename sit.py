# -*- coding: utf-8 -*- 
"""
Utility for 
  - Siron AML text files creation from database tables or views.
  - data cleansing according to data supply requirements
  - AML scoring running
  - KYC scoring running

DECLAIMER:  
THIS SOFTWARE IS BEING PROVIDED "AS IS", WITHOUT ANY EXPRESS OR IMPLIED
WARRANTY.  IN PARTICULAR, NEITHER THE AUTHOR NOR RDTeX MAKES ANY
REPRESENTATION OR WARRANTY OF ANY KIND CONCERNING THE MERCHANTABILITY
OF THIS SOFTWARE OR ITS FITNESS FOR ANY PARTICULAR PURPOSE.

"""

__author__ = "Mihail Kozyr (mihail.kozyr@gmail.com)"
__version__ = "$Revision: 1.7 $"
__date__ = "$Date: 2018/06/01 19:15:19 $"

import logging
import codecs
import time
import os
import datetime
import calendar
import re
import shutil
import glob
import argparse
import zipfile

import pyodbc

import config as c
logfile='sit_' + c.now + '.log'
logging.basicConfig(level=c.LOG_LEVEL, filename=logfile, format=c.LOG_FORMAT)
#logging.basicConfig(level=c.LOG_LEVEL, format=c.LOG_FORMAT)
log = logging.getLogger(__name__)

def print_header():
    print('')
    print('*******************************************************************************')
    print('*******************************************************************************')
    print('*******************************************************************************')
    print('******                                                                   ******')
    print('******         S i r o n    I n t e g r a t i o n    T o o l             ******')
    print('******                                                                   ******')
    print('******                           RDTEX, JSC                              ******')
    print('******                                                                   ******')
    print('*******************************************************************************')
    print('*******************************************************************************')
    print('*******************************************************************************')
    print('')

def print_finish_success():
    print('')
    print('*******************************************************************************')
    print('*******************************************************************************')
    print('*******************************************************************************')
    print('******                                                                   ******')
    print('******             F i n i s h e d    s u c c e s s f u l l y            ******')
    print('******                                                                   ******')
    print('*******************************************************************************')
    print('*******************************************************************************')
    print('*******************************************************************************')
    
def print_finish_error():
    print ('')    
    print('*******************************************************************************')
    print('*******************************************************************************')
    print('*******************************************************************************')
    print('******                                                                   ******')
    print('******          E R R O R    d u r i n g    p r o c e s s i n g          ******')
    print('******                                                                   ******')
    print('*******************************************************************************')
    print('*******************************************************************************')
    print('*******************************************************************************')
    print ('')
    
def error_exit(code, msg):
    print_finish_error()
    print("Return code: %s, %s" % (code, msg) )
    print("See log files for the details")
    exit(code)
    
def connect(dsn=c.DSN, usr=c.USER, pwd=c.PASSWORD):
  """Connect to Oracle"""
  odbc_connect_string = 'DSN=%s;UID=%s;PWD=%s' %\
     (dsn, usr, pwd)
  conn = pyodbc.connect(odbc_connect_string)
  conn.execute('alter session set nls_sort=binary')
  return conn
  
def ResultIter(cursor, arraysize=1000):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result  

class SironFile:
    fmt = ''
    warn_cnt = 0
    err_cnt = 0
    chars_to_clean = '"\n'
    """ Класс, содержащий основные атрибуты и методы для генерации текстовых
        файлов в соответствии с требованиями к данным для Siron AML/KYC
    """
    def __init__(self, filename, verify=True, query=None):
        self.filename = filename
        self.verify=verify
        self.query=query
        self.conn = connect()
        
    def len_ctrl(self, field_name, field_value, max_len, custid):
        """ 
        Метод выполняет следующие проверки
        1. Проверяет длинну поля, если текущая длинна привышает
            предел, выполняется его усечение 
            
        2. Меняет пустые LASTNAME (ФИО) на <unknown>
            
         Каждый раз при модификации поля выводится предупреждение в журнал"""   
        
        try:
            if field_value is None:
                return ''
                
            # Для записей с пустым ФИО делаем ФИО <unknown> и выводим в журнал
            if field_name == 'lastname' and len(field_value) == 0:
                errtxt = "Field LASTNAME for customer no {} is empty."+\
                           " lastname changed to <unknown>."
                log.warning(errtxt.format(custid))
                self.__class__.warn_cnt += 1
                field_value='<unknown>'
            
            # Усекаем длинные поля до максимально-допустимой длинны
            if (len(field_value) > max_len):
                errtxt = "{} for customer no {} exceeds {} characters."+\
                           " len is {} The value will be truncated."
                log.warning(errtxt.format(field_name.upper(),\
                                          custid,\
                                          max_len,\
                                          len(field_value)) )
                self.__class__.warn_cnt += 1
                field_value = field_value[:max_len]

            return field_value
        except Exception as e:
            exc = str(e)      
            exc += " field_name={}".format(field_name.upper())
            exc += " custid={}".format(str(custid))
            log.exception(exc)
            
    def normalize_string(self, s ):
        """Нормализация текстовых строк - удаление лишних символов и пр."""
        try:
           #chars_to_clean = '"\n'
           tab={}
           for character in self.chars_to_clean:
               tab[ord(character)] = ""
               
           return s.translate(tab)
        except Exception as e:                     
            exc = str(e)                           
            exc += " field_name={}".format(field_name)
            exc += " custid={}".format(str(custid))
            log.exception(exc)                     
           
    
    def date_ctrl(self, field_value, field_name, custid):
        """Проверка даты на корректность"""
        try:
            if len(field_value) == 0: 
                return(field_value)
            
            now_int = int(c.now_date.strftime('%Y%m%d'))
            if int(field_value) < 18610219:
                errtxt = "Field {} for customer no {} is too early date ({})"+\
                         " the date changed to 18610219"
                log.warning(errtxt.format(field_name.upper(), str(custid), field_value))
                field_value = '18610219'
                self.__class__.warn_cnt += 1
            elif int(field_value) > now_int and field_name.upper() == 'BIRTHDATE':
                errtxt = "Field {} for customer no {} is too late date ({})"+\
                         " the date changed to 19230510"
                log.warning(errtxt.format(field_name.upper(), str(custid), field_value))
                field_value = '19230510'
                self.__class__.warn_cnt += 1
            return field_value 
        except Exception as e:
            exc = str(e)      
            exc += " field_name={}".format(field_name.upper())
            exc += " custid={}".format(str(custid))
            log.exception(exc)            

    def flow_control(self, field_value, field_name, field_atts, custid):
        """ Контроль на корректность значений при выгрузке в файл  """
        seq, max_length, mand, is_key, alias, data_type = field_atts        
        try:

            if field_value is None:
                field_value = ''
            elif isinstance(field_value, datetime.date)\
              or isinstance(field_value, datetime.datetime):
                field_value = field_value.strftime('%Y%m%d')
            elif data_type == 'date':
                field_value = self.len_ctrl(field_name,\
                                            field_value,\
                                            max_length,
                                            custid)
                #field_value = self.date_ctrl(field_value, field_name, custid)
            elif data_type == 'number':
                field_value = "{0: >-17.2f}".format(field_value)
                #if field_value.find('-') > 0:
                #    field_value = '-'+field_value.replace('-', '')
            elif data_type == 'char':
                field_value = self.len_ctrl(field_name,\
                                            field_value,\
                                            max_length,
                                            custid)  
                norm = self.normalize_string(field_value)
                if field_value != norm:
                    field_value = self.normalize_string(field_value)
                    errtxt = '{} character field for customer {} was normalized'
                    #log.warning(errtxt.format(field_name.upper(), str(custid)))
                    self.__class__.warn_cnt += 1
        except Exception as e:
            exc = str(e)      
            exc += " field_name={}".format(field_name.upper())
            exc += " custid={}".format(str(custid))
            exc += " value={}".format(field_value)
            log.exception(exc)
            
        return field_value
        
    def unload(self):
        """ Метод для выгрузки во внешний файл """
        # Счетчик ошибок для exceptions
        try:
            log.info('*** {} start'.format(self.filename.upper()))
            start_time = time.time()
            
            # print column names in the cursor from config
            # for col in cursor.columns(table='AML_Person', schema='DBA'):
            #  log.debug(col.column_name)
            
            # Достаем формат строки из переменной конфига <FILENAME>_ROW
            entity = os.path.splitext(self.filename)[0].upper()
            file_row_format = entity + "_ROW"
            file_row_format = getattr(c, file_row_format)
            
            # Формируем строку Fixed length иди CSV файла через format mini
            # language. цикл по полям файла в конфиге, сортируем по полю SEQ
            for field, field_atts in sorted(file_row_format.items(),\
                                            key=lambda x: x[1][0]):
                seq, max_length, mand, is_key, sel_pos, data_type = field_atts
                
                # Разные форматы под Fixed Length и CSV
                if isinstance(self, SironFixedLengthFile):
                    self.fmt += "{%s:<%s}" % (field, max_length)
                    #if data_type == 'number':
                    #    self.fmt += "{%s: >-%s.2f}" % (field, max_length)
                    #else:    
                    #    self.fmt += "{%s:<%s}" % (field, max_length)
                elif isinstance(self, SironCSVFile):
                    self.fmt += "{%s}|" % (field)

            if isinstance(self, SironCSVFile):
                #обрезаем хвостовой разделитель
                self.fmt = self.fmt[:-1]
            log.debug('format='+self.fmt)
            
            # Курсор к данным
            if self.query == None:
                entity_query = getattr(c, entity+"_QUERY")
            else:
                entity_query = self.query
              
            log.debug('Query:'+entity_query)
            cursor = self.conn.cursor()
            cursor.execute(entity_query)
    
            # Сохраняем в файл cтроки курсора 
            #f = codecs.open(self.filename, 'w', 'utf-8')
            f = open(self.filename, mode='w', encoding='utf-8')
            cnt = 0
            for row in ResultIter(cursor, c.ARRAYSIZE):
                cnt += 1
                current_cust = row.CUSTNO
        
                #цикл по полям из <ENITTY>_ROW в конфиге,
                #сортируем по полю SEQ
                #делаем словарь custrow['column_name']='column_val'
                cust_row = {}
                for field, field_atts in sorted(file_row_format.items(),\
                                                key=lambda x: x[1][0]):
                    seq, max_length, mand, is_key, alias, data_type = field_atts                                            
                    if alias != '':
                        field_value = getattr(row, alias.upper())
                        #log.debug("Field:{}; data_type:{}".format(field, type(field_value)))
                        if self.verify: 
                            cust_row[field] = self.flow_control(field_value,\
                                                               field,\
                                                               field_atts,\
                                                               current_cust)
                        else: cust_row[field] = field_value 
                    else: cust_row[field] = ''
                
                # формируем строку текстового файла фиксированной длинны
                try:
                    result_row = self.fmt.format(**cust_row)
                except  Exception as e:
                    exc = str(e)      
                    exc += " row={}".format(cust_row)
                    self.err_cnt += 1
                    log.exception(exc)
                    
                f.write(result_row + '\n')
                
            f.close() 
            cursor.close()
            elapsed_time = time.time() - float(start_time)
            if elapsed_time == 0: elapsed_time =1/100000 # to avoid zero devide
            
            log.info('*** {fname} finished. warnings:{warns}, elapsed sec:{tim:.2f}, rows:{rows}, speed={rs:.2f} rows/sec'.format(fname=self.filename.upper(), warns=self.warn_cnt, tim=elapsed_time, rows=cnt, rs=cnt/elapsed_time))
            return self.err_cnt 
        except Exception as e:
            exc = str(e)
            log.exception(exc)
            return 1

class SironFixedLengthFile(SironFile):
    """ Выгрузка данных в файл с полями фиксированной длинны """ 
    pass
    
        
class SironCSVFile(SironFile):
    """ Выгрузка данных в файл с полями с разделителем """
    pass

def add_months(sourcedate,months):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + int(month / 12)
    month = month % 12 + 1
    day = min(sourcedate.day,calendar.monthrange(year,month)[1])
    return datetime.date(year,month,day)

def last_day(dat):
    last_day = calendar.monthrange(dat.year, dat.month)[1]
    return datetime.date(dat.year, dat.month, last_day)

def getstatusoutput(cmd, p_cwd):
    """Return (status, output) of executing cmd in a shell."""
    """This new implementation should work on all platforms."""
    import subprocess
    pipe = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, universal_newlines=True, cwd=p_cwd)  
    output = "".join(pipe.stdout.readlines()) 
    sts = pipe.wait()
    if sts is None: sts = 0
    return sts, output
    
def runner(start, end):
    """Запускает ежемесячную выгрузку от start до end
       параметры datetime.date """
    
    start = datetime.date(start.year, start.month, 1)
    end = last_day(end)
    
    curr = start
    while (curr <= end):
        #print(curr, last_day(curr) )
        ld =  last_day(curr)
        where = 'WHERE '
        where += 'transtimestamp BETWEEN '
        where += "DATE('"+curr.strftime('%Y-%m-%d')+"')"
        where += " AND DATETIME('"+ ld.strftime('%Y-%m-%d')+ " 23:59:59')"
        #print(where)
        
        sel = c.IN_TRANSACTION_QUERY
        pattern = re.compile(r'\bWHERE 1=1\b', re.IGNORECASE)
        sel = pattern.sub(where, sel)
        #print(sel)
        #trx = SironFixedLengthFile('in_transaction.txt', query=sel)
        #errors += trx.unload()
        #(status, output) = getstatusoutput('start_scoring.bat 0001')
        
        curr = add_months(curr, 1)


def debug():
    start=datetime.date(2014, 4, 1)
    end=last_day(datetime.date(2014, 12, 31))
    #runner(start, end)
    #exit(0)
    #(status, output) = getstatusoutput('start_scoring.bat 0002')
    f = open('in_latest_entry_date.txt', 'w')
    f.write(end.strftime('%Y%m%d'))
    f.close()
    shutil.copy2(r'in_latest_entry_date.txt', r'c:\kozyr')
    shutil.copy2(r'in_transaction.txt', r'c:\kozyr')
    #print(status)
    #print(output)

def test_copy():
    """Копируем выгрузки AML in_*.txt в input каталог"""
    files = glob.iglob(os.path.join('.', "in_l*.txt"))
    for file in files:
        shutil.copy2(file, c.AML_INPUT_DIR)

def network_share_auth(share, username=None, password=None, drive_letter='S'):
    """Context manager that mounts the given share using the given
    username and password to the given drive letter when entering
    the context and unmounts it when exiting."""
    cmd_parts = ["NET USE %s: %s" % (drive_letter, share)]
    if password:
        cmd_parts.append(password)
    if username:
        cmd_parts.append("/USER:%s" % username)
    os.system(" ".join(cmd_parts))
#    try:
#        yield
#    finally:
#        os.system("NET USE %s: /DELETE" % drive_letter)


def insert_scoring_step(cur, name, scorid):
    """ Вспомогательная функция для записи в журнал """ 
    stmt = "SELECT scoring_step_seq.nextval FROM sys.dual"  
    cur.execute(stmt)
    row = cur.fetchone()
    scoring_step_id = row[0]   
    stmt = "insert into SCORING_STEP (SCORING_ID, STEP_ID, NAME, RESULT) \
        VALUES(?, ?, ?, 'RUNNING')"
    log.debug(stmt)
    cur.execute(stmt, scorid, scoring_step_id, name)
    cur.commit()    
    return scoring_step_id
        
def update_scoring_step(cur, res, stepid):
    """ Вспомогательная функция для записи в журнал """
    stmt = "update SCORING_STEP SET RESULT = '%s', \
        STEP_END=CURRENT_TIMESTAMP \
        WHERE STEP_ID = %s" % (res, stepid)
    cur.execute(stmt)
    cur.commit()

def unload_scoring(start='', end=''):
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("-cd", "--custdelta", action="store_true",\
                        help="Daily delta for customers ")
        parser.add_argument("-cdd", "--custdeltaday", action="store",\
                        help="The date for customer delta scoring in YYYYMMDD\
                              format. The day from which the changed and added\
                              customers should be unloaded from DB, by default:\
                              the last scoring day.")
        parser.add_argument("-cbs", action="store_true",\
                        help="Syncronize staging with Core Banking System")
        parser.add_argument("-s", action="append",\
                        help="Scoring(s) to run. Usage: -s {AML|KYC|BO|ALL|NONE}. Default: NONE")
        parser.add_argument("-f", action="append",\
                        help="File(s) to unload. Usage: -f {CUSTOMER|ACCOUNT|TRANSACTION|CSM|COUNTRY|POD|BO|ALL|NONE}. Default=ALL")
        parser.add_argument("-tsd", action="store",\
                        help="Transaction start date in YYYYMMDD format mask. Default: first day of current month.")
        parser.add_argument("-ted", action="store",\
                        help="Transaction end date in YYYYMMDD format mask. Default: last day of current month.")
        parser.add_argument("-z", "--zip", action="store_true",\
                        help="Zipping old IN_*.TXT files to zip arcive input_<DATE>.zip .")
                            
        args = parser.parse_args()
        
       
        if args.s == None:
           scorings=['NONE']
        else:
          scorings = [s.upper() for s in args.s]
        
        if args.f == None:
           files_to_unload = ['ALL']
        else:
          files_to_unload = [f.upper() for f in args.f]
        
        for scoring in scorings:
            if (scoring not in ['AML', 'KYC', 'NONE', 'ALL', 'BO']):
                print('Unknown type of scoring: ' + scoring)
                print('Usage: -s {AML|KYC|BO|ALL|NONE}')
                raise
        
        errors = 0
        now = c.now
        # если параметры с интервалом дат не заданы,
        # определяем start и end как 1-й и последний день текущего месяца
        if start == '':
            if args.tsd == None or args.ted == None:
                start=c.now_date
                start = datetime.date(start.year, start.month, 1)
                end=last_day(c.now_date)
            else:
                start = datetime.datetime.strptime(args.tsd, '%Y%m%d').date()
                end = datetime.datetime.strptime(args.ted, '%Y%m%d').date()
        
        #start = datetime.date(2014, 11, 1)
        #end = datetime.date(2014, 11, 30)
        
        if args.custdelta:
           last_scoring_date = ''
           #Load last scoring date or get it from the parameter
           if args.custdeltaday == None:
               with open(c.LAST_SCORING_DATE_FILE, "r") as f:
                   last_scoring_date = f.readline().strip()
           else:
               last_scoring_date=args.custdeltaday
        
        print_header()    
        # Пишем в журнал загрузки. Отдельное соединение с БД.
        # Scoring
        logconn = connect(dsn=c.LOG_DSN,usr=c.LOG_USER, pwd=c.LOG_PASSWORD)
        logcursor = logconn.cursor()
        stmt = "SELECT aml_operator_seq.nextval FROM sys.dual"  
        logcursor.execute(stmt)
        row = logcursor.fetchone()
        scoring_id = row[0]
        stmt = "INSERT INTO aml_operator(runid, name, start_date, status) VALUES(?,?,SYSDATE,?)"
        logcursor.execute(stmt, scoring_id, "sit.py", "EXECUTING")
        logcursor.commit()
        
        if args.cbs:
            try:
                print('Step CBS       - Sync staging area and core banking system with ETL.MAIN')
                stmt = 'begin etl.main; end;'
                logcursor.execute(stmt)
            except Exception as e:
                stmt = "UPDATE aml_operator SET status = 'FAILED', end_date = SYSDATE WHERE runid = ?"
                logcursor.execute(stmt, scoring_id)
                logcursor.commit()
                log.exception(str(e))
                return -3
                

        
        # формируем фразу WHERE
        where = 'WHERE '
        
        #where += 'entrydate BETWEEN '
        #where += "TO_DATE('"+start.strftime('%Y-%m-%d')+"', 'YYYY-MM-DD')"\
        #  " AND TO_DATE('"+ end.strftime('%Y-%m-%d')+" 23:59:59', 'YYYY-MM-DD HH24:MI:SS')"
        
        where += "entrydate >= TO_DATE('"+start.strftime('%Y-%m-%d')+"', 'YYYY-MM-DD')"
        
        if args.custdelta:     
            where_last_scoring = ''
            if last_scoring_date:
                where_last_scoring = "WHERE ChangeDate > " + "CAST('" + last_scoring_date + "' AS DATETIME)"
        
        if files_to_unload != ['NONE']:
            print('*******************************************************************************')
            print('*******  Unload Siron Data Supply files from the database                ******')
            print('*******************************************************************************')
        
            if args.zip:
                print('Step ZIP       - Back up old files to archive input_%s.zip'%(now))
                # Create archive
                zipFileName = 'input_' + now + '.zip'
                zf = zipfile.ZipFile(zipFileName, mode='a', compression=zipfile.ZIP_DEFLATED)
                
                files = glob.iglob(os.path.join('.', "in_*.txt"))         
                for file in files:
                    # Add file to archive
                    log.debug('Add file ' + file + ' to archive')
                    zf.write(file)
                    
                zf.close()
            
        if 'CSM' in files_to_unload or 'ALL' in files_to_unload:
            fname = 'in_cust_serv_manager.txt'
            print('Step CSM       - Unloading Cust. Service Managers to '+fname)
            csm = SironFixedLengthFile(fname)
            errors += csm.unload()
        
        if 'COUNTRY' in files_to_unload or 'ALL' in files_to_unload:
            fname = 'in_country.txt'
            print('Step CNTR      - Unloading Countries reference to '+fname)
            cntry = SironFixedLengthFile(fname)
            errors += cntry.unload()
        
        if 'CUSTOMER' in files_to_unload or 'ALL' in files_to_unload:
            sel = c.IN_CUSTOMER_QUERY
            
            if args.custdelta:
                sel=sel.replace('WHERE 1=1', where_last_scoring)
                delta_char = 'D'
            else: 
                delta_char = ' '
                
            fname = 'in_customer.txt'
            print('Step CST%s      - Unloading Customers to %s'%(delta_char, fname))
            cust = SironFixedLengthFile(fname, query=sel)
            errors += cust.unload()
        
            sel = c.IN_CUSTOMER_EXTENSION_NOHIST_QUERY
            if args.custdelta:
                sel=sel.replace('WHERE 1=1', where_last_scoring)
            
            fname='in_customer_extension_nohist.txt'
            print('Step CSTEN%s    - Unloading Customers to %s'%(delta_char, fname))
            cust_e2 = SironCSVFile(fname, query=sel)
            errors += cust_e2.unload()
        
        
        if 'ACCOUNT' in files_to_unload or 'ALL' in files_to_unload:
            fname = 'in_account.txt'
            print('Step ACC       - Unloading Accounts to ' + fname)
            acc = SironFixedLengthFile(fname)
            errors += acc.unload()
            
            fname = 'in_account_extension_nohist.txt'
            print('Step ACCENH    - Unloading Accounts Extension Nohist to ' + fname)
            acc_e1 = SironCSVFile(fname)
            errors += acc_e1.unload()
        
        if 'POD' in files_to_unload or 'ALL' in files_to_unload:
            fname = 'in_power_of_disposal.txt'
            print('Step POD       - Unloading PODs to ' + fname)
            pod = SironFixedLengthFile(fname)
            errors += pod.unload()
        
        if 'BO' in files_to_unload or 'ALL' in files_to_unload:
            fname = 'in_beneficial_owner.txt'
            print('Step BO        - Unloading Beneficial Owners to ' + fname)
            rel = SironCSVFile(fname)
            rel.chars_to_clean = '\r\n'
            errors += rel.unload()
        
        if 'TRANSACTION' in files_to_unload or 'ALL' in files_to_unload:
            fname = 'in_transaction.txt'
            print('Step TRX       - Unloading Transactions to ' + fname)
            sel = c.IN_TRANSACTION_QUERY
            sel=sel.replace('WHERE 1=1', where)
            trx = SironFixedLengthFile(fname, query=sel)
            trx.chars_to_clean = '\r\n'
            errors += trx.unload()
            
            fname = 'in_transaction_extension.txt'
            print('Step TRX       - Unloading Transactions to ' + fname)
            sel = c.IN_TRANSACTION_EXTENSION_QUERY
            sel=sel.replace('WHERE 1=1', where)
            trx = SironCSVFile(fname, query=sel)
            errors += trx.unload()
        
        
        if errors == 0: 
            if (scorings != ['NONE'] and files_to_unload != ['NONE']):
               print('')
               print('*******************************************************************************')
               print('*******  Run loading and scoring processes                               ******')
               print('*******************************************************************************')            
               #update_scoring_step(logcursor, "SUCCESS", scoring_step_id)
               # Копируем выгрузки AML, KYC in_*.txt в input каталог"""
               print('Step COPF      - Copy data supply files to data/input folder ')
               files = glob.iglob(os.path.join('.', "in_*.txt"))         
               for full_filename in files:
                   filename = os.path.basename(full_filename)
                   shutil.copy2(full_filename, c.AML_INPUT_DIR)
                   if 'KYC' in scorings or scorings == ['ALL']:
                      if filename[0:11] == 'in_customer':
                        log.debug('Customer file found: ' + filename)
                        shutil.copy2(full_filename, c.KYC_INPUT_DIR)
        
            # скоринг KYC
            if 'KYC' in scorings or 'ALL' in scorings:
               print('Step KYCS      - Run Siron KYC batch scoring')
               scoring_logfile='scoringKYC_' + start.strftime('%Y%m%d') + '.log'
               
               scoring_step_id = insert_scoring_step(logcursor, 'SCORING_KYC',\
                   scoring_id)
                   
               (status, output) = getstatusoutput('start_scoring_prs.bat', r'c:\TONBELLER\SironKYC\system\scoring\batch')
               
               #Сохраняем журнал скоринга KYC
               with open(scoring_logfile, 'w') as f:
                   f.write('status='+str(status)+'\n')
                   f.write(output)                        
               
               if status == 0:
                   result = 'SUCCESS'
               else:
                   result = 'FAILED'
               update_scoring_step(logcursor, result, scoring_step_id)
               
               #Save to file last scoring datetime
               if status == 0:
                   with open(c.LAST_SCORING_DATE_FILE, 'w') as f:
                       score_date = c.now_date
                       f.write(score_date.strftime('%Y-%m-%d %H:%M:%S'))
        
            if 'BO' in scorings or 'ALL' in scorings:
                print('Step BOL       - Loading beneficial owners')
                
                scoring_logfile='relationAML_' + start.strftime('%Y%m%d') + '.log'
                # Run relation of AML
                scoring_step_id = insert_scoring_step(logcursor,'RELATION_AML',scoring_id)
                (status, output) = getstatusoutput('start_update_relations.bat 0001 in_beneficial_owner.txt', r'C:\TONBELLER\SironAML\system\scoring\watchlist\batch')
        
                # Save log of relation of AML
                with open(scoring_logfile, 'w') as f:
                   f.write('status='+str(status)+'\n')
                   f.write(output)
                
                if status == 0:
                   result = 'SUCCESS'
                   update_scoring_step(logcursor, result, scoring_step_id)            
                else:
                   result = 'FAILED'
                   update_scoring_step(logcursor, result, scoring_step_id)
                           
            if 'AML' in scorings or 'ALL' in scorings:
               print('Step AMLS      - Run scoring of Siron AML')
        
               scoring_logfile='scoringAML_' + start.strftime('%Y%m%d') + '.log'
               # Запускаем скоринг AML
               scoring_step_id = insert_scoring_step(logcursor, 'SCORING_AML',
                   scoring_id)
               (status, output) = getstatusoutput('start_scoring.bat 0001', \
                                            r'c:\TONBELLER\SironAML\system\scoring\batch')
               
               # Сохраняем журнал скоринга AML
               with open(scoring_logfile, 'w') as f:
                   f.write('status='+str(status)+'\n')
                   f.write(output)
               
               if status == 0:
                   result = 'SUCCESS'
                   update_scoring_step(logcursor, result, scoring_step_id)            
               else:
                   result = 'FAILED'
                   update_scoring_step(logcursor, result, scoring_step_id)
                   return -2
        else:
            update_scoring_step(logcursor, "FAILED", scoring_step_id)
            log.error('An error occured during unloading from the database. See details in ' + logfile)
            #with open(scoring_logfile, 'w') as f:
            #    f.write('An error occured during unloading from MS SQL Server. See details in ' + logfile)
            return -1
        stmt = "UPDATE aml_operator SET status = 'SUCCESS' , end_date = SYSDATE WHERE runid = ?"
        logcursor.execute(stmt, scoring_id)
        logcursor.commit()
        return 0
    except Exception as e:
        log.exception(str(e))
        result = -2
        stmt = "UPDATE aml_operator SET status = 'FAILED' , end_date = SYSDATE WHERE runid = ?"
        logcursor.execute(stmt, scoring_id)
        logcursor.commit()
        return result


def main():
    result_codes = {-1: "File unloading failure",\
                    -2: "Scoring failure",\
                    -3: "CBS ETL failure"}
    #for m in range(8, 12):
    #    start_date = datetime.date(2015, m, 1)
    #    end_date = last_day(start_date)
    #    unload_scoring(start_date, end_date)
    start_date = datetime.datetime.now()
    print('Started at: ' + start_date.strftime('%Y/%m/%d %H:%M'))
    
    result = unload_scoring()
    
    end_date = datetime.datetime.now()
    print('Finished at: ' + end_date.strftime('%Y/%m/%d %H:%M'))
    
    if result == 0:
        print_finish_success()
    else:
        error_exit(result, result_codes[result])
            

if __name__ == '__main__':
    try:
        log.info("Started")
        main()
        log.info("Finished")
    except Exception as e:
        log.error("failed")
        log.exception(str(e))
