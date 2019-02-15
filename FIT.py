"""
Python tools designed to automate, enhance and improve ICG's current tasks and procedures.
"""

__author__ = 'Claudio Mazzoni'
import datetime
import pandas as pd
import os
import time
import re
from dateutil.parser import parse
import zipfile
import subprocess
import sqlalchemy
from sqlalchemy.exc import TimeoutError, NoSuchTableError, UnboundExecutionError
import pyodbc
import cx_Oracle as cxo
import glob
from difflib import SequenceMatcher



def is_date(strings):
    """
    Simple check of date
    Arguments:
        strings: As String
    """
    # From Stack Overflow:
    # https://stackoverflow.com/questions/25341945/check-if-string-has-date-any-format
    
    try:
        parse(strings)
        return True
    except ValueError:
        return False


def files_infolder(path, ext='*'):
    """
    Check folder content, filter by file type
    Arguments:
        path: Path to check as String
        ext: File types based on the file extension name default all files in Path as String
    """
    files = []
    if ext is not None:
        for infile in glob.iglob(os.path.join(path, '*.{}'.format(ext))):
            files.append(infile)
    else:
        files = [f for f in glob.iglob(path + '*') if '.' not in f.split('\\')[-1]]

    return files


def diff_month(d1, d2):
    """
    Returns the month difference between two dates
    Arguments:
        d1: DateTime object
        d2: DateTime object
    """
    # From Stack Overflow
    # https://stackoverflow.com/questions/4039879/best-way-to-find-the-months-between-two-dates
    if isinstance(d1, pd.DataFrame) and isinstance(d2, pd.DataFrame):
        return (d1.year - d2.year) * 12 + d1.month - d2.month
    else:
        return (d1.dt.year - d2.dt.year) * 12 + d1.dt.month - d2.dt.month


def zipper(path_files, zip_name, destination='Self'):
    """
    Zip files in a selected folder
    Arguments:
        path_files: Full path of the content to be zipped as String
        zip_name: Name of the Zipped object as String
        destination: Where the zip file will be saved into Default 'Self' as String
    """
    zipf = zipfile.ZipFile('{}.zip'.format(zip_name), 'w', zipfile.ZIP_DEFLATED)
    if destination == 'Self':
        check_unique = ['\\'.join(pth.split('\\')[0:-1]) for pth in path_files]
        check_unique = list(set(check_unique))
        if len(check_unique) > 1:
            return 'LocationError: When Files are in multiple locations destinatination cannot be blank /n{}'
        else:
            desti = '\\'.join(path_files[0].split('\\')[0:-1])
    else:
        desti = destination
    for file in path_files:
        zipf.write(os.path.join(desti, file))


def terminate_program(program, hours=1):
    """
    Will close program for a pre deremined period of time
    Arguments:
        program: Name of program to close (as spelled on Task Manager -> Process) as String
        hours: Number of hours the function will run for, default 1 hour as Integer
    """
    hourly = hours * 60
    end_hour = time.time() + hourly
    while True:
        try:
            os.system("TASKKILL /F /IM {}.exe".format(program))
            time.sleep(10)
            if end_hour < time.time():
                break
        except:
            break


class QueryBuilder:
    """
    Functions Available:
        *   check_db_details
        *   check_tables_details
        *   check_columns_details

    """
    def __init__(self, conn):
        self.connection = conn

    def check_db_details(self):
        if 'mssql' == self.connection[0]:
            qry = "SELECT name FROM master.sys.databases"
            # try:
            returns = self.connection[1].execute(qry)  # multi=True Has been added
            # except e:
            #     print(e)
        elif 'hive' == self.connection[0]:
            qry = 'SHOW DATABASES;'
            # try:
            returns = self.connection[1].execute(qry)
            returns = returns.fetchall()
            # except e:
            #     print(e)
        elif 'pace' == self.connection[0]:
            qry = 'SELECT * FROM DBA_USERS'
            returns = self.connection[1].execute(qry)
            returns = returns.fetchall()
        else:
            returns = 'Error Invalid Connection'
        return returns

    def check_tables_details(self, db_name):
        if 'mssql' == self.connection[0]:
            returns = self.connection[1].table_names()
        elif 'hive' == self.connection[0]:
            qry = "SHOW tables IN {};".format(db_name)
            returns = self.connection[1].execute(qry)
            returns = returns.fetchall()
        elif 'pace' == self.connection[0]:
            qry = "SELECT owner, table_name FROM dba_tables WHERE owner = '{}'".format(db_name)
            returns = self.connection[1].execute(qry)
            returns = returns.fetchall()
        else:
            returns = 'Error Invalid Connection'
        return returns

    def check_columns_details(self, db_name, tbl_name):
        if 'mssql' == self.connection[0]:
            qry = "SELECT [COLUMN_NAME] FROM [{0}].INFORMATION_SCHEMA.COLUMNS " \
                  "WHERE TABLE_NAME = N'{1}'".format(db_name, tbl_name)
            returns = self.connection[1].execute(qry)  # multi=True Has been added
        elif 'hive' == self.connection[0]:
            qry = 'show columns IN {0}.{1};'.format(db_name, tbl_name)
            returns = self.connection[1].execute(qry)
            returns = returns.fetchall()
        elif 'pace' == self.connection[0]:
            qry = 'DESCRIBE {}.{};'.format(db_name, tbl_name)
            returns = self.connection[1].execute(qry)
            returns = returns.fetchall()
        else:
            returns = 'Error Invalid Connection'
        return returns

    @staticmethod
    def create_query_string(table, columns, where=None, order_by=None, group_by=None,
                            distinct=False, join_tabl=None, join_cols=None, join_type=None, col_as=None):
        """
        Arguments:
        :param table: Opt. Table SELECT
        :param columns: Opt. Column SELECT
        :param where: Opt. WHERE condition as dictionary exp {Column: Condition1, Column: [Condition1, !<>=Condition]}
        :param group_by: Opt. Group By condition as LIST exp [Column, Condition] Options: SUM, AVG, stddev_samp, COUNT
        :param distinct: Opt. DISTINCT boolean default FALSE
        :param order_by: Opt. ASC or DESC List exp [ColumnName, ASC/DESC]
        :param join_tabl: Opt. Dictionary exp {Table2: MatchingON}
        :param join_cols: Opt. List exp [Table2Column1, Table2Column2, Table2Column3] if '*' then ALL
        :param join_type: Opt. String options: LEFT, RIGHT, INNER, OUTER
        :param col_as: Opt. Output cols renamed, pass name as list and will be changed on the oder it is received
        :return SQL Query String:
        """
        gby = ''
        whr = ''
        ordby = ''
        dist = ''
        join = ' '
        columns_ = ''
        passed_unused = col_as
        if type(columns) is not list:
            return "ERROR Columns have invalid Datatype, expected list"
        if len(columns) == 1:
            columns_ = table + '.' + columns[0]
        else:
            colss = [table + '.' + i for i in columns]
            columns_ += ','.join(colss)
        if distinct is True:
            dist = 'DISTINCT'
        query = "SELECT {0} {1} FROM {2}".format(dist, columns_, table)
        if join_tabl is not None and join_type is not None:
            if type(join_tabl) is dict and len(join_tabl.keys()) == 1:
                for inx, val in join_tabl.items():
                    join += join_type + ' JOIN {0} ON {0}.{1}={2}.{1} '.format(inx, val, table)
                j_cols = ''
                if join_cols is '*':
                    j_cols += ',{}.'.format(join_tabl) + join_cols
                elif type(join_cols) is list:
                    key = list(join_tabl.keys())[0]
                    j_cols += ',{}.'.format(key)
                    j_cols = key + '.' + j_cols.join(join_cols)
                    j_cols += ',' + columns_
                query = query.replace(columns_, j_cols)
            else:
                return 'Param format error: You can only join one table'

        if where is not None:
            if type(where) is dict:
                whr += " WHERE "
                first_blood = True
                for inx, val in where.items():
                    if first_blood is True:
                        if type(val) is list:
                            if val[0][0] == '>' or val[0][0] == '<' or val[0][0] == '=':
                                if is_date(val[0][1:]) is True:
                                    whr += "{0}.{1} {2} '{3}' ".format(table, inx,
                                                                       val[0][0], val[0][1:])
                                else:
                                    whr += "{0}.{1} {2} {3} ".format(table, inx,
                                                                     val[0][0], val[0][1:])
                            elif val[0][0] == '!':
                                if val[0][1:].isdigit() is True:
                                    whr += "{0}.{1} NOT IN (".format(table, inx)
                                    whr += ",".join(val) + ")"
                                else:
                                    whr += "{0}.{1} NOT IN ('".format(table, inx)
                                    whr += "','".join(val) + "') "
                            elif val[0].isdigit() is True:
                                whr += "{0}.{1} IN (".format(table, inx)
                                whr += ",".join(val) + ") "
                            else:
                                whr += "{0}.{1} IN ('".format(table, inx)
                                for rqs in val:
                                    rqs.strip()
                                whr += "','".join(val) + "') "

                            first_blood = False
                        else:
                            return 'Invalid datatype for conditions please pass list'
                    else:
                        if type(val) is list:
                            if val[0][0] == '>' or val[0][0] == '<' or val[0][0] == '=':
                                if is_date(val[0][1:]) is True:
                                    whr += "{0}.{1} {2} '{3}' ".format(table, inx,
                                                                       val[0][0], val[0][1:])
                                else:
                                    whr += "{0}.{1} {2} {3} ".format(table, inx,
                                                                     val[0][0], val[0][1:])
                            elif val[0][0] == '!':
                                if val[0][1:].isdigit() is True:
                                    whr += "{0}.{1} NOT IN (".format(table, inx)
                                    whr += ",".join(val) + ")"
                                else:
                                    whr += "{0}.{1} NOT IN ('".format(table, inx)
                                    whr += "','".join(val) + "') "
                            elif val[0].isdigit() is True:
                                whr += "{0}.{1} IN (".format(table, inx)
                                whr += ",".join(val) + ") "
                            else:
                                whr += "{0}.{1} IN ('".format(table, inx)
                                for rqs in val:
                                    rqs.strip()
                                whr += "','".join(val) + "') "
                            # whr += " AND {0}.{1} IN('".format(table, inx)
                            # if type(val) is list:
                            #     whr += "','".join(val) + "') "
                        else:
                            return 'Invalid datatype for conditions please pass list'
            else:
                return 'WHERE condition invalid variable: Please pass Dict'

        if order_by is not None:
            if type(order_by) is list and len(order_by) == 2:
                ordby += ' ORDER BY {0} {1}'.format(order_by[0], order_by[1])
            else:
                return 'Param format error: List object should only have one column name & Asc or Desc'

        if group_by is not None:
            if type(group_by) is list and len(group_by) == 2:
                query = query.replace(table + '.' + group_by[0], '{1}({0})'.format(
                    table + '.' + group_by[0], group_by[1]))
                gby += 'GROUP BY {}'.format(group_by[0])   #  format(columns_)
            else:
                return 'WHERE condition invalid variable: Please pass List'

        query = query + join + whr + gby + ordby
        query = re.sub(' +', ' ', query)
        return query

    def get_schema_details(self):
        pass


class ICGUtilities:
    """
    Functions Available:
        *   bulk_bcp_upload
        *   create_connection
        *   qry_run
        *   create_email
        *   check_trading_day
    """
    def __init__(self):
        pass

    def bulk_bcp_upload(self, pandas_to_upload, database_tablename, server_name='NYM-ICGDB01-AT', create_table=True):
        """
        Use BCP to upload data to a SQL Server DB table on bulk
        Arguments:
            :param pandas_to_upload: Data to load as pandas DataFrame
            :param database_tablename: Database and Table name as String
            :param server_name: Server name, default NYM-ICGDB01-AT as String
            :param create_table: Optional - True, creates table else assumes table exist on DB, as Boolean
        Returns: Dictionary with results and error log.
        """
        table_name = database_tablename.split('.')[1]
        database_name = database_tablename.split('.')[0]
        conn = self.create_connection(connection_type='mssql', server=server_name,
                                      database=database_name)
        qry = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{}'".format(table_name)
        err_attach = []
        if create_table is True:
            if self.qry_run(conn, qry).scalar() is None:
                qry = 'CREATE TABLE [dbo].[{}]( '.format(table_name)
                cols = pandas_to_upload.columns.tolist()
                str_ = ''
                for c in cols:
                    dat_type = pandas_to_upload[c].infer_objects().dtype
                    if dat_type == 'float64' or dat_type == 'int64':
                        val_type = '[float] NULL,'
                    elif dat_type == 'datetime64':
                        try:
                            pandas_to_upload[c] = pd.to_datetime(pandas_to_upload[c])
                        except:
                            pass
                        val_type = '[date] NULL,'
                    else:
                        val_type = '[varchar](100) NULL,'
                    str_ = str_ + '[{0}]{1}'.format(c, val_type)
                qry = qry + str_[0:-1] + ')'
                self.qry_run(conn, qry)
        else:
            if self.qry_run(conn, qry).scalar() is None:
                return 'ERROR: DATABASE TABLE MISSING'

        report_name = str(int((datetime.now()).total_seconds()))
        pandas_to_upload.to_csv('C:\\temp\\{}.csv'.format(report_name), sep='|', index=False, header=False)
        bcp = 'bcp {0}..{1} in C:\\temp\\'.format(database_name, report_name)\
              + '{0}.csv -S {1} -T -t"|" -c -e "C:\\temp\\{0}_error_log.txt'.format(report_name, server_name)
        result_code = subprocess.call(bcp, shell=True)
        os.remove('C:\\temp\\{}.csv'.format(report_name))
        if os.stat('C:\\temp\\{0}_error_log.txt'.format(report_name)).st_size > 0:
            err_attach.append('C:\\temp\\{0}_error_log.txt'.format(report_name))
        else:
            os.remove('C:\\temp\\{0}_error_log.txt'.format(report_name))
        if len(err_attach) == 0:
            err_attach = None
        return {'Results': result_code, 'Error Log': err_attach}

    def create_connection(self, connection_type, server=None, database=None):
        """
        Create connection to DB (SSMS, PACE or Hive), returns connection or Error
        Arguments:
            :param connection_type: SQL Server or Hive as string
            :param server: Server name as string
            :param database: database as string
        """
        #   Passed variables types: 1. String, 2. String, 3. String
        db_conn = None
        try:
            if connection_type.lower() == 'mssql' and server is not None and database is not None:
                types = 'mssql'
                bd_conn_ms_data_bd = '{0}+pyodbc://{1}/' \
                                     '{2}?driver=SQL+Server+Native+Client+11.0'.format(types, server, database)
                db_conn = sqlalchemy.create_engine(bd_conn_ms_data_bd)
            elif connection_type.lower() == 'hive' and server is None and database is None:
                bd_conn_ms_data_bd = pyodbc.connect('DSN=DataLake_Prod_ODBC', autocommit=True)
                db_conn = bd_conn_ms_data_bd    # .cursor()
            elif connection_type.lower() == 'pace' and server is None and database is None:
                conn_str = cxo.makedsn(host='scan-pnjpcep1.newyorklife.com', port='1810',
                                       service_name='REPORTING')    # , server='DEDICATED')
                db_conn = cxo.connect(user='******', password='******', dsn=conn_str)

        except (UnboundExecutionError, TimeoutError, NoSuchTableError) as e:
            print('{} Error while creating the connection.'.format(e))
            quit()
            # raise ErrorHandle('Something wrong with the connection /n{}'.format(e))
        tag = connection_type
        return [tag, db_conn]

    def qry_run(self, connection, qry, is_dataframe=False):
        """
        Run query string and returns results, return types: Dataframe, ORM object or Error handl
        Arguments:
            :param connection: Declared connection object
            :param qry: Valid query as string
            :param is_dataframe: Optional - Boolean if True return DataFrame else execute query
        """
        # try:
        returns = None
        if is_dataframe is True and connection[0] == 'mssql':
            returns = pd.read_sql(sql=qry, con=connection[1])
        elif 'mssql' in connection[0] == 'mssql':
            if is_dataframe is False:
                returns = connection[1].execute(qry)
            else:
                returns = pd.read_sql(qry, connection[1])
        elif connection[0] == 'hive':
            if is_dataframe is False:
                returns = connection[1].execute(qry)
                returns = returns.fetchall()
            else:
                returns = pd.read_sql(qry, connection[1])
                # col = returns.fetchall()
                # returns = pd.DataFrame(col)
        elif connection[0] == 'pace' and is_dataframe is True:
            returns = pd.read_sql(qry, connection[1])
        # except:
        #      raise ErrorHandle('Something wrong with the qry /n{}'.format(e))

        return returns

    def save_data(self):
        pass

    def create_email(self, body, sender, subject, recipients=None, attachments=None):
        """
        Create and send email to passed recipients
        Arguments:
            :param body: body of email as String
            :param sender: sender of email as String
            :param subject: subject of email as String
            :param recipients: Optional - sender of email default None expect List variable
            :param attachments: Optional - attachments if any default None expect List variable
        """
        import smtplib
        if recipients is None:
            recipients = []
        from email.mime.multipart import MIMEMultipart
        from email.mime.base import MIMEBase
        from email.mime.text import MIMEText
        from email import encoders
        if len(recipients) == 0:
            raise ErrorHandle('Missing recipients')
        elif type(recipients) is not list:
            raise ErrorHandle('Invalid Datatype, expected list.')
        elif len(recipients) > 1:
            str_recipients = ', '.join(recipients)
        else:
            str_recipients = recipients[0]
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = str_recipients
        msg.attach(MIMEText(body))
        if attachments is not None:
            for att in attachments:
                part = MIMEBase('application', "octet-stream")
                part.set_payload(open(att, "rb").read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(att))
                msg.attach(part)

        s = smtplib.SMTP('mailsmtp.newyorklife.com')
        s.send_message(msg)
        s.quit()

    @staticmethod
    def check_trading_day(passed_date):
        """
        Check if passed date is a trading day
        Requires custom installation of 'bdateutil' & 'dateutil'
        Arguments:
            :param passed_date: DateTime object
        """
        from bdateutil.nyse_rules import NYSE_holidays
        if passed_date.weekday() > 4:
            return False
        else:
            if passed_date in NYSE_holidays(a=datetime.datetime(2000, 1, 1)):
                return False
            else:
                return True
