import configparser
import logging
from copy import copy
from logging import Formatter
import MySQLdb as mdb
from email.mime.base import MIMEBase
from email import encoders
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


config = configparser.ConfigParser()
config.read('./config.ini')

#master config
master_config = config['master_host']
master_ip = master_config['ip']
master_user = master_config['user']
master_passwd = master_config['password']
master_database = master_config['database']

#slave config
slave_config = config['slave_host']
slave_ip = slave_config['ip']
slave_user = slave_config['user']
slave_passwd = slave_config['password']
slave_database = slave_config['database']


#notify config
notify_config = config['notify']
SUBJECT = notify_config['subject']
EMAIL_FROM = notify_config['email_from'] 
EMAIL_TO = notify_config['email_to']
HOST = notify_config['host']
PORT = notify_config['port']




MAPPING = {
    'DEBUG'   : 37, # white
    'INFO'    : 36, # cyan
    'WARNING' : 33, # yellow
    'ERROR'   : 31, # red
    'CRITICAL': 41, # white on red bg
}

PREFIX = '\033['
SUFFIX = '\033[0m'

def notify_byemail(subject,msg):
    try:
        # Send the message via our own SMTP server, but don't include the
        # envelope header.
        message = MIMEMultipart()
        message['Subject'] = subject
        message['From'] = EMAIL_FROM
        message['To'] = ', '.join(EMAIL_TO)
        message.attach(MIMEText(msg, 'plain'))
        filename = 'report_'+str(time.strftime("%d-%m-%Y"))+'.log'
        attachment = open(os.path.join(parent_dir,filename), "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
        message.attach(part)
        s = smtplib.SMTP(host=HOST,port=PORT)
        s.sendmail(EMAIL_FROM, EMAIL_TO, message.as_string())
        s.quit()
    except Exception as error:
        raise(Exception(error))

class ColoredFormatter(Formatter):

    def __init__(self, patern):
        Formatter.__init__(self, patern)

    def format(self, record):
        colored_record = copy(record)
        levelname = colored_record.levelname
        seq = MAPPING.get(levelname, 31) # default white
        colored_levelname = ('{0}{1}m{2}{3}') \
            .format(PREFIX, seq, levelname, SUFFIX)
        colored_record.levelname = colored_levelname
        return Formatter.format(self, colored_record)

log = logging.getLogger("main")

# Add console handler using our custom ColoredFormatter
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
cf = ColoredFormatter("[%(name)s][%(levelname)s]  %(message)s (%(filename)s:%(lineno)d)")
ch.setFormatter(cf)
log.addHandler(ch)

log.setLevel(logging.DEBUG)

log.debug("Master: %s %s %s" % (master_ip,master_user,master_passwd))
log.debug("Slave: %s %s %s" % (slave_ip,slave_user,slave_passwd))
log.debug("Notify: %s" % (emails))

def connect_mysql(ip,user,password,database):
    try:
        con = mdb.connect(ip, user, password, 'information_schema')
        return con    
    except mdb.Error as e:
        raise Exception('Error Connect_mysql Server\nNão foi possível conectar ao servidor: ' + ip + '.\nMensagem de erro:\n'+str(e))

def mysql_query(conn, query):
    try:
	    cur = conn.cursor(MySQLdb.cursors.DictCursor)
	    cur.execute(query)
	    return cur
    except Exception as e:
        raise Exception("Execute Mysql Query\n"+str(e))

def verify_slave_stats(conn):
    try:
        result = mysql_query(conn, 'SHOW SLAVE STATUS')
        slave_row = result.fetchone()
        if slave_row is None:
            raise Exception('A verificação de status do slave retornou vazia.\n Verifique o localmento o banco de dados')
            
        status = {
            'position_read_master': slave_row['Read_Master_Log_Pos'],
            'position_exec': slave_row['Exec_Master_Log_Pos'],    
            'relay_log_space': slave_row['Relay_Log_Space'],
            'slave_lag':       slave_row['Seconds_Behind_Master'] if slave_row['Seconds_Behind_Master'] != None else 0,
        }

        if slave_row['Slave_IO_Running'] != 'Yes':
            raise Exception('O processo Slave_IO_Running está com algum problema.\nSlave_IO_Running = ' +slave_row['Slave_IO_Running'])
        if slave_row['Slave_SQL_Running'] != 'Yes':
            raise Exception('O processo Slave_SQL_Running está com algum problema.\nSlave_SQL_Running = ' +slave_row['Slave_SQL_Running'])
        if slave_row['Last_IO_Errno'] != '0' or slave_row['Last_IO_Error'] != "":
            raise Exception('O processo Last_IO_Errno está com algum problema.\nLast_IO_Errno = ' +slave_row['Last_IO_Errno']+"\nLast_IO_Error="+slave_row['Last_IO_Errno'])
        if slave_row['Last_SQL_Errno'] != '0' or slave_row['Last_SQL_Error'] != "":
            raise Exception('O processo Last_IO_Errno está com algum problema.\nLast_SQL_Errno = ' +slave_row['Last_SQL_Errno']+"\nLast_SQL_Error="+slave_row['Last_SQL_Error'])
        if slave_row['Slave_SQL_Running_State'] != "Slave has read all relay log; waiting for more updates":
            raise Exception('O processo Slave_SQL_Running_State está com algum problema.\nSlave_SQL_Running_State = ' +slave_row['Slave_SQL_Running_State'])
        
        return status
    except Exception as error:
        raise Exception('Function verify_slave_stats\nMensagem de erro:\n'+str(error)))

def verify_status_master(conn):
    try:
        result = mysql_query(conn, 'SHOW MASTER STATUS')
        master_row = result.fetchone()
        if master_row is None:
            raise Exception('A verificação de status do Master retornou vazia.\n Verifique o localmento o banco de dados')
      
        status ={
            'file': master_row['File']
            'position': master_row['Position']
            
        }
        return status
    except mdb.Error as e:
        raise Exception('Function verify_master_stats\nMensagem de erro:\n'+str(e))
        
        
def main():

    #connect slave
    try:
        con_slave = connect_mysql(slave_ip,slave_user,slave_passwd,slave_database)
        status_slave = verify_slave_stats(con_slave)
        #connect master
        con_master = connect_mysql(master_ip,master_user,master_passwd,master_database)
        status_master = verify_status_master(con_master)

        if(status_slave['position_read_master'] != status_master['position']):
            raise Exception('A replicação não está funcionando corretamente.\n'+'Position Slave: '+status_slave['position_read_master']+ " Position Master: "+status_master['position'])            
    
    except Exception as error:
        notify_byemail('Replicação com Problema',str(error))            

