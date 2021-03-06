#!/bin/bash 
# sed -i  '1047,1050 {s/^/#/}' /usr/local/lib/python3.6/site-packages/mysql/connector/connection.py
cat > /usr/bin/data_writer.py << EOF 
import socket
import sys
import mysql.connector
from mysql.connector import pooling
import logging
import shutil
import os
CONFIG_FILE='/usr/local/nagios/etc/data_writer.conf'
logging.basicConfig(level=logging.INFO, filename="/var/log/nagios/nagios_data_writer.log", filemode='a',
                    format=f'{socket.gethostname()} - %(asctime)s - %(levelname)s - %(message)s')
# signal handler
from signal import signal, SIGINT, SIGTERM, SIGHUP
def handler(signum, frame):
    if signum == 1:
        logging.info('Signal handler called with signal {} reading configuration for new database changes'.format(signum))
        read_configuration()
        mysql_connection_creator()
    elif signum in (2, 9, 15):
        logging.info("Signal handler called with signal {} shuting down.".format(signum))
        raise SystemExit()
    else:
        logging.error('Signal handler called with signal {}. unknown signal.'.format(signum))
# Reading configuration file and fill variables"
def Configuration_variable_finder(self):
    variable = self
    with open(CONFIG_FILE) as input_file:
        for line in input_file:
            if variable in line:
                value = line.split('=')[1].strip().lstrip().rstrip()
                return value
def read_configuration():
    global PID_FILE , POOL_SIZE, DB_HOST, DB_DATABASE, DB_USER, DB_PASS, SERVICE_MONITOR_TABLES, HOST_MONITOR_TABLES, JOIN_ECARE_TABLES, server_address, USER,GROUP
    POOL_SIZE = int(Configuration_variable_finder("pool_size"))
    DB_HOST = Configuration_variable_finder("db_host")
    DB_DATABASE = Configuration_variable_finder("instance_name")
    DB_USER = Configuration_variable_finder("db_user")
    DB_PASS = Configuration_variable_finder("db_pass")
    SERVICE_MONITOR_TABLES = ( Configuration_variable_finder("service_monitor_tables"))
    HOST_MONITOR_TABLES = Configuration_variable_finder("host_monitor_tables")
    JOIN_ECARE_TABLES = Configuration_variable_finder("join_ecare_tables")
    PID_FILE = Configuration_variable_finder("pid_file")
    server_address = Configuration_variable_finder("socket_file")
    USER = Configuration_variable_finder("service_user")
    GROUP = Configuration_variable_finder("service_group")
    # return PID_FILE , POOL_SIZE, DB_HOST, DB_DATABASE, DB_USER, DB_PASS, SERVICE_MONITOR_TABLES, HOST_MONITOR_TABLES, JOIN_ECARE_TABLES, server_address, USER,GROUP
###########################################
def check_pid(pid):
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        logging.info("PID: " + str(pid) + "is not running. service is stopped")
        create_pid_socket()
    else:
        logging.error("Another instance is already running exiting....")
        print("Another instance is already running exiting....")
        sys.exit()
def create_pid_socket():
    logging.info("starting the service")
    try:
        if os.path.exists(server_address):
            logging.info("Found an old socket file. Removing ....")
            os.unlink(server_address)
    except OSError as error_message:
        logging.error("Something went wrong while trying to remove old socket: " + str(error_message))
        open(PID_FILE, "w+")
        shutil.chown(PID_FILE, user=USER, group=GROUP)
    pid = os.getpid()
    open(PID_FILE, "w+").write(str(pid))
    # os.truncate(PID_FILE, 0)
    logging.info("Service is started with pid: " + str(pid))
def client_thread(connection):
    statement = ''
    while True:
        buff_data = connection.recv(1024)
        if len(buff_data) == 0:
            break
        elif len(buff_data) < 12:
            logging.error("Data lentgh is not enough." + str(buff_data))
        else:
            buff_data = buff_data.rstrip()
            data_split = buff_data.decode().rstrip("\\eom").rstrip('\n')
            data_split = data_split.split(',*')
            try:
                if data_split[1]:
                    logging.debug("Recieve \"" + data_split[1] + "\" going to put it to DB")
                    if data_split[0] in SERVICE_MONITOR_TABLES:
                        try:
                            node_data = {"TABLE_NAME": data_split[0], "HOST_NAME": data_split[1],
                                         "ADDRESS": data_split[2], "CURRENT_STAT": data_split[3],
                                         "OUTPUT": data_split[4], "LONG_OUTPUT": data_split[5],
                                         "LAST_UPDATE": data_split[6], "LAST_STAT_CHANGE": data_split[7]}
                            statement = str(
                                """insert into {0[TABLE_NAME]} (host_name,address,current_stat,output,long_output,last_update,last_stat_change) values ('{0[HOST_NAME]}','{0[ADDRESS]}',{0[CURRENT_STAT]},'{0[OUTPUT]}','{0[LONG_OUTPUT]}','{0[LAST_UPDATE]}','{0[LAST_STAT_CHANGE]}') on duplicate key update address= values (address), output = values(output), long_output = values(long_output), last_update = values (last_update)""".format(node_data))
                        except IndexError:
                            logging.error("Not enough index to insert Service result to related tables: " + str(
                                buff_data))
                            continue
                    elif data_split[0] in HOST_MONITOR_TABLES:
                        try:
                            node_data = {"TABLE_NAME": data_split[0], "HOST_NAME": data_split[1],
                                         "ADDRESS": data_split[2],
                                         "CURRENT_STAT": data_split[3], "OUTPUT": data_split[4],
                                         "LAST_UPDATE": data_split[5],
                                         "LAST_STAT_CHANGE": data_split[6]}
                            statement = str(
                                """insert into {0[TABLE_NAME ]} (host_name,address,current_stat,output,last_update,last_stat_change) values ('{0[HOST_NAME]}','{0[ADDRESS]}',{0[CURRENT_STAT]},'{0[OUTPUT]}','{0[LAST_UPDATE]}','{0[LAST_STAT_CHANGE]}') on duplicate key update address= values (address), output = values(output), last_update = values (last_update)""".format(node_data))
                        except IndexError:
                            logging.error(
                                "Not enough index to insert host status into related tables:" + str(
                                    buff_data))
                            continue
                    elif data_split[0] in JOIN_ECARE_TABLES:
                        try:
                            node_data = {"TABLE_NAME": data_split[0], "HOST_NAME": data_split[1],
                                         "ADDRESS": data_split[2],
                                         "VAR_NAME": data_split[3], "VAR_VALUE": data_split[4]}
                            statement = str(
                                """insert into {0[TABLE_NAME ]} (host_name,address,var_name,var_value) values ('{0[HOST_NAME]}','{0[ADDRESS]}','{0[VAR_NAME]}','{0[VAR_VALUE]}') on duplicate key update  var_value = value (var_value)""".format(node_data))
                        except IndexError:
                            logging.error("Not enough index to insert var_values into related tables:  " + str(
                                buff_data))
                            continue
                    else:
                        logging.error("Data does not match with any conditions "
                                      "can't define which table to use:" + str(buff_data))
                if statement:
                    connection_object = connection_pool.get_connection()
                    if connection_object.is_connected():
                        cursor = connection_object.cursor()
                        try:
                            logging.debug("Executing statement: " + statement)
                            cursor.execute(statement)
                            connection_object.commit()
                            if cursor.rowcount == 2:
                                result_m = 'UPDATE'
                            elif cursor.rowcount == 1:
                                result_m = 'INSERT'
                            else:
                                result_m = 'row count is 0!!!!'
                        except mysql.connector.errors.ProgrammingError as mysqlerror:
                            logging.error(
                                "Got error while executing th statement:" + str(
                                    mysqlerror) + "for statement" + statement)
                        finally:
                            logging.debug("statement execution for node " + node_data["HOST_NAME"] + " is: " + result_m)
                            cursor.close()
                            connection_object.close()
            except IndexError:
                logging.error("Not enough index to insert Service result to related tables: " + str(
                    buff_data))
    connection.close()
def mysql_connection_creator():
    global connection_pool
    try:
        connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="Nagios_data_writer",
            pool_size=POOL_SIZE,
            pool_reset_session=True,
            host=DB_HOST,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASS
        )
    except mysql.connector.Error as mysqlerror:
        logging.error("Couldn't connect to the databas du the following error:" + str(mysqlerror))
        sys.exit()
    else:
        logging.info("Connection pool is created successful.")
logging.debug("Checking PID File to check if another instance")
read_configuration()
if os.path.exists(PID_FILE) and open(PID_FILE).read():
    pid = open(PID_FILE).read()
    logging.debug("Found PID: " + str(pid) + "checking if its still running.")
    check_pid(pid)
else:
    logging.debug("Could not find any process. Starting it")
    create_pid_socket()
logging.info("Creating Connection pool to the database")
mysql_connection_creator()
nag_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
nag_socket.settimeout(None)
nag_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    logging.info("Binding Service to socket "+ server_address)
    nag_socket.bind(server_address)
    shutil.chown(server_address, user=USER, group=GROUP)
except socket.error as message:
    logging.critical('Bind failed. error code: ' + str(message[0]) + ' Message' + str(message[1]))
    sys.exit()
else:
    logging.info("Successfully bind to the socket")
nag_socket.listen()
signal(SIGINT, handler)
signal(SIGTERM, handler)
signal(SIGHUP, handler)
while True:
    try:
        connection = nag_socket.accept()[0]
    except socket.error as socketerror:
        logging.error("error while creating socket: " + str(socketerror))
        sys.exit()
    except KeyboardInterrupt:
        logging.info("Caught kill signal. shutting down the service.")
        sys.exit()
    else:
        client_thread(connection)
EOF
chmod +x /usr/bin/data_writer.py
mkdir -p /usr/local/nagios/etc/
cat > /usr/local/nagios/etc/data_writer.conf << EOF
#################################
# Nagios data writer config file #
#################################
# Below  parameters can take effect without restarting the service. bu sending HUP signal to the process.
# to force program to read configuration use "systemctl reload data_writer"
# Database name/address to create connection if the service works with local database use localhost
db_host = b2b-monitoring-db.mobinnet.net
# Database cerdential 
db_user = ping
db_pass = ping@123
# instance name. this version only support one instance.
instance_name = nagiosql
# Define number of connections to the Database 
pool_size = 8
# Table names wich nagios plugins work with. 
service_monitor_tables = test_service_monitor,test_service_monitor_p2p
host_monitor_tables = test_host_monitor,test_host_monitor_p2p
join_ecare_tables = test_join_ecare_data,test_join_ecare_data_p2p
# Below parameters need a service restart 
# PID File 
pid_file = /usr/local/nagios/nagios_data_writer.pid
# unix socket location
socket_file = /usr/local/nagios/nagios_data_writer.sock
#user and group wich plugins works with. consider that this parameters define the ownership of the socket file. so wrong use and group can cause error.
service_user = nagios
service_group = nagios
EOF
chown nagios:nagios /usr/local/nagios/etc/data_writer.conf 
cat > /etc/systemd/system/data_writer.service << EOF
[Unit]
Description=Nagios data writer
After=mod-gearman2-worker.target
[Service]
Type=mkdir
PIDFile=/usr/local/nagios/nagios_data_writer.pid
ExecStart=/usr/bin/python3.6 /usr/bin/data_writer.py
ExecReload=/usr/bin/kill -HUP $MAINPID
ExecStop=/usr/bin/kill -INT $MAINPID
Restart=always
KillMode=process
TimeoutStopSec=5
KillSignal=SIGINT
User=nagios
Group=nagios
[Install]
WantedBy=multi-user.target
EOF
mkdir -p /var/log/nagios/
chown -R nagios:nagios /usr/local/nagios
touch  /var/log/nagios/nagios_data_writer.log
chown nagios:nagios /var/log/nagios/nagios_data_writer.log
chmod 664 /etc/systemd/system/data_writer.service 
systemctl daemon-reload
systemctl enable data_writer
systemctl start data_writer
