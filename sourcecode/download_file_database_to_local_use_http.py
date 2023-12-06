import psycopg2
import requests
from hdfs import InsecureClient
import string
import random
import paramiko
from urllib.request import *

v_form = "('23년 결제 내역', '192.168.0.101','gridone','gridone','/home/gridone/','{file}', 'T'),"
i_form = "insert into info_ftp (description, ftp_ip, ftp_user, ftp_password, directory, file, state) values {v_form}"

test = True
# test = False

def make_test_data():
    string.ascii_letters
    ls_type = ['.hwp','.pdf','.xlsx','.jpg','.doc','.ppt','.zip']
    ls_files = []

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('192.168.0.101',22,'gridone','gridone')

    for i in range(0,100):
        st1 = random.choice(string.ascii_letters)+random.choice(string.ascii_letters)+random.choice(string.ascii_letters)+str(int(random.random()*1000000))+random.choice(ls_type)
        ls_files.append(st1)
        with ssh.open_sftp() as sftp:
            with sftp.file(f'/home/gridone/{st1}', 'w') as file:
                file.write('_')

    v_str = ""
    for file in ls_files:
        v_str += v_form.format(file=file)
    v_str = v_str[:-1]
    return v_str

def con_postgresql():
    conn_hub = psycopg2.connect(host="192.168.0.101",
                            dbname="postgres",
                            user="postgres",
                            password="postgres",
                            port=5432)
    cur_hub = conn_hub.cursor()
    cur_hub.execute("SELECT * FROM info_server")
    rows = cur_hub.fetchall()
    conn_hub.close()

    i = 4

    host = rows[i][1]
    port = rows[i][2]
    dbname = rows[i][3]
    user = rows[i][4]
    password = rows[i][5]
    table = rows[i][6]

    print(f'''host={host},\nport={port},\ndbname={dbname},\nuser={user},\npassword={password},\ntable={table}''')

    conn_link = psycopg2.connect(host=host,
                                 dbname=dbname,
                                 user=user,
                                 password=password,
                                 port=port)
    cur_link = conn_link.cursor()

    if test:
        v_str = make_test_data()
        cur_link.execute(i_form.format(v_form=v_str))
        conn_link.commit()

    cur_link.execute(f"SELECT * FROM {table} WHERE state = 'T'")

    rows = cur_link.fetchall()
    return conn_link,cur_link,rows

conn_link,cur_link,rows = con_postgresql()

def get_file_use_http():
    response = requests.get(rows[0][1])
    with open(rows[0][0],'wb') as file:
        file.write(response.content)

get_file_use_http()
