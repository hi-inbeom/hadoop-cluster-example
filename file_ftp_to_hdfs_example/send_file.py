import psycopg2
from hdfs import InsecureClient
import string
import random
import paramiko
from urllib.request import *

# v_form is Insert values
v_form = "('description', '000.000.000.000','user','password','save_path','{file}', 'T'),"
i_form = "insert into info_ftp (description, ftp_ip, ftp_user, ftp_password, directory, file, state) values {v_form}"

class send_file():
    def con_postgresql(self):
        # connection to postgresql that has server information
        conn_hub = psycopg2.connect(host="000.000.000.000",dbname="postgres",user="postgres",password="postgres",port=5432)
        cur_hub = conn_hub.cursor()
        cur_hub.execute("SELECT * FROM info_server")
        rows = cur_hub.fetchall()
        conn_hub.close()

        # hard coding variable about server information row count
        i = 4
        try :
            host,port,dbname,user,password,table = rows[i][1],rows[i][2],rows[i][3],rows[i][4],rows[i][5],rows[i][6]
        except :
            print(rows)
        
        # connection to postgresql that has data information and data loading server information
        conn_link = psycopg2.connect(host=host,dbname=dbname,user=user,password=password,port=port)
        cur_link = conn_link.cursor()
        cur_link.execute(f"SELECT * FROM {table} WHERE state = 'T'")
        
        # get data list
        rows = cur_link.fetchall()
        return conn_link,cur_link,rows

    def make_test_data(self):
        # connnect data information table
        conn_link,cur_link,rows = self.con_postgresql()
        # data types
        ls_type = ['.hwp','.pdf','.xlsx','.jpg','.doc','.ppt','.zip']
        ls_files = []

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect('000.000.000.000',22,'user','password')

        # algorism about make 100 datas
        for i in range(0,100):
            # make temprary file name
            st1 = random.choice(string.ascii_letters)+random.choice(string.ascii_letters)+random.choice(string.ascii_letters)+str(int(random.random()*1000000))+random.choice(ls_type)
            try:
                sftp = ssh.open_sftp()
            except:
                print('host server is closed')
            try:
                with sftp.file(f'/home/gridone/{st1}', 'w') as file:
                    file.write('_')
                ls_files.append(st1)
            except:
                for j in range(0, 5):
                    try :
                        st1 = random.choice(string.ascii_letters) + random.choice(string.ascii_letters) + random.choice(string.ascii_letters) + str(int(random.random() * 1000000)) + random.choice(ls_type)
                        with sftp.file(f'/home/gridone/{st1}', 'w') as file:
                            file.write('_')
                        ls_files.append(st1)
                    except:
                        continue

        v_str = ""
        for file in ls_files:
            v_str += v_form.format(file=file)
        v_str = v_str[:-1]

        cur_link.execute(i_form.format(v_form=v_str))
        conn_link.commit()

    def send_file_ftp_to_hdfs(self,conn_link,cur_link,rows):
        in_str = ''
        for row in rows:
            idx = row[0]
            address_ip = row[2]
            user = row[3]
            password = row[4]
            path = row[5]
            file = row[6]
            # print(f'address_ip={address_ip},\nuser={user},\npassword={password},\npath={path},\nfile={file},\n')
            try:
                client = InsecureClient(f'http://{address_ip}:9870',user=user)
            except:
                print('hdfs server is close')
            with urlopen(f'ftp://{user}:{password}@{address_ip}/{file}') as response:
                content = response.read()
                # write data on hdfs server
                # connected to hdfs when declaration client, so you aren`t need about hadoop http information
                with client.write(f'/test/{file}') as w:
                    w.write(content)
            in_str += str(idx)+","

        cur_link.execute(f"UPDATE info_ftp SET state = 'F' WHERE idx in ({in_str[:-1]})")
        conn_link.commit()
