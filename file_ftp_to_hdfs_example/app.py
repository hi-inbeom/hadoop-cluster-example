from send_file import send_file

cls = send_file

test = True
test = False

if test:
    cls.make_test_data()

conn_link,cur_link,rows = cls.con_postgresql()
cls.send_file_ftp_to_hdfs(conn_link,cur_link,rows)
