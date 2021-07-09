import cx_Oracle

username = 'system'
password = 'sys'
sid = 'orcl'
host = '127.0.0.1'
port = '1522'


def prepare_db():
    con = cx_Oracle.connect(f'{username}/{password}@{host}:{port}/{sid}')
    curs = con.cursor()
    try:
        curs.execute('create table test_application_bind_user(id number, username varchar2(50))')
        curs.execute("insert into test_application_bind_user values (1, 'JOHN')")
        curs.execute("insert into test_application_bind_user values (2, 'SAM')")
        curs.execute("insert into test_application_bind_user values (3, 'WILL')")
        curs.execute('''create table MOCK_DATA (
                id INT,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(50),
                gender VARCHAR(50),
                ip_address VARCHAR(20)
            )''')
        curs.execute("insert into MOCK_DATA (id, first_name, last_name, email, gender, ip_address) values (1, "
                     "'Gerti', 'Lebang', 'glebang0@elegantthemes.com', 'Agender', '159.220.133.10')")
        curs.execute("insert into MOCK_DATA (id, first_name, last_name, email, gender, ip_address) values (2, "
                     "'Anissa', 'Pead', 'apead1@archive.org', 'Agender', '89.13.48.126')")
        curs.execute("insert into MOCK_DATA (id, first_name, last_name, email, gender, ip_address) values (3, 'Ami', "
                     "'Caitlin', 'acaitlin2@cornell.edu', 'Polygender', '164.251.146.6')")
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print(error)
    con.commit()
    curs.close()
    con.close()


def query_application_user():
    con = cx_Oracle.connect(f'{username}/{password}@{host}:{port}/{sid}')
    curs = con.cursor()
    application_user = 'WILL'
    try:
        print('-------Executing queries for Query User capture-------')
        curs.execute(f'SELECT 1 /* appuser = ~{application_user}~ */ FROM DUAL')
        print(f'-------Executing query for {application_user} appuser-------')
        curs.execute('SELECT * FROM MOCK_DATA')
        for row in curs:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print(error)
    curs.close()
    con.close()


def binding_application_user():
    con = cx_Oracle.connect(f'{username}/{password}@{host}:{port}/{sid}')
    curs = con.cursor()
    try:
        print('')
        print('-------Executing queries for Binding User capture-------')
        curs.execute('SELECT username FROM test_application_bind_user where username = :id', id='JOHN')
        print(f'-------Executing query for appuser-------')
        curs.execute('SELECT * FROM MOCK_DATA')
        for row in curs:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print(error)
    curs.close()
    con.close()


def sap_ecc_application_user():
    con = cx_Oracle.connect(f'{username}/{password}@{host}:{port}/{sid}')
    curs = con.cursor()
    application_user = 'TEST'
    try:
        print('')
        print('-------Executing queries for SAP ECC capture-------')
        print('-------Changing appuser to ' + application_user + ' -------')
        con.client_identifier = application_user
        curs.execute("SELECT * FROM MOCK_DATA")
        for row in curs:
            print(row)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print(error)
    curs.close()
    con.close()


def clean_db():
    con = cx_Oracle.connect(f'{username}/{password}@{host}:{port}/{sid}')
    curs = con.cursor()
    try:
        curs.execute('drop table test_application_bind_user')
        curs.execute('drop table MOCK_DATA')
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print(error)
    curs.close()
    con.close()


prepare_db()
query_application_user()
binding_application_user()
sap_ecc_application_user()
clean_db()