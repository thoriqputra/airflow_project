from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
from minio import Minio
import pandas as pd, glob, os, sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from pathlib import Path
import random
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import XCom
from airflow.utils.db import provide_session

dag = DAG(
    dag_id="project_final",
    start_date=datetime.datetime.now(),
    schedule_interval= None,
    description="Project Final Health",
)

path    = "/home/airflow/hospital"
dateTime = datetime.datetime.now()    
date    = dateTime.strftime("%Y-%m-%d")

# Create Minio client
minio_client = Minio(
    "docker-minio:9000",
    access_key="ddbDiUgF59XqWXSq",
    secret_key="NeFYx7OH8mbsMabZcC4VQ8xUrGP1kT7y",
    secure=False
)

bucket_name = "chum-bucket"

conn = PostgresHook(postgres_conn_id='postgre_airflow_project_final').get_conn()
cursor = conn.cursor()

# The mail addresses and password
sender_address = 'thoriq.putra96@gmail.com'
appPassword = 'pshnfggowlbpnjat'
arrayReceiver = ['thoriq.putra96@gmail.com', 'ahmadfadilfatanm@gmail.com', '3kobudisantoso@gmail.com']
receiver_address    = ", ".join(arrayReceiver)
# receiver_address = 'thoriq.putra96@gmail.com'

def format_currency(number):
    currency = str(number)

    if len(currency) <= 3:
        return 'Rp '+ currency
    else:
        start = currency[-3:]
        end = currency[:-3]

    return format_currency(end) + '.' + start

def create_file_to_local():
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)

    randomStock = random.randint(10,99)
    randomStock2 = random.randint(10,99)
    randomStock3 = random.randint(10,99)
    randomStock4 = random.randint(10,99)
    randomStock5 = random.randint(10,99)
    randomStock6 = random.randint(10,99)

    data = {
            "code": ["A01", "A02", "A03", "B01", "B02", "B03"],
            "name": ["Paracetamol", "Amlodipine", "Decolgen", "Sangobion", "Atorvastatin", "Insto"],
            "stock": [randomStock, randomStock2, randomStock3, randomStock4, randomStock5, randomStock6],
            "price": ["1500", "25000", "2500", "5000", "50000", "10000"],
    }

    df = pd.DataFrame(data)
    fileName    = str(path+'/medicine_'+dateTime.strftime("%Y%m%d%H")+'.csv')

    check   = check_file_exist(fileName)

    if check == 0:
        df.to_csv(fileName, index=False)

        print("Success create file...")
    else:
        print("File sudah ada di local...")

def check_file_exist(fileName):
    print("Sedang melakukan pengecekan file di local...")

    allFiles = glob.glob(str(path+'/*.csv'), recursive=False)
    allFiles.sort(key=os.path.getmtime)

    getFile = allFiles.pop()

    if getFile == fileName:
        return 1
    else:
        return 0

def check_file_local():
    print("Sedang melakukan pengecekan file di local...")

    allFiles = glob.glob(str(path+'/*.csv'), recursive=False)
    allFiles.sort(key=os.path.getmtime)

    getFile = allFiles.pop()

    splitFile = getFile.split("/")
    file = splitFile[3]+"/"+splitFile[4]

    return file

def upload_to_minio():
    responseFromLocal = check_file_local()

    objects = minio_client.list_objects(bucket_name, recursive=True)

    countFile = 0
    arrFile = []
    for obj in objects:
        if obj.object_name == responseFromLocal:
            arrFile.append(obj.object_name)

    countFile = len(arrFile)

    if countFile < 1:
        # Upload file to Minio
        minio_client.fput_object("chum-bucket", responseFromLocal, '/home/airflow/'+responseFromLocal)
        
        print("Berhasil mengupload file. . .")
    else:
        print("Sudah ada file pada server minio...")
    
# def preprocessing_from_minio():
#     responseFromLocal = check_file_local()

#     objects = minio_client.list_objects(bucket_name, recursive=True)
    
#     for obj in objects:
#         if obj.object_name == responseFromLocal:
#             # Read file from Minio
#             response = minio_client.get_object(obj.bucket_name, obj.object_name)

#             df = pd.read_csv(response)

#             for index, row in df.iterrows():
#                 cursor.execute('SELECT * FROM stock WHERE code = %s', (row["code"],))
#                 countRows   = cursor.rowcount

#                 if countRows < 1:
#                     params = (row["code"], row["name"], row["stock"], row["price"])

#                     cursor.execute("INSERT INTO stock (code, name, stock, price) VALUES (%s, %s, %s, %s)", params)
#                     conn.commit()
#                     print("Data berhasil di insert.")
#                 else:
#                     cursor.execute('SELECT * FROM stock WHERE code = %s AND created_at BETWEEN %s AND %s ', (row["code"], date+" 00:00:00", date+" 23:59:59",))
#                     countRows   = cursor.rowcount
                    
#                     if countRows < 1:
#                         params = (row["code"], row["name"], row["stock"], row["price"], row["code"])

#                         cursor.execute("UPDATE stock set code = %s AND name = %s AND stock = %s AND price = %s WHERE code = %s ", params)
#                         conn.commit()
#                         print("Data berhasil di update.")
#                     else:
#                         print("Code "+row["code"]+" sudah ada pada tanggal "+date)

def preprocessing_from_minio(**kwargs):
    responseFromLocal = check_file_local()

    objects = minio_client.list_objects(bucket_name, recursive=True)
    
    for obj in objects:
        if obj.object_name == responseFromLocal:
            # Read file from Minio
            response = minio_client.get_object(obj.bucket_name, obj.object_name)

            df = pd.read_csv(response)
            print(df)
            params  = []
            arrayCode   = []
            for index, row in df.iterrows():
                params.append([row["code"], row["name"], str(row["stock"]), str(row["price"])])
                arrayCode.append("'"+str(row["code"])+"'")
            
            joinCode    = ', '.join(arrayCode)

            cursor.execute('SELECT * FROM stock WHERE code IN ('+joinCode+')')
            countRows   = cursor.rowcount
            result = cursor.fetchall()

            if countRows < 1:
                kwargs["ti"].xcom_push(key="params", value = params) # push it as an airflow xcom
                
                return "insert_data"
            else:
                sql = 'SELECT * FROM stock WHERE code IN ('+joinCode+') AND created_at BETWEEN %s AND %s '

                cursor.execute(sql, (date+" 00:00:00", date+" 23:59:59",))
                countRows   = cursor.rowcount

                if countRows < 1:
                    kwargs["ti"].xcom_push(key="params", value = params) # push it as an airflow xcom
                    return "update_data"
                else:
                    send_email("failed")


def insert(ti):
    arrayParams = ti.xcom_pull(key='params')
    
    for x in range(len(arrayParams)):
        params  = (arrayParams[x][0], arrayParams[x][1], arrayParams[x][2], arrayParams[x][3])
        cursor.execute("INSERT INTO stock (code, name, stock, price) VALUES (%s, %s, %s, %s)", params)
        conn.commit()

        print("Data berhasil di insert.")

    send_email("success")

def update(ti):
    arrayParams = ti.xcom_pull(key='params')
    
    for x in range(len(arrayParams)):
        params  = (arrayParams[x][0], arrayParams[x][1], arrayParams[x][2], arrayParams[x][3], dateTime, arrayParams[x][0])
        cursor.execute("UPDATE stock SET code = %s, name = %s, stock = %s, price = %s, updated_at = %s WHERE code = %s ", params)
        conn.commit()

        print("Data berhasil di update.")

    send_email("success")

def parsing_data():
    print("Data berhasil di baca.")

    cursor.execute('SELECT * FROM stock WHERE updated_at BETWEEN %s AND %s ', (date+" 00:00:00", date+" 23:59:59",))
    result = cursor.fetchall()

    code    = []
    name    = []
    stock   = []
    price   = []

    for res in result:
        code.append(res[0])
        name.append(res[1])
        stock.append(res[2])
        price.append(res[3])

    return code, name, stock, price

def send_email(status):
    if status == "success":
        response = parsing_data()
        
        code    = response[0]
        name    = response[1]
        stock   = response[2]
        price   = response[3]

        arr_col    = []

        for x in range(len(code)):
            arr_col.append("<tr>"
                                "<td>"+code[x]+"</td>"
                                "<td>"+name[x]+"</td>"
                                "<td>"+str(stock[x])+"</td>"
                                "<td>"+str(format_currency(price[x]))+"</td>"
                            "</tr>")

        col    = '\n'.join(arr_col)

        # Create the body of the message (a plain-text and an HTML version).
        html = """\
        <html>
            <head></head>
            <body>
                <p>Berikut merupakan persediaan obat-obatan : </p><br>
                <table border='1'>
                    <thead>
                        <tr>
                            <th>Code</th>
                            <th>Name</th>
                            <th>Stock</th>
                            <th>Price</th>
                        </tr>
                    </thead>
                    <tbody>
                        """+col+"""
                    </tbody>
                </table>
            </body>
        </html>
        """
    else:
        # Create the body of the message (a plain-text and an HTML version).
        html = """\
        <html>
            <head></head>
            <body>
                <p>Ketersediaan obat telah di tambahkan pada hari ini.</p><br>
            </body>
        </html>
        """

    #Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = 'Stock Medicine' # The subject line

    # The body and the attachments for the mail
    # Record the MIME types of both parts - text/plain and text/html.
    html_content = MIMEText(html, 'html')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    message.attach(html_content)

    #Create SMTP session for sending the mail
    session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
    session.starttls() #enable security
    session.login(sender_address, appPassword) #login with mail_id and password
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()
    
    print('Email telah terkirim.')

# Task to create file to airflow
createFile = PythonOperator(
    task_id='create_file',
    python_callable=create_file_to_local,
    dag=dag
)

# Task to upload file to Minio
uploadFile = PythonOperator(
    task_id='upload_file',
    python_callable=upload_to_minio,
    dag=dag
)

createTable = PostgresOperator(
    task_id = 'create_table',
    postgres_conn_id = 'postgre_airflow_project_final',
    sql = '''
        create table if not exists stock (
            code VARCHAR NOT NULL PRIMARY KEY,
            name VARCHAR NOT NULL,
            stock INTEGER NOT NULL not null default 0,
            price INTEGER NOT NULL not null default 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    ''',
    dag = dag
)

# processReadFile = PythonOperator(
#     task_id='read_file',
#     python_callable=preprocessing_from_minio,
#     dag = dag
# )

preProcessing = BranchPythonOperator(
    task_id='preprocessing_from_minio',
    python_callable=preprocessing_from_minio,
    provide_context=True,
    dag=dag,
)

insertData = PythonOperator(
    task_id='insert_data',
    python_callable=insert
)

updateData = PythonOperator(
    task_id='update_data',
    python_callable=update
)

complete = DummyOperator(task_id="complete")

# Set task dependencies
createFile >> uploadFile >> createTable >> preProcessing >> [insertData, updateData] >> complete