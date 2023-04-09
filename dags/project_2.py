from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from minio import Minio
import pandas as pd, glob, os, airflow.utils.dates, sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

# Create Minio client
minio_client = Minio(
    "docker-minio:9000",
    access_key="ddbDiUgF59XqWXSq",
    secret_key="NeFYx7OH8mbsMabZcC4VQ8xUrGP1kT7y",
    secure=False
)

bucket_name = "chum-bucket"

conn = PostgresHook(postgres_conn_id='postgre_airflow').get_conn()
cursor = conn.cursor()

# The mail addresses and password
sender_address = 'thoriq.putra96@gmail.com'
appPassword = 'ochtztghivmcmmgj'
receiver_address = 'thoriq.putra96@gmail.com'

def upload_to_minio():
    # Upload file to Minio
    minio_client.fput_object("chum-bucket", "product_20230407.csv", "/home/airflow/product_20230407.csv")

def read_from_minio():
    objects = minio_client.list_objects(bucket_name, recursive=True)
    
    for obj in objects:
        # Read file from Minio
        response = minio_client.get_object(obj.bucket_name, obj.object_name)

        df = pd.read_csv(response)

        for index, row in df.iterrows():
            cursor.execute('SELECT * FROM project_2 where sku = %s', (row["sku"],))
            countRows   = cursor.rowcount

            if countRows < 1:
                insert_data(row["sku"], row["sold"], row["price"], row["baseprice"])
            else:
                print("SKU "+row['sku']+" sudah di insert sebelumnya.")        

def insert_data(sku, sold, price, baseprice):
    params = (sku, sold, price, baseprice)

    cursor.execute("INSERT INTO project_2 (sku, sold, price, baseprice) VALUES (%s, %s, %s, %s)", params)
    conn.commit()
    print("Data berhasil di insert.")

def parsing_data():
    print("Data berhasil di baca.")

    cursor.execute('SELECT * FROM project_2')
    result = cursor.fetchall()

    sold = []
    price = []
    baseprice = []
    
    for res in result:
        sold.append(res[1])
        price.append(res[2])
        baseprice.append(res[3])

    return sold, price, baseprice


def send_email():
    response = parsing_data()
    
    total_sold = sum(response[0])
    total_price = sum(response[1])
    total_baseprice = sum(response[2])

    profit = total_sold * (total_price - total_baseprice)

    # Create the body of the message (a plain-text and an HTML version).
    html = """\
    <html>
        <head></head>
        <body>
            <p>Berikut adalah hasil dari total price : <br>
            </p>
            <table border='1'>
                <thead>
                    <tr>
                        <th>Total Price</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>"""+str(profit)+"""</td>
                    </tr>
                </tbody>
            </table>
        </body>
    </html>
    """

    #Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = receiver_address
    message['Subject'] = 'Summary Hasil Total Price' # The subject line

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

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 7),
}

with DAG('project_2', default_args=default_args, schedule_interval='@daily') as dag:
    
    # Task to upload file to Minio
    # upload_file = PythonOperator(
    #     task_id='upload_file',
    #     python_callable=upload_to_minio
    # )

    # Task to download file from Minio
    # download_file = PythonOperator(
    #     task_id='download_file',
    #     python_callable=download_from_minio
    # )

    createTable = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgre_airflow',
        sql = '''
            create table if not exists project_2 (
                sku VARCHAR NOT NULL PRIMARY KEY,
                sold INTEGER NOT NULL not null default 0,
                price INTEGER NOT NULL not null default 0,
                baseprice INTEGER NOT NULL not null default 0
            );
        ''',
        dag = dag
    )

    process_read_file = PythonOperator(
        task_id='read_file',
        python_callable=read_from_minio
    )

    process_send_email = PythonOperator(
        task_id='send_email',
        python_callable=send_email
    )

    # Set task dependencies
    createTable >> process_read_file >> process_send_email