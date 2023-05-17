"""
Created on Mon Apr 17 19:45:04 2023

@author: gowrav
"""

from million_models import Customers, CustomersInsert, CustomersUpdate, CustomersDelete
from subprocess import PIPE, Popen
from datetime import datetime
from sqlalchemy import update, insert, delete, except_
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, select
import time

metadata = MetaData()


# @task
def keepalived_status():
    terminal = Popen(['systemctl', 'status', 'keepalived.service'],
                     stdout=PIPE,
                     stderr=PIPE)
    stdout, stderr = terminal.communicate()
    return stdout.decode().upper().split()[-2]


def transaction():
    hostname = os.getenv('hostname')
    database_name = os.getenv('database_name')
    user = os.getenv('user')
    password = os.getenv('password')
    port = os.getenv('port')
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{hostname}:{port}/{database_name}')
    sessionfactory = sessionmaker(bind=engine)
    session = sessionfactory()
    return session


# @task
def retrieve_data():
    with transaction() as session:
        rows = session.query(Customers).count()
        print(datetime.now().strftime("%H:%M:%S"), 'No Of Records in Customers Table are', rows)
        rows = session.query(CustomersInsert).count()
        print(datetime.now().strftime("%H:%M:%S"), 'No Of Records in Customers Insert Table are', rows)
        print(datetime.now().strftime("%H:%M:%S"), 'Started Fetching Delta Data')
        return (

            session.query(Customers.name, Customers.country, Customers.phone, Customers.email)
                .filter(Customers.phone.in_(except_(select([Customers.phone]), select([CustomersInsert.phone]))))
                .distinct()
                .order_by(Customers.phone)
                .all()
        )


def retrieve_data_with_original_query():
    with transaction() as session:
        rows = session.query(Customers).count()
        print(datetime.now().strftime("%H:%M:%S"), 'No Of Records in Customers Table are', rows)
        rows = session.query(CustomersInsert).count()
        print(datetime.now().strftime("%H:%M:%S"), 'No Of Records in Customers Insert Table are', rows)
        print(datetime.now().strftime("%H:%M:%S"), 'Started Fetching Delta Data')
        phone_numbers = session.query(CustomersInsert.phone).distinct()
        return (
            session.query(Customers.name, Customers.country, Customers.phone, Customers.email)
                .filter(~Customers.phone.in_(phone_numbers))
                .distinct()
                .order_by(Customers.phone)
                .all()
        )


def transformation(data):
    with transaction() as session:
        print(datetime.now().strftime("%H:%M:%S"), 'Total Records are ', len(data))
        print(datetime.now().strftime("%H:%M:%S"), 'Started')
        j = 0
        for i in data:
            j += 1
            print(datetime.now().strftime("%H:%M:%S"), f'Record {j}')
            print(datetime.now().strftime("%H:%M:%S"), 'Processing Record ', i.phone)

            # Inserting Data
            session.execute(insert(CustomersInsert).values(i))

            # Updating data
            record = {'name': i.name.lower(), 'country': i.country.upper(), 'phone': i.phone,
                      'email': i.email.upper()}
            session.execute(update(CustomersUpdate).where(CustomersUpdate.phone == record['phone']).values(record))

            # Deleting Data
            session.execute(delete(CustomersDelete).where(CustomersDelete.phone == record['phone']))

            session.commit()
            time.sleep(5)
            print(datetime.now().strftime("%H:%M:%S"), 'Completed Record ', i.phone)
    return


# @task
def transformation_original(data):
    with transaction() as session:
        print(datetime.now().strftime("%H:%M:%S"), 'Total Records are ', len(data))
        # print(datetime.now().strftime("%H:%M:%S"), 'Started')
        j = 0
        all_records = []
        for i in data:
            record = {'name': i.name, 'country': i.country, 'phone': i.phone, 'email': i.email}
            all_records.append(record)
        session.bulk_insert_mappings(CustomersInsert, all_records)
        # print(datetime.now().strftime("%H:%M:%S"), 'Started Commit')
        session.commit()
        # print(datetime.now().strftime("%H:%M:%S"), 'Ended Commit')
        # for i in data:
        #    j += 1
        #    print(datetime.now().strftime("%H:%M:%S"), f'Record {j}')
        #
        #    # Inserting Data
        #    session.execute(insert(CustomersInsert).values(i))
        #    session.commit()
        #    print(datetime.now().strftime("%H:%M:%S"), 'Completed Record ', i.phone)
    print(datetime.now().strftime("%H:%M:%S"), 'Operation Completed')
    return
