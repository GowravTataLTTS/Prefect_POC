"""
Created on Mon Apr 17 19:45:04 2023

@author: gowrav
"""

import time
from million_models import Customers, CustomersInsert
from prefect import task, flow
from subprocess import PIPE, Popen
import schedule
from multiprocessing import Process
from datetime import datetime
from sqlalchemy import update, insert, delete, except_, union_all, alias, func
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
    print(datetime.now().strftime("%H:%M:%S"), 'Started Fetching Delta Data')
    with transaction() as session:
        customers_phone = union_all(select([Customers.phone]), select([CustomersInsert.phone])).alias('Custom_Union')
        all_ids = select(customers_phone).select([func.phone, func.count(1)]).group_by(func.phone)
        new_ids = select([func.phone]).where(func.count < 2)

        delta_customers = (
            session.query(Customers.name, Customers.country, Customers.phone, Customers.email)
                .filter(Customers.phone.in_(new_ids))
                .distinct()
                .all()
        )
        print(datetime.now().strftime("%H:%M:%S"), 'Finished Fetching Delta Data')
        return delta_customers


# @task
def transformation(data):
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
