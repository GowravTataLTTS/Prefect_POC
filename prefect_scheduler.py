#!/usr/bin/python3
from task_all import *
from models import Customers, CustomersInsert, CustomersUpdate, CustomersDelete
from prefect import task, flow
from subprocess import PIPE, Popen
from datetime import datetime
from sqlalchemy import update, insert, delete
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


@task
def keepalived_status():
    terminal = Popen(['ip', 'a', 's', 'ens192'],
                     stdout=PIPE,
                     stderr=PIPE)
    stdout, stderr = terminal.communicate()
    if '10.88.64.134' in stdout.decode():
        return "KEEPALIVED MASTER"
    return "KEEPALIVED BACKUP"


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


@task
def retrieve_data():
    print(datetime.now().strftime("%H:%M:%S"), 'Started All Users Data')
    with transaction() as session:
        customers_phone_id = session.query(CustomersInsert.phone).distinct()
        return (
            session.query(Customers.name, Customers.country, Customers.phone, Customers.email)
                .filter(~Customers.phone.in_(customers_phone_id))
                .distinct()
                .all()
        )


@task
def transformation(data):
    with transaction() as session:
        print(datetime.now().strftime("%H:%M:%S"), 'Started Inserting Data')
        for i in data:
            # Inserting Data
            session.execute(insert(CustomersInsert).values(i))
            # Updating data
            record = {'name': i.name.lower(), 'country': i.country.upper(), 'phone': i.phone,
                      'email': i.email.upper()}
            session.execute(update(CustomersUpdate).where(CustomersUpdate.number == record['phone']).values(record))

            # Deleting Data
            session.execute(delete(CustomersDelete).where(CustomersDelete.number == new['phone']).values(i))

            session.commit()
            print(datetime.now().strftime("%H:%M:%S"), 'Ended Inserting Data')
    return


@flow
def trigger():
    status = keepalived_status()
    print(datetime.now().strftime("%H:%M:%S"), f'Status of keepalived is {status}')
    if status == "KEEPALIVED MASTER":
        print(datetime.now().strftime("%H:%M:%S"), 'Entered Flow')
        first_data = retrieve_data()
        updated_data = transformation(first_data)
        print(datetime.now().strftime("%H:%M:%S"), "Flow is completed")
        return
    else:
        print(datetime.now().strftime("%H:%M:%S"),  status)
    return
