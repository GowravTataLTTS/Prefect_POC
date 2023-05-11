from models import Customers, CustomersInsert, CustomersUpdate, CustomersDelete
from datetime import datetime
from sqlalchemy import delete
from sqlalchemy import MetaData
from task_all import transaction

metadata = MetaData()


def retrieve_data():
    print(datetime.now().strftime("%H:%M:%S"), 'Received Data')
    with transaction() as session:
        return (
            session.query(Customers.name, Customers.country, Customers.phone, Customers.email)
                .all()
        )


def transformation(data):
    complete_data = []
    for i in data:
        record = {'name': i.name, 'country': i.country, 'phone': i.phone,
                  'email': i.email}
        complete_data.append(record)
    with transaction() as session:
        customers_phone_id = session.query(Customers.phone).distinct()
        session.execute(delete(CustomersInsert).where(CustomersInsert.phone.in_(customers_phone_id)))
        session.bulk_insert_mappings(CustomersUpdate, complete_data)
        session.bulk_insert_mappings(CustomersDelete, complete_data)
        print("Finished")


def checker():
    all_data = retrieve_data()
    transformation(data=all_data)
    return


if __name__ == '__main__':
    checker()
