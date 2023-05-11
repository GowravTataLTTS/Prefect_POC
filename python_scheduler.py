#!/usr/bin/python3
from task_all import *
import schedule
from multiprocessing import Process
from datetime import datetime


# @flow
def trigger():
    first_data = retrieve_data()
    updated_data = transformation(first_data)
    return


def prefect_checker():
    print(datetime.now().strftime("%H:%M:%S"), 'Entered Flow')
    status = keepalived_status()
    print(datetime.now().strftime("%H:%M:%S"), f'Status of keepalived is {status}')
    if status == "MASTER":
        trigger()
        print(datetime.now().strftime("%H:%M:%S"), "Flow is completed")
    else:
        print(datetime.now().strftime("%H:%M:%S"), "BACKUP")


def run_per_time():
    print(datetime.now().strftime("%H:%M:%S"), 'Scheduler Triggered')
    schedule.every(1).minute.do(prefect_checker)
    while True:
        schedule.run_pending()


def run_job():
    print(datetime.now().strftime("%H:%M:%S"), 'Entered Run Job')
    p = Process(target=run_per_time)
    p.start()
    p.join()


if __name__ == '__main__':
    run_job()
