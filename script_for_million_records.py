#!/usr/bin/python3
from more_tasks import *
import schedule
from multiprocessing import Process
from datetime import datetime


def trigger():
    first_data = retrieve_data()
    updated_data = transformation(first_data)
    return



if __name__ == '__main__':
    trigger()
