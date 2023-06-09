#!/bin/bash/python3
from sqlalchemy import Column, MetaData, Text, Integer
from sqlalchemy.ext.declarative import declarative_base

metadata = MetaData()

Base = declarative_base(metadata=metadata)


class Customers(Base):
    __tablename__ = 'customers'
    name = Column(Text)
    country = Column(Text, nullable=False)
    phone = Column(Text, primary_key=True)
    email = Column(Text)

    def __init__(self, name, country, phone, email):
        self.name = name
        self.country = country
        self.phone = phone
        self.email = email

    def __repr__(self):
        return f"{self.name},{self.country},{self.phone},{self.email}"


class CustomersInsert(Base):
    __tablename__ = 'customer_insert'
    name = Column(Text)
    country = Column(Text, nullable=False)
    phone = Column(Text, primary_key=True)
    email = Column(Text)

    def __init__(self, name, country, phone, email):
        self.name = name
        self.country = country
        self.phone = phone
        self.email = email

    def __repr__(self):
        return f"{self.name},{self.country},{self.phone},{self.email}"


class CustomersUpdate(Base):
    __tablename__ = 'customer_update'
    name = Column(Text)
    country = Column(Text, nullable=False)
    phone = Column(Text, primary_key=True)
    email = Column(Text)

    def __init__(self, name, country, phone, email):
        self.name = name
        self.country = country
        self.phone = phone
        self.email = email

    def __repr__(self):
        return f"{self.name},{self.country},{self.phone},{self.email}"


class CustomersDelete(Base):
    __tablename__ = 'customer_delete'
    name = Column(Text)
    country = Column(Text, nullable=False)
    phone = Column(Text, primary_key=True)
    email = Column(Text)

    def __init__(self, name, country, phone, email):
        self.name = name
        self.country = country
        self.phone = phone
        self.email = email

    def __repr__(self):
        return f"{self.name},{self.country},{self.phone},{self.email}"
