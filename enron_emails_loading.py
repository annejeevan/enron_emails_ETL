#!/usr/bin/python
#################################################################
# by Jeevan Anand Anne
#
# This script is to parse the enron data and load it into PostgresSQL
#################################################################

import os
import os.path
import re
from sqlalchemy import create_engine, MetaData, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, DateTime, Float, Boolean, ForeignKey, VARCHAR


#create an engine to connect to the database
engine = create_engine('postgresql://jeevananand.anne@localhost:5432/enron_test1', echo=True)


# create all tables
meta = MetaData()
base = declarative_base(metadata=meta)

class Emails(base):
    __tablename__ = 'emails'
    id = Column(Integer, primary_key = True)
    message_id = Column(String)
    email_from = Column(String)
    email_to = Column(String)
    email_cc = Column(String)
    email_bcc = Column(String)
    email_reply_to = Column(String)
    email_date = Column(DateTime)
    sender = Column(String)
    subject = Column(String)
    mime_type = Column(String)
    disposition_type = Column(String)
    filename = Column(String)

    addresses = relationship('Recipient', backref='email_ref')

    label_by_key = dict(
        message_id='message_id',
        email_date='email_date',
        email_from='from',
        email_to='to',
        email_cc='cc',
        email_bcc='bcc',
        email_reply_to='reply_to',
        sender='sender',
        subject='subject',
        filename='filename',
        mime_type='mime_type',
        disposition_type='disposition_type'
    )
    key_by_label = {label:key for key, label in label_by_key.items()}

class Recipient(base):
    __tablename__ = 'recipient_info'
    id = Column(Integer, primary_key = True)
    email_id = Column(Integer, ForeignKey('emails.id'), nullable=False)
    send_type = Column(String)
    recipient = Column(String)
    address_types = ['to', 'cc', 'bcc']
    key_by_address_type = {item:Emails.key_by_label[item] for item in address_types}


base.metadata.create_all(engine)


def parse_enron_email_log(line):
    """
    Reading the log file and outputs a key value pairs

    input -> String (log file name)
    output -> Dataframe
    """
    #remove the righ most space
    line = line.rstrip()
    ####### converting the lines to key, values

    # If it doesn't match, it's either an error or list of values
    try:
        #extracting only date
        email_details_date = re.findall(r'(\[.*?\])\s.*', line)
        email_details_date = email_details_date[0].translate(str.maketrans({'[': '', ']': ''}))

        #extracting only field columns
        email_field_names = re.findall(r'\[.*\]\s(message_id)=<.*>, (from)=".*", (to)=".*", (cc)=".*", (bcc)=".*", (reply_to)=".*", (sender)=".*", (subject)=".*", (filename)=".*", (mime_type)=".*", (disposition_type)=".*"',line)

        #extracting only field values
        email_field_values = re.findall(r'\[.*\]\smessage_id=<(.*)>, from="(.*)", to="(.*)", cc="(.*)", bcc="(.*)", reply_to="(.*)", sender="(.*)", subject="(.*)", filename="(.*)", mime_type="(.*)", disposition_type="(.*)"',line)

        #combining two lists and converting to a key value pairs - dictionaries
        email_details = dict(zip(list(email_field_names[0]), list(email_field_values[0])))

        #creating a new key for date part we extracted above
        email_details['email_date'] = email_details_date

    except IndexError:
        print("ERROR: line failed to match email details regexp.")
        return {}

    return email_details


# creating an empty list to store the key value pairs of each line
enron_email_dict = []

for dirpath, dirnames, filenames in os.walk('/Users/jeevananand.anne/Downloads/enron_emails/enron_email_logs'):
    #parsing through all the log files present
    for filename in filenames:
        filepath = os.path.join(dirpath, filename) #log filepath
        with open(filepath, 'r') as file:
            #reading line by line of a log file
            for line in file:
                enron_email_dict.append(parse_enron_email_log(line))

addresses = []

# create database session
Session = sessionmaker()
Session.configure(bind=engine)
session = Session()

#checking if there are any errors
key_errors = 0

#print(enron_email_dict)

#looping through the email line by line, also extracting to, cc, bcc which are inserted to recipient_info table
for email in enron_email_dict:
    try:
        kw = {Emails.key_by_label[label]:value for label, value in email.items()}
        emails_row = Emails( **kw)
        for address_type, column_name in Recipient.key_by_address_type.items():
            addresses = kw.get(column_name)
            if addresses:
                for address in addresses.split(','):
                    addresses_row = Recipient(send_type=address_type, recipient=address.strip())
                    emails_row.addresses.append(addresses_row)
        session.add(emails_row)
        session.commit()
    except KeyError as k:
        print('KeyError in message {}: {}'.format(email, k))
        key_errors += 1
    except ValueError as v:
        print('ValueError in message {}: {}'.format(email, v))
    except SQLAlchemyError as e:
        print(e)
    finally:
        session.close()

print("")
print('Count of KeyErrors: {}'.format(key_errors))
print("")
print("##### Data Imported to database tables emails & recipient_info tables #####")
