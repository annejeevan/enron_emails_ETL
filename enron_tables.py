
from sqlalchemy import create_engine, MetaData, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, DateTime, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError


engine = create_engine('postgresql://mgow:**********@localhost:5432/enron_emails', echo = True)
meta = MetaData()
Base = declarative_base(metadata=meta)


class Emails(Base):
    __tablename__ = 'emails'
    id = Column(Integer, primary_key = True)
    message_id = Column(String)
    sent_datetime = Column(DateTime)
    message_from = Column(String)
    message_to = Column(String)
    message_cc = Column(String)
    message_bcc = Column(String)
    subject = Column(String)
    body = Column(String)
    mime_version = Column(Float)
    content_type = Column(String)
    content_transfer_encoding = Column(String)
    x_from = Column(String)
    x_to = Column(String)
    x_cc = Column(String)
    x_bcc = Column(String)
    x_folder = Column(String)
    x_origin = Column(String)
    x_filename = Column(String)
    label_by_key = dict(
        message_id = 'Message-ID',
        sent_datetime = 'Date',
        message_from = 'From',
        message_to = 'To',
        message_cc = 'Cc',
        message_bcc = 'Bcc',
        subject = 'Subject',
        mime_version = 'Mime-Version',
        content_type = 'Content-Type',
        content_transfer_encoding = 'Content-Transfer-Encoding',
        x_from = 'X-From',
        x_to = 'X-To',
        x_cc = 'X-cc',
        x_bcc = 'X-bcc',
        x_folder = 'X-Folder',
        x_origin = 'X-Origin',
        x_filename = 'X-FileName'
    )
    key_by_label = {label:key for key, label in label_by_key.items()}


class Addresses(Base):
    __tablename__ = 'addresses'
    id = Column(Integer, primary_key = True)
    email_id = Column(Integer, ForeignKey('emails.id'), nullable = False)
    sender = Column(String, ForeignKey('emails.message_from'))
    send_type = Column(String)
    sent_to = Column(String
