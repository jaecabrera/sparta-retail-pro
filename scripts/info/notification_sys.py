import os
from typing import Optional
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dataclasses import dataclass
from ..extract.sgtime import now as sgtz
from pathlib import Path
import pickle
from dotenv import load_dotenv


load_dotenv('../.env')
TEMP_PATH = Path(os.getenv("TEMP_PATH"))


def get_facet(temp_path) -> pd.DataFrame:
    """
    :description:
        To load json file from newly acquired data
    :return: pd.DataFrame
    """
    # Load path defaults and get temporary data path

    # Get a list of all files in the folder
    table = [f.name for f in Path.iterdir(temp_path) \
             if f.suffix == '.json' and f.name.startswith(f"{sgtz()[:8]}")]

    table_facet = pd.read_json(temp_path / table[0])
    return table_facet


@dataclass
class EmailParams:
    email_from: str = "jaecabrera.ds@gmail.com"
    email_to: str = "jaecabrera.cp@gmail.com"
    smtp_port: int = 587
    smtp_server: str = 'smtp.gmail.com'
    g_key: Optional[str] = None

    def __init__(self, subject):
        self.subject = subject

    @staticmethod
    def load_account_key(key_path: Path):
        with open(key_path, 'rb') as key:
            pickle_key = pickle.load(key)

        return pickle_key[0]


def send_emails(params: EmailParams, data_table: pd.DataFrame):
    """
    :param params:
    :param data_table:
    :return:
    """

    body = f"""
    
    {data_table.to_html()}
    """

    # email structure
    message = MIMEMultipart()
    message['From'] = params.email_from
    message['To'] = params.email_to
    message['Subject'] = params.subject

    # Attach the body of the message
    message.attach(MIMEText(body, 'html'))

    # Connect with the server
    print("Connecting to server...")
    server = smtplib.SMTP(params.smtp_server, params.smtp_port)
    server.starttls()
    server.login(params.email_from, params.load_account_key(
        Path(r"D:\lagoon_credentials\gmail_pass.pickle")))

    print("Successfully connected to server")
    text = message.as_string()
    print("Email sent")
    server.sendmail(params.email_from, params.email_to, text)
    print("Closing Port")
    server.quit()
