from database.aurora_connector import AuroraDSQLConnector
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
load_dotenv()


if __name__ == "__main__":
    aConnector = AuroraDSQLConnector()
    a = aConnector.test_connection()
    print("TEST RESULT: ", a)