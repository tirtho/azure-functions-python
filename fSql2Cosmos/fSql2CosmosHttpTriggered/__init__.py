import logging
import json
import azure.functions as func
from datetime import datetime
import os
import pyodbc
import uuid

def main(req: func.HttpRequest, mydoc: func.Out[func.Document]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        requestBody = req.get_json()
    except ValueError:
        logging.info('Error reading http request body')
        return func.HttpResponse("The Http triggered function executed successfully, but no request body found, so nothing was saved in CosmosDB", status_code=200)

    SQL_SERVER = os.environ['sqldb_server']
    SQL_DB = os.environ['sqldb_database']
    SQL_USER = os.environ['sqldb_user']
    SQL_PASSWORD = os.environ['sqldb_password']
    SQL_DRIVER = '{ODBC Driver 17 for SQL Server}'
    with pyodbc.connect('DRIVER=' + SQL_DRIVER + ';SERVER=' + SQL_SERVER + ';PORT=1433;DATABASE='+ SQL_DB + ';UID=' + SQL_USER + ';PWD=' + SQL_PASSWORD) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT randomNumber, time FROM RandomNumbers ORDER BY NEWID()")
            row = cursor.fetchone()
            timestamp = row.time.strftime("%m/%d/%Y, %H:%M:%S")
            payload = {
                "payloadId": str(uuid.uuid4()),
                "randomId": row.randomNumber,
                "time": timestamp,
            }
            #print("Request body: ", requestBody)
            for (k, v) in requestBody.items():
                payload[str(k)] = v
            print("Payload: ", payload)
            mydoc.set(func.Document.from_json(json.dumps(payload)))
    return func.HttpResponse(f"This http triggered function added in CosmosDB the document: %s" %(payload), status_code=202)
