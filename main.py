##Importing Important Python packages.
import pandas as pd
import snowflake.connector as sf
import json
from flask import Flask,request

app=Flask(__name__)


user="lpandey"
password="Oct@2022"
account="ak04740.ap-southeast-1"
database="POC"
warehouse="POC_WH"
schema="POC_OWNER"
role="POC_OWNER_ROLE"

conn=sf.connect(user=user,password=password,account=account)



def run_query(conn, query):
    print("Executing the {} query".format(query))
    cursor = conn.cursor()
    try:
        cursor.execute(query)
    except Exception as e:
        print(e)
    cursor.close()

def run_query1(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    records=str(cursor.fetchone()[0])
    cursor.close()
    return records

@app.route('/')
def api_test():
    return "Hello , Welcome to Snowflake API. Your Test connection is working Fine "

@app.route('/aggregator',methods=['GET','POST'])
def stats_collector():
    query_to_be_executed=request.args.get("query")
    extracted_data=run_query1(conn, query_to_be_executed)
    print(extracted_data)
    return extracted_data

@app.route('/data_fetcher',methods=['GET','POST'])
def data_extractor():
    query_to_be_executed = request.args.get("query")
    data_from_table = pd.read_sql(query_to_be_executed, conn)
    return {"data": json.loads(data_from_table.to_json(orient='records'))}
##http://127.0.0.1:5000/data_fetcher?query=select%20*%20from%20"POC"."POC_OWNER"."TALPA_DATASET" limit 5;

if __name__=='__main__':

    app.run()