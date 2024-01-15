import azure.functions as func
import logging
#from predict_optimize_script import predict_rop, optimize_parameters
import json
import requests
from azure.data.tables import TableServiceClient
from datetime import datetime
import uuid



app = func.FunctionApp()
@app.function_name(name="ml_trigger_func")
@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="eventhubforoilandgas",
                               connection="eventhubforoilandgas_connectionstring") 
def ml_trigger_func(azeventhub: func.EventHubEvent):
    logging.info('Python EventHub trigger processed an event: %s',
                azeventhub.get_body().decode('utf-8'))
    
    event_data = azeventhub.get_body().decode('utf-8')
    logging.info('Received event data: %s', event_data)
    event_data_json = json.loads(event_data)
    logging.info('Received event data: %s now running predictive model' )
    event_data_json = json.loads(event_data)
    print(event_data_json)
    url_pred = "https://appforoilandgas.wittyflower-21a1035d.eastus.azurecontainerapps.io/predict"
    data_variable = event_data_json
    print(data_variable)
    response_pred = requests.post(url_pred, json=data_variable)
    # Check if the request was successful (status code 200)
    if response_pred.status_code == 200:
        logging.info('Got response from predict api')
        response_pred_json = response_pred.json()
        print(response_pred_json)
    else:
        logging.info('Error in response from predict api')
        response_pred_json = None
    
    url_opt = "https://appforoilandgas.wittyflower-21a1035d.eastus.azurecontainerapps.io/optimize"
    data_variable = event_data_json
    response_opt = requests.post(url_opt, json=data_variable)
    # Check if the request was successful (status code 200)
    if response_opt.status_code == 200:
        logging.info('Got response from optimize api')
        response_opt_json = response_opt.json()
        print(response_opt_json)
    else:
        logging.info('Error in response from optimize api')
        response_opt_json = None

    #lets store everything in a table
    connection_string = "DefaultEndpointsProtocol=https;AccountName=mloilandgas5917582980;AccountKey=fXFPz9+WXGXjd//vfpo0hNPtek0Oxle9KyNttetoTfgKaEqAqnwj9hyfpkRwlifGk55o7AYVIZ6S+AStAJTRAQ==;EndpointSuffix=core.windows.net"
    table_service = TableServiceClient.from_connection_string(conn_str=connection_string)
    table_client = table_service.get_table_client(table_name="finaldata")
    partition_key = datetime.now().strftime("%Y-%m-%d")  # e.g., "2024-01-15"
    row_key = str(uuid.uuid4())  # Generates a unique identifier
    data_variable['PartitionKey'] = partition_key
    data_variable['RowKey'] = row_key
    data_variable['Predicted_ROP'] = response_pred_json['prediction']
    data_variable['Optimized_WOB'] = response_opt_json['WOB']
    data_variable['Optimized_RPM'] = response_opt_json['RPM']
    
    try:
        table_client.create_entity(entity=data_variable)
        logging.info("Data inserted into Azure Table")
    except Exception as e:
        logging.error(f"Error inserting data into Azure Table: {e}")

    

    
