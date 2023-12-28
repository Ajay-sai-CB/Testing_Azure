import azure.functions as func
import logging
from classapi import SqlDataFetcher
import json

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Define the connection string
    connection_string = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "Server=tcp:synw-smb-sfda-dev-ondemand.sql.azuresynapse.net,1433;"
        "DATABASE=smb-dev-sfda-synw-sqldb;"
        "UID=funcuser;"
        "PWD=Smuser@098!!!;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    # Create an instance of the SqlDataFetcher class
    data_fetcher = SqlDataFetcher(connection_string)
    data_result = []

    try:
        # Fetch data from the "Payment" view with specific conditions and limit/offset
        data_result = data_fetcher.fetch_data(view_name="Payment",
                                              year_start="2023", year_end="2025",
                                              month_start="01", month_end="12",
                                              account_id="0017X00001571soQAA",
                                              limit=100, offset=0)

        # Logging results instead of print
        logging.info("Filtered Data from 'Payment' view:")
        for row in data_result:
            logging.info(row)

    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return func.HttpResponse("Internal Server Error", status_code=500)

    return func.HttpResponse(json.dumps(data_result), mimetype="application/json", status_code=200)
