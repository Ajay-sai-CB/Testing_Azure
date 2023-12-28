import pyodbc

class SqlDataFetcher:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.conn = None

    def fetch_data(self, view_name, year_start, year_end, month_start, month_end, account_id, limit, offset):
        data = None  # Initialize data variable

        try:
            # Establish the connection
            self.conn = pyodbc.connect(self.connection_string)
            cursor = self.conn.cursor()

            # Your SQL query to select data with specified conditions and limit/offset
            select_query = f"SELECT * FROM {view_name} WHERE " \
                           f"(Year_p >= 'Year={year_start}' AND Year_p <= 'Year={year_end}') AND " \
                           f"(Month_p >= 'Month={month_start}' AND Month_p <= 'Month={month_end}') AND " \
                           f"(AccountID_p = 'AccountId={account_id}') " \
                           f"ORDER BY AccountId_p " \
                           f"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY;"


            # Execute the query to select data
            cursor.execute(select_query)

            # Fetch the results
            data = cursor.fetchall()

        except pyodbc.Error as e:
            print(f"Error: {e}")

        finally:
            # Close the connection if it's open
            if self.conn:
                self.conn.close()

        return data

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

# Fetch data from the "Payment" view with specific conditions and limit/offset
data_result = data_fetcher.fetch_data(view_name="Payment",
                                      year_start="2023", year_end="2025",
                                      month_start="01", month_end="12",
                                      account_id="0017X00001571soQAA",
                                      limit=100, offset=0)

# Print or use data_result as needed
print("Filtered Data from 'Payment' view:")
for row in data_result:
    print(row)
