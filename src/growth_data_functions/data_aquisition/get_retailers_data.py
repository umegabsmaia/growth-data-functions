import warnings
import pandas as pd
from google.cloud import bigquery
from google.cloud import bigquery_storage
from growth_data_functions.queries.new_queries.build_retailers_query import build_retailers_query
warnings.filterwarnings("ignore")

def get_retailers_data(
    project: str = 'prd-ume-data',
    database: str = 'prd_datastore_public',
    table: str = 'retailers'
) -> pd.DataFrame:
    
    """ 
    This code defines a function named get_origination_data that retrieves data 
    related to 'ume clients' from a BigQuery table and returns it as a 
    pandas DataFrame.

    Args:
        bq_project (str): The BigQuery project name.
        database (str): The BigQuery database.
        table (str): The name of the table where the clients data is stored.

    returns:
        df (pd.DataFrame): A pandas DataFrame containing the clients data 
        retrieved from the specified BigQuery table.

    Example:
        ```{python}
        import umebehavior 
        
        df = umebehavior.get_clients(
                project = 'data-store-248214',
                database = 'ume_data',
                table = 'application_data'
        )
        ```
    """

    # check for empty strings
    if project == '' or database == '' or table == '':
        raise ValueError("project, database, and table must be non-empty strings.")
    
    # connect bigquery
    bqclient = bigquery.Client(project = project)
    bqstorageclient = bigquery_storage.BigQueryReadClient()

    # create the renegotiation query
    query = build_retailers_query(project, database, table)
 
    # Download the data
    df = bqclient.query(query) \
        .result() \
        .to_dataframe(bqstorage_client = bqstorageclient)
    
    return df