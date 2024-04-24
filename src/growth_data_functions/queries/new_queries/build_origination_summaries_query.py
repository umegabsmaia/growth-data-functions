from datetime import datetime, timedelta

def build_origination_summaries_query(
        project: str,
        database: str,
        table: str,
        start_date:str = "2024-01-01",
        end_date:str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
) -> str:
    """
    Query to retrive the base origination data.
    
    Args:
        project (str): The bigquery project.
        database (str): The bigquery database.
        table (str): The table where are the clients data to retrieve.
    
    Returns:
        query (str): A query to retrieve the basic origination data.
    """

    # check for empty strings
    if project == '' or database == '' or table == '':
        raise ValueError("project, database, and table must be non-empty strings.")
    
    # Change time to Manaus
    query = f"""
    
    SELECT 
        id, 
        financedValue, 
        DATE(`prd-ume-data.prd_datastore_public.origination_summaries`.originationTimestamp, '-04:00') as orig_date, 
        retailerId,
        borrowerId,
        contractId
        
    FROM `{project}.{database}.{table}`
    
    WHERE orig_date >= DATE('{start_date}')
    AND orig_date <= DATE('{end_date}')
    AND `prd-ume-data.prd_datastore_public.origination_summaries`.canceledOn is null
    """

    return query


# Fazer uma get pro cliente e uma get pras stores