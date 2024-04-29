def build_contracts_query(
        project: str,
        database: str,
        table: str
) -> str:
    """
    Query to retrive the contracts query.
    
    Args:
        project (str): The bigquery project.
        database (str): The bigquery database.
        table (str): The table where are the clients data to retrieve.
    
    Returns:
        query (str): A query to retrieve the contracts that have been made recently.
    """

    # check for empty strings
    if project == '' or database == '' or table == '':
        raise ValueError("project, database, and table must be non-empty strings.")
    
    query = f"""
        
        SELECT 
            id as contractId,
            storeId,
            sourceProduct,
            purchaseType
            
            FROM `{project}.{database}.{table}`
            
            WHERE canceledOn is NULL
        
    """

    return query