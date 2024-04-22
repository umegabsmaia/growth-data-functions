def build_origination_summaries_query(
        project: str,
        database: str,
        table: str
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
    
    query = f"""
    
    SELECT 
        id, 
        financedValue,
        originationTimestamp,
        createdOn,
        retailerId,
        borrowerId,
        contractId 
        
    FROM `{project}.{database}.{table}`
    """

    return query