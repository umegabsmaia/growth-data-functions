def build_address_query(
        project: str,
        database: str,
        table: str,
        federativeUnit: list[str]
) -> str:
    """
    Query to retrive the address of the clients or stores.
    
    Args:
        project (str): The bigquery project.
        database (str): The bigquery database.
        table (str): The table where are the clients data to retrieve.
    
    Returns:
        query (str): A query to retrieve the address of the clients or stores.
    """

    # check for empty strings
    if project == '' or database == '' or table == '':
        raise ValueError("project, database, and table must be non-empty strings.")
    
    query = f"""
        
        SELECT 
            
            id,
            zipCode,
            federativeUnit
             
        FROM `{project}.{database}.{table}`
        
        WHERE federativeUnit IN {federativeUnit}
    """

    return query