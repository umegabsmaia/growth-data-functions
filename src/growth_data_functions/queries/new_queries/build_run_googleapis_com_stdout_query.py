def build_run_googleapis_com_stdout_query(
        project: str,
        database: str,
        table: str
) -> str:
    """
    Query to retrive information about the retailers.
    
    Args:
        project (str): The bigquery project.
        database (str): The bigquery database.
        table (str): The table where are the clients data to retrieve.
    
    Returns:
        query (str): A query to retrieve information about retailers.
    """

    # check for empty strings
    if project == '' or database == '' or table == '':
        raise ValueError("project, database, and table must be non-empty strings.")
    
    query = f"""

        SELECT 
            *
                FROM `{project}.{database}.{table}`
        
    """

    return query