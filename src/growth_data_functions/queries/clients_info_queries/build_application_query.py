def build_application_query(
        project: str,
        database: str,
        table: str,
        approved: bool = True,
) -> str:
    """
    Query to retrive the applications data.
    
    Args:
        project (str): The bigquery project.
        database (str): The bigquery database.
        table (str): The table where are the clients data to retrieve.
    
    Returns:
        query (str): A query to retrieve the applications data.
    """

    # check for empty strings
    if project == '' or database == '' or table == '':
        raise ValueError("project, database, and table must be non-empty strings.")
    
    if approved == True:
        
        query = f"""
        SELECT 
            id,
            borrowerId,
            storeId,
        FROM (
            SELECT 
                a.id,
                a.borrowerId,
                a.storeId,
                a.status AS application_status,
                ROW_NUMBER() OVER(PARTITION BY a.borrowerId ORDER BY a.createdOn) AS row_num
            FROM 
                `{project}.{database}.{table}` a
            WHERE 
                a.status = 'APPROVED'
        ) AS ranked_apps
        WHERE 
        row_num = 1;
        """ 
        return query
    
    else:
        
        query = f"""
        SELECT 
            id, borrowerId, storeId
        FROM 
            `{project}.{database}.{table}`
        """
        return query
