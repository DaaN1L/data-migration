SELECT_INCREMENT_QUERY = """
    SELECT {columns}
    FROM {schema}.{table}
    WHERE "{increment}" BETWEEN '{from_}' AND '{to_}'
"""