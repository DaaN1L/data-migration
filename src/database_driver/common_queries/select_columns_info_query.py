SELECT_COLUMNS_INFO_QUERY = """
    SELECT column_name,
           ordinal_position,
           column_default,
           is_nullable,
           data_type
      FROM information_schema.columns
     WHERE table_schema = '{schema}'
       AND table_name = '{table}'
  ORDER BY ordinal_position
"""
