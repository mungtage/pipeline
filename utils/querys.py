def make_query_insert(fields):
      query = f'''INSERT INTO animal_info ({", ".join(fields)})
          VALUES(%({")s, %(".join(fields)})s)'''
      return query

def make_query_truncate(table_name):
      query=f'''TRUNCATE TABLE {table_name};'''
      return query

def make_query_select():
      query = f'''SELECT * FROM animal_info'''
      return query
      