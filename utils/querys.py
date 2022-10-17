def make_insert_query(fields):
    query = f'''INSERT INTO animal_info ({", ".join(fields)})
          VALUES(%({")s, %(".join(fields)})s)'''
    return query