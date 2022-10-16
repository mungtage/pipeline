def make_insert_query(sample_data):
    keys = list(sample_data.keys())
    query = f'''INSERT INTO animal_info {", ".join(keys)}
          VALUES(%({")s, %(".join(keys)})s)'''
    return query