def create_db(cur, db_name) : 
    db_cursor = cur.cursor()
    db_cursor.execute("CREATE DATABASE dbms_project_sem3")
    db_cursor.execute("USE dbms_project_sem3")