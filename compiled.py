import mysql.connector as myconnect
import pandas as pd
from password import *
from creating_database import create_db
from creating_tables import create_tables
from insert_into_tables import *

mydb = myconnect.connect(
  host=get_host(),
  user=get_user(),
  password=get_password()
)

create_db(mydb, "dbms_project_sem3")
create_tables(mydb)

csv_path = "TMDB_movie_dataset_v11.csv"
df = load_and_clean_csv(csv_path)

insert_from_df(mydb, df, batch_size=200)

mydb.close() 