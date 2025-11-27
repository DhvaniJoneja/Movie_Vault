import mysql.connector as myconnect
import pandas as pd
from password import *
from creating_database import create_db
from creating_tables import create_tables
from insert_into_tables import *
import streamlit as st
import matplotlib.pyplot as plt

mydb = myconnect.connect(
  host=get_host(),
  user=get_user(),
  password=get_password()
)

mycursor=mydb.cursor()
mycursor.execute("USE dbms_project_sem3")
mycursor.close()

def get_movies_with_vote_above(threshold=7.0):
    mycursor=mydb.cursor()
    sql = """
        SELECT mi.TITLE, mp.VOTE_AVG
        FROM MOVIE_INFO mi
        JOIN MOVIE_POPULARITY mp ON mp.M_ID = mi.ID
        WHERE mp.VOTE_AVG > %s
        ORDER BY mp.VOTE_AVG DESC;
    """
    mycursor.execute(sql, (threshold,))
    rows = mycursor.fetchall()  
    mycursor.close()
    return rows

def get_movies_released_after(year=2020):
    mycursor=mydb.cursor()
    sql = """
        SELECT mi.TITLE, ms.RELEASE_DATE
        FROM MOVIE_INFO mi
        JOIN MOVIE_STATUS ms ON ms.M_ID = mi.ID
        WHERE ms.STATUS = 'Released' AND YEAR(ms.RELEASE_DATE) > %s
        ORDER BY ms.RELEASE_DATE DESC;
    """
    mycursor.execute(sql, (year,))
    rows = mycursor.fetchall()
    mycursor.close()
    return rows

def get_all_original_languages():
    mycursor = mydb.cursor()
    sql = """
        SELECT DISTINCT ORIGINAL_LANG
        FROM MOVIE_CATEGORY
        WHERE ORIGINAL_LANG IS NOT NULL
        ORDER BY ORIGINAL_LANG;
    """
    mycursor.execute(sql)
    rows = mycursor.fetchall() 
    mycursor.close()
    # convert to flat list of strings
    langs = [r[0] for r in rows]
    return langs

def get_movies_by_language(lang):
    mycursor = mydb.cursor()
    sql = """
        SELECT mi.TITLE, mc.ORIGINAL_LANG
        FROM MOVIE_INFO mi
        JOIN MOVIE_CATEGORY mc ON mc.M_ID = mi.ID
        WHERE mc.ORIGINAL_LANG = %s
        ORDER BY mi.TITLE;
    """
    mycursor.execute(sql, (lang,))
    rows = mycursor.fetchall()
    mycursor.close()
    return rows

def get_most_frequent_original_language():
    mycursor = mydb.cursor()
    sql = """
        SELECT MC.ORIGINAL_LANG, COUNT(*) AS Count
        FROM MOVIE_CATEGORY MC
        GROUP BY MC.ORIGINAL_LANG
        ORDER BY Count DESC
        LIMIT 20;
    """
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    mycursor.close()
    return rows

def get_top_profitable_movies(limit=10):
    mycursor = mydb.cursor()
    sql = """
        SELECT MI.TITLE, ME.REVENUE, ME.BUDGET, 
               (ME.REVENUE - ME.BUDGET) AS PROFIT
        FROM MOVIE_INFO MI
        JOIN MOVIE_ECONOMICS ME ON MI.ID = ME.M_ID
        ORDER BY PROFIT DESC
        LIMIT %s;
    """
    mycursor.execute(sql, (limit,))
    rows = mycursor.fetchall()
    mycursor.close()
    return rows

def get_avg_revenue_per_language():
    mycursor = mydb.cursor()
    sql = """
        SELECT 
            mc.ORIGINAL_LANG AS Language,
            AVG(me.REVENUE) AS Avg_Revenue
        FROM MOVIE_CATEGORY mc
        JOIN MOVIE_ECONOMICS me ON mc.M_ID = me.M_ID
        WHERE me.REVENUE IS NOT NULL
        GROUP BY mc.ORIGINAL_LANG
        ORDER BY Avg_Revenue DESC;
    """
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    mycursor.close()
    return rows

def get_movies_with_revenue_above_1b():
    mycursor = mydb.cursor()
    sql = """
        SELECT mi.TITLE, me.REVENUE
        FROM MOVIE_INFO mi
        JOIN MOVIE_ECONOMICS me ON me.M_ID = mi.ID
        WHERE me.REVENUE > 1000000000
        ORDER BY me.REVENUE DESC;
    """
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    mycursor.close()
    return rows

def get_low_budget_high_rated_movies():
    mycursor = mydb.cursor()
    sql = """
        SELECT MI.TITLE, MP.VOTE_AVG, ME.BUDGET
        FROM MOVIE_INFO MI
        JOIN MOVIE_POPULARITY MP ON MI.ID = MP.M_ID
        JOIN MOVIE_ECONOMICS ME ON MI.ID = ME.M_ID
        WHERE ME.BUDGET < 20000000 
          AND MP.VOTE_AVG > 7.5
        ORDER BY MP.VOTE_AVG DESC;
    """
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    mycursor.close()
    return rows

def get_most_popular_release_years():
    mycursor = mydb.cursor()
    sql = """
        SELECT 
            YEAR(MS.RELEASE_DATE) AS YEAR,
            COUNT(*) AS TOTAL_MOVIES,
            AVG(MP.POPULARITY) AS AVG_POPULARITY
        FROM MOVIE_STATUS MS
        JOIN MOVIE_POPULARITY MP ON MS.M_ID = MP.M_ID
        WHERE MS.RELEASE_DATE IS NOT NULL
        GROUP BY YEAR
        ORDER BY AVG_POPULARITY DESC;
    """
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    mycursor.close()
    return rows


def get_genre_popularity_profit():
    mycursor = mydb.cursor()
    sql = """
        SELECT MG.GENRE,
               AVG(MP.POPULARITY) AS AVG_POPULARITY,
               AVG(ME.REVENUE - ME.BUDGET) AS AVG_PROFIT
        FROM MOVIE_GENRE MG
        JOIN MOVIE_POPULARITY MP ON MG.M_ID = MP.M_ID
        JOIN MOVIE_ECONOMICS ME ON MG.M_ID = ME.M_ID
        GROUP BY MG.GENRE
        ORDER BY AVG_PROFIT DESC;
    """
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    mycursor.close()
    return rows

def get_genre_popularity_in_period(start_year, end_year):
    mycursor = mydb.cursor()
    sql = """
        SELECT
          mg.GENRE,
          COUNT(*) AS movie_count,
          AVG(mp.POPULARITY) AS avg_popularity,
          SUM(mp.POPULARITY) AS total_popularity
        FROM MOVIE_GENRE mg
        JOIN MOVIE_STATUS ms    ON mg.M_ID = ms.M_ID
        JOIN MOVIE_POPULARITY mp ON mg.M_ID = mp.M_ID
        WHERE ms.RELEASE_DATE IS NOT NULL
          AND YEAR(ms.RELEASE_DATE) BETWEEN %s AND %s
        GROUP BY mg.GENRE
        HAVING movie_count > 0
        ORDER BY avg_popularity DESC;
    """
    mycursor.execute(sql, (start_year, end_year))
    rows = mycursor.fetchall()
    mycursor.close()
    return rows

def get_loss_making_movies():
    mycursor = mydb.cursor()
    sql = """
        SELECT 
            mi.TITLE,
            me.BUDGET,
            me.REVENUE,
            (me.BUDGET - me.REVENUE) AS LOSS
        FROM MOVIE_INFO mi
        JOIN MOVIE_ECONOMICS me ON mi.ID = me.M_ID
        WHERE me.BUDGET IS NOT NULL
          AND me.REVENUE IS NOT NULL
          AND me.BUDGET > me.REVENUE
        ORDER BY LOSS DESC;
    """
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    mycursor.close()
    return rows


st.title("Movie Vault")

option = st.selectbox(
    "Choose a query:",
    ("-- Select --",
     "Movies with vote average above X",
     "Movies released after year Y",
     "Choose movies based on language",
     "Top 20 most frequently used original languages",
     "Top N most profitable movies",
     "Average revenue per language", 
     "Movies with revenue greater than 1B",
     "Low-budget but high-rated movies",
     "Most popular movie release years",
     "Genre popularity vs profit",
     "Genres Popularity in a Time Period",
      "Movies that made a loss")
)

if option == "Movies with vote average above X":
    threshold = st.number_input("Enter vote average threshold:", min_value=0.0, max_value=10.0, value=7.0, step=0.1)
    if st.button("Run Query"):
        rows = get_movies_with_vote_above(threshold)
        df = pd.DataFrame(rows, columns=["Title", "Vote Average"])
        st.dataframe(df)

elif option == "Movies released after year Y":
    year = st.number_input("Enter year:", min_value=1900, max_value=2100, value=2020, step=1)
    if st.button("Run Query"):
        rows = get_movies_released_after(year)
        df = pd.DataFrame(rows, columns=["Title", "Release Date"])
        st.dataframe(df)

elif option == "Choose movies based on language":
    languages = get_all_original_languages()
    choices = ["-- Select language --"] + languages
    selected = st.selectbox("Choose original language:", choices)
    if selected and selected != "-- Select language --":
        rows = get_movies_by_language(selected)
        if not rows:
            st.write("No movies found for language:", selected)
        else:
            df = pd.DataFrame(rows, columns=["Title", "Original Language"])
            st.dataframe(df)

elif option == "Top 20 most frequently used original languages":
    if st.button("Run Query"):
        rows = get_most_frequent_original_language()
        df = pd.DataFrame(rows, columns=["Original Language", "Count"])

        if df.empty:
            st.write("No data found.")
        else:
            df = df.sort_values("Count", ascending=False).reset_index(drop=True)

            fig, ax = plt.subplots(figsize=(10, 6))

            ax.bar(df["Original Language"], df["Count"])

            ax.set_xlabel("Original Language")
            ax.set_ylabel("Number of Movies")
            ax.set_title("Most Frequently Used Original Languages")

            plt.xticks(rotation=45, ha="right")

            plt.tight_layout()
            st.pyplot(fig)

elif option == "Top N most profitable movies":
    n = st.number_input("Enter number of movies (Top N):", min_value=1, max_value=100, value=10, step=1)
    if st.button("Run Query"):
        rows = get_top_profitable_movies(n)
        df = pd.DataFrame(rows, columns=["Title", "Revenue", "Budget", "Profit"])
        st.dataframe(df)

        st.subheader("Profit Comparison Chart")
        st.bar_chart(df.set_index("Title")["Profit"])

elif option == "Average revenue per language":
    if st.button("Run Query"):
        rows = get_avg_revenue_per_language()
        df = pd.DataFrame(rows, columns=["Language", "Avg Revenue"])

        st.dataframe(df)

        st.subheader("Average Revenue per Language")

        fig, ax = plt.subplots(figsize=(12, 6))
        ax.bar(df["Language"], df["Avg Revenue"])

        ax.set_xlabel("Language")
        ax.set_ylabel("Average Revenue")
        ax.set_title("Average Revenue per Language")
        plt.xticks(rotation=45, ha="right")

        st.pyplot(fig)

elif option == "Movies with revenue greater than 1B":
    if st.button("Run Query"):
        rows = get_movies_with_revenue_above_1b()
        df = pd.DataFrame(rows, columns=["Title", "Revenue"])
        st.dataframe(df)

elif option == "Low-budget but high-rated movies":
    if st.button("Run Query"):
        rows = get_low_budget_high_rated_movies()
        df = pd.DataFrame(rows, columns=["Title", "Vote Average", "Budget"])
        st.dataframe(df)

elif option == "Most popular movie release years":
    if st.button("Run Query"):
        rows = get_most_popular_release_years()
        df = pd.DataFrame(rows, columns=["Year", "Total Movies", "Avg Popularity"])
        st.dataframe(df)
        if not df.empty:
            st.subheader("Avg Popularity per Year")

            fig, ax = plt.subplots(figsize=(12, 6))

            ax.bar(df["Year"], df["Avg Popularity"])
            ax.set_xlabel("Year")
            ax.set_ylabel("Average Popularity")
            ax.set_title("Most Popular Movie Release Years")

            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            st.pyplot(fig)

elif option == "Genre popularity vs profit":
    if st.button("Run Query"):
        rows = get_genre_popularity_profit()
        df = pd.DataFrame(rows, columns=["Genre", "Avg Popularity", "Avg Profit"])

        st.dataframe(df)

        if df.empty:
            st.write("No data found.")
        else:
            fig1, ax1 = plt.subplots(figsize=(10,6))
            ax1.scatter(df["Avg Popularity"], df["Avg Profit"])
            for i, g in enumerate(df["Genre"]):
                ax1.annotate(g, (df["Avg Popularity"].iat[i], df["Avg Profit"].iat[i]), xytext=(4,4), textcoords="offset points", fontsize=9)
            ax1.set_xlabel("Avg Popularity")
            ax1.set_ylabel("Avg Profit")
            ax1.set_title("Popularity vs Profit by Genre")
            st.pyplot(fig1)

            df_top = df.sort_values("Avg Profit", ascending=False).head(20).reset_index(drop=True)
            df_top["Avg Profit (M)"] = df_top["Avg Profit"].astype(float) / 1_000_000

            fig2, ax2 = plt.subplots(figsize=(10,6))
            ax2.barh(df_top["Genre"], df_top["Avg Profit (M)"])
            ax2.set_xlabel("Avg Profit (millions)")
            ax2.set_title("Top 20 Genres by Avg Profit (millions)")
            plt.tight_layout()
            st.pyplot(fig2)

elif option == "Genres Popularity in a Time Period":

    start_year = st.number_input("Start year", min_value=1900, max_value=2100, value=2000, step=1)
    end_year   = st.number_input("End year",   min_value=1900, max_value=2100, value=2020, step=1)

    if st.button("Run Query"):
        rows = get_genre_popularity_in_period(start_year, end_year)
        if not rows:
            st.write("No data found for the selected period.")
        else:
            df = pd.DataFrame(rows, columns=["Genre", "Movie Count", "Avg Popularity", "Total Popularity"])
            st.dataframe(df)

            df_plot = df.sort_values("Avg Popularity", ascending=False).head(20).reset_index(drop=True)
            fig, ax = plt.subplots(figsize=(10,6))
            ax.barh(df_plot["Genre"], df_plot["Avg Popularity"])
            ax.set_xlabel("Average Popularity")
            ax.set_title(f"Top Genres by Average Popularity ({start_year} - {end_year})")
            plt.gca().invert_yaxis() 
            plt.tight_layout()
            st.pyplot(fig)

elif option == "Movies that made a loss":
    if st.button("Run Query"):
        rows = get_loss_making_movies()
        df = pd.DataFrame(rows, columns=["Title", "Budget", "Revenue", "Loss"])
        st.dataframe(df)
