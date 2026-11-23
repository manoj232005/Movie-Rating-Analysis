import os
import zipfile
import urllib.request
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).parent
DATA_DIR = ROOT / "data"
MOVIES_CSV = DATA_DIR / "movies.csv"
RATINGS_CSV = DATA_DIR / "ratings.csv"
DATA_URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"

st.set_page_config(page_title="CineScope | Movie Rating Analytics", page_icon="🎬", layout="wide")

st.markdown("""
<style>
.stApp {background: linear-gradient(135deg,#0b1020 0%,#111827 55%,#17132b 100%); color:#f8fafc;}
[data-testid="stHeader"] {background: transparent;}
.block-container {padding-top: 1.5rem; max-width: 1450px;}
.hero {padding: 1.4rem 1.6rem; border:1px solid #303952; border-radius:18px;
       background:linear-gradient(110deg,rgba(91,33,182,.35),rgba(30,64,175,.18)); margin-bottom:1rem;}
.hero h1 {font-size:2.5rem; margin:0; color:#fff;}
.hero p {color:#cbd5e1; margin:.35rem 0 0 0;}
div[data-testid="stMetric"] {background:#171f35; border:1px solid #303952; padding:15px; border-radius:14px;}
section[data-testid="stSidebar"] {background:#10182b;}
h2,h3 {color:#e9e7ff;}
.small-note {color:#9ca3af; font-size:.88rem;}
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_spark():
    return (SparkSession.builder
            .appName("MovieRatingAnalysis")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate())

def ensure_dataset():
    DATA_DIR.mkdir(exist_ok=True)
    if MOVIES_CSV.exists() and RATINGS_CSV.exists():
        return "MovieLens latest-small dataset"
    zip_path = DATA_DIR / "ml-latest-small.zip"
    try:
        with st.spinner("Downloading MovieLens dataset (about 1 MB)..."):
            urllib.request.urlretrieve(DATA_URL, zip_path)
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(DATA_DIR)
            extracted = DATA_DIR / "ml-latest-small"
            (extracted / "movies.csv").replace(MOVIES_CSV)
            (extracted / "ratings.csv").replace(RATINGS_CSV)
            zip_path.unlink(missing_ok=True)
            try:
                extracted.rmdir()
            except OSError:
                pass
        return "MovieLens latest-small dataset"
    except Exception as exc:
        st.error(f"Could not download MovieLens automatically: {exc}")
        st.info("Check your internet connection and refresh the page. You can also manually download ml-latest-small.zip from https://grouplens.org/datasets/movielens/ and extract movies.csv and ratings.csv into the data folder.")
        st.stop()

@st.cache_data
def load_analysis(_cache_key):
    spark = get_spark()
    movies_spark = spark.read.option("header", True).option("inferSchema", True).csv(str(MOVIES_CSV))
    ratings_spark = spark.read.option("header", True).option("inferSchema", True).csv(str(RATINGS_CSV))
    movies_spark = movies_spark.dropna(subset=["movieId", "title"])
    ratings_spark = ratings_spark.dropna(subset=["userId", "movieId", "rating"])
    ratings_spark = ratings_spark.filter((F.col("rating") >= 0.5) & (F.col("rating") <= 5.0))

    summary = ratings_spark.agg(
        F.count("*").alias("total_ratings"),
        F.countDistinct("userId").alias("total_users"),
        F.countDistinct("movieId").alias("rated_movies"),
        F.round(F.avg("rating"), 2).alias("avg_rating")
    ).first().asDict()

    movie_stats = (ratings_spark.groupBy("movieId")
                   .agg(F.count("*").alias("rating_count"),
                        F.round(F.avg("rating"), 2).alias("avg_rating"))
                   .join(movies_spark, "movieId", "inner")
                   .orderBy(F.desc("rating_count")))
    top_popular = movie_stats.limit(15).toPandas()

    well_rated = (movie_stats.filter(F.col("rating_count") >= 50)
                  .orderBy(F.desc("avg_rating"), F.desc("rating_count"))
                  .limit(15).toPandas())

    genre_rows = (movies_spark.select("movieId", F.explode(F.split("genres", r"\|")).alias("genre"))
                  .join(ratings_spark.select("movieId", "rating"), "movieId")
                  .groupBy("genre")
                  .agg(F.count("*").alias("rating_count"),
                       F.round(F.avg("rating"), 2).alias("avg_rating"))
                  .filter(F.col("genre") != "(no genres listed)")
                  .orderBy(F.desc("rating_count")).toPandas())

    distribution = (ratings_spark.groupBy("rating").count()
                    .orderBy("rating").toPandas())

    user_activity = (ratings_spark.groupBy("userId").count()
                     .orderBy(F.desc("count")).limit(15).toPandas())

    year_rows = (movies_spark
        .withColumn(
            "year_text",
            F.regexp_extract("title", r"\((\d{4})\)\s*$", 1)
        )
        .withColumn(
            "year",
            F.when(F.col("year_text") != "", F.col("year_text").cast("int"))
        )
        .filter(F.col("year").isNotNull() & (F.col("year") > 1900))
    )

    year_stats = (year_rows.join(ratings_spark, "movieId")
                  .groupBy("year")
                  .agg(F.count("*").alias("rating_count"),
                       F.round(F.avg("rating"), 2).alias("avg_rating"))
                  .orderBy("year").toPandas())

    # Recommendation demo: highest average rating among movies with at least 30 ratings.
    recommendations = (movie_stats.filter(F.col("rating_count") >= 30)
                       .orderBy(F.desc("avg_rating"), F.desc("rating_count"))
                       .limit(20).toPandas())

    return summary, top_popular, well_rated, genre_rows, distribution, user_activity, year_stats, recommendations


st.markdown("""
<div class="hero">
  <h1>🎬 CineScope</h1>
  <p>Movie Rating Analysis & Discovery · Big Data Analytics powered by Apache Spark</p>
</div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.title("🎞️ CineScope")
    st.caption("Analytics workspace")
    st.markdown("---")
    st.markdown("**Dataset**")
    st.write("MovieLens latest-small")
    st.markdown("**Processing engine**")
    st.write("Apache Spark (PySpark)")
    st.markdown("---")
    st.caption("Educational project · Ratings are user-submitted and do not represent universal quality.")

dataset_name = ensure_dataset()
try:
    data = load_analysis(str(MOVIES_CSV.stat().st_mtime) + str(RATINGS_CSV.stat().st_mtime))
except Exception as e:
    st.error("Spark could not process the dataset.")
    st.exception(e)
    st.stop()

summary, popular, well_rated, genres, distribution, users, years, recommendations = data

st.caption(f"Dataset: {dataset_name}  |  Processing: PySpark local mode")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total ratings", f"{summary['total_ratings']:,}")
c2.metric("Unique users", f"{summary['total_users']:,}")
c3.metric("Movies rated", f"{summary['rated_movies']:,}")
c4.metric("Average rating", f"{summary['avg_rating']:.2f} / 5")

tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🎭 Genre Insights", "🏆 Movie Rankings", "👥 User Activity"])

with tab1:
    left, right = st.columns(2)
    with left:
        st.subheader("Rating distribution")
        fig = px.bar(distribution, x="rating", y="count", text="count",
                     labels={"rating":"Star rating","count":"Number of ratings"},
                     color="rating", color_continuous_scale="Purples")
        fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)
    with right:
        st.subheader("Movie releases over time")
        if not years.empty:
            fig = px.line(years, x="year", y="rating_count", markers=True,
                          labels={"year":"Release year","rating_count":"Ratings received"})
            fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Release-year information is not available.")
    st.subheader("Most-rated movies")
    st.dataframe(popular[["title","rating_count","avg_rating"]].rename(
        columns={"title":"Movie","rating_count":"Number of ratings","avg_rating":"Average rating"}),
        use_container_width=True, hide_index=True)

with tab2:
    st.subheader("Which genres attract the most ratings?")
    fig = px.bar(genres.sort_values("rating_count", ascending=True), x="rating_count", y="genre",
                 orientation="h", color="avg_rating", color_continuous_scale="Viridis",
                 labels={"rating_count":"Number of ratings","genre":"Genre","avg_rating":"Average rating"})
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)
    st.subheader("Average rating by genre")
    st.dataframe(genres.sort_values("avg_rating", ascending=False).rename(
        columns={"genre":"Genre","rating_count":"Ratings","avg_rating":"Average rating"}),
        use_container_width=True, hide_index=True)

with tab3:
    st.subheader("Highest-rated movies")
    st.caption("To reduce small-sample bias, only movies with at least 50 ratings are included.")
    st.dataframe(well_rated[["title","avg_rating","rating_count"]].rename(
        columns={"title":"Movie","avg_rating":"Average rating","rating_count":"Number of ratings"}),
        use_container_width=True, hide_index=True)
    st.subheader("Popular movies")
    st.dataframe(popular[["title","rating_count","avg_rating"]].rename(
        columns={"title":"Movie","rating_count":"Number of ratings","avg_rating":"Average rating"}),
        use_container_width=True, hide_index=True)
    st.subheader("Simple rating-based recommendations")
    st.caption("A baseline ranking: high average rating, with a minimum of 30 ratings. This is not a personalized recommender.")
    st.dataframe(recommendations[["title","avg_rating","rating_count"]].rename(
        columns={"title":"Suggested movie","avg_rating":"Average rating","rating_count":"Number of ratings"}),
        use_container_width=True, hide_index=True)

with tab4:
    st.subheader("Most active users")
    fig = px.bar(users.sort_values("count", ascending=True), x="count", y="userId",
                 orientation="h", labels={"count":"Ratings submitted","userId":"User ID"})
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(users.rename(columns={"userId":"User ID","count":"Ratings submitted"}),
                 use_container_width=True, hide_index=True)

st.markdown("---")
st.markdown("<p class='small-note'>Built with Python, Apache Spark (PySpark), Streamlit, Pandas and Plotly. Dataset: MovieLens latest-small by GroupLens Research.</p>", unsafe_allow_html=True)
