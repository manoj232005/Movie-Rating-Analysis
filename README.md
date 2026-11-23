# CineScope — Movie Rating Analysis

An educational Big Data Analytics project that explores movie ratings using **Apache Spark (PySpark)** and presents insights in an interactive **Streamlit** dashboard.

## Features
- Overall dataset KPIs: ratings, users, movies and average score
- Rating distribution
- Genre-level rating volume and average rating
- Most-rated movies
- Highest-rated movies with a minimum-rating threshold to reduce small-sample bias
- Most active users
- Release-year rating trends
- Simple rating-based movie suggestions (not personalized)

## Technology
- Python
- Apache Spark / PySpark for distributed-style data processing
- Streamlit for the web interface
- Plotly for interactive visualizations
- MovieLens latest-small dataset from GroupLens Research

## Requirements
- Python 3.9–3.12 recommended
- Java 8, 11 or 17 (Java 17 recommended for current PySpark releases)
- Git (for version control)

## Run on Windows
1. Install Python and Java. Ensure `python`, `java` and `git` work in a terminal.
2. Open this folder in VS Code.
3. Open Terminal → New Terminal.
4. Create and activate a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks activation, use Command Prompt and run:

```cmd
.venv\Scripts\activate.bat
```

5. Install dependencies:

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
```

6. Launch:

```bash
streamlit run app.py
```

The dashboard opens in your browser. On first run, the app downloads the MovieLens latest-small dataset (approximately 1 MB) into `data/`.

## Dataset
MovieLens latest-small includes `movies.csv` and `ratings.csv`. The app downloads it from:
https://files.grouplens.org/datasets/movielens/ml-latest-small.zip

If automatic download fails, download the ZIP manually from https://grouplens.org/datasets/movielens/, extract it, and copy `movies.csv` and `ratings.csv` into the `data/` folder.

## How Spark is used
The app reads CSV files into Spark DataFrames, removes incomplete rows, filters invalid ratings, and performs distributed aggregations:
- counts and averages
- group-by movie and user
- genre expansion and aggregation
- joins between ratings and movie metadata
- release-year trend calculations

The Spark session uses local mode (`local[*]`) so it can run on a student laptop without a Hadoop cluster. This demonstrates Spark DataFrame processing, but it is not a multi-node cluster deployment.

## GitHub upload
Create an empty public repository on GitHub named `Movie-Rating-Analysis`. Then run these commands from this folder:

```bash
git init
git add .
git commit -m "Initial commit - Movie Rating Analysis dashboard"
git branch -M main
git remote add origin https://github.com/YOUR-USERNAME/Movie-Rating-Analysis.git
git push -u origin main
```

Replace `YOUR-USERNAME` with your GitHub username. Do not commit virtual environments or secrets.

## Project limitations
- The recommendation table is a simple popularity/average-rating baseline, not a personalized recommendation model.
- Spark runs locally on one machine; no cluster is configured.
- MovieLens ratings are a sample of user-submitted preferences and may not represent all audiences.
