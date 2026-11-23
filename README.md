# 🎬 CineScope — Movie Rating Analysis & Discovery

<p align="center">
  **Big Data Analytics Project | Apache Spark | PySpark | Streamlit**
</p>

CineScope is an interactive movie rating analytics dashboard built using **Apache Spark (PySpark)** and **Streamlit**. It analyzes the MovieLens dataset to discover movie popularity, genre trends, rating distributions, user activity and year-wise rating patterns.

The project demonstrates how distributed data processing concepts can be applied to real-world movie rating data.

---

## 📸 Dashboard Preview

### 1. Dashboard Overview
![Dashboard Overview](screenshots/01-overview.png)

### 2. Genre Insights
![Genre Insights](screenshots/02-genre-insights.png)

### 3. Movie Rankings
![Movie Rankings](screenshots/03-movie-rankings.png)

### 4. User Activity
![User Activity](screenshots/04-user-activity.png)

### 5. Year-wise Analysis
![Year-wise Analysis](screenshots/05-year-analysis.png)

### 6. Source Code
![Source Code](screenshots/06-project-code.png)

### 7. Project Structure
![Project Structure](screenshots/07-project-structure.png)

### 8. Application Running
![Terminal Output](screenshots/08-terminal-output.png)

### 9. Dataset
![Dataset Files](screenshots/09-dataset-files.png)

---

## 🚀 Features

- **Movie Statistics:** Analyze total movies, ratings and average rating.
- **Genre Analysis:** Compare movie genres based on rating counts and average ratings.
- **Movie Rankings:** Identify popular and highly rated movies.
- **User Activity:** Explore user rating patterns and activity levels.
- **Rating Distribution:** Understand how users rate movies.
- **Year-wise Analysis:** Explore rating trends across movie release years.
- **Interactive Dashboard:** Explore results through Streamlit and Plotly visualizations.

## 🛠️ Technologies Used

| Technology | Purpose |
|---|---|
| Python | Programming language |
| Apache Spark | Big data processing engine |
| PySpark | Spark DataFrame operations |
| Streamlit | Interactive dashboard |
| Pandas | Data handling and conversion |
| Plotly | Interactive visualizations |
| MovieLens | Movie rating dataset |

## 📊 Dataset

This project uses the **MovieLens latest-small dataset** published by GroupLens.

Dataset: [MovieLens Latest Small](https://grouplens.org/datasets/movielens/latest/)

The dataset includes:
- `movies.csv` — Movie IDs, titles and genres.
- `ratings.csv` — User ratings and timestamps.

The application reads these CSV files and processes them using PySpark.

## 🏗️ System Architecture

```text
       MovieLens Dataset
               |
               v
        CSV Data Files
               |
               v
       PySpark DataFrames
               |
               v
       Data Cleaning &
       Transformation
               |
               v
       Aggregation &
       Statistical Analysis
               |
               v
       Pandas DataFrames
               |
               v
       Streamlit Dashboard
               |
               v
       Plotly Visualizations
```

## ⚙️ Installation and Setup

### Prerequisites

- Python installed
- Java Development Kit (JDK 17 recommended)
- Git
- Visual Studio Code (optional)

### 1. Clone the repository

```bash
git clone https://github.com/YOUR-USERNAME/Movie-Rating-Analysis.git
cd Movie-Rating-Analysis
```

### 2. Create a virtual environment

Windows:

```powershell
py -m venv .venv
```

### 3. Activate the environment

```powershell
.venv\Scripts\activate
```

### 4. Install dependencies

```powershell
py -m pip install -r requirements.txt
```

### 5. Run the application

```powershell
py -m streamlit run app.py
```

Open the local URL displayed in the terminal, usually:

```text
http://localhost:8501
```

### 6. Dataset setup

If the dataset is not already present, download the MovieLens latest-small ZIP file:

https://files.grouplens.org/datasets/movielens/ml-latest-small.zip

Extract it and place `movies.csv` and `ratings.csv` inside the `data/` directory.

## 📁 Project Structure

```text
Movie-Rating-Analysis/
│
├── app.py
├── requirements.txt
├── README.md
├── .gitignore
├── run_windows.bat
│
├── data/
│   ├── README.txt
│   ├── movies.csv
│   └── ratings.csv
│
└── screenshots/
    ├── 01-overview.png
    ├── 02-genre-insights.png
    ├── 03-movie-rankings.png
    ├── 04-user-activity.png
    ├── 05-year-analysis.png
    ├── 06-project-code.png
    ├── 07-project-structure.png
    ├── 08-terminal-output.png
    └── 09-dataset-files.png
```

## 🔍 Big Data Processing Concepts

The project demonstrates several data processing operations using PySpark:

- Data ingestion using Spark DataFrames.
- Data cleaning and filtering.
- Data transformation using column operations.
- Grouping and aggregation.
- Joining movie and rating datasets.
- Sorting and ranking.
- Statistical analysis of ratings.

## ⚠️ Project Scope and Limitations

- Uses the MovieLens latest-small benchmark dataset.
- Spark runs in local mode on the development machine (`local[*]`).
- This is an educational Big Data analytics project, not a multi-node production cluster.
- Movie rankings are based on available user ratings and minimum rating-count thresholds.
- The project demonstrates rating-based analysis, not personalized machine-learning recommendations.

## 🔮 Future Enhancements

- Integrate a larger MovieLens dataset.
- Deploy Spark on a multi-node cluster.
- Implement collaborative filtering using Spark MLlib.
- Develop personalized movie recommendations.
- Add additional interactive filters and visualizations.
- Deploy the dashboard online.

## 🎓 Project Information

**Project Title:** Movie Rating Analysis & Discovery

**Project Name:** CineScope

**Domain:** Big Data Analytics

**Processing Engine:** Apache Spark (PySpark)

**Dashboard Framework:** Streamlit

---

*Developed as an academic Big Data Analytics project to explore movie ratings and demonstrate large-scale data processing concepts.*