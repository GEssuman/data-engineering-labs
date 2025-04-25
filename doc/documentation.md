
# TMDB Movie Data Analysis with PySpark

## Project Overview  
This project builds a scalable movie data analysis pipeline using **PySpark** and the **TMDB API**. The system fetches movie metadata, transforms and cleans the data, and provides KPIs and insights into trends like franchise performance, director success, and genre popularity.

---

## Technologies & Tools Used
- **PySpark** – Distributed data processing  
- **TMDB API** – Movie metadata source  
- **Matplotlib**, **Seaborn** – Visualization  
- **Python** – Core language  
- **JSON** – Intermediate data format  
- **Logging** – For pipeline transparency  
- **Jupyter Notebook / Python Scripts** – Development environment (`In Docker`)

---

## ETL Pipeline (Extract, Transform, Load)

### Extract
- Movie and credit data are fetched from the **TMDB API** using a list of movie IDs.
- Each response is parsed and loaded into **Spark DataFrames** using a defined schema.
- Fetched raw data is cached in JSON files locally for offline processing.

### Transform
- Dropped irrelevant columns (`adult`, `homepage`, `video`, etc.).
- Flattened and evaluated nested JSON fields (like `genres`, `production_companies`, `cast`).
- Replaced unrealistic values (e.g., zero budget/revenue), missing data, and known placeholders (`No Data`).
- Converted monetary fields (`budget`, `revenue`) to millions of USD.
- Ensured at least 10 non-null values per row and removed duplicates.
- Merged movie and credit datasets and filtered for **"Released"** movies.
- Reordered and reindexed columns for analysis.

### Load
- The transformed dataset is saved to disk as a single JSON file under `./data/processed_data`.

---

## KPI Generation & Metrics

Implemented core KPIs using custom logic:
- **Top Revenue and Budget Titles**
- **Top Rated Movies**
- **Genre Performance Averages**
- **Success Metrics by Year**
- **Director and Franchise Summaries**

All KPIs are logged and available for further export or reporting.

---

## Advanced Insights

### Actor & Director-Based Filtering
- Best Science Fiction Action movie starring **Bruce Willis**.
- Films directed by **Quentin Tarantino** featuring **Uma Thurman**.

### Franchise vs Standalone Analysis
- Compared success metrics like **ROI**, **revenue**, and **ratings** between franchise and standalone films.

### Top Franchises
- Identified franchises with the most financial and critical success using aggregated statistics.

---

## Visualization

Visualizations are created using **Matplotlib** and **Seaborn**, with highlights including:
- **Budget vs Revenue**
- **Return on Investment by Genre**
- **Popularity vs Rating Scatterplot**
- **Box Office Trends Over Time**
- **Franchise vs Standalone Success**

---

## Project Structure

```
.
├── main.py                # Entry point for ETL, KPI, and insight generation
├── utils.py               # Reusable transformation and helper functions
├── schema.py              # PySpark schemas for movie and credit data
├── data/
│   ├── movies_raw_data.json
│   ├── credits_raw_data.json
│   └── processed_data/
├── example.log            # Logs generated from the pipeline
```

---

## How to Run

---
## Docker-Based Setup
 ### Prerequisites
 - Docker
 - Docker Compose

 ### Environment Setup
 1. Create a `.env` file in the root directory:
 ```
 API_ACCESS_TOKEN=your_tmdb_api_key_here
 ```
 2. Thee Docker Compose file uses the env_file directivee to access this key

 ### Create The Docker Container
 ```
 docker-compose up --build
```
 ### Run the Spark File
 ```bash
 docker exec -it <container_name> spark-submit main.py
 ```
 or launch the Jupyter Notebook
 ```bash
 docker exec -it <container_name> jupyter notebok
 ```
---


