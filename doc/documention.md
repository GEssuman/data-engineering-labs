# TMDB Movie Data Analysis – Project Documentation

## Project Overview
This project focuses on building a movie data analysis pipeline using **Python**, **Pandas**, and the **TMDB API**. The goal was to fetch movie data, clean and preprocess it, perform exploratory and advanced analyses, and visualize insights related to financial performance, popularity, franchises, and directors.

---

## 1. API Data Extraction
I utilized the **TMDB API** to retrieve detailed metadata for a selected list of movie IDs.


- Parsed JSON responses and stored them in a **Pandas DataFrame**
- Carefully reviewed the API documentation for structure and data interpretation

---

## 2. Data Cleaning & Preprocessing

### Data Preparation
- Dropped some irrelevant columns such as` adults`, `imdb_id` and `status`
- Extracted some key data from JSON-like columns (`genres`, `production_companies`)
- concatenated elemnets' list and separated elements with `"|"`

### Handling Missing & Incorrect Data
- Converted columns to appropriate types; e.g., "Converted release_date to datetime, cast numeric fields to float64".
- Replaced unrealistic values:
- Converted some columns such as`budget` and `revenue` to **million USD**
- Cleaned placeholder strings like `"No Data"` in `tagline` and `overview`
- Removed duplicates and rows missing essential values
- Kept only rows where at least **10 columns** had non-null values


### Finalizing DataFrame
- Reordered columns for analysis:


## 3. KPI Implementation & Analysis

### Ranking & Filtering
Implemented a **User-Defined Function (UDF)** to rank movies based on some columns:

### Advanced Movie Search Queries
Performed filtered queries such as:
- **Search 1**: Best-rated **Science Fiction Action** movies starring **Bruce Willis**
- **Search 2**: Movies starring **Uma Thurman** and directed by **Quentin Tarantino**, sorted by **runtime**

### Franchise vs. Standalone Performance
Compared metrics between franchise and standalone movies:

### Franchise & Director Success Metrics
- **Top Franchises** evaluated by:
- Total movies
- Total & Mean Budget/Revenue
- Average Rating
- **Top Directors** evaluated by:
- Movie Count
- Total Revenue
- Mean Rating

---

## 4. Data Visualization

Used **Matplotlib** and **Pandas** plotting for visual insights

- Revenue vs. Budget Trends
- ROI Distribution by Genre
- Popularity vs. Rating
- Yearly Box Office Trends
- Franchise vs. Standalone Performance

---


## Tools Used
- Python
- Pandas
- TMDB API
- Matplotlib
- Jupyter Notebook
- Git

---

## Conclusion
This project allowed me to demonstrate the end-to-end pipeline of working with external APIs, processing structured and nested data, performing insightful analysis using KPIs, and producing visualizations to support data-driven storytelling.
