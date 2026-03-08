# IV HRI Scraper

## What our scraper can do: 

We can parse through 5+ Imperial Valley news sources (English + Spanish). We utilize newspaper4k for intelligent article extraction. With our inventory of heat related keywords and contexts, we implement a weighted scoring protocol in which words that are more indicative of heat related illness are rated higher. We also make note of the article link, date accessed, publish date, authors, and the key words + contexts that our scraper flagged. We then organize our results in an SQLite database. We have limitations put in place to ensure ethical rate limiting and human-in-the-loop error handling. Our model generates detailed reports and logs. 

---

## Quick Start 

### 1️ Install Dependencies
```bash
pip install newspaper4k beautifulsoup4 requests lxml --break-system-packages
```

### 2️ Run the Scraper
```bash
python imperial_valley_scraper.py
```

### 3️ View Results
```bash
# Check the report
cat scraping_report.txt

# Query the database
sqlite3 imperial_valley_heat_deaths.db "SELECT title, heat_score FROM articles ORDER BY heat_score DESC LIMIT 10;"
```


---

## Features

### Bilingual Support
```python
# English keywords
'heat-related death', 'heat stroke', 'died from heat'

# Spanish keywords  
'muerte por calor', 'golpe de calor', 'hipertermia fatal'
```

### Weighted Scoring Tool
```
Primary Death Keywords:    10 points each
Location Specific:          8 points each
Contextual Death:           7 points each
Heat Illness:               5 points each
Medical/Coroner:            3 points each
Environmental Context:      2 points each
```

### News Sources Configured
1. Imperial Valley Press (ivpressonline.com)
2. Calexico Chronicle (calexicochronicle.com)
3. Holtville Tribune (holtvilletribune.com)
4. The Desert Review (thedesertreview.com)
5. Adelante Valle (Spanish section)

### Database Tables
- **articles** - All scraped content with scores
- **keyword_matches** - Detailed match tracking
- **scrape_sessions** - Session logs and statistics
- **error_log** - Error tracking for debugging

---

## Scoring Ex

**Article:** "First probable heat-related deaths reported"

```
✓ "heat-related death" × 2     = 20 points
✓ "heat stroke" × 1             = 5 points  
✓ "heat exhaustion" × 2         = 10 points
✓ "excessive heat warning" × 2  = 4 points

TOTAL: 39 points → HIGHLY RELEVANT 
```

---

## BEFORE RUNNING!!

### 1. Check robots.txt
```bash
# Example
curl https://www.ivpressonline.com/robots.txt
```

### 2. Hit a little test run
Edit `imperial_valley_scraper.py`:
```python
MAX_ARTICLES_PER_SOURCE = 10  # Just 10 articles for testing
NEWS_SOURCES = [NEWS_SOURCES[0]]  # Just IV Press
```

### 3. Monitor the Log
```bash
tail -f scraper.log
```

---

## If you would like, you can customize it!
## You can: 

### Add More Sources
```python
NEWS_SOURCES.append({
    'name': 'New Source',
    'url': 'https://newssite.com',
    'sections': ['/local-news/'],
    'language': 'en',
    'bias': 'LOCAL-UNRATED'
})
```

### Adjust the sensitivity of what is saved to the DB
```python
# More strict (only save high-scoring articles)
if score >= 20:  # Instead of score > 0
    save_article(...)

# More permissive
KEYWORDS_EN['primary_death']['weight'] = 15  # Increase from 10
```

### Export to a CSV file
```python
import sqlite3, csv

conn = sqlite3.connect('imperial_valley_heat_deaths.db')
cursor = conn.cursor()
cursor.execute('SELECT title, source, heat_score, published_date FROM articles WHERE heat_score > 0')

with open('heat_deaths.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Title', 'Source', 'Score', 'Date'])
    writer.writerows(cursor.fetchall())
```




