
##Downloads articles from Archive.org snapshots instead of live site.
##(hopefully) NO RATE LIMITING Uses archived versions!


import sqlite3
import re
import hashlib
import time
import random
import requests
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from bs4 import BeautifulSoup
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('archive_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Config
REQUEST_DELAY_MIN = 2
REQUEST_DELAY_MAX = 4
MIN_SCORE_THRESHOLD = 10

# CDX API to find archived snapshots
CDX_API = "http://web.archive.org/cdx/search/cdx"

# Keywords (same as before)
KEYWORDS_EN = {
    'primary_death': {
        'keywords': [
            r'heat\s+death', r'heat-related\s+death', r'heat-caused\s+death',
            r'heat\s+fatality', r'died\s+from\s+heat', r'died\s+of\s+heat',
            r'heat\s+exposure\s+death', r'hyperthermia\s+death',
            r'heat\s+stroke\s+death', r'died\s+from\s+hyperthermia',
            r'succumbed\s+to\s+heat', r'heat\s+related\s+fatality',
            r'heat\s+victim', r'heat\s+casualty'
        ],
        'weight': 10
    },
    'heat_illness': {
        'keywords': [
            r'heat\s+stroke', r'heat\s+exhaustion', r'hyperthermia',
            r'heat\s+illness', r'heat\s+related\s+illness', r'heat\s+emergency',
            r'heat-associated', r'severe\s+dehydration'
        ],
        'weight': 5
    },
    'contextual_death': {
        'keywords': [
            r'found\s+dead.*heat', r'body\s+found.*heat',
            r'unresponsive.*heat', r'pronounced\s+dead.*heat',
            r'died\s+after.*heat\s+wave', r'succumbed.*heat',
            r'extreme\s+heat.*died'
        ],
        'weight': 7
    },
    'environmental': {
        'keywords': [
            r'excessive\s+heat\s+warning', r'heat\s+wave', r'extreme\s+heat',
            r'triple-digit\s+temperature', r'record\s+heat', r'blistering\s+heat',
            r'record\s+breaking\s+heat', r'dangerous\s+heat', r'heat\s+advisory',
            r'scorching\s+(?:heat|temperature)', r'heat\s+claims\s+lives',
            r'deadly\s+heat', r'heat\s+turns\s+deadly'
        ],
        'weight': 2
    },
    'location_specific': {
        'keywords': [
            r'died\s+in\s+vehicle.*heat', r'found\s+in\s+car.*heat',
            r'outdoor\s+death.*heat', r'homeless.*heat\s+death',
            r'farm\s+worker.*heat\s+death', r'agricultural\s+worker.*heat',
            r'(?:air\s+conditioning|A/C)\s+failure.*death',
            r'no\s+(?:A/C|air\s+conditioning).*death',
            r'mobile\s+home.*heat\s+death'
        ],
        'weight': 8
    },
    'medical_coroner': {
        'keywords': [
            r'coroner.*heat', r'medical\s+examiner.*heat', r'autopsy.*heat',
            r'cause\s+of\s+death.*heat', r'heat\s+related\s+cause',
            r'environmental\s+heat.*death', r'heat\s+as\s+contributing\s+factor'
        ],
        'weight': 3
    },
    'exclusions': {
        'keywords': [
            r'heated\s+argument', r'heated\s+debate', r'heat\s+of\s+the\s+moment',
            r'preheat', r'heat\s+pump', r'heating\s+system', r'heated\s+game',
            r'heated\s+competition'
        ],
        'weight': -100
    }
}

# ============================================================================
# db
# ============================================================================

def init_database(db_path: str = 'imperial_valley_heat_deaths.db') -> sqlite3.Connection:
    """Initialize database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            url TEXT UNIQUE NOT NULL,
            url_hash TEXT UNIQUE,
            title TEXT,
            published_date DATE,
            scraped_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            text_content TEXT,
            heat_score REAL,
            category TEXT,
            UNIQUE(url_hash)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS keyword_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER,
            keyword TEXT,
            keyword_type TEXT,
            match_count INTEGER,
            weight INTEGER,
            FOREIGN KEY (article_id) REFERENCES articles(id)
        )
    ''')
    
    conn.commit()
    return conn

# ============================================================================
# ARCHIVE.ORG FUNCTIONS
# ============================================================================

def find_archived_snapshot(url: str, start_date: str = "20240601", end_date: str = "20240930") -> Optional[str]:
    """
    Find a Wayback Machine snapshot for a URL within date range.
    
    Returns: Archive.org snapshot URL or None
    """
    params = {
        'url': url,
        'from': start_date,
        'to': end_date,
        'output': 'json',
        'fl': 'timestamp',
        'filter': 'statuscode:200',
        'limit': 1
    }
    
    try:
        response = requests.get(CDX_API, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if len(data) > 1:  # First row is headers
            timestamp = data[1][0]
            archive_url = f"https://web.archive.org/web/{timestamp}/{url}"
            return archive_url
        
    except Exception as e:
        logger.debug(f"No archived snapshot found for {url[:50]}...")
    
    return None

def scrape_from_archive(archive_url: str, original_url: str) -> Optional[Dict]:
    """Scrape article from Archive.org."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Academic Research)'}
        response = requests.get(archive_url, headers=headers, timeout=20)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract title
        title_tag = soup.find('h1') or soup.find('title')
        title = title_tag.get_text(strip=True) if title_tag else "No title"
        
        # Extract main content
        # Try common article content selectors
        content = None
        for selector in ['article', '.article-body', '.entry-content', 'main', '.content']:
            content_tag = soup.select_one(selector)
            if content_tag:
                content = content_tag.get_text(separator='\n', strip=True)
                break
        
        if not content:
            # Fall back to all paragraphs
            paragraphs = soup.find_all('p')
            content = '\n\n'.join([p.get_text(strip=True) for p in paragraphs])
        
        if len(content) < 100:
            logger.warning(f"  Content too short ({len(content)} chars)")
            return None
        
        return {
            'url': original_url,
            'url_hash': hashlib.md5(original_url.encode()).hexdigest(),
            'title': title,
            'text': content,
            'published_date': None
        }
        
    except Exception as e:
        logger.error(f"Error scraping archive: {e}")
        return None

# ============================================================================
# SCORING
# ============================================================================

def calculate_heat_score(text: str) -> Tuple[float, List[Dict], Dict]:
    """Calculate heat score."""
    text_lower = text.lower()
    score = 0
    all_matches = []
    category_scores = {}
    
    for pattern in KEYWORDS_EN['exclusions']['keywords']:
        if re.search(pattern, text_lower, re.IGNORECASE):
            return 0.0, [], {}
    
    for category, config in KEYWORDS_EN.items():
        if category == 'exclusions':
            continue
            
        for pattern in config['keywords']:
            matches = re.findall(pattern, text_lower, re.IGNORECASE)
            if matches:
                match_count = len(matches)
                points = match_count * config['weight']
                score += points
                
                for match in matches:
                    all_matches.append({
                        'keyword': match,
                        'category': category,
                        'weight': config['weight'],
                        'points': config['weight']
                    })
        
        category_matches = [m for m in all_matches if m['category'] == category]
        if category_matches:
            category_scores[category] = {
                'matches': len(category_matches),
                'score': sum(m['points'] for m in category_matches)
            }
    
    return score, all_matches, category_scores

def classify_relevance(score: float) -> str:
    """Classify relevance."""
    if score >= 50:
        return "EXTREMELY_RELEVANT"
    elif score >= 20:
        return "HIGHLY_RELEVANT"
    elif score >= 10:
        return "MODERATELY_RELEVANT"
    elif score > 0:
        return "MINIMALLY_RELEVANT"
    else:
        return "NOT_RELEVANT"

# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================

def check_if_scraped(url: str, conn: sqlite3.Connection) -> bool:
    """Check if already scraped."""
    cursor = conn.cursor()
    url_hash = hashlib.md5(url.encode()).hexdigest()
    cursor.execute('SELECT id FROM articles WHERE url_hash = ?', (url_hash,))
    return cursor.fetchone() is not None

def save_article(article_data: Dict, score: float, matches: List[Dict], 
                 category: str, conn: sqlite3.Connection) -> int:
    """Save article."""
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO articles 
            (source, url, url_hash, title, published_date, text_content,
             heat_score, category)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            'Imperial Valley Press',
            article_data['url'],
            article_data['url_hash'],
            article_data['title'],
            article_data.get('published_date'),
            article_data['text'],
            score,
            category
        ))
        
        article_id = cursor.lastrowid
        
        if article_id > 0:
            for match in matches:
                cursor.execute('''
                    INSERT INTO keyword_matches
                    (article_id, keyword, keyword_type, match_count, weight)
                    VALUES (?, ?, ?, ?, ?)
                ''', (article_id, match['keyword'], match['category'], 1, match['weight']))
            
            conn.commit()
            return article_id
        
    except Exception as e:
        logger.error(f"Save failed: {e}")
        conn.rollback()
        return -1
    
    return -1

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main workflow."""
    print("="*80)
    print("ARCHIVE-AWARE URL SCRAPER")
    print("="*80)
    print("\nDownloads articles from Archive.org snapshots")
    print("NO RATE LIMITING - Uses archived versions!\n")
    
    url_file = input("Enter URL file path (default: article_urls_sample_100.txt): ").strip()
    if not url_file:
        url_file = 'article_urls_sample_100.txt'
    
    try:
        with open(url_file, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        print(f"\n✓ Loaded {len(urls)} URLs from {url_file}")
    except FileNotFoundError:
        print(f"\nFile NOT FOUND: {url_file}")
        return
    
    conn = init_database()
    
    print(f"\n{'='*80}")
    print("SCRAPING FROM ARCHIVE.ORG")
    print(f"{'='*80}\n")
    
    stats = {
        'total': len(urls),
        'processed': 0,
        'saved': 0,
        'score_too_low': 0,
        'errors': 0,
        'already_scraped': 0,
        'no_archive': 0
    }
    
    for i, url in enumerate(urls, 1):
        print(f"\n[{i}/{len(urls)}] {url[:70]}...")
        
        if check_if_scraped(url, conn):
            print("  → Already in database")
            stats['already_scraped'] += 1
            continue
        
        # Find archived snapshot
        archive_url = find_archived_snapshot(url)
        if not archive_url:
            print("  ✗ No archived snapshot found")
            stats['no_archive'] += 1
            continue
        
        print(f"  → Found archive snapshot")
        
        # Delay
        delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
        time.sleep(delay)
        
        # Scrape from archive
        article_data = scrape_from_archive(archive_url, url)
        if not article_data:
            stats['errors'] += 1
            continue
        
        stats['processed'] += 1
        print(f"  ✓ Scraped: {article_data['title'][:60]}...")
        
        # Score
        full_text = article_data['title'] + '\n\n' + article_data['text']
        score, matches, categories = calculate_heat_score(full_text)
        relevance = classify_relevance(score)
        
        print(f"  Score: {score:.1f} ({relevance})")
        
        if score >= MIN_SCORE_THRESHOLD:
            for cat, data in categories.items():
                print(f"    - {cat}: {data['matches']} matches ({data['score']} pts)")
            
            article_id = save_article(article_data, score, matches, relevance, conn)
            if article_id > 0:
                stats['saved'] += 1
                print(f"  💾 SAVED (ID: {article_id})")
        else:
            print(f"  → Score below threshold ({MIN_SCORE_THRESHOLD})")
            stats['score_too_low'] += 1
    
    # Summary
    print(f"\n{'='*80}")
    print("FINAL SUMMARY")
    print(f"{'='*80}")
    print(f"URLs loaded: {stats['total']}")
    print(f"Processed: {stats['processed']}")
    print(f"Saved (score >= {MIN_SCORE_THRESHOLD}): {stats['saved']}")
    print(f"Score too low: {stats['score_too_low']}")
    print(f"No archive found: {stats['no_archive']}")
    print(f"Already in database: {stats['already_scraped']}")
    print(f"Errors: {stats['errors']}")
    
    hit_rate = (stats['saved'] / stats['processed'] * 100) if stats['processed'] > 0 else 0
    print(f"\n Hit rate: {hit_rate:.1f}%")
    
    conn.close()
    print(f"\n Complete! Database: imperial_valley_heat_deaths.db")

if __name__ == '__main__':
    main()
