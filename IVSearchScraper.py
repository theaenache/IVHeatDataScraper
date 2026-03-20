"""
SEARCH-BASED IMPERIAL VALLEY HEAT DEATH SCRAPER
================================================

Uses site search engines to find heat-related articles.
Much more efficient than browsing - only fetches relevant articles!

Advantages:
- 75%+ hit rate (vs 4% browsing)
- Fewer requests = less likely to get blocked
- Targeted results only
- Better for rate limiting
"""

import sqlite3
import re
import hashlib
import time
import random
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import logging

try:
    from newspaper import Article, Config
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: Required packages not installed.")
    print("Install with: pip install newspaper4k beautifulsoup4 requests lxml")
    exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_search.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Longer delays to avoid rate limiting
REQUEST_DELAY_MIN = 25
REQUEST_DELAY_MAX = 40

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 60  # Wait 1 minute before retry

MIN_SCORE_THRESHOLD = 10

# ============================================================================
# KEYWORDS (Same as before)
# ============================================================================

# English Keywords (EXACT same as working scraper)
KEYWORDS_EN = {
    'primary_death': {  # Weight: 10
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
    'heat_illness': {  # Weight: 5
        'keywords': [
            r'heat\s+stroke', r'heat\s+exhaustion', r'hyperthermia',
            r'heat\s+illness', r'heat\s+related\s+illness', r'heat\s+emergency',
            r'heat-associated', r'severe\s+dehydration'
        ],
        'weight': 5
    },
    'contextual_death': {  # Weight: 7
        'keywords': [
            r'found\s+dead.*heat', r'body\s+found.*heat',
            r'unresponsive.*heat', r'pronounced\s+dead.*heat',
            r'died\s+after.*heat\s+wave', r'succumbed.*heat',
            r'extreme\s+heat.*died'
        ],
        'weight': 7
    },
    'environmental': {  # Weight: 2
        'keywords': [
            r'excessive\s+heat\s+warning', r'heat\s+wave', r'extreme\s+heat',
            r'triple-digit\s+temperature', r'record\s+heat', r'blistering\s+heat',
            r'record\s+breaking\s+heat', r'dangerous\s+heat', r'heat\s+advisory',
            r'scorching\s+(?:heat|temperature)', r'heat\s+claims\s+lives',
            r'deadly\s+heat', r'heat\s+turns\s+deadly'
        ],
        'weight': 2
    },
    'location_specific': {  # Weight: 8
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
    'medical_coroner': {  # Weight: 3
        'keywords': [
            r'coroner.*heat', r'medical\s+examiner.*heat', r'autopsy.*heat',
            r'cause\s+of\s+death.*heat', r'heat\s+related\s+cause',
            r'environmental\s+heat.*death', r'heat\s+as\s+contributing\s+factor'
        ],
        'weight': 3
    },
    'exclusions': {  # Auto-reject
        'keywords': [
            r'heated\s+argument', r'heated\s+debate', r'heat\s+of\s+the\s+moment',
            r'preheat', r'heat\s+pump', r'heating\s+system', r'heated\s+game',
            r'heated\s+competition'
        ],
        'weight': -100
    }
}

# Spanish Keywords (from working scraper)
KEYWORDS_ES = {
    'primary_death': {
        'keywords': [
            r'muerte\s+por\s+calor', r'falleció\s+por\s+calor',
            r'murió\s+por\s+calor', r'sucumbió\s+por\s+calor',
            r'falleció\s+por\s+el\s+calor', r'hipertermia\s+fatal'
        ],
        'weight': 10
    },
    'heat_illness': {
        'keywords': [
            r'golpe\s+de\s+calor', r'insolación', r'hipertermia',
            r'deshidratación\s+severa', r'enfermedad\s+por\s+calor'
        ],
        'weight': 5
    },
    'environmental': {
        'keywords': [
            r'ola\s+de\s+calor', r'calor\s+extremo', r'temperatura\s+récord',
            r'aviso\s+de\s+calor', r'calor\s+peligroso', r'calor\s+mortal'
        ],
        'weight': 2
    }
}

# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================

def init_database(db_path: str = 'imperial_valley_heat_deaths.db') -> sqlite3.Connection:
    """Initialize SQLite database."""
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
            scraped_date TIMESTAMP DEFAULT CURRENT TIMESTAMP,
            text_content TEXT,
            heat_score REAL,
            category TEXT,
            search_keywords TEXT,
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
    logger.info(f"Database initialized: {db_path}")
    return conn

# ============================================================================
# SCORING FUNCTIONS
# ============================================================================

def calculate_heat_score(text: str, language: str = 'en') -> Tuple[float, List[Dict], Dict]:
    """Calculate heat-related death relevance score."""
    text_lower = text.lower()
    score = 0
    all_matches = []
    category_scores = {}
    
    keywords_dict = KEYWORDS_EN
    
    # Check exclusions first
    for pattern in keywords_dict['exclusions']['keywords']:
        if re.search(pattern, text_lower, re.IGNORECASE):
            return 0.0, [], {}
    
    # Score each category
    for category, config in keywords_dict.items():
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
        
        if all_matches:
            category_matches = [m for m in all_matches if m['category'] == category]
            if category_matches:
                category_scores[category] = {
                    'matches': len(category_matches),
                    'score': sum(m['points'] for m in category_matches)
                }
    
    return score, all_matches, category_scores

def classify_relevance(score: float) -> str:
    """Classify article relevance based on score."""
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
# SEARCH FUNCTIONS
# ============================================================================

def search_imperial_valley_press(keywords: str, start_date: datetime, 
                                  end_date: datetime, max_results: int = 50) -> List[str]:
    """Search Imperial Valley Press using their search engine."""
    logger.info("Searching Imperial Valley Press...")
    
    # Their search URL format
    search_url = f"https://www.ivpressonline.com/search/?q={keywords.replace(' ', '+')}&sd={start_date.strftime('%m/%d/%Y')}&ed={end_date.strftime('%m/%d/%Y')}"
    
    urls = []
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Academic Research - Heat Death Study)'}
        response = requests.get(search_url, headers=headers, timeout=15)
        
        if response.status_code == 429:
            logger.warning("Rate limited on search - waiting...")
            time.sleep(RETRY_DELAY)
            response = requests.get(search_url, headers=headers, timeout=15)
        
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find article links
        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'article_' in href and '.html' in href:
                if href.startswith('/'):
                    full_url = 'https://www.ivpressonline.com' + href
                elif href.startswith('http'):
                    full_url = href
                else:
                    continue
                
                if full_url not in urls:
                    urls.append(full_url)
                    
                if len(urls) >= max_results:
                    break
        
        logger.info(f"  Found {len(urls)} articles")
        
    except Exception as e:
        logger.error(f"Search failed: {e}")
    
    return urls

# ============================================================================
# SCRAPING FUNCTIONS WITH RETRY
# ============================================================================

def get_url_hash(url: str) -> str:
    """Generate hash for URL."""
    return hashlib.md5(url.encode()).hexdigest()

def check_if_scraped(url: str, conn: sqlite3.Connection) -> bool:
    """Check if URL already in database."""
    cursor = conn.cursor()
    url_hash = get_url_hash(url)
    cursor.execute('SELECT id FROM articles WHERE url_hash = ?', (url_hash,))
    return cursor.fetchone() is not None

def scrape_article_with_retry(url: str, language: str = 'en', 
                               max_retries: int = MAX_RETRIES) -> Optional[Dict]:
    """Scrape article with retry logic for rate limiting."""
    
    for attempt in range(max_retries):
        try:
            config = Config()
            config.browser_user_agent = 'Mozilla/5.0 (Academic Research - Heat Death Study)'
            config.request_timeout = 15
            config.language = language
            
            article = Article(url, config=config, language=language)
            article.download()
            article.parse()
            
            article_data = {
                'url': url,
                'url_hash': get_url_hash(url),
                'title': article.title,
                'text': article.text,
                'published_date': article.publish_date.strftime('%Y-%m-%d') if article.publish_date else None,
                'language': language
            }
            
            logger.info(f"  ✓ Scraped: {article.title[:60]}...")
            return article_data
            
        except Exception as e:
            if '429' in str(e):
                if attempt < max_retries - 1:
                    wait_time = RETRY_DELAY * (attempt + 1)
                    logger.warning(f"  ⚠️ Rate limited (429) - waiting {wait_time}s before retry {attempt+1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"  ✗ Failed after {max_retries} retries: {e}")
                    return None
            else:
                logger.error(f"  ✗ Error scraping: {e}")
                return None
    
    return None

def save_article(article_data: Dict, source_name: str, score: float, 
                 matches: List[Dict], category: str, search_keywords: str,
                 conn: sqlite3.Connection) -> int:
    """Save article to database."""
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO articles 
            (source, url, url_hash, title, published_date, text_content,
             heat_score, category, search_keywords)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            source_name, article_data['url'], article_data['url_hash'],
            article_data['title'], article_data.get('published_date'),
            article_data['text'], score, category, search_keywords
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
            logger.info(f"  💾 SAVED (ID: {article_id}, Score: {score:.1f})")
            return article_id
        
    except Exception as e:
        logger.error(f"  ✗ Save failed: {e}")
        conn.rollback()
        return -1
    
    return -1

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main search-based scraping workflow."""
    print("="*80)
    print("SEARCH-BASED IMPERIAL VALLEY HEAT DEATH SCRAPER")
    print("="*80)
    
    # Get search parameters
    print("\n🔍 SEARCH CONFIGURATION")
    print("-" * 80)
    print("Enter search keywords for heat-related deaths.")
    print("\nRecommended keywords:")
    print('  heat death OR "heat stroke" OR "heat wave" OR "heat illness"')
    print('  "heat-related death" OR hyperthermia OR "heat exhaustion"')
    print("-" * 80)
    
    keywords = input("\nEnter search keywords: ").strip()
    if not keywords:
        keywords = 'heat death OR "heat stroke" OR "heat wave"'
        print(f"Using default: {keywords}")
    
    # Get date range
    print("\n📅 DATE RANGE")
    print("-" * 80)
    
    while True:
        start_str = input("Start date (YYYY-MM-DD): ").strip()
        try:
            start_date = datetime.strptime(start_str, '%Y-%m-%d')
            break
        except:
            print("❌ Invalid format. Use YYYY-MM-DD")
    
    while True:
        end_str = input("End date (YYYY-MM-DD): ").strip()
        try:
            end_date = datetime.strptime(end_str, '%Y-%m-%d')
            if end_date < start_date:
                print("❌ End date must be after start date")
                continue
            break
        except:
            print("❌ Invalid format. Use YYYY-MM-DD")
    
    # Get max results
    max_results_str = input("\nMax results per source (default 50): ").strip()
    max_results = int(max_results_str) if max_results_str else 50
    
    print(f"\n✓ Configuration:")
    print(f"  Keywords: {keywords}")
    print(f"  Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"  Max results per source: {max_results}")
    
    # Initialize database
    conn = init_database()
    
    # Search phase
    print(f"\n{'='*80}")
    print("PHASE 1: SEARCHING")
    print(f"{'='*80}")
    
    all_urls = []
    
    # Search Imperial Valley Press
    urls = search_imperial_valley_press(keywords, start_date, end_date, max_results)
    for url in urls:
        all_urls.append(('Imperial Valley Press', url))
    
    print(f"\n📊 SEARCH SUMMARY")
    print(f"{'='*80}")
    print(f"Total URLs found: {len(all_urls)}")
    
    if len(all_urls) == 0:
        print("\n⚠️  No articles found for these keywords.")
        print("Try different keywords or date range.")
        return
    
    # Scraping phase
    print(f"\n{'='*80}")
    print("PHASE 2: SCRAPING & SCORING")
    print(f"{'='*80}")
    print(f"\n⏰ Using {REQUEST_DELAY_MIN}-{REQUEST_DELAY_MAX} second random delays")
    print("⏰ This will take a while to avoid rate limiting...\n")
    
    stats = {
        'total': len(all_urls),
        'processed': 0,
        'saved': 0,
        'score_too_low': 0,
        'errors': 0,
        'already_scraped': 0
    }
    
    for i, (source, url) in enumerate(all_urls, 1):
        print(f"\n[{i}/{len(all_urls)}] {url[:80]}...")
        
        # Check if already scraped
        if check_if_scraped(url, conn):
            print("  → Already in database")
            stats['already_scraped'] += 1
            continue
        
        # Random delay to look more human
        delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
        time.sleep(delay)
        
        # Scrape with retry
        article_data = scrape_article_with_retry(url)
        if not article_data:
            stats['errors'] += 1
            continue
        
        stats['processed'] += 1
        
        # Score
        full_text = article_data['title'] + '\n\n' + article_data['text']
        score, matches, categories = calculate_heat_score(full_text, 'en')
        relevance = classify_relevance(score)
        
        print(f"  Score: {score:.1f} ({relevance})")
        
        if score >= MIN_SCORE_THRESHOLD:
            for category, data in categories.items():
                print(f"    - {category}: {data['matches']} matches ({data['score']} pts)")
            
            article_id = save_article(
                article_data, source, score, matches, relevance, keywords, conn
            )
            if article_id > 0:
                stats['saved'] += 1
        else:
            print(f"  → Score below threshold ({MIN_SCORE_THRESHOLD}), not saving")
            stats['score_too_low'] += 1
    
    # Final summary
    print(f"\n{'='*80}")
    print("FINAL SUMMARY")
    print(f"{'='*80}")
    print(f"Search keywords: {keywords}")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"URLs found: {stats['total']}")
    print(f"Successfully processed: {stats['processed']}")
    print(f"Already in database: {stats['already_scraped']}")
    print(f"Saved (score >= {MIN_SCORE_THRESHOLD}): {stats['saved']}")
    print(f"Score too low: {stats['score_too_low']}")
    print(f"Errors: {stats['errors']}")
    
    hit_rate = (stats['saved'] / stats['processed'] * 100) if stats['processed'] > 0 else 0
    print(f"\n✨ Hit rate: {hit_rate:.1f}% (articles saved / processed)")
    
    if stats['saved'] > 0:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT title, heat_score, category
            FROM articles
            ORDER BY scraped_date DESC
            LIMIT 10
        ''')
        
        print(f"\n📰 NEWLY SAVED ARTICLES (Top 10):")
        for i, (title, score, category) in enumerate(cursor.fetchall(), 1):
            print(f"\n{i}. [{score:.1f}] {title[:70]}")
            print(f"   {category}")
    
    conn.close()
    print(f"\n✅ Complete! Database: imperial_valley_heat_deaths.db")

if __name__ == '__main__':
    main()
