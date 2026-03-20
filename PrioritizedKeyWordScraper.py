"""

Searches iteratively by keyword priority:
1. Primary death keywords (10 pts) - "heat death", "heat fatality"
2. Location-specific (8 pts) - "farm worker heat death"
3. Contextual death (7 pts) - "found dead heat"
4. Heat illness (5 pts) - "heat stroke", "heat exhaustion"
5. Medical/coroner (3 pts) - "coroner heat"
6. Environmental (2 pts) - "heat wave", "extreme heat"

Stops when enough articles found or all keywords exhausted.
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
        logging.FileHandler('scraper_priority_search.log'),
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
# PRIORITY-ORDERED KEYWORDS
# ============================================================================

# Keywords organized by priority (highest to lowest)
KEYWORD_PRIORITIES = [
    {
        'name': 'primary_death',
        'priority': 1,
        'weight': 10,
        'search_terms': [
            '"heat death"',
            '"heat-related death"',
            '"heat fatality"',
            '"died from heat"',
            '"heat stroke death"'
        ],
        'regex_patterns': [
            r'heat\s+death', r'heat-related\s+death', r'heat-caused\s+death',
            r'heat\s+fatality', r'died\s+from\s+heat', r'died\s+of\s+heat',
            r'heat\s+exposure\s+death', r'hyperthermia\s+death',
            r'heat\s+stroke\s+death', r'died\s+from\s+hyperthermia',
            r'succumbed\s+to\s+heat', r'heat\s+related\s+fatality',
            r'heat\s+victim', r'heat\s+casualty'
        ]
    },
    {
        'name': 'location_specific',
        'priority': 2,
        'weight': 8,
        'search_terms': [
            '"farm worker" heat death',
            '"agricultural worker" heat',
            'homeless "heat death"'
        ],
        'regex_patterns': [
            r'died\s+in\s+vehicle.*heat', r'found\s+in\s+car.*heat',
            r'outdoor\s+death.*heat', r'homeless.*heat\s+death',
            r'farm\s+worker.*heat\s+death', r'agricultural\s+worker.*heat',
            r'(?:air\s+conditioning|A/C)\s+failure.*death',
            r'no\s+(?:A/C|air\s+conditioning).*death',
            r'mobile\s+home.*heat\s+death'
        ]
    },
    {
        'name': 'contextual_death',
        'priority': 3,
        'weight': 7,
        'search_terms': [
            '"found dead" heat',
            '"body found" heat'
        ],
        'regex_patterns': [
            r'found\s+dead.*heat', r'body\s+found.*heat',
            r'unresponsive.*heat', r'pronounced\s+dead.*heat',
            r'died\s+after.*heat\s+wave', r'succumbed.*heat',
            r'extreme\s+heat.*died'
        ]
    },
    {
        'name': 'heat_illness',
        'priority': 4,
        'weight': 5,
        'search_terms': [
            '"heat stroke"',
            '"heat exhaustion"',
            'hyperthermia',
            '"heat illness"'
        ],
        'regex_patterns': [
            r'heat\s+stroke', r'heat\s+exhaustion', r'hyperthermia',
            r'heat\s+illness', r'heat\s+related\s+illness', r'heat\s+emergency',
            r'heat-associated', r'severe\s+dehydration'
        ]
    },
    {
        'name': 'medical_coroner',
        'priority': 5,
        'weight': 3,
        'search_terms': [
            'coroner heat',
            '"medical examiner" heat',
            'autopsy heat'
        ],
        'regex_patterns': [
            r'coroner.*heat', r'medical\s+examiner.*heat', r'autopsy.*heat',
            r'cause\s+of\s+death.*heat', r'heat\s+related\s+cause',
            r'environmental\s+heat.*death', r'heat\s+as\s+contributing\s+factor'
        ]
    },
    {
        'name': 'environmental',
        'priority': 6,
        'weight': 2,
        'search_terms': [
            '"heat wave"',
            '"extreme heat"',
            '"heat advisory"',
            '"excessive heat warning"'
        ],
        'regex_patterns': [
            r'excessive\s+heat\s+warning', r'heat\s+wave', r'extreme\s+heat',
            r'triple-digit\s+temperature', r'record\s+heat', r'blistering\s+heat',
            r'record\s+breaking\s+heat', r'dangerous\s+heat', r'heat\s+advisory',
            r'scorching\s+(?:heat|temperature)', r'heat\s+claims\s+lives',
            r'deadly\s+heat', r'heat\s+turns\s+deadly'
        ]
    }
]

# Exclusion patterns
EXCLUSION_PATTERNS = [
    r'heated\s+argument', r'heated\s+debate', r'heat\s+of\s+the\s+moment',
    r'preheat', r'heat\s+pump', r'heating\s+system', r'heated\s+game',
    r'heated\s+competition'
]

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
            scraped_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            text_content TEXT,
            heat_score REAL,
            category TEXT,
            search_priority INTEGER,
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

def calculate_heat_score(text: str) -> Tuple[float, List[Dict], Dict]:
    """Calculate heat-related death relevance score."""
    text_lower = text.lower()
    score = 0
    all_matches = []
    category_scores = {}
    
    # Check exclusions first
    for pattern in EXCLUSION_PATTERNS:
        if re.search(pattern, text_lower, re.IGNORECASE):
            return 0.0, [], {}
    
    # Score each priority category
    for priority_group in KEYWORD_PRIORITIES:
        category = priority_group['name']
        weight = priority_group['weight']
        
        for pattern in priority_group['regex_patterns']:
            matches = re.findall(pattern, text_lower, re.IGNORECASE)
            if matches:
                match_count = len(matches)
                points = match_count * weight
                score += points
                
                for match in matches:
                    all_matches.append({
                        'keyword': match,
                        'category': category,
                        'weight': weight,
                        'points': weight
                    })
        
        # Track category scores
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
# SEARCH FUNCTIONS (PRIORITY-BASED)
# ============================================================================

def search_by_priority(start_date: datetime, end_date: datetime, 
                       max_total_articles: int = 50) -> List[Dict]:
    """
    Search iteratively by priority until we have enough articles.
    
    Returns list of {url, priority, search_term}
    """
    all_urls = []
    seen_urls = set()
    
    print(f"\n{'='*80}")
    print("PRIORITY-BASED SEARCH")
    print(f"{'='*80}")
    print(f"Target: {max_total_articles} total articles")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n")
    
    for priority_group in KEYWORD_PRIORITIES:
        if len(all_urls) >= max_total_articles:
            print(f"\n✓ Reached target of {max_total_articles} articles, stopping search")
            break
        
        priority = priority_group['priority']
        name = priority_group['name']
        weight = priority_group['weight']
        
        print(f"\n{'─'*80}")
        print(f"PRIORITY {priority}: {name.upper()} (Weight: {weight} pts)")
        print(f"{'─'*80}")
        
        for search_term in priority_group['search_terms']:
            if len(all_urls) >= max_total_articles:
                break
            
            print(f"\nSearching: {search_term}")
            
            # Build search URL
            search_url = f"https://www.ivpressonline.com/search/?q={search_term.replace(' ', '+')}&sd={start_date.strftime('%m/%d/%Y')}&ed={end_date.strftime('%m/%d/%Y')}"
            
            try:
                headers = {'User-Agent': 'Mozilla/5.0 (Academic Research)'}
                response = requests.get(search_url, headers=headers, timeout=15)
                
                if response.status_code == 429:
                    logger.warning("  Rate limited - waiting 60s...")
                    time.sleep(60)
                    response = requests.get(search_url, headers=headers, timeout=15)
                
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                found_this_term = 0
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if 'article_' in href and '.html' in href:
                        if href.startswith('/'):
                            full_url = 'https://www.ivpressonline.com' + href
                        elif href.startswith('http'):
                            full_url = href
                        else:
                            continue
                        
                        if full_url not in seen_urls:
                            all_urls.append({
                                'url': full_url,
                                'priority': priority,
                                'category': name,
                                'search_term': search_term
                            })
                            seen_urls.add(full_url)
                            found_this_term += 1
                
                print(f"  → Found {found_this_term} new articles (Total: {len(all_urls)})")
                time.sleep(random.uniform(3, 6))  # Short delay between searches
                
            except Exception as e:
                logger.error(f"  ✗ Search failed: {e}")
    
    print(f"\n{'='*80}")
    print(f"SEARCH COMPLETE: {len(all_urls)} unique articles found")
    print(f"{'='*80}")
    
    return all_urls

# ============================================================================
# SCRAPING FUNCTIONS
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

def scrape_article_with_retry(url: str, max_retries: int = MAX_RETRIES) -> Optional[Dict]:
    """Scrape article with retry logic."""
    
    for attempt in range(max_retries):
        try:
            config = Config()
            config.browser_user_agent = 'Mozilla/5.0 (Academic Research)'
            config.request_timeout = 15
            
            article = Article(url, config=config)
            article.download()
            article.parse()
            
            return {
                'url': url,
                'url_hash': get_url_hash(url),
                'title': article.title,
                'text': article.text,
                'published_date': article.publish_date.strftime('%Y-%m-%d') if article.publish_date else None
            }
            
        except Exception as e:
            if '429' in str(e):
                if attempt < max_retries - 1:
                    wait_time = RETRY_DELAY * (attempt + 1)
                    logger.warning(f"  ⚠️ Rate limited - waiting {wait_time}s (retry {attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
            logger.error(f"  ✗ Error: {e}")
            return None
    
    return None

def save_article(article_data: Dict, priority: int, category: str, search_term: str,
                 score: float, matches: List[Dict], relevance: str,
                 conn: sqlite3.Connection) -> int:
    """Save article to database."""
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO articles 
            (source, url, url_hash, title, published_date, text_content,
             heat_score, category, search_priority, search_keywords)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            'Imperial Valley Press',
            article_data['url'],
            article_data['url_hash'],
            article_data['title'],
            article_data.get('published_date'),
            article_data['text'],
            score,
            relevance,
            priority,
            search_term
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
        logger.error(f"  ✗ Save failed: {e}")
        conn.rollback()
        return -1
    
    return -1

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main priority-based search workflow."""
    print("="*80)
    print("PRIORITY-BASED IMPERIAL VALLEY HEAT DEATH SCRAPER")
    print("="*80)
    print("\nSearches by keyword priority (highest to lowest):")
    print("  1. Primary death (10 pts) - 'heat death', 'heat fatality'")
    print("  2. Location-specific (8 pts) - 'farm worker heat death'")
    print("  3. Contextual death (7 pts) - 'found dead heat'")
    print("  4. Heat illness (5 pts) - 'heat stroke', 'heat exhaustion'")
    print("  5. Medical/coroner (3 pts) - 'coroner heat'")
    print("  6. Environmental (2 pts) - 'heat wave', 'extreme heat'")
    
    # Get parameters
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
    
    max_articles_str = input("\nMax total articles (default 50): ").strip()
    max_articles = int(max_articles_str) if max_articles_str else 50
    
    print(f"\n✓ Configuration:")
    print(f"  Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"  Max articles: {max_articles}")
    
    # Initialize database
    conn = init_database()
    
    # Search by priority
    discovered_urls = search_by_priority(start_date, end_date, max_articles)
    
    if len(discovered_urls) == 0:
        print("\n⚠️  No articles found.")
        return
    
    # Scraping phase
    print(f"\n{'='*80}")
    print("SCRAPING & SCORING")
    print(f"{'='*80}")
    print(f"\n⏰ Using {REQUEST_DELAY_MIN}-{REQUEST_DELAY_MAX}s random delays\n")
    
    stats = {
        'total': len(discovered_urls),
        'processed': 0,
        'saved': 0,
        'score_too_low': 0,
        'errors': 0,
        'already_scraped': 0
    }
    
    for i, url_info in enumerate(discovered_urls, 1):
        url = url_info['url']
        priority = url_info['priority']
        category = url_info['category']
        search_term = url_info['search_term']
        
        print(f"\n[{i}/{len(discovered_urls)}] Priority {priority} ({category})")
        print(f"  {url[:75]}...")
        
        if check_if_scraped(url, conn):
            print("  → Already in database")
            stats['already_scraped'] += 1
            continue
        
        # Delay
        delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
        time.sleep(delay)
        
        # Scrape
        article_data = scrape_article_with_retry(url)
        if not article_data:
            stats['errors'] += 1
            continue
        
        stats['processed'] += 1
        logger.info(f"  ✓ Scraped: {article_data['title'][:60]}...")
        
        # Score
        full_text = article_data['title'] + '\n\n' + article_data['text']
        score, matches, categories = calculate_heat_score(full_text)
        relevance = classify_relevance(score)
        
        print(f"  Score: {score:.1f} ({relevance})")
        
        if score >= MIN_SCORE_THRESHOLD:
            for cat, data in categories.items():
                print(f"    - {cat}: {data['matches']} matches ({data['score']} pts)")
            
            article_id = save_article(
                article_data, priority, category, search_term,
                score, matches, relevance, conn
            )
            if article_id > 0:
                stats['saved'] += 1
                logger.info(f"  💾 SAVED (ID: {article_id}, Score: {score:.1f})")
        else:
            print(f"  → Score below threshold ({MIN_SCORE_THRESHOLD})")
            stats['score_too_low'] += 1
    
    # Summary
    print(f"\n{'='*80}")
    print("FINAL SUMMARY")
    print(f"{'='*80}")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"URLs found: {stats['total']}")
    print(f"Processed: {stats['processed']}")
    print(f"Saved (score >= {MIN_SCORE_THRESHOLD}): {stats['saved']}")
    print(f"Score too low: {stats['score_too_low']}")
    print(f"Errors: {stats['errors']}")
    
    hit_rate = (stats['saved'] / stats['processed'] * 100) if stats['processed'] > 0 else 0
    print(f"\n✨ Hit rate: {hit_rate:.1f}%")
    
    conn.close()
    print(f"\n✅ Complete! Database: imperial_valley_heat_deaths.db")

if __name__ == '__main__':
    main()
