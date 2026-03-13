"""
Date Aware IV HRI Scraper

This version now has the capability to let us specify the exact date range, see total articles found in that range, and choose how many to scrape.
This lets us target specific summer months in our scrape. 
"""

import sqlite3
import re
import hashlib
import time
from datetime import datetime, timedelta
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
        logging.FileHandler('scraper_date_aware.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGS
# ============================================================================

NEWS_SOURCES = [
    {
        'name': 'Imperial Valley Press',
        'url': 'https://www.ivpressonline.com',
        'archive_url': 'https://www.ivpressonline.com/search/?sd={start_date}&ed={end_date}&s=start_time&sd=desc',
        'sections': ['/news/local/', '/news/'],
        'language': 'en',
        'bias': 'LOCAL-UNRATED'
    },
    {
        'name': 'Calexico Chronicle',
        'url': 'https://calexicochronicle.com',
        'sections': ['/'],
        'language': 'en',
        'bias': 'LOCAL-UNRATED'
    },
    {
        'name': 'Holtville Tribune',
        'url': 'https://holtvilletribune.com',
        'sections': ['/category/regional-news/'],
        'language': 'en',
        'bias': 'LOCAL-UNRATED'
    }
]

# Keywords 
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
            r'preheat', r'heat\s+pump', r'heating\s+system', r'heated\s+game'
        ],
        'weight': -100
    }
}

REQUEST_DELAY = 12
MIN_SCORE_THRESHOLD = 10  # Only save articles with score >= 10, easily editable if this is too lenient

# ============================================================================
# DATABASE SETUP
# ============================================================================

def init_database(db_path: str = 'imperial_valley_heat_deaths.db') -> sqlite3.Connection:
    """Initialize SQLite database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_bias TEXT,
            language TEXT,
            url TEXT UNIQUE NOT NULL,
            url_hash TEXT UNIQUE,
            title TEXT,
            authors TEXT,
            published_date DATE,
            scraped_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            text_content TEXT,
            keywords_matched TEXT,
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
    
    # Check exclusions
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
# DATE-AWARE URL DISCOVERY
# ============================================================================

def is_valid_article_url(url: str) -> bool:
    """Validate if URL is an actual article."""
    url_lower = url.lower()
    
    # Exclude external domains
    external_domains = ['facebook.com', 'twitter.com', 'wa.me', 'linkedin.com']
    if any(domain in url_lower for domain in external_domains):
        return False
    
    # Must be from valid news domains
    valid_domains = ['ivpressonline.com', 'calexicochronicle.com', 'holtvilletribune.com']
    if not any(domain in url_lower for domain in valid_domains):
        return False
    
    # Exclude navigation pages
    exclude_patterns = ['/users/', '/login', '/signup', '/search', '/category/',
                       '/tag/', '/author/', '/page/', '/feed', '/rss']
    for pattern in exclude_patterns:
        if pattern in url_lower:
            return False
    
    # Must match article patterns
    if 'ivpressonline.com' in url_lower:
        if 'article_' in url_lower and '.html' in url_lower:
            if '/article_' in url_lower and url_lower.count('article_') == 1:
                return True
    
    if any(domain in url_lower for domain in ['chronicle', 'tribune']):
        if re.search(r'/20\d{2}/\d{2}/\d{2}/', url_lower) and len(url) > 40:
            return True
    
    return False

def extract_date_from_url(url: str) -> Optional[datetime]:
    """Try to extract publication date from URL pattern."""
    # Pattern: /YYYY/MM/DD/
    match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', url)
    if match:
        try:
            year, month, day = match.groups()
            return datetime(int(year), int(month), int(day))
        except:
            pass
    return None

def discover_articles_in_date_range(source: Dict, start_date: datetime, 
                                    end_date: datetime) -> List[Dict]:
    """
    Discover all articles in a date range.
    
    Returns:
        List of dicts with {url, estimated_date}
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"DISCOVERING ARTICLES: {source['name']}")
    logger.info(f"Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    logger.info(f"{'='*80}")
    
    discovered_articles = []
    
    # Try archive URL if available
    if 'archive_url' in source:
        archive_url = source['archive_url'].format(
            start_date=start_date.strftime('%m/%d/%Y'),
            end_date=end_date.strftime('%m/%d/%Y')
        )
        logger.info(f"Trying archive URL: {archive_url}")
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Academic Research - Heat Death Study)'}
            response = requests.get(archive_url, headers=headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            links = soup.find_all('a', href=True)
            
            for link in links:
                href = link['href']
                if href.startswith('/'):
                    full_url = source['url'] + href
                elif href.startswith('http'):
                    full_url = href
                else:
                    continue
                
                if is_valid_article_url(full_url) and full_url not in [a['url'] for a in discovered_articles]:
                    date = extract_date_from_url(full_url)
                    discovered_articles.append({
                        'url': full_url,
                        'estimated_date': date
                    })
            
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            logger.warning(f"Archive URL failed: {e}")
    
    # Fallback: Browse sections and filter by date pattern
    if len(discovered_articles) == 0:
        logger.info("Falling back to section browsing...")
        
        for section in source['sections']:
            try:
                url = source['url'] + section
                headers = {'User-Agent': 'Mozilla/5.0 (Academic Research)'}
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                links = soup.find_all('a', href=True)
                
                for link in links:
                    href = link['href']
                    if href.startswith('/'):
                        full_url = source['url'] + href
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        continue
                    
                    if is_valid_article_url(full_url):
                        date = extract_date_from_url(full_url)
                        
                        # Filter by date if we can extract it
                        if date:
                            if start_date <= date <= end_date:
                                if full_url not in [a['url'] for a in discovered_articles]:
                                    discovered_articles.append({
                                        'url': full_url,
                                        'estimated_date': date
                                    })
                        else:
                            # Can't determine date from URL, include anyway
                            if full_url not in [a['url'] for a in discovered_articles]:
                                discovered_articles.append({
                                    'url': full_url,
                                    'estimated_date': None
                                })
                
                time.sleep(REQUEST_DELAY)
            except Exception as e:
                logger.error(f"Error browsing {url}: {e}")
    
    # Sort by date if available
    discovered_articles.sort(key=lambda x: x['estimated_date'] if x['estimated_date'] else datetime.min, reverse=True)
    
    logger.info(f"📊 Found {len(discovered_articles)} articles in date range")
    
    return discovered_articles

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

def scrape_article_newspaper4k(url: str, language: str = 'en') -> Optional[Dict]:
    """Scrape article using newspaper4k."""
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
            'authors': ', '.join(article.authors) if article.authors else None,
            'text': article.text,
            'published_date': article.publish_date.strftime('%Y-%m-%d') if article.publish_date else None,
            'language': language
        }
        
        logger.info(f"  ✓ Scraped: {article.title[:60]}...")
        return article_data
        
    except Exception as e:
        logger.error(f"  ✗ Error scraping: {e}")
        return None

def save_article(article_data: Dict, source_name: str, source_bias: str, 
                 score: float, matches: List[Dict], category: str,
                 conn: sqlite3.Connection) -> int:
    """Save article to database."""
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO articles 
            (source, source_bias, language, url, url_hash, title, authors,
             published_date, text_content, keywords_matched, heat_score, category)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            source_name, source_bias, article_data['language'],
            article_data['url'], article_data['url_hash'],
            article_data['title'], article_data.get('authors'),
            article_data.get('published_date'), article_data['text'],
            str(matches), score, category
        ))
        
        article_id = cursor.lastrowid
        
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

# ============================================================================
# MAIN INTERACTIVE WORKFLOW
# ============================================================================

def main():
    """Main interactive scraping workflow."""
    print("="*80)
    print("DATE-AWARE IMPERIAL VALLEY HEAT DEATH SCRAPER")
    print("="*80)
    
    # Get date range from user - DIRECT INPUT
    print("\n📅 DATE RANGE SELECTION")
    print("-" * 80)
    print("Enter the exact date range you want to scrape.")
    print("\nFormat: YYYY-MM-DD (e.g., 2024-06-01)")
    print("\nCommon ranges:")
    print("  Summer 2024: 2024-06-01 to 2024-09-30")
    print("  Summer 2023: 2023-06-01 to 2023-09-30")
    print("  July 2024:   2024-07-01 to 2024-07-31")
    print("  All 2024:    2024-01-01 to 2024-12-31")
    print("-" * 80)
    
    # Get start date
    while True:
        start_str = input("\nEnter START date (YYYY-MM-DD): ").strip()
        try:
            start_date = datetime.strptime(start_str, '%Y-%m-%d')
            break
        except:
            print("Invalid format. Use YYYY-MM-DD (e.g., 2024-06-01)")
    
    # Get end date
    while True:
        end_str = input("Enter END date (YYYY-MM-DD): ").strip()
        try:
            end_date = datetime.strptime(end_str, '%Y-%m-%d')
            if end_date < start_date:
                print("*edit!!* End date must be after start date")
                continue
            break
        except:
            print("Invalid format. Use YYYY-MM-DD (e.g., 2024-09-30)")
    
    print(f"\n✓ Selected: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Calculate duration
    duration = (end_date - start_date).days
    print(f"✓ Duration: {duration} days")
    
    # Initialize database
    conn = init_database()
    
    # Discovery phase
    print(f"\n{'='*80}")
    print("PHASE 1: DISCOVERING ARTICLES")
    print(f"{'='*80}")
    
    all_discovered = []
    for source in NEWS_SOURCES:
        articles = discover_articles_in_date_range(source, start_date, end_date)
        for article in articles:
            article['source'] = source
        all_discovered.extend(articles)
    
    print(f"\n📊 DISCOVERY SUMMARY")
    print(f"{'='*80}")
    print(f"Total articles found: {len(all_discovered)}")
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Group by source
    by_source = {}
    for article in all_discovered:
        source_name = article['source']['name']
        by_source[source_name] = by_source.get(source_name, 0) + 1
    
    print(f"\nBy source:")
    for source_name, count in sorted(by_source.items(), key=lambda x: x[1], reverse=True):
        print(f"  {source_name:30} {count:4} articles")
    
    if len(all_discovered) == 0:
        print("\n!!No articles found in date range.")
        print("This could mean:")
        print("  - Website structure doesn't support date filtering")
        print("  - No articles published in this date range")
        print("  - Try using the search scraper instead")
        return
    
    # Let user choose how many to scrape
    print(f"\n{'='*80}")
    print("ARTICLE SELECTION")
    print(f"{'='*80}")
    print(f"Total articles found in date range: {len(all_discovered)}")
    print(f"\nHow many would you like to scrape?")
    print(f"  - Enter a number (e.g., 50, 100, 255)")
    print(f"  - Or press Enter to scrape ALL {len(all_discovered)} articles")
    
    selection = input(f"\nEnter number of articles to scrape (or press Enter for all): ").strip()
    
    if selection == '':
        articles_to_scrape = all_discovered
        print(f"\n✓ Will scrape all {len(articles_to_scrape)} articles")
    else:
        try:
            num = int(selection)
            if num > len(all_discovered):
                print(f"\n⚠️ Requested {num} but only {len(all_discovered)} available")
                articles_to_scrape = all_discovered
                print(f"✓ Will scrape all {len(articles_to_scrape)} articles")
            elif num < 1:
                print(f"\n⚠️ Invalid number. Will scrape 20 articles")
                articles_to_scrape = all_discovered[:20]
            else:
                articles_to_scrape = all_discovered[:num]
                print(f"\n✓ Will scrape {len(articles_to_scrape)} articles")
        except:
            print(f"\n⚠️ Invalid input. Will scrape 50 articles")
            articles_to_scrape = all_discovered[:50]
    
    # Scraping phase
    print(f"\n{'='*80}")
    print("PHASE 3: SCRAPING & SCORING")
    print(f"{'='*80}")
    
    stats = {
        'total': len(articles_to_scrape),
        'processed': 0,
        'already_scraped': 0,
        'saved': 0,
        'score_too_low': 0,
        'errors': 0
    }
    
    for i, article_info in enumerate(articles_to_scrape, 1):
        url = article_info['url']
        source = article_info['source']
        
        print(f"\n[{i}/{len(articles_to_scrape)}] {url[:80]}...")
        
        # Check if already scraped
        if check_if_scraped(url, conn):
            print("  → Already in database")
            stats['already_scraped'] += 1
            continue
        
        time.sleep(REQUEST_DELAY)
        
        # Scrape
        article_data = scrape_article_newspaper4k(url, source['language'])
        if not article_data:
            stats['errors'] += 1
            continue
        
        stats['processed'] += 1
        
        # Score
        full_text = article_data['title'] + '\n\n' + article_data['text']
        score, matches, categories = calculate_heat_score(full_text, source['language'])
        relevance = classify_relevance(score)
        
        print(f"  Score: {score:.1f} ({relevance})")
        
        if score >= MIN_SCORE_THRESHOLD:
            for category, data in categories.items():
                print(f"    - {category}: {data['matches']} matches ({data['score']} pts)")
            
            article_id = save_article(
                article_data, source['name'], source['bias'],
                score, matches, relevance, conn
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
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"Total discovered: {len(all_discovered)}")
    print(f"Selected to scrape: {stats['total']}")
    print(f"Successfully processed: {stats['processed']}")
    print(f"Already in database: {stats['already_scraped']}")
    print(f"Saved (score >= {MIN_SCORE_THRESHOLD}): {stats['saved']}")
    print(f"Score too low: {stats['score_too_low']}")
    print(f"Errors: {stats['errors']}")
    
    if stats['saved'] > 0:
        # Show top articles
        cursor = conn.cursor()
        cursor.execute('''
            SELECT title, heat_score, category, published_date
            FROM articles
            ORDER BY scraped_date DESC
            LIMIT 10
        ''')
        
        print(f"\n📰 NEWLY SAVED ARTICLES (Top 10):")
        for i, (title, score, category, date) in enumerate(cursor.fetchall(), 1):
            print(f"\n{i}. [{score:.1f}] {title[:70]}")
            print(f"   {category} | {date}")
    
    conn.close()
    print(f"\nComplete! DB: imperial_valley_heat_deaths.db")

if __name__ == '__main__':
    main()
