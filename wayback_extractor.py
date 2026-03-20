"""
#HALLELUJAH!! NO RATE LIMITING!!!!! Uses archive.org instead of live site!
"""

import requests
import re
import time
from datetime import datetime, timedelta
from typing import List, Set
from urllib.parse import urljoin
from bs4 import BeautifulSoup

print("="*80)
print("WAYBACK MACHINE ARTICLE EXTRACTOR")
print("="*80)
print("\nExtracts article URLs from archived Imperial Valley Press pages")
print("Date range: Summer 2024 (June 1 - September 30, 2024)")
print("\n" + "="*80)

# Configuration
BASE_URL = "ivpressonline.com"
START_DATE = "20240601"  # YYYYMMDD format
END_DATE = "20240930"

# Wayback Machine CDX API
CDX_API = "http://web.archive.org/cdx/search/cdx"

def get_snapshots(url: str, start_date: str, end_date: str) -> List[str]:
    """
    Get all Wayback Machine snapshot timestamps for a URL.
    
    Args:
        url: Base URL to search
        start_date: Start date YYYYMMDD
        end_date: End date YYYYMMDD
    
    Returns:
        List of timestamps
    """
    print(f"\nQuerying Wayback Machine for snapshots...")
    print(f"   URL: {url}")
    print(f"   Date range: {start_date} to {end_date}")
    
    params = {
        'url': url,
        'from': start_date,
        'to': end_date,
        'output': 'json',
        'fl': 'timestamp',
        'filter': 'statuscode:200',  # Only successful captures
        'collapse': 'timestamp:8'  # One per day
    }
    
    try:
        response = requests.get(CDX_API, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # First row is headers, skip it
        timestamps = [row[0] for row in data[1:]]
        
        print(f" Found {len(timestamps)} snapshots")
        return timestamps
        
    except Exception as e:
        print(f" Error: {e}")
        return []

def extract_article_urls_from_snapshot(base_url: str, timestamp: str) -> Set[str]:
    """
    Extract article URLs from a specific Wayback Machine snapshot.
    
    Args:
        base_url: Base URL (e.g., ivpressonline.com/news)
        timestamp: Wayback timestamp (YYYYMMDDHHMMSS)
    
    Returns:
        Set of article URLs
    """
    wayback_url = f"https://web.archive.org/web/{timestamp}/{base_url}"
    article_urls = set()
    
    try:
        # Add delay to be polite to archive.org
        time.sleep(1)
        
        headers = {'User-Agent': 'Mozilla/5.0 (Academic Research)'}
        response = requests.get(wayback_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all links
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Clean up Wayback Machine wrapper URLs
            # Archive.org wraps URLs like: /web/20240701/https://ivpressonline.com/...
            if '/web/' in href and 'ivpressonline.com' in href:
                # Extract the original URL
                match = re.search(r'/web/\d+/(https?://[^"\']+)', href)
                if match:
                    href = match.group(1)
            
            # Check if it's an article URL
            if 'ivpressonline.com' in href and 'article_' in href and '.html' in href:
                # Make sure it's absolute
                if not href.startswith('http'):
                    href = 'https://' + href.lstrip('/')
                
                article_urls.add(href)
        
        return article_urls
        
    except Exception as e:
        print(f"  Error extracting from snapshot: {e}")
        return set()

def main():
    """Main extraction workflow."""
    
    all_article_urls = set()
    
    # URLs to check in archive
    sections_to_check = [
        f"https://{BASE_URL}/news/",
        f"https://{BASE_URL}/news/local/",
        f"https://{BASE_URL}/"
    ]
    
    print("\n" + "="*80)
    print("PHASE 1: FINDING SNAPSHOTS")
    print("="*80)
    
    all_timestamps = []
    
    for section in sections_to_check:
        timestamps = get_snapshots(section, START_DATE, END_DATE)
        all_timestamps.extend([(section, ts) for ts in timestamps])
    
    print(f"\n📊 Total snapshots to process: {len(all_timestamps)}")
    
    if len(all_timestamps) == 0:
        print("\nNo snapshots found. Try different date range or URL.")
        return
    
    # Limit to avoid overwhelming (sample evenly)
    if len(all_timestamps) > 50:
        print(f"\nFound {len(all_timestamps)} snapshots - sampling 50 for speed")
        # Sample evenly across the date range
        step = len(all_timestamps) // 50
        all_timestamps = all_timestamps[::step][:50]
    
    print(f"\n{'='*80}")
    print("PHASE 2: EXTRACTING ARTICLE URLS")
    print(f"{'='*80}")
    print(f"\nProcessing {len(all_timestamps)} snapshots...")
    print("This may take 2-5 minutes...\n")
    
    for i, (section, timestamp) in enumerate(all_timestamps, 1):
        # Format timestamp for display
        date_str = f"{timestamp[:4]}-{timestamp[4:6]}-{timestamp[6:8]}"
        
        print(f"[{i}/{len(all_timestamps)}] {date_str} - {section.split('/')[-2] if '/' in section else 'home'}")
        
        urls = extract_article_urls_from_snapshot(section, timestamp)
        
        new_urls = urls - all_article_urls
        if new_urls:
            print(f"      ✓ Found {len(new_urls)} new articles (Total: {len(all_article_urls) + len(new_urls)})")
            all_article_urls.update(new_urls)
        else:
            print(f"      → No new articles")
    
    # Save to file
    print(f"\n{'='*80}")
    print("PHASE 3: SAVING RESULTS")
    print(f"{'='*80}")
    
    output_file = 'article_urls_summer_2024.txt'
    
    with open(output_file, 'w') as f:
        for url in sorted(all_article_urls):
            f.write(url + '\n')
    
    print(f"\nSUCCESS!")
    print(f"\nRESULTS:")
    print(f"   Total unique articles found: {len(all_article_urls)}")
    print(f"   Saved to: {output_file}")
    
    print(f"\nSAMPLE URLS:")
    for i, url in enumerate(sorted(all_article_urls)[:10], 1):
        print(f"   {i}. {url[:70]}...")
    
    if len(all_article_urls) > 10:
        print(f"   ... and {len(all_article_urls) - 10} more")
    
    print(f"\n{'='*80}")
    print("NEXT STEPS:")
    print(f"{'='*80}")
    print(f"\n1. Review the URLs in {output_file}")
    print(f"2. Use the URL scraper to download and score these articles")
    print(f"3. Articles are from Summer 2024 when heat deaths occurred!")
    print(f"\nNo rate limiting - these are from Archive.org! 🎉")
    print(f"\n{'='*80}")

if __name__ == '__main__':
    main()
