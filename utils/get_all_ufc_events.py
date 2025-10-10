import requests
from bs4 import BeautifulSoup

def scrape_event_links():
    """Scrape all UFC event links from the page"""
    
    url = "http://ufcstats.com/statistics/events/completed?page=all"
    
    print("Fetching event links...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    response = requests.get(url, headers=headers, timeout=30)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all event links in the table
    events_table = soup.select_one('table.b-statistics__table-events')
    event_links = []
    
    if events_table:
        links = events_table.select('a.b-link')
        event_links = [link.get('href') for link in links if link.get('href')]
    
    print(f"Found {len(event_links)} event links")
    
    # Save to file
    with open('ufc_event_links.txt', 'w') as f:
        for link in event_links:
            f.write(link + '\n')
    
    print(f"✓ Saved to ufc_event_links.txt")

if __name__ == "__main__":
    scrape_event_links()