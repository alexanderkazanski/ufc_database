import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time

def scrape_fighter_profile(fighter_url):
    """
    Scrape fighter profile data including physical stats and career statistics
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    fighter_data = {}
    
    try:
        response = requests.get(fighter_url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Get fighter name
        name_tag = soup.find('span', class_='b-content__title-highlight')
        fighter_data['Name'] = name_tag.text.strip() if name_tag else 'N/A'
        
        # Get nickname
        nickname_tag = soup.find('p', class_='b-content__Nickname')
        fighter_data['Nickname'] = nickname_tag.text.strip() if nickname_tag else 'N/A'
        
        # Find the fighter details section
        details_div = soup.find('div', class_='b-fight-details')
        
        if details_div:
            # Get all list items from the details section
            list_items = details_div.find_all('li', class_='b-list__box-list-item')
            
            for item in list_items:
                # Get the label (title)
                label_tag = item.find('i', class_='b-list__box-item-title')
                if label_tag:
                    label = label_tag.get_text(strip=True).replace(':', '').strip()
                    
                    # Get the value (everything after the label)
                    value = item.get_text(separator=' ', strip=True)
                    value = value.replace(label, '').replace(':', '').strip()
                    
                    if label and value:
                        fighter_data[label] = value
        
        return fighter_data
        
    except Exception as e:
        print(f"      Error scraping fighter profile: {e}")
        return {}

def scrape_fight_detail_stats(fight_url):
    """
    Scrape detailed fight statistics from individual fight page
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(fight_url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        fight_stats = {}
        
        # Find all stat tables
        stat_tables = soup.find_all('table', class_='b-fight-details__table')
        
        if not stat_tables:
            return {}
        
        for table in stat_tables:
            tbody = table.find('tbody')
            if not tbody:
                continue
            
            # Check for section headers to identify table type
            section_header = table.find_previous('p', class_='b-fight-details__table-header')
            section_name = 'Totals'
            
            if section_header:
                header_text = section_header.get_text(strip=True)
                if 'Round' in header_text:
                    section_name = header_text.replace(' ', '_')
                elif 'Significant Strikes' in header_text:
                    section_name = 'Sig_Strikes'
            
            rows = tbody.find_all('tr')
            
            for row_idx, row in enumerate(rows):
                fighter_num = row_idx + 1
                cells = row.find_all('td')
                
                # Skip if not enough cells
                if len(cells) < 2:
                    continue
                
                stat_prefix = f'F{fighter_num}_{section_name}_'
                
                # Extract stats based on cell count and content
                try:
                    cell_idx = 0
                    
                    # Skip first cell if it's empty or contains fighter name
                    first_cell = cells[0].get_text(strip=True)
                    if first_cell and not any(c.isdigit() for c in first_cell):
                        cell_idx = 1
                    
                    # Extract based on available cells
                    if len(cells) > cell_idx:
                        fight_stats[f'{stat_prefix}KD'] = cells[cell_idx].get_text(strip=True)
                        cell_idx += 1
                    
                    if len(cells) > cell_idx:
                        fight_stats[f'{stat_prefix}Sig_Str'] = cells[cell_idx].get_text(strip=True)
                        cell_idx += 1
                    
                    if len(cells) > cell_idx:
                        fight_stats[f'{stat_prefix}Sig_Str_Pct'] = cells[cell_idx].get_text(strip=True)
                        cell_idx += 1
                    
                    if len(cells) > cell_idx:
                        fight_stats[f'{stat_prefix}Total_Str'] = cells[cell_idx].get_text(strip=True)
                        cell_idx += 1
                    
                    if len(cells) > cell_idx:
                        fight_stats[f'{stat_prefix}TD'] = cells[cell_idx].get_text(strip=True)
                        cell_idx += 1
                    
                    if len(cells) > cell_idx:
                        fight_stats[f'{stat_prefix}TD_Pct'] = cells[cell_idx].get_text(strip=True)
                        cell_idx += 1
                    
                    if len(cells) > cell_idx:
                        fight_stats[f'{stat_prefix}Sub_Att'] = cells[cell_idx].get_text(strip=True)
                        cell_idx += 1
                    
                    if len(cells) > cell_idx:
                        fight_stats[f'{stat_prefix}Rev'] = cells[cell_idx].get_text(strip=True)
                        cell_idx += 1
                    
                    if len(cells) > cell_idx:
                        fight_stats[f'{stat_prefix}Ctrl'] = cells[cell_idx].get_text(strip=True)
                        cell_idx += 1
                    
                    # For significant strikes breakdown table
                    if section_name == 'Sig_Strikes' and len(cells) > cell_idx:
                        if len(cells) > cell_idx:
                            fight_stats[f'{stat_prefix}Head'] = cells[cell_idx].get_text(strip=True)
                            cell_idx += 1
                        
                        if len(cells) > cell_idx:
                            fight_stats[f'{stat_prefix}Body'] = cells[cell_idx].get_text(strip=True)
                            cell_idx += 1
                        
                        if len(cells) > cell_idx:
                            fight_stats[f'{stat_prefix}Leg'] = cells[cell_idx].get_text(strip=True)
                            cell_idx += 1
                        
                        if len(cells) > cell_idx:
                            fight_stats[f'{stat_prefix}Distance'] = cells[cell_idx].get_text(strip=True)
                            cell_idx += 1
                        
                        if len(cells) > cell_idx:
                            fight_stats[f'{stat_prefix}Clinch'] = cells[cell_idx].get_text(strip=True)
                            cell_idx += 1
                        
                        if len(cells) > cell_idx:
                            fight_stats[f'{stat_prefix}Ground'] = cells[cell_idx].get_text(strip=True)
                
                except Exception as e:
                    print(f"    Warning: Error parsing row {row_idx} in section {section_name}: {e}")
                    continue
        
        return fight_stats
    
    except Exception as e:
        print(f"  Error scraping fight detail stats: {e}")
        return {}

def scrape_fight_details(event_url, event_name, event_date, event_location, scrape_detailed_stats=True, scrape_fighter_profiles=True):
    """
    Scrape fight details from an individual event page
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(event_url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        fights_table = soup.find('table', class_='b-fight-details__table')
        
        if not fights_table:
            return []
        
        fights_data = []
        rows = fights_table.find('tbody').find_all('tr', class_='b-fight-details__table-row')
        
        for fight_idx, row in enumerate(rows, 1):
            cells = row.find_all('td', class_='b-fight-details__table-col')
            
            if len(cells) >= 9:
                # Extract fighter names and URLs
                fighters_cell = cells[1]
                fighter_links = fighters_cell.find_all('a', class_='b-link')
                
                fighter_1_name = fighter_links[0].text.strip() if len(fighter_links) > 0 else 'N/A'
                fighter_1_url = fighter_links[0]['href'] if len(fighter_links) > 0 and fighter_links[0].has_attr('href') else None
                
                fighter_2_name = fighter_links[1].text.strip() if len(fighter_links) > 1 else 'N/A'
                fighter_2_url = fighter_links[1]['href'] if len(fighter_links) > 1 and fighter_links[1].has_attr('href') else None
                
                # Extract fight result icons
                result_icons = fighters_cell.find_all('i')
                fighter_1_result = 'N/A'
                fighter_2_result = 'N/A'
                
                if len(result_icons) >= 2:
                    if 'b-fight-details__person-status_style_green' in result_icons[0].get('class', []):
                        fighter_1_result = 'W'
                    elif 'b-fight-details__person-status_style_gray' in result_icons[0].get('class', []):
                        fighter_1_result = 'D'
                    else:
                        fighter_1_result = 'L'
                    
                    if 'b-fight-details__person-status_style_green' in result_icons[1].get('class', []):
                        fighter_2_result = 'W'
                    elif 'b-fight-details__person-status_style_gray' in result_icons[1].get('class', []):
                        fighter_2_result = 'D'
                    else:
                        fighter_2_result = 'L'
                
                # Extract basic fight info
                weight_class = cells[6].text.strip() if len(cells) > 6 else 'N/A'
                method = cells[7].text.strip() if len(cells) > 7 else 'N/A'
                round_finished = cells[8].text.strip() if len(cells) > 8 else 'N/A'
                time_finished = cells[9].text.strip() if len(cells) > 9 else 'N/A'
                
                # Extract summary stats
                kd = cells[2].find_all('p') if len(cells) > 2 else []
                kd_1 = kd[0].text.strip() if len(kd) > 0 else 'N/A'
                kd_2 = kd[1].text.strip() if len(kd) > 1 else 'N/A'
                
                str_data = cells[3].find_all('p') if len(cells) > 3 else []
                str_1 = str_data[0].text.strip() if len(str_data) > 0 else 'N/A'
                str_2 = str_data[1].text.strip() if len(str_data) > 1 else 'N/A'
                
                td_data = cells[4].find_all('p') if len(cells) > 4 else []
                td_1 = td_data[0].text.strip() if len(td_data) > 0 else 'N/A'
                td_2 = td_data[1].text.strip() if len(td_data) > 1 else 'N/A'
                
                sub_data = cells[5].find_all('p') if len(cells) > 5 else []
                sub_1 = sub_data[0].text.strip() if len(sub_data) > 0 else 'N/A'
                sub_2 = sub_data[1].text.strip() if len(sub_data) > 1 else 'N/A'
                
                # Get fight detail URL
                fight_detail_url = None
                first_cell_link = cells[0].find('a', class_='b-flag')
                if first_cell_link and first_cell_link.has_attr('href'):
                    fight_detail_url = first_cell_link['href']
                
                fight_data = {
                    'Event Name': event_name,
                    'Event Date': event_date,
                    'Event Location': event_location,
                    'Fighter 1': fighter_1_name,
                    'Fighter 1 URL': fighter_1_url,
                    'Fighter 1 Result': fighter_1_result,
                    'Fighter 2': fighter_2_name,
                    'Fighter 2 URL': fighter_2_url,
                    'Fighter 2 Result': fighter_2_result,
                    'Weight Class': weight_class,
                    'Method': method,
                    'Round': round_finished,
                    'Time': time_finished,
                    'Fighter 1 KD': kd_1,
                    'Fighter 2 KD': kd_2,
                    'Fighter 1 Str': str_1,
                    'Fighter 2 Str': str_2,
                    'Fighter 1 TD': td_1,
                    'Fighter 2 TD': td_2,
                    'Fighter 1 Sub': sub_1,
                    'Fighter 2 Sub': sub_2,
                    'Fight Detail URL': fight_detail_url
                }
                
                print(f"    → Fight {fight_idx}: {fighter_1_name} vs {fighter_2_name}")
                
                # Scrape fighter profile data if enabled
                if scrape_fighter_profiles:
                    if fighter_1_url:
                        print(f"      → Scraping {fighter_1_name} profile...")
                        f1_profile = scrape_fighter_profile(fighter_1_url)
                        for key, value in f1_profile.items():
                            fight_data[f'Fighter 1 {key}'] = value
                        time.sleep(0.3)
                    
                    if fighter_2_url:
                        print(f"      → Scraping {fighter_2_name} profile...")
                        f2_profile = scrape_fighter_profile(fighter_2_url)
                        for key, value in f2_profile.items():
                            fight_data[f'Fighter 2 {key}'] = value
                        time.sleep(0.3)
                
                # Scrape detailed fight stats if enabled
                if scrape_detailed_stats and fight_detail_url:
                    print(f"      → Scraping fight details...")
                    detailed_stats = scrape_fight_detail_stats(fight_detail_url)
                    fight_data.update(detailed_stats)
                    time.sleep(0.5)
                
                fights_data.append(fight_data)
        
        return fights_data
    
    except Exception as e:
        print(f"  Error scraping fight details from {event_url}: {e}")
        return []

def scrape_ufc_events_and_fights(max_events=None, scrape_detailed_stats=True, scrape_fighter_profiles=True):
    """
    Scrape completed UFC events and all fight details from each event
    """
    url = "http://ufcstats.com/statistics/events/completed"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        print("Fetching events list...")
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        events_table = soup.find('table', class_='b-statistics__table-events')
        
        if not events_table:
            print("Could not find events table on the page")
            return None
        
        rows = events_table.find('tbody').find_all('tr', class_='b-statistics__table-row')
        
        all_fights_data = []
        events_processed = 0
        
        events_to_process = rows[:max_events] if max_events else rows
        
        print(f"Found {len(rows)} total events. Processing {len(events_to_process)} events...")
        print(f"Fighter profiles scraping: {'ENABLED' if scrape_fighter_profiles else 'DISABLED'}")
        print(f"Detailed fight stats scraping: {'ENABLED' if scrape_detailed_stats else 'DISABLED'}")
        
        for idx, row in enumerate(events_to_process, 1):
            cells = row.find_all('td', class_='b-statistics__table-col')
            
            if len(cells) >= 2:
                event_link_tag = cells[0].find('a', class_='b-link')
                event_name = event_link_tag.text.strip() if event_link_tag else 'N/A'
                event_url = event_link_tag['href'] if event_link_tag and event_link_tag.has_attr('href') else None
                event_date = cells[1].text.strip() if len(cells) > 1 else 'N/A'
                event_location = cells[2].text.strip() if len(cells) > 2 else 'N/A'
                
                if event_url:
                    print(f"\n[{idx}/{len(events_to_process)}] Scraping: {event_name}")
                    
                    fights = scrape_fight_details(event_url, event_name, event_date, event_location, 
                                                 scrape_detailed_stats, scrape_fighter_profiles)
                    all_fights_data.extend(fights)
                    
                    print(f"  ✓ Found {len(fights)} fights")
                    events_processed += 1
                    
                    time.sleep(1)
        
        df = pd.DataFrame(all_fights_data)
        
        print("\n" + "=" * 70)
        print(f"Successfully scraped {events_processed} events with {len(all_fights_data)} total fights")
        
        if len(df) > 0:
            print(f"\nSample data (first fight):")
            print(df.head(1).T)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ufc_complete_data_{timestamp}.csv"
            df.to_csv(filename, index=False)
            print(f"\nData saved to {filename}")
            print(f"Total columns: {len(df.columns)}")
        
        return df
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the webpage: {e}")
        return None
    except Exception as e:
        print(f"Error parsing data: {e}")
        return None

if __name__ == "__main__":
    print("=" * 70)
    print("UFC Stats Complete Scraper")
    print("Events + Fights + Fighter Profiles + Detailed Stats")
    print("=" * 70)
    
    # Configuration:
    # - max_events: Number of events to scrape (None for all)
    # - scrape_detailed_stats: Include round-by-round fight statistics
    # - scrape_fighter_profiles: Include fighter physical stats and career stats
    
    df = scrape_ufc_events_and_fights(
        max_events=2, 
        scrape_detailed_stats=True,
        scrape_fighter_profiles=True
    )
    
    if df is not None and len(df) > 0:
        print("\n" + "=" * 70)
        print("Scraping Complete!")
        print(f"Total fights scraped: {len(df)}")
        print(f"Unique events: {df['Event Name'].nunique()}")