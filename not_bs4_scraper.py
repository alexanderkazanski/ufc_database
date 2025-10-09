import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time

def get_soup(url, retries=3):
    """Fetch and parse HTML content with retries"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            if attempt < retries - 1:
                print(f"  Retry {attempt + 1}/{retries} after error: {e}")
                time.sleep(2)
            else:
                print(f"  Failed to fetch {url}: {e}")
                return None

def scrape_fighter_fight_history(fighter_url):
    """Scrape complete fight history for a fighter"""
    try:
        print(f"  Loading fighter history page...")
        soup = get_soup(fighter_url)
        if not soup:
            return []
        
        fight_history = []
        
        # Find the fight history table
        history_table = soup.select_one('table.b-fight-details__table')
        if not history_table:
            print("  No fight history table found")
            return []
        
        tbody = history_table.find('tbody')
        if not tbody:
            return []
        
        rows = tbody.select('tr.b-fight-details__table-row')
        print(f"  Found {len(rows)} fights in history")
        
        for row_idx, row in enumerate(rows, 1):
            try:
                cells = row.select('td.b-fight-details__table-col')
                if len(cells) >= 7:
                    # Extract result (W/L/D/NC)
                    result = cells[0].get_text(strip=True) if len(cells) > 0 else 'N/A'
                    
                    # Extract opponent name and link
                    opponent_link = None
                    opponent_name = 'N/A'
                    opponent_element = cells[1].select_one('a.b-link')
                    if opponent_element:
                        opponent_name = opponent_element.get_text(strip=True)
                        opponent_link = opponent_element.get('href')
                    
                    # Extract fight details
                    weight_class = cells[6].get_text(strip=True) if len(cells) > 6 else 'N/A'
                    method = cells[7].get_text(strip=True) if len(cells) > 7 else 'N/A'
                    round_num = cells[8].get_text(strip=True) if len(cells) > 8 else 'N/A'
                    fight_time = cells[9].get_text(strip=True) if len(cells) > 9 else 'N/A'
                    
                    # Extract event name and link
                    event_name = 'N/A'
                    event_link = None
                    event_element = cells[5].select_one('a.b-link')
                    if event_element:
                        event_name = event_element.get_text(strip=True)
                        event_link = event_element.get('href')
                    
                    # Extract event date
                    event_date = cells[5].get_text(strip=True).split('\n')[-1].strip() if len(cells) > 5 else 'N/A'
                    
                    # Extract stats (KD, Str, TD, Sub)
                    kd = cells[2].get_text(strip=True) if len(cells) > 2 else 'N/A'
                    strikes = cells[3].get_text(strip=True) if len(cells) > 3 else 'N/A'
                    takedowns = cells[4].get_text(strip=True) if len(cells) > 4 else 'N/A'
                    
                    fight_history.append({
                        'Result': result,
                        'Opponent': opponent_name,
                        'Opponent_URL': opponent_link,
                        'KD': kd,
                        'Str': strikes,
                        'TD': takedowns,
                        'Weight_Class': weight_class,
                        'Method': method,
                        'Round': round_num,
                        'Time': fight_time,
                        'Event': event_name,
                        'Event_URL': event_link,
                        'Event_Date': event_date
                    })
                    
                    # Show progress for long histories
                    if row_idx % 5 == 0:
                        print(f"  Processed {row_idx}/{len(rows)} fights...")
                        
            except Exception as e:
                print(f"  Warning: Error parsing fight history row {row_idx}: {e}")
                continue
        
        print(f"  ✓ Extracted {len(fight_history)} fights from history")
        return fight_history
        
    except Exception as e:
        print(f"  Error scraping fighter history: {e}")
        return []

def scrape_fighter_profile(fighter_url, scrape_history=True):
    """Scrape fighter profile data including physical stats, career statistics, and fight history"""
    fighter_data = {}
    
    try:
        print(f"  Loading fighter profile...")
        soup = get_soup(fighter_url)
        if not soup:
            return {}
        
        # Get fighter name
        name_element = soup.select_one('span.b-content__title-highlight')
        fighter_data['Name'] = name_element.get_text(strip=True) if name_element else 'N/A'
        print(f"  Fighter: {fighter_data['Name']}")
        
        # Get nickname
        nickname_element = soup.select_one('p.b-content__Nickname')
        fighter_data['Nickname'] = nickname_element.get_text(strip=True) if nickname_element else 'N/A'
        
        # Get record from title
        title_element = soup.select_one('span.b-content__title-record')
        if title_element:
            fighter_data['Record'] = title_element.get_text(strip=True).replace('Record:', '').strip()
        else:
            fighter_data['Record'] = 'N/A'
        
        # Find the fighter details section - physical attributes
        details_div = soup.select_one('div.b-fight-details')
        if details_div:
            list_items = details_div.select('li.b-list__box-list-item')
            for item in list_items:
                try:
                    label_tag = item.select_one('i.b-list__box-item-title')
                    if label_tag:
                        label = label_tag.get_text(strip=True).replace(':', '').strip()
                        value = item.get_text(strip=True)
                        value = value.replace(label, '').replace(':', '').strip()
                        if label and value:
                            fighter_data[label] = value
                except Exception:
                    continue
        
        # Get career statistics from the stats section
        career_stats_div = soup.select_one('div.b-list__info-box-left')
        if career_stats_div:
            stat_items = career_stats_div.select('li.b-list__box-list-item')
            for item in stat_items:
                try:
                    full_text = item.get_text(strip=True)
                    title_element = item.select_one('i.b-list__box-item-title')
                    if title_element:
                        label = title_element.get_text(strip=True).replace(':', '').strip()
                        value = full_text.replace(label, '').replace(':', '').strip()
                        if label and value:
                            fighter_data[f'Career_{label}'] = value
                except Exception:
                    continue
        
        # Scrape fight history if enabled
        if scrape_history:
            print(f"  Fetching fight history...")
            fight_history = scrape_fighter_fight_history(fighter_url)
            fighter_data['Fight_History'] = fight_history
            fighter_data['Total_Fights_In_History'] = len(fight_history)
        
        print(f"  ✓ Profile complete")
        return fighter_data
        
    except Exception as e:
        print(f"  Error scraping fighter profile: {e}")
        return {}

def scrape_fight_detail_stats(fight_url):
    """Scrape detailed fight statistics from individual fight page (round-by-round stats)"""
    try:
        soup = get_soup(fight_url)
        if not soup:
            return {}
        
        fight_stats = {}
        
        # Find all stat tables
        stat_tables = soup.select('table.b-fight-details__table')
        if not stat_tables:
            return {}
        
        for table in stat_tables:
            tbody = table.find('tbody')
            if not tbody:
                continue
            
            # Check for section headers to identify table type
            section_name = 'Totals'
            prev_sibling = table.find_previous_sibling()
            while prev_sibling:
                if prev_sibling.get('class') and 'b-fight-details__table-header' in prev_sibling.get('class', []):
                    section_header = prev_sibling.get_text(strip=True)
                    if 'Round' in section_header:
                        section_name = section_header.strip().replace(' ', '_')
                    elif 'Significant Strikes' in section_header:
                        section_name = 'Sig_Strikes'
                    break
                prev_sibling = prev_sibling.find_previous_sibling()
            
            rows = tbody.find_all('tr')
            for row_idx, row in enumerate(rows):
                fighter_num = row_idx + 1
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue
                
                stat_prefix = f'F{fighter_num}_{section_name}_'
                
                try:
                    cell_idx = 0
                    # Skip first cell if it's empty or contains fighter name
                    first_cell = cells[0].get_text(strip=True)
                    if first_cell and not any(c.isdigit() for c in first_cell):
                        cell_idx = 1
                    
                    # Extract stats
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
                            cell_idx += 1
                            
                except Exception as e:
                    print(f"  Warning: Error parsing row {row_idx} in section {section_name}: {e}")
                    continue
        
        return fight_stats
        
    except Exception as e:
        print(f"  Error scraping fight detail stats: {e}")
        return {}

def scrape_fight_details(event_url, event_name, event_date, event_location, 
                         scrape_detailed_stats=True, scrape_fighter_profiles=True, 
                         scrape_fighter_history=True):
    """Scrape fight details from an individual event page"""
    try:
        soup = get_soup(event_url)
        if not soup:
            return []
        
        fights_table = soup.select_one('table.b-fight-details__table')
        if not fights_table:
            return []
        
        fights_data = []
        tbody = fights_table.find('tbody')
        if not tbody:
            return []
        
        rows = tbody.select('tr.b-fight-details__table-row')
        
        for fight_idx, row in enumerate(rows, 1):
            try:
                cells = row.select('td.b-fight-details__table-col')
                if len(cells) >= 9:
                    # Extract fighter names and URLs
                    fighters_cell = cells[1]
                    fighter_links = fighters_cell.select('a.b-link')
                    
                    fighter_1_name = fighter_links[0].get_text(strip=True) if len(fighter_links) > 0 else 'N/A'
                    fighter_1_url = fighter_links[0].get('href') if len(fighter_links) > 0 else None
                    fighter_2_name = fighter_links[1].get_text(strip=True) if len(fighter_links) > 1 else 'N/A'
                    fighter_2_url = fighter_links[1].get('href') if len(fighter_links) > 1 else None
                    
                    # Extract fight result icons
                    result_icons = fighters_cell.find_all('i')
                    fighter_1_result = 'N/A'
                    fighter_2_result = 'N/A'
                    
                    if len(result_icons) >= 2:
                        icon_classes_1 = result_icons[0].get('class', [])
                        if 'b-fight-details__person-status_style_green' in icon_classes_1:
                            fighter_1_result = 'W'
                        elif 'b-fight-details__person-status_style_gray' in icon_classes_1:
                            fighter_1_result = 'D'
                        else:
                            fighter_1_result = 'L'
                        
                        icon_classes_2 = result_icons[1].get('class', [])
                        if 'b-fight-details__person-status_style_green' in icon_classes_2:
                            fighter_2_result = 'W'
                        elif 'b-fight-details__person-status_style_gray' in icon_classes_2:
                            fighter_2_result = 'D'
                        else:
                            fighter_2_result = 'L'
                    
                    # Extract basic fight info
                    weight_class = cells[6].get_text(strip=True) if len(cells) > 6 else 'N/A'
                    method = cells[7].get_text(strip=True) if len(cells) > 7 else 'N/A'
                    round_finished = cells[8].get_text(strip=True) if len(cells) > 8 else 'N/A'
                    time_finished = cells[9].get_text(strip=True) if len(cells) > 9 else 'N/A'
                    
                    # Extract summary stats
                    kd_elements = cells[2].find_all('p') if len(cells) > 2 else []
                    kd_1 = kd_elements[0].get_text(strip=True) if len(kd_elements) > 0 else 'N/A'
                    kd_2 = kd_elements[1].get_text(strip=True) if len(kd_elements) > 1 else 'N/A'
                    
                    str_elements = cells[3].find_all('p') if len(cells) > 3 else []
                    str_1 = str_elements[0].get_text(strip=True) if len(str_elements) > 0 else 'N/A'
                    str_2 = str_elements[1].get_text(strip=True) if len(str_elements) > 1 else 'N/A'
                    
                    td_elements = cells[4].find_all('p') if len(cells) > 4 else []
                    td_1 = td_elements[0].get_text(strip=True) if len(td_elements) > 0 else 'N/A'
                    td_2 = td_elements[1].get_text(strip=True) if len(td_elements) > 1 else 'N/A'
                    
                    sub_elements = cells[5].find_all('p') if len(cells) > 5 else []
                    sub_1 = sub_elements[0].get_text(strip=True) if len(sub_elements) > 0 else 'N/A'
                    sub_2 = sub_elements[1].get_text(strip=True) if len(sub_elements) > 1 else 'N/A'
                    
                    # Get fight detail URL
                    fight_detail_url = None
                    first_cell_link = cells[0].select_one('a.b-flag')
                    if first_cell_link:
                        fight_detail_url = first_cell_link.get('href')
                    
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
                    
                    print(f"  → Fight {fight_idx}: {fighter_1_name} vs {fighter_2_name}")
                    
                    # Scrape fighter profile data if enabled
                    if scrape_fighter_profiles:
                        if fighter_1_url:
                            print(f"  → Scraping {fighter_1_name} profile...")
                            f1_profile = scrape_fighter_profile(fighter_1_url, scrape_history=scrape_fighter_history)
                            
                            for key, value in f1_profile.items():
                                if key != 'Fight_History':
                                    fight_data[f'Fighter 1 {key}'] = value
                            
                            if 'Fight_History' in f1_profile:
                                fight_data['Fighter 1 Fight History'] = f1_profile['Fight_History']
                            
                            time.sleep(0.5)
                        
                        if fighter_2_url:
                            print(f"  → Scraping {fighter_2_name} profile...")
                            f2_profile = scrape_fighter_profile(fighter_2_url, scrape_history=scrape_fighter_history)
                            
                            for key, value in f2_profile.items():
                                if key != 'Fight_History':
                                    fight_data[f'Fighter 2 {key}'] = value
                            
                            if 'Fight_History' in f2_profile:
                                fight_data['Fighter 2 Fight History'] = f2_profile['Fight_History']
                            
                            time.sleep(0.5)
                    
                    # Scrape detailed fight stats if enabled
                    if scrape_detailed_stats and fight_detail_url:
                        print(f"  → Scraping round-by-round fight details...")
                        detailed_stats = scrape_fight_detail_stats(fight_detail_url)
                        fight_data.update(detailed_stats)
                        time.sleep(0.5)
                    
                    fights_data.append(fight_data)
                    
            except Exception as e:
                print(f"  Warning: Error extracting fight {fight_idx} data: {e}")
                continue
        
        return fights_data
        
    except Exception as e:
        print(f"  Error scraping fight details from {event_url}: {e}")
        return []

def scrape_ufc_events_and_fights(max_events=None, max_pages=None, scrape_detailed_stats=True,
                                 scrape_fighter_profiles=True, scrape_fighter_history=True):
    """
    Scrape completed UFC events and all fight details from each event
    
    Parameters:
    - max_events: Number of events to scrape (None for all)
    - max_pages: Number of pages to scrape (None for all pages)
    - scrape_detailed_stats: Include round-by-round fight statistics
    - scrape_fighter_profiles: Include fighter physical stats and career stats
    - scrape_fighter_history: Include complete fight history for each fighter
    """
    try:
        # First, determine how many pages exist
        print("Checking pagination...")
        soup = get_soup("http://ufcstats.com/statistics/events/completed")
        if not soup:
            print("Failed to load main page")
            return pd.DataFrame()
        
        total_pages = 1
        pagination = soup.select('ul.b-statistics__paginate li a')
        if pagination:
            page_numbers = []
            for link in pagination:
                try:
                    page_text = link.get_text(strip=True)
                    if page_text.isdigit():
                        page_numbers.append(int(page_text))
                except:
                    pass
            if page_numbers:
                total_pages = max(page_numbers)
        
        print(f"Found {total_pages} total pages of events")
        
        # Determine how many pages to scrape
        pages_to_scrape = min(total_pages, max_pages) if max_pages else total_pages
        print(f"Will scrape {pages_to_scrape} pages")
        
        all_fights_data = []
        events_processed = 0
        total_events_count = 0
        
        print(f"Fighter profiles scraping: {'ENABLED' if scrape_fighter_profiles else 'DISABLED'}")
        print(f"Fighter history scraping: {'ENABLED' if scrape_fighter_history else 'DISABLED'}")
        print(f"Detailed fight stats scraping: {'ENABLED' if scrape_detailed_stats else 'DISABLED'}")
        
        # Iterate through each page
        for page_num in range(1, pages_to_scrape + 1):
            print(f"\n{'#'*70}")
            print(f"# PAGE {page_num}/{pages_to_scrape}")
            print(f"{'#'*70}")
            
            # Navigate to the page
            if page_num == 1:
                page_url = "http://ufcstats.com/statistics/events/completed"
            else:
                page_url = f"http://ufcstats.com/statistics/events/completed?page={page_num}"
            
            soup = get_soup(page_url)
            if not soup:
                print(f"Could not load page {page_num}")
                continue
            
            # Get events from this page
            events_table = soup.select_one('table.b-statistics__table-events')
            if not events_table:
                print(f"Could not find events table on page {page_num}")
                continue
            
            tbody = events_table.find('tbody')
            if not tbody:
                continue
            
            rows = tbody.select('tr.b-statistics__table-row')
            print(f"Found {len(rows)} events on page {page_num}")
            
            # Check if we've hit max_events limit
            events_to_process_on_page = rows
            if max_events and (total_events_count + len(rows)) > max_events:
                remaining = max_events - total_events_count
                events_to_process_on_page = rows[:remaining]
                print(f"Limiting to {remaining} events to meet max_events={max_events}")
            
            # Process each event on this page
            for idx, row in enumerate(events_to_process_on_page, 1):
                cells = row.select('td.b-statistics__table-col')
                if len(cells) >= 2:
                    try:
                        event_link_tag = cells[0].select_one('a.b-link')
                        event_name = event_link_tag.get_text(strip=True) if event_link_tag else 'N/A'
                        event_url = event_link_tag.get('href') if event_link_tag else None
                    except:
                        event_name = 'N/A'
                        event_url = None
                    
                    event_date = cells[1].get_text(strip=True) if len(cells) > 1 else 'N/A'
                    event_location = cells[2].get_text(strip=True) if len(cells) > 2 else 'N/A'
                    
                    if event_url:
                        total_events_count += 1
                        print(f"\n{'='*70}")
                        print(f"[Event {total_events_count}] Scraping: {event_name}")
                        print(f"Date: {event_date} | Location: {event_location}")
                        print(f"{'='*70}")
                        
                        fights = scrape_fight_details(event_url, event_name, event_date, event_location,
                                                     scrape_detailed_stats, scrape_fighter_profiles,
                                                     scrape_fighter_history)
                        all_fights_data.extend(fights)
                        print(f"  ✓ Found {len(fights)} fights in this event")
                        events_processed += 1
                        time.sleep(1)
            
            # Check if we've reached max_events
            if max_events and total_events_count >= max_events:
                print(f"\n✓ Reached max_events limit ({max_events})")
                break
        
        # Create main dataframe
        df = pd.DataFrame(all_fights_data)
        df_main = None
        
        print("\n" + "=" * 70)
        print(f"Successfully scraped {events_processed} events with {len(all_fights_data)} total fights")
        
        if len(df) > 0:
            # Save main data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Create a copy without fight history for main CSV
            df_main = df.copy()
            
            # Extract and save fighter histories separately
            all_fighter_histories = []
            for idx, row in df.iterrows():
                # Fighter 1 history
                if 'Fighter 1 Fight History' in row and row['Fighter 1 Fight History']:
                    for fight in row['Fighter 1 Fight History']:
                        history_entry = {
                            'Fighter_Name': row['Fighter 1'],
                            'Fighter_URL': row['Fighter 1 URL'],
                            'Event_Name': row['Event Name'],
                            'Event_Date': row['Event Date'],
                            **fight
                        }
                        all_fighter_histories.append(history_entry)
                
                # Fighter 2 history
                if 'Fighter 2 Fight History' in row and row['Fighter 2 Fight History']:
                    for fight in row['Fighter 2 Fight History']:
                        history_entry = {
                            'Fighter_Name': row['Fighter 2'],
                            'Fighter_URL': row['Fighter 2 URL'],
                            'Event_Name': row['Event Name'],
                            'Event_Date': row['Event Date'],
                            **fight
                        }
                        all_fighter_histories.append(history_entry)
            
            # Remove fight history columns from main dataframe
            if 'Fighter 1 Fight History' in df_main.columns:
                df_main = df_main.drop(columns=['Fighter 1 Fight History'])
            if 'Fighter 2 Fight History' in df_main.columns:
                df_main = df_main.drop(columns=['Fighter 2 Fight History'])
            
            # Save main fights data
            main_filename = f"ufc_fights_{timestamp}.csv"
            df_main.to_csv(main_filename, index=False)
            print(f"\n✓ Main fights data saved to: {main_filename}")
            print(f"  Total columns: {len(df_main.columns)}")
            print(f"  Total rows: {len(df_main)}")
            
            # Save fighter histories
            if all_fighter_histories:
                df_history = pd.DataFrame(all_fighter_histories)
                history_filename = f"ufc_fighter_histories_{timestamp}.csv"
                df_history.to_csv(history_filename, index=False)
                print(f"\n✓ Fighter histories saved to: {history_filename}")
                print(f"  Total columns: {len(df_history.columns)}")
                print(f"  Total rows: {len(df_history)}")
            
            print(f"\n{'='*70}")
            print("SAMPLE DATA (First Fight):")
            print(f"{'='*70}")
            print(df_main.head(1).T)
        else:
            print("\n⚠ No data was collected. Check if events have fights or if there were errors.")
        
        return df_main if df_main is not None else pd.DataFrame()
        
    except Exception as e:
        print(f"Error during scraping: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

if __name__ == "__main__":
    print("=" * 70)
    print("UFC STATS COMPREHENSIVE SCRAPER (BeautifulSoup4)")
    print("=" * 70)
    print("This scraper collects:")
    print("  • All UFC events")
    print("  • All fights in each event")
    print("  • Fighter profiles (physical stats, career stats)")
    print("  • Complete fight history for each fighter")
    print("  • Round-by-round fight statistics")
    print("=" * 70)
    
    # Configuration:
    # - max_events: Number of events to scrape (None for all, recommend starting with 2-5 for testing)
    # - scrape_detailed_stats: Include round-by-round fight statistics (adds more time)
    # - scrape_fighter_profiles: Include fighter physical stats and career stats
    # - scrape_fighter_history: Include complete fight history for each fighter (adds significant time)
    
    # WARNING: Scraping all events with full fighter histories will take MANY HOURS
    # Start with max_events=2 to test, then increase gradually
    
    df = scrape_ufc_events_and_fights(
        max_events=None,  # Start small for testing!
        max_pages=None,
        scrape_detailed_stats=True,
        scrape_fighter_profiles=True,
        scrape_fighter_history=True  # Set to False to speed up significantly
    )
    
    if df is not None and len(df) > 0:
        print("\n" + "=" * 70)
        print("✓ SCRAPING COMPLETE!")
        print("=" * 70)
        print(f"Total fights scraped: {len(df)}")
        print(f"Unique events: {df['Event Name'].nunique()}")
        print(f"Unique fighters: {len(set(df['Fighter 1'].tolist() + df['Fighter 2'].tolist()))}")
        print("\nFiles saved in current directory")
        print("=" * 70)
    else:
        print("\n✗ Scraping failed or returned no data")