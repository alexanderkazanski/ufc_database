import requests
from bs4 import BeautifulSoup
import json
import pdb

def get_fighter_details(fighter_url):
    """Extract detailed fighter information from their profile page"""
    response = requests.get(fighter_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    fighter_data = {}
    
    # Get fighter name
    name_elem = soup.find('span', class_='b-content__title-highlight')
    fighter_data['name'] = name_elem.text.strip() if name_elem else 'N/A'
    
    # Get nickname
    nickname_elem = soup.find('p', class_='b-content__Nickname')
    if nickname_elem:
        nickname = nickname_elem.text.strip().replace('Nickname:', '').strip()
        fighter_data['nickname'] = nickname if nickname else 'N/A'
    else:
        fighter_data['nickname'] = 'N/A'
    
    # Get physical stats (Height, Weight, Reach, Stance, DOB)
    stats_list = soup.find_all('li', class_='b-list__box-list-item')
    
    for stat in stats_list:
        text = stat.text.strip()
        if 'Height:' in text:
            fighter_data['height'] = text.replace('Height:', '').strip()
        elif 'Weight:' in text:
            fighter_data['weight'] = text.replace('Weight:', '').strip()
        elif 'Reach:' in text:
            fighter_data['reach'] = text.replace('Reach:', '').strip()
        elif 'STANCE:' in text:
            fighter_data['stance'] = text.replace('STANCE:', '').strip()
        elif 'DOB:' in text:
            fighter_data['dob'] = text.replace('DOB:', '').strip()
    
    # Get career statistics
    career_stats = {}
    stats_boxes = soup.find_all('div', class_='b-list__info-box-left')
    
    for box in stats_boxes:
        label_elem = box.find('i', class_='b-list__box-item-title')
        value_elem = box.find('i', class_='b-list__box-item-title')
        
        if label_elem and value_elem:
            stats_text = box.text.strip().split('\n')
            if len(stats_text) >= 2:
                label = stats_text[0].strip()
                value = stats_text[-1].strip()
                career_stats[label] = value
    
    # Alternative method for career stats
    career_section = soup.find('div', class_='b-list__info-box-left clearfix')
    if career_section:
        stat_items = career_section.find_all('li', class_='b-list__box-list-item')
        for item in stat_items:
            text_parts = item.text.strip().split(':')
            if len(text_parts) == 2:
                key = text_parts[0].strip()
                value = text_parts[1].strip()
                career_stats[key] = value
    
    fighter_data['career_stats'] = career_stats
    
    return fighter_data

def scrape_ufc_event(url):
    """Scrape UFC event details and first fight information"""
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    event_data = {}
    
    # Get event name
    event_name = soup.find('h2', class_='b-content__title')
    event_data['event_name'] = event_name.text.strip() if event_name else 'N/A'
    
    # Get date and location
    details = soup.find_all('li', class_='b-list__box-list-item')
    
    for detail in details:
        text = detail.text.strip()
        if 'Date:' in text:
            event_data['date'] = text.replace('Date:', '').strip()
        elif 'Location:' in text:
            event_data['location'] = text.replace('Location:', '').strip()
    
    # Get first fight from the table
    fight_table = soup.find('table', class_='b-fight-details__table')
    
    if fight_table:
        tbody = fight_table.find('tbody')
        if tbody:
            first_fight_row = tbody.find('tr', class_='b-fight-details__table-row')
            
            if first_fight_row:
                fight_data = {}
                
                # Get fighter names and URLs
                fighters = first_fight_row.find_all('a', class_='b-link')
                
                if len(fighters) >= 2:
                    fighter1_url = fighters[0].get('href')
                    fighter2_url = fighters[1].get('href')
                    
                    fighter1_name = fighters[0].text.strip()
                    fighter2_name = fighters[1].text.strip()
                    
                    # Determine winner - look for W/L column (first data column)
                    winner = 'N/A'
                    
                    # Get all td elements in the row
                    all_cols = first_fight_row.find_all('td', class_='b-fight-details__table-col')
                    
                    # The first column should be the W/L column
                    if len(all_cols) > 0:
                        wl_column = all_cols[0]
                        
                        # Find the first b-flag__text - it's always at the top
                        first_flag = wl_column.find('i', class_='b-flag__text')
                        
                        if first_flag:
                            flag_text = first_flag.text.strip().lower()
                            
                            # Check if it's a win (not NC/nc or draw)
                            if 'win' in flag_text and 'nc' not in flag_text:
                                # Winner is always the first fighter (top position)
                                winner = fighter1_name
                            elif 'nc' in flag_text:
                                winner = 'No Contest'
                            elif 'draw' in flag_text:
                                winner = 'Draw'
                            else:
                                # If first fighter didn't win, second fighter won
                                winner = fighter2_name
                    
                    # Get detailed fighter information
                    print(f"Fetching details for {fighter1_name}...")
                    fighter1_details = get_fighter_details(fighter1_url)
                    
                    print(f"Fetching details for {fighter2_name}...")
                    fighter2_details = get_fighter_details(fighter2_url)
                    
                    fight_data['fighter1'] = fighter1_details
                    fight_data['fighter2'] = fighter2_details
                    fight_data['winner'] = winner
                    
                    # Get fight method, round, time
                    stats_cells = first_fight_row.find_all('td')
                    if len(stats_cells) >= 7:
                        fight_data['method'] = stats_cells[7].text.strip() if len(stats_cells) > 7 else 'N/A'
                        fight_data['round'] = stats_cells[8].text.strip() if len(stats_cells) > 8 else 'N/A'
                        fight_data['time'] = stats_cells[9].text.strip() if len(stats_cells) > 9 else 'N/A'
                
                event_data['first_fight'] = fight_data
    
    return event_data

# Main execution
if __name__ == "__main__":
    url = "http://ufcstats.com/event-details/8944a0f9b2f0ce6d"
    
    print("Scraping UFC event data...")
    print("-" * 50)
    
    event_info = scrape_ufc_event(url)
    
    # Print results in a readable format
    print(f"\nEVENT: {event_info.get('event_name', 'N/A')}")
    print(f"DATE: {event_info.get('date', 'N/A')}")
    print(f"LOCATION: {event_info.get('location', 'N/A')}")
    print("\n" + "=" * 50)
    print("FIRST FIGHT")
    print("=" * 50)
    
    if 'first_fight' in event_info:
        fight = event_info['first_fight']
        
        print(f"\nWINNER: {fight.get('winner', 'N/A')}")
        print(f"METHOD: {fight.get('method', 'N/A')}")
        print(f"ROUND: {fight.get('round', 'N/A')}")
        print(f"TIME: {fight.get('time', 'N/A')}")
        
        # Fighter 1 details
        if 'fighter1' in fight:
            f1 = fight['fighter1']
            print("\n" + "-" * 50)
            print("FIGHTER 1")
            print("-" * 50)
            print(f"Name: {f1.get('name', 'N/A')}")
            print(f"Nickname: {f1.get('nickname', 'N/A')}")
            print(f"Height: {f1.get('height', 'N/A')}")
            print(f"Weight: {f1.get('weight', 'N/A')}")
            print(f"Reach: {f1.get('reach', 'N/A')}")
            print(f"Stance: {f1.get('stance', 'N/A')}")
            print(f"DOB: {f1.get('dob', 'N/A')}")
            print("\nCareer Statistics:")
            for stat, value in f1.get('career_stats', {}).items():
                print(f"  {stat}: {value}")
        
        # Fighter 2 details
        if 'fighter2' in fight:
            f2 = fight['fighter2']
            print("\n" + "-" * 50)
            print("FIGHTER 2")
            print("-" * 50)
            print(f"Name: {f2.get('name', 'N/A')}")
            print(f"Nickname: {f2.get('nickname', 'N/A')}")
            print(f"Height: {f2.get('height', 'N/A')}")
            print(f"Weight: {f2.get('weight', 'N/A')}")
            print(f"Reach: {f2.get('reach', 'N/A')}")
            print(f"Stance: {f2.get('stance', 'N/A')}")
            print(f"DOB: {f2.get('dob', 'N/A')}")
            print("\nCareer Statistics:")
            for stat, value in f2.get('career_stats', {}).items():
                print(f"  {stat}: {value}")
    
    # Save to JSON file
    print("\n" + "=" * 50)
    with open('ufc_event_data.json', 'w') as f:
        json.dump(event_info, f, indent=2)
    print("Data saved to 'ufc_event_data.json'")