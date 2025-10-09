import sqlite3
import pandas as pd
import json
from datetime import datetime
import os

class UFCDatabase:
    """Load UFC scraped data into a SQLite database with proper schema"""
    
    def __init__(self, db_name="ufc_stats.db"):
        self.db_name = db_name
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        print(f"✓ Connected to database: {db_name}")
    
    def create_schema(self):
        """Create database schema with normalized tables"""
        
        # Events table
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_name TEXT NOT NULL UNIQUE,
            event_date TEXT,
            event_location TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Fighters table
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS fighters (
            fighter_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fighter_url TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            nickname TEXT,
            record TEXT,
            height TEXT,
            weight TEXT,
            reach TEXT,
            stance TEXT,
            dob TEXT,
            career_slpm TEXT,
            career_str_acc TEXT,
            career_sapm TEXT,
            career_str_def TEXT,
            career_td_avg TEXT,
            career_td_acc TEXT,
            career_td_def TEXT,
            career_sub_avg TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Fights table
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS fights (
            fight_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER,
            fighter_1_id INTEGER,
            fighter_2_id INTEGER,
            fighter_1_result TEXT,
            fighter_2_result TEXT,
            weight_class TEXT,
            method TEXT,
            round INTEGER,
            time TEXT,
            fight_detail_url TEXT,
            
            -- Summary stats
            fighter_1_kd INTEGER,
            fighter_2_kd INTEGER,
            fighter_1_str TEXT,
            fighter_2_str TEXT,
            fighter_1_td TEXT,
            fighter_2_td TEXT,
            fighter_1_sub INTEGER,
            fighter_2_sub INTEGER,
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (event_id) REFERENCES events(event_id),
            FOREIGN KEY (fighter_1_id) REFERENCES fighters(fighter_id),
            FOREIGN KEY (fighter_2_id) REFERENCES fighters(fighter_id)
        )
        """)
        
        # Detailed fight stats table (round-by-round)
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS fight_stats (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fight_id INTEGER,
            fighter_number INTEGER,
            round_type TEXT,
            
            -- Core stats
            kd INTEGER,
            sig_str TEXT,
            sig_str_pct TEXT,
            total_str TEXT,
            td TEXT,
            td_pct TEXT,
            sub_att INTEGER,
            rev INTEGER,
            ctrl TEXT,
            
            -- Sig strikes breakdown
            head TEXT,
            body TEXT,
            leg TEXT,
            distance TEXT,
            clinch TEXT,
            ground TEXT,
            
            FOREIGN KEY (fight_id) REFERENCES fights(fight_id)
        )
        """)
        
        # Fighter fight history table
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS fighter_history (
            history_id INTEGER PRIMARY KEY AUTOINCREMENT,
            fighter_id INTEGER,
            opponent_name TEXT,
            opponent_url TEXT,
            result TEXT,
            kd TEXT,
            str TEXT,
            td TEXT,
            weight_class TEXT,
            method TEXT,
            round TEXT,
            time TEXT,
            event_name TEXT,
            event_url TEXT,
            event_date TEXT,
            
            FOREIGN KEY (fighter_id) REFERENCES fighters(fighter_id)
        )
        """)
        
        # Create indexes for better query performance
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_fights_event ON fights(event_id)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_fights_fighter1 ON fights(fighter_1_id)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_fights_fighter2 ON fights(fighter_2_id)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_history_fighter ON fighter_history(fighter_id)")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_stats_fight ON fight_stats(fight_id)")
        
        self.conn.commit()
        print("✓ Database schema created")
    
    def insert_or_get_event(self, event_name, event_date, event_location):
        """Insert event or get existing event_id"""
        self.cursor.execute("""
            INSERT OR IGNORE INTO events (event_name, event_date, event_location)
            VALUES (?, ?, ?)
        """, (event_name, event_date, event_location))
        
        self.cursor.execute("SELECT event_id FROM events WHERE event_name = ?", (event_name,))
        return self.cursor.fetchone()[0]
    
    def insert_or_update_fighter(self, fighter_data, prefix="Fighter 1"):
        """Insert or update fighter and return fighter_id"""
        fighter_url = fighter_data.get(f'{prefix} URL')
        if not fighter_url:
            return None
        
        # Check if fighter exists
        self.cursor.execute("SELECT fighter_id FROM fighters WHERE fighter_url = ?", (fighter_url,))
        result = self.cursor.fetchone()
        
        fighter_info = {
            'fighter_url': fighter_url,
            'name': fighter_data.get(f'{prefix}', 'N/A'),
            'nickname': fighter_data.get(f'{prefix} Nickname', 'N/A'),
            'record': fighter_data.get(f'{prefix} Record', 'N/A'),
            'height': fighter_data.get(f'{prefix} Height', 'N/A'),
            'weight': fighter_data.get(f'{prefix} Weight', 'N/A'),
            'reach': fighter_data.get(f'{prefix} Reach', 'N/A'),
            'stance': fighter_data.get(f'{prefix} STANCE', 'N/A'),
            'dob': fighter_data.get(f'{prefix} DOB', 'N/A'),
            'career_slpm': fighter_data.get(f'{prefix} Career_SLpM', 'N/A'),
            'career_str_acc': fighter_data.get(f'{prefix} Career_Str. Acc.', 'N/A'),
            'career_sapm': fighter_data.get(f'{prefix} Career_SApM', 'N/A'),
            'career_str_def': fighter_data.get(f'{prefix} Career_Str. Def', 'N/A'),
            'career_td_avg': fighter_data.get(f'{prefix} Career_TD Avg.', 'N/A'),
            'career_td_acc': fighter_data.get(f'{prefix} Career_TD Acc.', 'N/A'),
            'career_td_def': fighter_data.get(f'{prefix} Career_TD Def.', 'N/A'),
            'career_sub_avg': fighter_data.get(f'{prefix} Career_Sub. Avg.', 'N/A')
        }
        
        if result:
            # Update existing fighter
            fighter_id = result[0]
            self.cursor.execute("""
                UPDATE fighters SET
                    name = ?, nickname = ?, record = ?, height = ?, weight = ?,
                    reach = ?, stance = ?, dob = ?, career_slpm = ?, career_str_acc = ?,
                    career_sapm = ?, career_str_def = ?, career_td_avg = ?, career_td_acc = ?,
                    career_td_def = ?, career_sub_avg = ?, updated_at = CURRENT_TIMESTAMP
                WHERE fighter_id = ?
            """, (
                fighter_info['name'], fighter_info['nickname'], fighter_info['record'],
                fighter_info['height'], fighter_info['weight'], fighter_info['reach'],
                fighter_info['stance'], fighter_info['dob'], fighter_info['career_slpm'],
                fighter_info['career_str_acc'], fighter_info['career_sapm'], fighter_info['career_str_def'],
                fighter_info['career_td_avg'], fighter_info['career_td_acc'], fighter_info['career_td_def'],
                fighter_info['career_sub_avg'], fighter_id
            ))
        else:
            # Insert new fighter
            self.cursor.execute("""
                INSERT INTO fighters (
                    fighter_url, name, nickname, record, height, weight, reach, stance, dob,
                    career_slpm, career_str_acc, career_sapm, career_str_def, career_td_avg,
                    career_td_acc, career_td_def, career_sub_avg
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                fighter_info['fighter_url'], fighter_info['name'], fighter_info['nickname'],
                fighter_info['record'], fighter_info['height'], fighter_info['weight'],
                fighter_info['reach'], fighter_info['stance'], fighter_info['dob'],
                fighter_info['career_slpm'], fighter_info['career_str_acc'], fighter_info['career_sapm'],
                fighter_info['career_str_def'], fighter_info['career_td_avg'], fighter_info['career_td_acc'],
                fighter_info['career_td_def'], fighter_info['career_sub_avg']
            ))
            fighter_id = self.cursor.lastrowid
        
        return fighter_id
    
    def insert_fight(self, fight_data, event_id, fighter_1_id, fighter_2_id):
        """Insert fight record and return fight_id"""
        try:
            round_num = int(fight_data.get('Round', 0)) if fight_data.get('Round', 'N/A').isdigit() else None
        except:
            round_num = None
        
        self.cursor.execute("""
            INSERT INTO fights (
                event_id, fighter_1_id, fighter_2_id, fighter_1_result, fighter_2_result,
                weight_class, method, round, time, fight_detail_url,
                fighter_1_kd, fighter_2_kd, fighter_1_str, fighter_2_str,
                fighter_1_td, fighter_2_td, fighter_1_sub, fighter_2_sub
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id, fighter_1_id, fighter_2_id,
            fight_data.get('Fighter 1 Result', 'N/A'),
            fight_data.get('Fighter 2 Result', 'N/A'),
            fight_data.get('Weight Class', 'N/A'),
            fight_data.get('Method', 'N/A'),
            round_num,
            fight_data.get('Time', 'N/A'),
            fight_data.get('Fight Detail URL'),
            self._parse_int(fight_data.get('Fighter 1 KD', 0)),
            self._parse_int(fight_data.get('Fighter 2 KD', 0)),
            fight_data.get('Fighter 1 Str', 'N/A'),
            fight_data.get('Fighter 2 Str', 'N/A'),
            fight_data.get('Fighter 1 TD', 'N/A'),
            fight_data.get('Fighter 2 TD', 'N/A'),
            self._parse_int(fight_data.get('Fighter 1 Sub', 0)),
            self._parse_int(fight_data.get('Fighter 2 Sub', 0))
        ))
        
        return self.cursor.lastrowid
    
    def insert_fight_stats(self, fight_data, fight_id):
        """Insert detailed round-by-round fight statistics"""
        # Extract all stat columns from fight_data
        for key, value in fight_data.items():
            if key.startswith('F1_') or key.startswith('F2_'):
                # Parse the key: F1_Totals_KD or F1_Round_1_KD
                parts = key.split('_')
                fighter_num = 1 if parts[0] == 'F1' else 2
                round_type = '_'.join(parts[1:-1])  # Everything between fighter and stat name
                stat_name = parts[-1]
                
                # Get or create stat record for this fighter/round combination
                self.cursor.execute("""
                    SELECT stat_id FROM fight_stats 
                    WHERE fight_id = ? AND fighter_number = ? AND round_type = ?
                """, (fight_id, fighter_num, round_type))
                
                result = self.cursor.fetchone()
                
                if not result:
                    # Create new stat record
                    self.cursor.execute("""
                        INSERT INTO fight_stats (fight_id, fighter_number, round_type)
                        VALUES (?, ?, ?)
                    """, (fight_id, fighter_num, round_type))
                    stat_id = self.cursor.lastrowid
                else:
                    stat_id = result[0]
                
                # Update the specific stat column
                stat_column_map = {
                    'KD': 'kd',
                    'Sig': 'sig_str',
                    'Str': 'sig_str',
                    'Pct': 'sig_str_pct',
                    'Total': 'total_str',
                    'TD': 'td',
                    'Sub': 'sub_att',
                    'Rev': 'rev',
                    'Ctrl': 'ctrl',
                    'Head': 'head',
                    'Body': 'body',
                    'Leg': 'leg',
                    'Distance': 'distance',
                    'Clinch': 'clinch',
                    'Ground': 'ground'
                }
                
                # Find matching column
                for pattern, col_name in stat_column_map.items():
                    if pattern in stat_name:
                        try:
                            self.cursor.execute(f"""
                                UPDATE fight_stats SET {col_name} = ?
                                WHERE stat_id = ?
                            """, (value, stat_id))
                        except:
                            pass
                        break
    
    def insert_fighter_history(self, fighter_id, history_data):
        """Insert fighter's fight history"""
        if not history_data:
            return
        
        for fight in history_data:
            self.cursor.execute("""
                INSERT OR IGNORE INTO fighter_history (
                    fighter_id, opponent_name, opponent_url, result, kd, str, td,
                    weight_class, method, round, time, event_name, event_url, event_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                fighter_id,
                fight.get('Opponent', 'N/A'),
                fight.get('Opponent_URL'),
                fight.get('Result', 'N/A'),
                fight.get('KD', 'N/A'),
                fight.get('Str', 'N/A'),
                fight.get('TD', 'N/A'),
                fight.get('Weight_Class', 'N/A'),
                fight.get('Method', 'N/A'),
                fight.get('Round', 'N/A'),
                fight.get('Time', 'N/A'),
                fight.get('Event', 'N/A'),
                fight.get('Event_URL'),
                fight.get('Event_Date', 'N/A')
            ))
    
    def _parse_int(self, value):
        """Safely parse integer from string"""
        try:
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.isdigit():
                return int(value)
        except:
            pass
        return None
    
    def load_csv_to_database(self, fights_csv, history_csv=None):
        """Load data from CSV files into database"""
        print(f"\nLoading data from {fights_csv}...")
        
        # Read the main fights CSV
        df_fights = pd.read_csv(fights_csv)
        print(f"✓ Loaded {len(df_fights)} fights from CSV")
        
        total_fights = len(df_fights)
        
        for idx, row in df_fights.iterrows():
            if idx % 10 == 0:
                print(f"  Processing fight {idx + 1}/{total_fights}...")
            
            # Insert event
            event_id = self.insert_or_get_event(
                row.get('Event Name', 'N/A'),
                row.get('Event Date', 'N/A'),
                row.get('Event Location', 'N/A')
            )
            
            # Insert or update fighters
            fighter_1_id = self.insert_or_update_fighter(row, 'Fighter 1')
            fighter_2_id = self.insert_or_update_fighter(row, 'Fighter 2')
            
            # Insert fight
            fight_id = self.insert_fight(row, event_id, fighter_1_id, fighter_2_id)
            
            # Insert detailed fight stats
            self.insert_fight_stats(row.to_dict(), fight_id)
        
        self.conn.commit()
        print(f"✓ Loaded {total_fights} fights into database")
        
        # Load fighter histories if provided
        if history_csv and os.path.exists(history_csv):
            print(f"\nLoading fighter histories from {history_csv}...")
            df_history = pd.read_csv(history_csv)
            print(f"✓ Loaded {len(df_history)} history records from CSV")
            
            # Group by fighter
            for fighter_url, group in df_history.groupby('Fighter_URL'):
                # Get fighter_id
                self.cursor.execute("SELECT fighter_id FROM fighters WHERE fighter_url = ?", (fighter_url,))
                result = self.cursor.fetchone()
                if result:
                    fighter_id = result[0]
                    history_records = group.to_dict('records')
                    self.insert_fighter_history(fighter_id, history_records)
            
            self.conn.commit()
            print(f"✓ Loaded fighter histories into database")
    
    def get_stats(self):
        """Print database statistics"""
        print("\n" + "=" * 70)
        print("DATABASE STATISTICS")
        print("=" * 70)
        
        self.cursor.execute("SELECT COUNT(*) FROM events")
        print(f"Total Events: {self.cursor.fetchone()[0]}")
        
        self.cursor.execute("SELECT COUNT(*) FROM fighters")
        print(f"Total Fighters: {self.cursor.fetchone()[0]}")
        
        self.cursor.execute("SELECT COUNT(*) FROM fights")
        print(f"Total Fights: {self.cursor.fetchone()[0]}")
        
        self.cursor.execute("SELECT COUNT(*) FROM fight_stats")
        print(f"Total Fight Stat Records: {self.cursor.fetchone()[0]}")
        
        self.cursor.execute("SELECT COUNT(*) FROM fighter_history")
        print(f"Total Fighter History Records: {self.cursor.fetchone()[0]}")
        
        print("=" * 70)
    
    def query_examples(self):
        """Show some example queries"""
        print("\n" + "=" * 70)
        print("EXAMPLE QUERIES")
        print("=" * 70)
        
        # Most recent events
        print("\n1. Most Recent Events:")
        self.cursor.execute("""
            SELECT event_name, event_date, event_location
            FROM events
            ORDER BY event_date DESC
            LIMIT 5
        """)
        for row in self.cursor.fetchall():
            print(f"   {row[0]} - {row[1]} ({row[2]})")
        
        # Top fighters by number of fights
        print("\n2. Fighters with Most Fights in Database:")
        self.cursor.execute("""
            SELECT f.name, COUNT(*) as fight_count
            FROM fighters f
            JOIN fights ON f.fighter_id = fights.fighter_1_id OR f.fighter_id = fights.fighter_2_id
            GROUP BY f.fighter_id
            ORDER BY fight_count DESC
            LIMIT 5
        """)
        for row in self.cursor.fetchall():
            print(f"   {row[0]}: {row[1]} fights")
        
        print("\n" + "=" * 70)
    
    def close(self):
        """Close database connection"""
        self.conn.close()
        print(f"✓ Database connection closed")

def main():
    """Main function to load CSV data into database"""
    print("=" * 70)
    print("UFC DATA TO DATABASE LOADER")
    print("=" * 70)
    
    # Find the most recent CSV files
    import glob
    fight_files = sorted(glob.glob("ufc_fights_*.csv"), reverse=True)
    history_files = sorted(glob.glob("ufc_fighter_histories_*.csv"), reverse=True)
    
    if not fight_files:
        print("❌ No UFC fights CSV files found!")
        print("Please run the scraper first to generate CSV files.")
        return
    
    fights_csv = fight_files[0]
    history_csv = history_files[0] if history_files else None
    
    print(f"\nFound files:")
    print(f"  Fights: {fights_csv}")
    if history_csv:
        print(f"  History: {history_csv}")
    
    # Initialize database
    db = UFCDatabase("ufc_stats.db")
    
    # Create schema
    db.create_schema()
    
    # Load data
    db.load_csv_to_database(fights_csv, history_csv)
    
    # Show statistics
    db.get_stats()
    
    # Show example queries
    db.query_examples()
    
    # Close connection
    db.close()
    
    print("\n✓ Data successfully loaded into database: ufc_stats.db")
    print("You can now query the database using SQLite tools or Python!")

if __name__ == "__main__":
    main()