import sqlite3
from datetime import datetime
from typing import Optional, Dict, Any

"""
UFC Database Manager - Stores fighter profiles, events, and fight results.

Career Statistics Definitions:
- SLpM (Significant Strikes Landed per Minute): Avg significant strikes landed per minute
- Str. Acc. (Significant Striking Accuracy): % of significant strikes that land
- SApM (Significant Strikes Absorbed per Minute): Avg significant strikes absorbed per minute
- Str. Def. (Significant Strike Defence): % of opponent's strikes that did not land
- TD Avg. (Takedown Average): Avg takedowns landed per 15 minutes
- TD Acc. (Takedown Accuracy): % of takedown attempts that land
- TD Def. (Takedown Defense): % of opponent's TD attempts that did not land
- Sub. Avg. (Submission Average): Avg submission attempts per 15 minutes
"""

class UFCDatabase:
    def __init__(self, db_name: str = "ufc_data.db"):
        """Initialize the UFC database with tables for events, fighters, and fight results."""
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self._create_tables()
    
    def _create_tables(self):
        """Create the necessary tables for storing UFC data."""
        
        # Events table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_name TEXT NOT NULL,
                event_date DATE NOT NULL,
                UNIQUE(event_name, event_date)
            )
        ''')
        
        # Fighters table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS fighters (
                fighter_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                height TEXT,
                weight REAL,
                reach REAL,
                stance TEXT,
                dob DATE,
                slpm REAL,
                str_acc REAL,
                sapm REAL,
                str_def REAL,
                td_avg REAL,
                td_acc REAL,
                td_def REAL,
                sub_avg REAL
            )
        ''')
        
        # Fight results table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS fight_results (
                fight_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                fighter_id INTEGER NOT NULL,
                kd INTEGER,
                sig_str INTEGER,
                td INTEGER,
                sub INTEGER,
                result TEXT,
                method TEXT,
                method_detail TEXT,
                round INTEGER,
                time TEXT,
                FOREIGN KEY (event_id) REFERENCES events(event_id),
                FOREIGN KEY (fighter_id) REFERENCES fighters(fighter_id)
            )
        ''')
        
        self.conn.commit()
    
    def add_event(self, event_name: str, event_date: str) -> int:
        """
        Add a UFC event to the database.
        
        Args:
            event_name: Name of the event (e.g., "UFC 320: Ankalaev vs. Pereira 2")
            event_date: Date in format "MMM. DD, YYYY" or "YYYY-MM-DD"
        
        Returns:
            event_id: The ID of the inserted/existing event
        """
        # Parse date
        try:
            if ',' in event_date:
                date_obj = datetime.strptime(event_date, "%b. %d, %Y")
            else:
                date_obj = datetime.strptime(event_date, "%Y-%m-%d")
            formatted_date = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format: {event_date}")
        
        self.cursor.execute('''
            INSERT OR IGNORE INTO events (event_name, event_date)
            VALUES (?, ?)
        ''', (event_name, formatted_date))
        
        self.cursor.execute('''
            SELECT event_id FROM events WHERE event_name = ? AND event_date = ?
        ''', (event_name, formatted_date))
        
        event_id = self.cursor.fetchone()[0]
        self.conn.commit()
        return event_id
    
    def add_fighter(self, name: str, height: Optional[str] = None, 
                   weight: Optional[float] = None, reach: Optional[float] = None,
                   stance: Optional[str] = None, dob: Optional[str] = None,
                   career_stats: Optional[Dict[str, float]] = None) -> int:
        """
        Add or update a fighter in the database.
        
        Args:
            name: Fighter's name
            height: Height (e.g., "6' 3\"")
            weight: Weight in lbs
            reach: Reach in inches
            stance: Fighting stance (e.g., "Orthodox")
            dob: Date of birth in format "MMM DD, YYYY"
            career_stats: Dictionary with keys: slpm, str_acc, sapm, str_def, 
                         td_avg, td_acc, td_def, sub_avg
        
        Returns:
            fighter_id: The ID of the inserted/updated fighter
        """
        # Parse DOB if provided
        formatted_dob = None
        if dob:
            try:
                dob_obj = datetime.strptime(dob, "%b %d, %Y")
                formatted_dob = dob_obj.strftime("%Y-%m-%d")
            except ValueError:
                formatted_dob = dob
        
        # Extract career stats
        stats = career_stats or {}
        
        self.cursor.execute('''
            INSERT INTO fighters (name, height, weight, reach, stance, dob, 
                                 slpm, str_acc, sapm, str_def, td_avg, td_acc, td_def, sub_avg)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                height = COALESCE(excluded.height, height),
                weight = COALESCE(excluded.weight, weight),
                reach = COALESCE(excluded.reach, reach),
                stance = COALESCE(excluded.stance, stance),
                dob = COALESCE(excluded.dob, dob),
                slpm = COALESCE(excluded.slpm, slpm),
                str_acc = COALESCE(excluded.str_acc, str_acc),
                sapm = COALESCE(excluded.sapm, sapm),
                str_def = COALESCE(excluded.str_def, str_def),
                td_avg = COALESCE(excluded.td_avg, td_avg),
                td_acc = COALESCE(excluded.td_acc, td_acc),
                td_def = COALESCE(excluded.td_def, td_def),
                sub_avg = COALESCE(excluded.sub_avg, sub_avg)
        ''', (name, height, weight, reach, stance, formatted_dob,
              stats.get('slpm'), stats.get('str_acc'), stats.get('sapm'),
              stats.get('str_def'), stats.get('td_avg'), stats.get('td_acc'),
              stats.get('td_def'), stats.get('sub_avg')))
        
        self.cursor.execute('SELECT fighter_id FROM fighters WHERE name = ?', (name,))
        fighter_id = self.cursor.fetchone()[0]
        self.conn.commit()
        return fighter_id
    
    def add_fight_result(self, event_id: int, fighter_id: int, 
                        kd: int = 0, sig_str: int = 0, td: int = 0, sub: int = 0,
                        result: Optional[str] = None,
                        method: Optional[str] = None, method_detail: Optional[str] = None,
                        round_num: Optional[int] = None, time: Optional[str] = None):
        """
        Add fight statistics for a fighter in an event.
        
        Args:
            event_id: ID of the event
            fighter_id: ID of the fighter
            kd: Knockdowns
            sig_str: Significant strikes landed
            td: Takedowns landed
            sub: Submission attempts
            result: Fight result - "Win", "Loss", "Draw", or "NC" (No Contest)
            method: Finish method (e.g., "KO/TKO", "Submission", "Decision")
            method_detail: Details of the method (e.g., "Elbows")
            round_num: Round number
            time: Time of finish (e.g., "1:20")
        """
        self.cursor.execute('''
            INSERT INTO fight_results (event_id, fighter_id, kd, sig_str, td, sub,
                                      result, method, method_detail, round, time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (event_id, fighter_id, kd, sig_str, td, sub, 
              result, method, method_detail, round_num, time))
        
        self.conn.commit()
    
    def get_fighter_stats(self, fighter_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve all stats for a fighter."""
        self.cursor.execute('''
            SELECT * FROM fighters WHERE name = ?
        ''', (fighter_name,))
        
        row = self.cursor.fetchone()
        if not row:
            return None
        
        columns = [desc[0] for desc in self.cursor.description]
        return dict(zip(columns, row))
    
    def get_event_fights(self, event_name: str) -> list:
        """Retrieve all fights for a specific event."""
        self.cursor.execute('''
            SELECT f.name, fr.kd, fr.sig_str, fr.td, fr.sub, 
                   fr.result, fr.method, fr.method_detail, fr.round, fr.time
            FROM fight_results fr
            JOIN fighters f ON fr.fighter_id = f.fighter_id
            JOIN events e ON fr.event_id = e.event_id
            WHERE e.event_name = ?
        ''', (event_name,))
        
        return self.cursor.fetchall()
    
    def close(self):
        """Close the database connection."""
        self.conn.close()


# Example usage
if __name__ == "__main__":
    # Initialize database
    db = UFCDatabase("ufc_data.db")
    
    # Add event
    event_id = db.add_event("UFC 320: Ankalaev vs. Pereira 2", "Oct. 04, 2025")
    
    # Add fighter with career stats
    career_stats = {
        'slpm': 3.65,
        'str_acc': 52,
        'sapm': 2.59,
        'str_def': 56,
        'td_avg': 0.79,
        'td_acc': 22,
        'td_def': 87,
        'sub_avg': 0.0
    }
    
    fighter_id = db.add_fighter(
        name="Magomed Ankalaev",
        height="6' 3\"",
        weight=205,
        reach=75,
        stance="Orthodox",
        dob="Jun 02, 1992",
        career_stats=career_stats
    )
    
    # Add fight result
    db.add_fight_result(
        event_id=event_id,
        fighter_id=fighter_id,
        kd=0,
        sig_str=28,
        td=0,
        sub=0,
        result="Win",
        method="KO/TKO",
        method_detail="Elbows",
        round_num=1,
        time="1:20"
    )
    
    # Query data
    print("Fighter Stats:", db.get_fighter_stats("Magomed Ankalaev"))
    print("\nEvent Fights:", db.get_event_fights("UFC 320: Ankalaev vs. Pereira 2"))
    
    db.close()