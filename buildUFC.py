import sqlite3
import csv
import re
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

"""
UFC Database Manager - Fixed and hardened import_from_csv and supporting helpers.

Key fixes & improvements:
- Robust parsing for "X of Y" fields and messy numeric strings (e.g. "4 of 928 of 45").
- Handles swapped/mis-labeled Event Date / Event Location by heuristics.
- Corrected ON CONFLICT typo (td_def update).
- Avoid double-count increments: only increment counters when an insertion actually happened.
- More flexible round parsing: recognizes F1_Totals_, F2_Totals_, ... blocks and maps them
  to rounds and fighter 1/2 using odd/even block numbers.
- Safer percentage parsing and numeric fallbacks.
"""

class UFCDatabase:
    def __init__(self, db_name: str = "ufc_data.db"):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self._create_tables()

    def _create_tables(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_name TEXT NOT NULL,
                event_date DATE NOT NULL,
                event_location TEXT,
                UNIQUE(event_name, event_date)
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS fighters (
                fighter_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                nickname TEXT,
                profile_url TEXT,
                height TEXT,
                weight TEXT,
                reach TEXT,
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

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS fight_results (
                fight_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                fighter_id INTEGER NOT NULL,
                opponent_id INTEGER,
                weight_class TEXT,
                kd INTEGER,
                sig_str INTEGER,
                td INTEGER,
                sub INTEGER,
                result TEXT,
                method TEXT,
                method_detail TEXT,
                round INTEGER,
                time TEXT,
                fight_url TEXT,
                FOREIGN KEY (event_id) REFERENCES events(event_id),
                FOREIGN KEY (fighter_id) REFERENCES fighters(fighter_id),
                FOREIGN KEY (opponent_id) REFERENCES fighters(fighter_id)
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS round_stats (
                round_stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                fight_id INTEGER NOT NULL,
                round_number INTEGER NOT NULL,
                kd INTEGER,
                sig_str INTEGER,
                sig_str_pct TEXT,
                total_str INTEGER,
                td INTEGER,
                td_pct TEXT,
                sub_att INTEGER,
                rev INTEGER,
                ctrl_time TEXT,
                FOREIGN KEY (fight_id) REFERENCES fight_results(fight_id)
            )
        ''')

        # helpful indexes
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_fighters_name ON fighters(name)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_name ON events(event_name)')

        self.conn.commit()

    #
    # -- Helper parsing functions
    #
    def _extract_first_int(self, value: Optional[str], default: int = 0) -> int:
        """Return first integer found in a string, or default."""
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return int(value)
        s = str(value).strip()
        if s == '' or s.upper() == 'N/A' or s == '---':
            return default
        m = re.search(r'(-?\d+)', s)
        return int(m.group(1)) if m else default

    def _extract_first_float(self, value: Optional[str], default: Optional[float] = None) -> Optional[float]:
        """Return first float-like token found (handles percentages), or default."""
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).strip()
        if s == '' or s.upper() == 'N/A' or s == '---':
            return default
        # remove any trailing % and find first float-ish token
        s_clean = s.replace('%', ' %')
        m = re.search(r'(-?\d+(?:\.\d+)?)', s_clean)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                return default
        return default

    def _extract_first_percent_str(self, value: Optional[str]) -> Optional[str]:
        """Return a clean percentage string (like '44%') if found, else None."""
        if not value:
            return None
        s = str(value).strip()
        m = re.search(r'(-?\d+(?:\.\d+)?)\s*%?', s)
        if m:
            return f"{m.group(1)}%"
        return None

    def _parse_landed_attempted(self, value: Optional[str]) -> Tuple[int, Optional[int]]:
        """
        Parse strings like:
         - "4 of 9"
         - "4 of 928 of 45"  (we'll take first two meaningful ints: landed, attempted if present)
         - "0 of 00 of 0"
         Return (landed, attempted_or_None)
        """
        if not value:
            return 0, None
        if isinstance(value, (int, float)):
            return int(value), None
        s = str(value).strip()
        # find all integers
        nums = re.findall(r'(-?\d+)', s)
        if not nums:
            return 0, None
        landed = int(nums[0])
        attempted = int(nums[1]) if len(nums) > 1 else None
        return landed, attempted

    def _safe_str(self, v: Optional[str]) -> Optional[str]:
        if v is None: return None
        s = str(v).strip()
        return s if s and s.upper() != 'N/A' else None

    #
    # -- Database add/get functions (made safer & non-duplicating)
    #
    def add_event(self, event_name: str, event_date: str, event_location: Optional[str] = None,
                  default_date: str = "2025-01-01") -> Tuple[int, bool]:
        """
        Add or return existing event.
        Returns (event_id, inserted_boolean)
        """
        event_name = event_name.strip() if event_name else 'Unknown Event'
        # try to parse event_date; if not parseable, use default_date
        formatted_date = None
        if event_date:
            ed = str(event_date).strip()
            # Heuristic: if string contains a year or month name, attempt parse
            if re.search(r'\b(19|20)\d{2}\b', ed) or re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b', ed, re.I):
                for fmt in ("%b %d, %Y", "%b. %d, %Y", "%Y-%m-%d"):
                    try:
                        formatted_date = datetime.strptime(ed, fmt).strftime("%Y-%m-%d")
                        break
                    except Exception:
                        continue
        if not formatted_date:
            # fallback to default_date
            try:
                # validate default_date as YYYY-MM-DD
                formatted_date = datetime.strptime(default_date, "%Y-%m-%d").strftime("%Y-%m-%d")
            except Exception:
                # ultimate fallback: today's date
                formatted_date = datetime.now().strftime("%Y-%m-%d")

        # Check existing
        self.cursor.execute('SELECT event_id FROM events WHERE event_name = ? AND event_date = ?', (event_name, formatted_date))
        row = self.cursor.fetchone()
        if row:
            return row[0], False

        self.cursor.execute('INSERT INTO events (event_name, event_date, event_location) VALUES (?, ?, ?)',
                            (event_name, formatted_date, event_location))
        self.conn.commit()
        return self.cursor.lastrowid, True

    def add_fighter(self, name: str, nickname: Optional[str] = None,
                   profile_url: Optional[str] = None,
                   height: Optional[str] = None,
                   weight: Optional[str] = None, reach: Optional[str] = None,
                   stance: Optional[str] = None, dob: Optional[str] = None,
                   career_stats: Optional[Dict[str, float]] = None) -> Tuple[int, bool]:
        """
        Add or update a fighter. Returns (fighter_id, inserted_boolean)
        """
        name = name.strip() if name else 'Unknown Fighter'
        formatted_dob = None
        if dob and dob != 'N/A':
            try:
                dob_obj = datetime.strptime(dob, "%b %d, %Y")
                formatted_dob = dob_obj.strftime("%Y-%m-%d")
            except ValueError:
                try:
                    dob_obj = datetime.strptime(dob, "%b. %d, %Y")
                    formatted_dob = dob_obj.strftime("%Y-%m-%d")
                except ValueError:
                    formatted_dob = dob

        stats = career_stats or {}

        # If fighter exists, update with COALESCE-like behavior
        self.cursor.execute('SELECT fighter_id FROM fighters WHERE name = ?', (name,))
        existing = self.cursor.fetchone()
        if existing:
            fighter_id = existing[0]
            self.cursor.execute('''
                UPDATE fighters SET
                    nickname = COALESCE(?, nickname),
                    profile_url = COALESCE(?, profile_url),
                    height = COALESCE(?, height),
                    weight = COALESCE(?, weight),
                    reach = COALESCE(?, reach),
                    stance = COALESCE(?, stance),
                    dob = COALESCE(?, dob),
                    slpm = COALESCE(?, slpm),
                    str_acc = COALESCE(?, str_acc),
                    sapm = COALESCE(?, sapm),
                    str_def = COALESCE(?, str_def),
                    td_avg = COALESCE(?, td_avg),
                    td_acc = COALESCE(?, td_acc),
                    td_def = COALESCE(?, td_def),
                    sub_avg = COALESCE(?, sub_avg)
                WHERE fighter_id = ?
            ''', (
                nickname, profile_url, height, weight, reach, stance, formatted_dob,
                stats.get('slpm'), stats.get('str_acc'), stats.get('sapm'), stats.get('str_def'),
                stats.get('td_avg'), stats.get('td_acc'), stats.get('td_def'), stats.get('sub_avg'),
                fighter_id
            ))
            self.conn.commit()
            return fighter_id, False

        # Insert new fighter
        self.cursor.execute('''
            INSERT INTO fighters (name, nickname, profile_url, height, weight, reach, stance, dob, 
                                 slpm, str_acc, sapm, str_def, td_avg, td_acc, td_def, sub_avg)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (name, nickname, profile_url, height, weight, reach, stance, formatted_dob,
              stats.get('slpm'), stats.get('str_acc'), stats.get('sapm'),
              stats.get('str_def'), stats.get('td_avg'), stats.get('td_acc'),
              stats.get('td_def'), stats.get('sub_avg')))
        self.conn.commit()
        return self.cursor.lastrowid, True

    def add_fight_result(self, event_id: int, fighter_id: int,
                        opponent_id: Optional[int] = None,
                        weight_class: Optional[str] = None,
                        kd: int = 0, sig_str: int = 0, td: int = 0, sub: int = 0,
                        result: Optional[str] = None,
                        method: Optional[str] = None, method_detail: Optional[str] = None,
                        round_num: Optional[int] = None, time: Optional[str] = None,
                        fight_url: Optional[str] = None) -> int:
        self.cursor.execute('''
            INSERT INTO fight_results (event_id, fighter_id, opponent_id, weight_class,
                                      kd, sig_str, td, sub, result, method, method_detail, 
                                      round, time, fight_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (event_id, fighter_id, opponent_id, weight_class, kd, sig_str, td, sub,
              result, method, method_detail, round_num, time, fight_url))
        self.conn.commit()
        return self.cursor.lastrowid

    def add_round_stats(self, fight_id: int, round_number: int,
                       kd: int = 0, sig_str: int = 0, sig_str_pct: Optional[str] = None,
                       total_str: int = 0, td: int = 0, td_pct: Optional[str] = None,
                       sub_att: int = 0, rev: int = 0, ctrl_time: Optional[str] = None):
        self.cursor.execute('''
            INSERT INTO round_stats (fight_id, round_number, kd, sig_str, sig_str_pct,
                                    total_str, td, td_pct, sub_att, rev, ctrl_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (fight_id, round_number, kd, sig_str, sig_str_pct, total_str,
              td, td_pct, sub_att, rev, ctrl_time))
        self.conn.commit()

    def _parse_stat(self, value: str) -> Optional[float]:
        """Parse a stat value, handling percentages and empty strings."""
        if not value or value == 'N/A' or value.strip() == '':
            return None
        # remove trailing percentages and take the first numeric token
        return self._extract_first_float(value, default=None)

    #
    # -- CSV Import
    #
    def import_from_csv(self, csv_file_path: str, default_date: str = "2025-01-01") -> Dict[str, int]:
        events_added = 0
        fighters_added = 0
        fights_added = 0
        rounds_added = 0

        with open(csv_file_path, 'r', encoding='utf-8', errors='ignore') as file:
            reader = csv.DictReader(file)

            # Build list of round-block keys from header (e.g. F1_Totals_..., F2_Totals_..., ...)
            header_keys = reader.fieldnames or []

            # For each row
            for row in reader:
                # Clean method
                method_raw = row.get('Method') or ''
                method_raw = method_raw.strip()
                method_lines = [ln.strip() for ln in method_raw.splitlines() if ln.strip()]
                method_type = method_lines[0] if method_lines else None
                method_detail = method_lines[1] if len(method_lines) > 1 else None

                # Heuristic for event date/location: sometimes CSV has swapped columns
                raw_event_date = self._safe_str(row.get('Event Date'))
                raw_event_location = self._safe_str(row.get('Event Location'))

                event_date_candidate = None
                event_location_candidate = None

                def looks_like_date(s: Optional[str]) -> bool:
                    if not s:
                        return False
                    return bool(re.search(r'\b(19|20)\d{2}\b', s) or re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b', s, re.I) or re.search(r'\d{4}-\d{2}-\d{2}', s))

                if looks_like_date(raw_event_date):
                    event_date_candidate = raw_event_date
                    event_location_candidate = raw_event_location
                elif looks_like_date(raw_event_location):
                    event_date_candidate = raw_event_location
                    event_location_candidate = raw_event_date
                else:
                    # Neither looks like a date, assume event_date not present; use default_date and use Event Date as location if it looks like one.
                    event_date_candidate = default_date
                    event_location_candidate = raw_event_date or raw_event_location

                # Add event (returns id and whether inserted)
                event_id, inserted_event = self.add_event(
                    event_name=row.get('Event Name') or 'Unknown Event',
                    event_date=event_date_candidate,
                    event_location=event_location_candidate,
                    default_date=default_date
                )
                if inserted_event:
                    events_added += 1

                # Fighter 1 career stats
                f1_stats = {
                    'slpm': self._parse_stat(row.get('Fighter 1 SLpM', '') or row.get('Fighter 1 SLpM', '')),
                    'str_acc': self._parse_stat(row.get('Fighter 1 Str. Acc.', '') or row.get('Fighter 1 Str. Acc.', '')),
                    'sapm': self._parse_stat(row.get('Fighter 1 SApM', '') or row.get('Fighter 1 SApM', '')),
                    'str_def': self._parse_stat(row.get('Fighter 1 Str. Def', '') or row.get('Fighter 1 Str. Def', '')),
                    'td_avg': self._parse_stat(row.get('Fighter 1 TD Avg.', '') or row.get('Fighter 1 TD Avg.', '')),
                    'td_acc': self._parse_stat(row.get('Fighter 1 TD Acc.', '') or row.get('Fighter 1 TD Acc.', '')),
                    'td_def': self._parse_stat(row.get('Fighter 1 TD Def.', '') or row.get('Fighter 1 TD Def.', '')),
                    'sub_avg': self._parse_stat(row.get('Fighter 1 Sub. Avg.', '') or row.get('Fighter 1 Sub. Avg.', ''))
                }

                fighter1_name = row.get('Fighter 1 Name') or row.get('Fighter 1') or 'Unknown Fighter 1'
                fighter1_id, inserted_f1 = self.add_fighter(
                    name=fighter1_name,
                    nickname=self._safe_str(row.get('Fighter 1 Nickname')) or None,
                    profile_url=self._safe_str(row.get('Fighter 1 URL')) or None,
                    height=self._safe_str(row.get('Fighter 1 Height')) or None,
                    weight=self._safe_str(row.get('Fighter 1 Weight')) or None,
                    reach=self._safe_str(row.get('Fighter 1 Reach')) or None,
                    stance=self._safe_str(row.get('Fighter 1 STANCE')) or None,
                    dob=self._safe_str(row.get('Fighter 1 DOB')) or None,
                    career_stats=f1_stats
                )
                if inserted_f1:
                    fighters_added += 1

                # Fighter 2 career stats
                f2_stats = {
                    'slpm': self._parse_stat(row.get('Fighter 2 SLpM', '') or row.get('Fighter 2 SLpM', '')),
                    'str_acc': self._parse_stat(row.get('Fighter 2 Str. Acc.', '') or row.get('Fighter 2 Str. Acc.', '')),
                    'sapm': self._parse_stat(row.get('Fighter 2 SApM', '') or row.get('Fighter 2 SApM', '')),
                    'str_def': self._parse_stat(row.get('Fighter 2 Str. Def', '') or row.get('Fighter 2 Str. Def', '')),
                    'td_avg': self._parse_stat(row.get('Fighter 2 TD Avg.', '') or row.get('Fighter 2 TD Avg.', '')),
                    'td_acc': self._parse_stat(row.get('Fighter 2 TD Acc.', '') or row.get('Fighter 2 TD Acc.', '')),
                    'td_def': self._parse_stat(row.get('Fighter 2 TD Def.', '') or row.get('Fighter 2 TD Def.', '')),
                    'sub_avg': self._parse_stat(row.get('Fighter 2 Sub. Avg.', '') or row.get('Fighter 2 Sub. Avg.', ''))
                }

                fighter2_name = row.get('Fighter 2 Name') or row.get('Fighter 2') or 'Unknown Fighter 2'
                fighter2_id, inserted_f2 = self.add_fighter(
                    name=fighter2_name,
                    nickname=self._safe_str(row.get('Fighter 2 Nickname')) or None,
                    profile_url=self._safe_str(row.get('Fighter 2 URL')) or None,
                    height=self._safe_str(row.get('Fighter 2 Height')) or None,
                    weight=self._safe_str(row.get('Fighter 2 Weight')) or None,
                    reach=self._safe_str(row.get('Fighter 2 Reach')) or None,
                    stance=self._safe_str(row.get('Fighter 2 STANCE')) or None,
                    dob=self._safe_str(row.get('Fighter 2 DOB')) or None,
                    career_stats=f2_stats
                )
                if inserted_f2:
                    fighters_added += 1

                # Parse fight-level numeric fields (be robust)
                # For result fields, filter N/A to None
                f1_result = row.get('Fighter 1 Result')
                f1_result = f1_result if f1_result and f1_result.upper() != 'N/A' else None
                f2_result = row.get('Fighter 2 Result')
                f2_result = f2_result if f2_result and f2_result.upper() != 'N/A' else None

                # KD/Str/TD/Sub fields - try to extract first int where necessary
                f1_kd = self._extract_first_int(row.get('Fighter 1 KD'))
                f2_kd = self._extract_first_int(row.get('Fighter 2 KD'))
                # Fighter-level strikes/takedowns/subs may be landed or "landed of attempted" strings; prefer first integer
                f1_str = self._extract_first_int(row.get('Fighter 1 Str'))
                f2_str = self._extract_first_int(row.get('Fighter 2 Str'))
                f1_td = self._extract_first_int(row.get('Fighter 1 TD'))
                f2_td = self._extract_first_int(row.get('Fighter 2 TD'))
                f1_sub = self._extract_first_int(row.get('Fighter 1 Sub'))
                f2_sub = self._extract_first_int(row.get('Fighter 2 Sub'))

                # Round number/time fields may be messy, try to parse int for round
                round_parsed = None
                if row.get('Round'):
                    try:
                        round_parsed = int(str(row.get('Round')).strip())
                    except Exception:
                        round_parsed = None

                fight_url = self._safe_str(row.get('Fight Detail URL'))

                # Add fight results for both fighters
                f1_fight_id = self.add_fight_result(
                    event_id=event_id,
                    fighter_id=fighter1_id,
                    opponent_id=fighter2_id,
                    weight_class=self._safe_str(row.get('Weight Class')),
                    kd=f1_kd,
                    sig_str=f1_str,
                    td=f1_td,
                    sub=f1_sub,
                    result=f1_result,
                    method=method_type,
                    method_detail=method_detail,
                    round_num=round_parsed,
                    time=self._safe_str(row.get('Time')),
                    fight_url=fight_url
                )
                fights_added += 1

                f2_fight_id = self.add_fight_result(
                    event_id=event_id,
                    fighter_id=fighter2_id,
                    opponent_id=fighter1_id,
                    weight_class=self._safe_str(row.get('Weight Class')),
                    kd=f2_kd,
                    sig_str=f2_str,
                    td=f2_td,
                    sub=f2_sub,
                    result=f2_result,
                    method=method_type,
                    method_detail=method_detail,
                    round_num=round_parsed,
                    time=self._safe_str(row.get('Time')),
                    fight_url=fight_url
                )
                fights_added += 1

                # --- Round parsing logic (robust for headers like F1_Totals_..., F2_Totals_..., F3_Totals_...) ---
                # Build a map of block_number -> keys present for that block in the header
                # Block numbers are extracted from keys like 'F3_Totals_KD' -> block 3
                block_nums = set()
                block_key_pattern = re.compile(r'^F(\d+)_Totals_(.+)$')
                for key in header_keys:
                    if key is None:
                        continue
                    m = block_key_pattern.match(key)
                    if m:
                        block_nums.add(int(m.group(1)))
                sorted_blocks = sorted(block_nums)

                # For each block, map block->round and fighter:
                # block 1 -> round 1 fighter1
                # block 2 -> round 1 fighter2
                # block 3 -> round 2 fighter1
                # block 4 -> round 2 fighter2, etc.
                for block in sorted_blocks:
                    # compute round and fighter index
                    round_num = (block + 1) // 2  # 1-based
                    fighter_index = 1 if (block % 2) == 1 else 2
                    base_prefix = f'F{block}_Totals_'

                    # presence check: if KD or Sig_Str column not present or empty, skip
                    kd_key = f'{base_prefix}KD'
                    sig_key = f'{base_prefix}Sig_Str'
                    total_str_key = f'{base_prefix}Total_Str'
                    td_key = f'{base_prefix}TD'
                    sub_att_key = f'{base_prefix}Sub_Att'
                    sig_pct_key = f'{base_prefix}Sig_Str_Pct'
                    td_pct_key = f'{base_prefix}TD_Pct'
                    rev_key = f'{base_prefix}Rev'
                    ctrl_key = f'{base_prefix}Ctrl'

                    # If no KD or Sig_Str found for this block, skip
                    if not any(k in row and row.get(k) and str(row.get(k)).strip() not in ['', 'N/A', '---'] for k in (kd_key, sig_key, total_str_key)):
                        continue

                    # parse KD
                    kd_val = self._extract_first_int(row.get(kd_key))

                    # parse sig landed & attempted
                    sig_landed, sig_attempted = self._parse_landed_attempted_safe(row.get(sig_key))

                    # parse total strikes (landed or first int)
                    total_str_landed, total_str_attempted = self._parse_landed_attempted_safe(row.get(total_str_key))

                    # parse td
                    td_landed, td_attempted = self._parse_landed_attempted_safe(row.get(td_key))

                    # parse sub attempts, rev, ctrl
                    sub_att = self._extract_first_int(row.get(sub_att_key))
                    rev = self._extract_first_int(row.get(rev_key))
                    ctrl_time = self._safe_str(row.get(ctrl_key))
                    sig_pct = self._extract_first_percent_str(row.get(sig_pct_key))
                    td_pct = self._extract_first_percent_str(row.get(td_pct_key))

                    # choose which fight id to attach to
                    fight_id_for_block = f1_fight_id if fighter_index == 1 else f2_fight_id

                    # add round stats
                    self.add_round_stats(
                        fight_id=fight_id_for_block,
                        round_number=round_num,
                        kd=kd_val,
                        sig_str=sig_landed,
                        sig_str_pct=sig_pct,
                        total_str=total_str_landed,
                        td=td_landed,
                        td_pct=td_pct,
                        sub_att=sub_att,
                        rev=rev,
                        ctrl_time=ctrl_time
                    )
                    rounds_added += 1

        return {
            'events': events_added,
            'fighters': fighters_added,
            'fights': fights_added,
            'rounds': rounds_added
        }

    # Helper used inside import loop; separated to keep code tidy
    def _parse_landed_attempted_safe(self, value: Optional[str]) -> Tuple[int, Optional[int]]:
        """
        A small wrapper to handle values like:
          - None -> (0, None)
          - '---' -> (0, None)
          - '0 of 00 of 0' -> (0, 0)
          - '4 of 928 of 45' -> (4, 928) (we take first two numbers seen)
        """
        if not value:
            return 0, None
        if isinstance(value, (int, float)):
            return int(value), None
        s = str(value).strip()
        if s == '' or s.upper() == 'N/A' or s == '---':
            return 0, None
        nums = re.findall(r'(-?\d+)', s)
        if not nums:
            return 0, None
        landed = int(nums[0])
        attempted = int(nums[1]) if len(nums) > 1 else None
        return landed, attempted

    #
    # -- Query helpers
    #
    def get_fighter_stats(self, fighter_name: str) -> Optional[Dict[str, Any]]:
        self.cursor.execute('SELECT * FROM fighters WHERE name = ?', (fighter_name,))
        row = self.cursor.fetchone()
        if not row:
            return None
        columns = [desc[0] for desc in self.cursor.description]
        return dict(zip(columns, row))

    def get_fighter_fight_history(self, fighter_name: str) -> List[Dict[str, Any]]:
        self.cursor.execute('''
            SELECT e.event_name, e.event_date, e.event_location,
                   f1.name as fighter, f2.name as opponent,
                   fr.weight_class, fr.result, fr.method, fr.method_detail,
                   fr.round, fr.time, fr.kd, fr.sig_str, fr.td, fr.sub,
                   fr.fight_url
            FROM fight_results fr
            JOIN fighters f1 ON fr.fighter_id = f1.fighter_id
            LEFT JOIN fighters f2 ON fr.opponent_id = f2.fighter_id
            JOIN events e ON fr.event_id = e.event_id
            WHERE f1.name = ?
            ORDER BY e.event_date DESC
        ''', (fighter_name,))
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

    def get_fight_round_stats(self, fight_id: int) -> List[Dict[str, Any]]:
        self.cursor.execute('''
            SELECT * FROM round_stats WHERE fight_id = ? ORDER BY round_number
        ''', (fight_id,))
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

    def close(self):
        self.conn.close()


# Example usage guard
if __name__ == "__main__":
    db = UFCDatabase("ufc_data.db")

    print("Importing data from CSV...")
    stats = db.import_from_csv("your_fight_data.csv", default_date="2025-10-04")
    print("Imported:")
    print(f"  - {stats['events']} event records")
    print(f"  - {stats['fighters']} fighter records")
    print(f"  - {stats['fights']} fight records")
    print(f"  - {stats['rounds']} round statistics")

    # Quick query example
    fighter = db.get_fighter_stats("Alex Pereira")
    if fighter:
        print("\nAlex Pereira Stats:")
        print(f"  Nickname: {fighter.get('nickname')}")
        print(f"  Height: {fighter.get('height')}, Reach: {fighter.get('reach')}")
        print(f"  SLpM: {fighter.get('slpm')}, Str. Acc: {fighter.get('str_acc')}%")

    db.close()
