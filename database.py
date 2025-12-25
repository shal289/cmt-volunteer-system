import sqlite3
import logging
from typing import List, Dict, Optional
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages SQLite database operations"""
    
    def __init__(self, db_path: str = 'volunteer_data.db'):
        self.db_path = db_path
        self.conn = None
        self.init_database()
    
    def get_connection(self) -> sqlite3.Connection:
        """Get database connection"""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
        return self.conn
    
    def init_database(self):
        """Initialize database schema"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Members table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS members (
                member_id INTEGER PRIMARY KEY AUTOINCREMENT,
                member_name TEXT NOT NULL UNIQUE,
                bio_or_comment TEXT NOT NULL,
                last_active_date TEXT,
                raw_date TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        ''')
        
        # Skills table (normalized many-to-many)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS skills (
                skill_id INTEGER PRIMARY KEY AUTOINCREMENT,
                skill_name TEXT NOT NULL UNIQUE,
                category TEXT,
                created_at TEXT NOT NULL
            )
        ''')
        
        # Member-Skills junction table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS member_skills (
                member_id INTEGER NOT NULL,
                skill_id INTEGER NOT NULL,
                enrichment_version INTEGER NOT NULL,
                confidence REAL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (member_id, skill_id, enrichment_version),
                FOREIGN KEY (member_id) REFERENCES members(member_id) ON DELETE CASCADE,
                FOREIGN KEY (skill_id) REFERENCES skills(skill_id) ON DELETE CASCADE
            )
        ''')
        
        # Personas table (supports versioning)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS member_personas (
                persona_id INTEGER PRIMARY KEY AUTOINCREMENT,
                member_id INTEGER NOT NULL,
                persona_type TEXT NOT NULL,
                confidence_score REAL NOT NULL,
                reasoning TEXT,
                enrichment_version INTEGER NOT NULL,
                is_current BOOLEAN DEFAULT 1,
                created_at TEXT NOT NULL,
                FOREIGN KEY (member_id) REFERENCES members(member_id) ON DELETE CASCADE
            )
        ''')
        
        # Enrichment metadata table (versioning and tracking)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS enrichment_runs (
                run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_timestamp TEXT NOT NULL,
                model_name TEXT,
                prompt_version TEXT,
                records_processed INTEGER,
                status TEXT,
                notes TEXT
            )
        ''')
        
        # Processing status table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processing_log (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                member_id INTEGER,
                member_name TEXT,
                processing_stage TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (member_id) REFERENCES members(member_id) ON DELETE CASCADE
            )
        ''')
        
        # Indexes for performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_members_active_date ON members(last_active_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_personas_current ON member_personas(is_current, member_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_skills_name ON skills(skill_name)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_processing_status ON processing_log(status, timestamp)')
        
        conn.commit()
        logger.info("Database initialized successfully")
    
    def insert_member(self, name: str, bio: str, last_active_date: Optional[str], raw_date: str) -> int:
        """Insert or update a member record"""
        conn = self.get_connection()
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        
        try:
            cursor.execute('''
                INSERT INTO members (member_name, bio_or_comment, last_active_date, raw_date, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(member_name) DO UPDATE SET
                    bio_or_comment = excluded.bio_or_comment,
                    last_active_date = excluded.last_active_date,
                    raw_date = excluded.raw_date,
                    updated_at = excluded.updated_at
            ''', (name, bio, last_active_date, raw_date, now, now))
            
            member_id = cursor.lastrowid if cursor.lastrowid else cursor.execute(
                'SELECT member_id FROM members WHERE member_name = ?', (name,)
            ).fetchone()[0]
            
            conn.commit()
            return member_id
            
        except Exception as e:
            logger.error(f"Error inserting member {name}: {e}")
            conn.rollback()
            raise
    
    def get_or_create_skill(self, skill_name: str, category: Optional[str] = None) -> int:
        """Get skill_id or create if doesn't exist"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        skill_name = skill_name.strip().lower()
        
        cursor.execute('SELECT skill_id FROM skills WHERE skill_name = ?', (skill_name,))
        row = cursor.fetchone()
        
        if row:
            return row[0]
        
        # Create new skill
        now = datetime.now().isoformat()
        cursor.execute('''
            INSERT INTO skills (skill_name, category, created_at)
            VALUES (?, ?, ?)
        ''', (skill_name, category, now))
        
        conn.commit()
        return cursor.lastrowid
    
    def insert_enrichment(self, member_id: int, skills: List[str], persona: str, 
                         confidence: float, reasoning: str, version: int):
        """Insert enrichment data for a member"""
        conn = self.get_connection()
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        
        try:
            # Mark previous personas as not current
            cursor.execute('''
                UPDATE member_personas 
                SET is_current = 0 
                WHERE member_id = ? AND is_current = 1
            ''', (member_id,))
            
            # Insert new persona
            cursor.execute('''
                INSERT INTO member_personas (member_id, persona_type, confidence_score, reasoning, enrichment_version, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (member_id, persona, confidence, reasoning, version, now))
            
            # Insert skills
            for skill_name in skills:
                if skill_name.strip():
                    skill_id = self.get_or_create_skill(skill_name)
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO member_skills (member_id, skill_id, enrichment_version, confidence, created_at)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (member_id, skill_id, version, confidence, now))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error inserting enrichment for member_id {member_id}: {e}")
            conn.rollback()
            raise
    
    def log_processing(self, member_id: Optional[int], member_name: str, stage: str, 
                      status: str, error_msg: Optional[str] = None):
        """Log processing status"""
        conn = self.get_connection()
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        
        cursor.execute('''
            INSERT INTO processing_log (member_id, member_name, processing_stage, status, error_message, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (member_id, member_name, stage, status, error_msg, now))
        
        conn.commit()
    
    def create_enrichment_run(self, model_name: str, prompt_version: str) -> int:
        """Create a new enrichment run record"""
        conn = self.get_connection()
        cursor = conn.cursor()
        now = datetime.now().isoformat()
        
        cursor.execute('''
            INSERT INTO enrichment_runs (run_timestamp, model_name, prompt_version, status)
            VALUES (?, ?, ?, ?)
        ''', (now, model_name, prompt_version, 'in_progress'))
        
        conn.commit()
        return cursor.lastrowid
    
    def update_enrichment_run(self, run_id: int, records_processed: int, status: str, notes: Optional[str] = None):
        """Update enrichment run status"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE enrichment_runs
            SET records_processed = ?, status = ?, notes = ?
            WHERE run_id = ?
        ''', (records_processed, status, notes, run_id))
        
        conn.commit()
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None


if __name__ == "__main__":
    # Test database creation
    db = DatabaseManager('test_volunteer_data.db')
    print("Database initialized successfully")
    
    # Test insert
    member_id = db.insert_member(
        "Test User",
        "Python developer interested in finance",
        "2024-12-25",
        "2024-12-25"
    )
    print(f"Inserted member with ID: {member_id}")
    
    # Test enrichment
    db.insert_enrichment(
        member_id,
        ["python", "finance", "derivatives"],
        "Active Learner",
        0.85,
        "Shows interest and some skills",
        1
    )
    print("Inserted enrichment data")
    
    db.close()