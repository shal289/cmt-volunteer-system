import pandas as pd
import sqlite3
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataNormalizer:
    """Handles data validation and normalization"""
    
    @staticmethod
    def normalize_name(name: str) -> str:
        """Normalize member names"""
        if pd.isna(name):
            return None
        return ' '.join(name.strip().title().split())
    
    @staticmethod
    def normalize_date(date_str: str) -> Optional[str]:
        """
        Normalize dates to ISO format (YYYY-MM-DD)
        Returns None if date is invalid
        """
        if pd.isna(date_str) or not date_str:
            return None
        
        date_str = str(date_str).strip()
        
        # Try multiple date formats
        formats = [
            '%Y-%m-%d',      # 2024-06-12
            '%d/%m/%y',      # 12/05/24
            '%Y/%m/%d',      # 2024/06/12
            '%d-%m-%Y',      # 12-05-2024
            '%Y.%m.%d',      # 2024.02.14
            '%b %d %Y',      # Jan 7 2024
            '%d-%m-%y',      # 15-02-2024
        ]
        
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date: {date_str}")
        return None
    
    @staticmethod
    def validate_record(row: pd.Series) -> tuple[bool, str]:
        """
        Validate a single record
        Returns (is_valid, error_message)
        """
        if pd.isna(row.get('member_name')):
            return False, "Missing member name"
        
        if pd.isna(row.get('bio_or_comment')):
            return False, "Missing bio/comment"
        
        return True, ""


class CSVIngester:
    """Handles CSV ingestion and initial processing"""
    
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self.normalizer = DataNormalizer()
        self.errors = []
    
    def load_and_validate(self) -> pd.DataFrame:
        """Load CSV and perform initial validation"""
        logger.info(f"Loading CSV from {self.csv_path}")
        
        try:
            df = pd.read_csv(self.csv_path)
            logger.info(f"Loaded {len(df)} records")
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            raise
        
        # Normalize column names
        df.columns = df.columns.str.strip().str.lower()
        
        return df
    
    def process(self) -> pd.DataFrame:
        """Process and normalize the data"""
        df = self.load_and_validate()
        
        processed_records = []
        
        for idx, row in df.iterrows():
            # Validate record
            is_valid, error_msg = self.normalizer.validate_record(row)
            
            if not is_valid:
                self.errors.append({
                    'row_index': idx,
                    'error': error_msg,
                    'raw_data': row.to_dict()
                })
                logger.warning(f"Row {idx} validation failed: {error_msg}")
                continue
            
            # Normalize data
            processed_record = {
                'member_name': self.normalizer.normalize_name(row['member_name']),
                'bio_or_comment': str(row['bio_or_comment']).strip(),
                'last_active_date': self.normalizer.normalize_date(row.get('last_active_date')),
                'raw_date': str(row.get('last_active_date', '')),
                'ingestion_timestamp': datetime.now().isoformat(),
                'processing_status': 'normalized'
            }
            
            processed_records.append(processed_record)
        
        logger.info(f"Successfully processed {len(processed_records)} records")
        logger.info(f"Failed to process {len(self.errors)} records")
        
        # Save errors to file
        if self.errors:
            with open('processing_errors.json', 'w') as f:
                json.dump(self.errors, f, indent=2)
            logger.info("Saved processing errors to processing_errors.json")
        
        return pd.DataFrame(processed_records)


if __name__ == "__main__":
    # Test the ingestion pipeline
    ingester = CSVIngester('members_raw.csv')
    df = ingester.process()
    print(df.head())
    print(f"\nProcessed {len(df)} records successfully")
    print(f"Errors: {len(ingester.errors)}")