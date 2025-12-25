import logging
import sys
from pathlib import Path
from typing import Optional
import pandas as pd

# Import our modules
from main import CSVIngester
from ai_enrichment import AIEnricher
from database import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class VolunteerPipeline:
    """Orchestrates the complete volunteer data pipeline"""
    
    def __init__(self, csv_path: str, db_path: str = 'volunteer_data.db', 
                 api_key: Optional[str] = None):
        """
        Initialize pipeline
        
        Args:
            csv_path: Path to input CSV file
            db_path: Path to SQLite database
            api_key: OpenRouter API key (or use OPENROUTER_API_KEY env var)
        """
        self.csv_path = csv_path
        self.db_path = db_path
        self.api_key = api_key
        
        logger.info("Initializing Volunteer Pipeline")
        logger.info(f"CSV: {csv_path}")
        logger.info(f"Database: {db_path}")
    
    def run(self):
        """Execute the complete pipeline"""
        logger.info("=" * 80)
        logger.info("STARTING VOLUNTEER PIPELINE")
        logger.info("=" * 80)
        
        # Step 1: Ingest and normalize
        logger.info("\n[STEP 1] Ingesting and normalizing CSV data...")
        ingester = CSVIngester(self.csv_path)
        df = ingester.process()
        
        if len(df) == 0:
            logger.error("No valid records to process. Exiting.")
            return
        
        logger.info(f"✓ Successfully normalized {len(df)} records")
        
        # Step 2: Initialize database
        logger.info("\n[STEP 2] Initializing database...")
        db = DatabaseManager(self.db_path)
        logger.info("✓ Database initialized")
        
        # Step 3: Load members into database
        logger.info("\n[STEP 3] Loading members into database...")
        member_records = []
        
        for _, row in df.iterrows():
            try:
                member_id = db.insert_member(
                    name=row['member_name'],
                    bio=row['bio_or_comment'],
                    last_active_date=row['last_active_date'],
                    raw_date=row['raw_date']
                )
                
                member_records.append({
                    'member_id': member_id,
                    'member_name': row['member_name'],
                    'bio_or_comment': row['bio_or_comment']
                })
                
                db.log_processing(
                    member_id=member_id,
                    member_name=row['member_name'],
                    stage='ingestion',
                    status='success'
                )
                
            except Exception as e:
                logger.error(f"Failed to insert {row['member_name']}: {e}")
                db.log_processing(
                    member_id=None,
                    member_name=row['member_name'],
                    stage='ingestion',
                    status='error',
                    error_msg=str(e)
                )
        
        logger.info(f"✓ Loaded {len(member_records)} members into database")
        
        # Step 4: AI Enrichment
        logger.info("\n[STEP 4] Starting AI enrichment...")
        
        try:
            enricher = AIEnricher(api_key=self.api_key)
        except ValueError as e:
            logger.error(f"Failed to initialize AI enricher: {e}")
            logger.error("Set OPENROUTER_API_KEY environment variable or pass api_key parameter")
            return
        
        # Create enrichment run
        run_id = db.create_enrichment_run(
            model_name=enricher.model_name,
            prompt_version="v1.0"
        )
        
        enriched_count = 0
        failed_count = 0
        
        for i, record in enumerate(member_records):
            logger.info(f"Enriching [{i+1}/{len(member_records)}]: {record['member_name']}")
            
            try:
                result = enricher.enrich_bio(record['bio_or_comment'])
                
                db.insert_enrichment(
                    member_id=record['member_id'],
                    skills=result.skills,
                    persona=result.persona,
                    confidence=result.confidence_score,
                    reasoning=result.reasoning,
                    version=run_id
                )
                
                db.log_processing(
                    member_id=record['member_id'],
                    member_name=record['member_name'],
                    stage='enrichment',
                    status='success'
                )
                
                enriched_count += 1
                
                logger.info(f"  → Persona: {result.persona}, Confidence: {result.confidence_score:.2f}, Skills: {len(result.skills)}")
                
            except Exception as e:
                logger.error(f"Failed to enrich {record['member_name']}: {e}")
                db.log_processing(
                    member_id=record['member_id'],
                    member_name=record['member_name'],
                    stage='enrichment',
                    status='error',
                    error_msg=str(e)
                )
                failed_count += 1
        
        # Update enrichment run
        db.update_enrichment_run(
            run_id=run_id,
            records_processed=enriched_count,
            status='completed',
            notes=f"Successfully enriched {enriched_count} records, {failed_count} failures"
        )
        
        logger.info(f"✓ AI enrichment completed: {enriched_count} success, {failed_count} failed")
        
        # Step 5: Generate summary
        logger.info("\n[STEP 5] Generating pipeline summary...")
        self._generate_summary(db, enriched_count, failed_count)
        
        # Cleanup
        db.close()
        
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"\nDatabase created: {self.db_path}")
        logger.info(f"Logs saved: pipeline.log, etl_pipeline.log")
        logger.info("\nNext steps:")
        logger.info("  1. Query data: python query_interface.py stats")
        logger.info("  2. Find mentors: python query_interface.py mentors --location Mumbai")
        logger.info("  3. Review low confidence: python query_interface.py low-confidence")
    
    def _generate_summary(self, db: DatabaseManager, enriched: int, failed: int):
        """Generate and display pipeline summary"""
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # Get persona distribution
        cursor.execute('''
            SELECT persona_type, COUNT(*) as count, AVG(confidence_score) as avg_conf
            FROM member_personas
            WHERE is_current = 1
            GROUP BY persona_type
            ORDER BY count DESC
        ''')
        
        print("\n" + "-" * 80)
        print("PIPELINE SUMMARY")
        print("-" * 80)
        print(f"Total records processed: {enriched + failed}")
        print(f"Successfully enriched: {enriched}")
        print(f"Failed: {failed}")
        print("\nPersona Distribution:")
        
        for row in cursor.fetchall():
            print(f"  {row[0]:25s} {row[1]:3d} members (avg confidence: {row[2]:.2f})")
        
        # Top skills
        cursor.execute('''
            SELECT s.skill_name, COUNT(*) as count
            FROM member_skills ms
            JOIN skills s ON ms.skill_id = s.skill_id
            GROUP BY s.skill_name
            ORDER BY count DESC
            LIMIT 10
        ''')
        
        print("\nTop 10 Skills:")
        for row in cursor.fetchall():
            print(f"  {row[0]:25s} {row[1]:3d} members")
        
        # Low confidence warnings
        cursor.execute('''
            SELECT COUNT(*) as count
            FROM member_personas
            WHERE is_current = 1 AND confidence_score < 0.5
        ''')
        
        low_conf_count = cursor.fetchone()[0]
        if low_conf_count > 0:
            print(f"\n⚠ Warning: {low_conf_count} members have confidence score < 0.5")
            print("  Run: python query_interface.py low-confidence")
        
        print("-" * 80)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='CMT Volunteer Pipeline')
    parser.add_argument('csv_file', type=str, help='Path to input CSV file')
    parser.add_argument('--db', type=str, default='volunteer_data.db', help='Database path')
    parser.add_argument('--api-key', type=str, help='OpenRouter API key')
    
    args = parser.parse_args()
    
    # Validate CSV exists
    if not Path(args.csv_file).exists():
        logger.error(f"CSV file not found: {args.csv_file}")
        sys.exit(1)
    
    # Run pipeline
    pipeline = VolunteerPipeline(
        csv_path=args.csv_file,
        db_path=args.db,
        api_key=args.api_key
    )
    
    pipeline.run()


if __name__ == "__main__":
    main()