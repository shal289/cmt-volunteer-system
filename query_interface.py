import sqlite3
import argparse
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import json


class VolunteerQueryEngine:
    """Query engine for volunteer database"""
    
    def __init__(self, db_path: str = 'volunteer_data.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
    
    def query_mentors(self, location: Optional[str] = None, 
                     min_confidence: float = 0.0,
                     recency_days: Optional[int] = None,
                     required_skills: Optional[List[str]] = None) -> List[Dict]:
        """
        Query for potential mentors with ranking
        
        Args:
            location: Filter by location (searches in bio)
            min_confidence: Minimum confidence score
            recency_days: Only include members active within N days
            required_skills: List of required skills
        """
        query = '''
            SELECT 
                m.member_id,
                m.member_name,
                m.bio_or_comment,
                m.last_active_date,
                p.persona_type,
                p.confidence_score,
                p.reasoning,
                GROUP_CONCAT(DISTINCT s.skill_name) as skills,
                julianday('now') - julianday(m.last_active_date) as days_since_active
            FROM members m
            JOIN member_personas p ON m.member_id = p.member_id
            LEFT JOIN member_skills ms ON m.member_id = ms.member_id
            LEFT JOIN skills s ON ms.skill_id = s.skill_id
            WHERE p.is_current = 1
            AND p.persona_type = 'Mentor Material'
            AND p.confidence_score >= ?
        '''
        
        params = [min_confidence]
        
        if location:
            query += ' AND (m.bio_or_comment LIKE ? OR m.member_name LIKE ?)'
            location_pattern = f'%{location}%'
            params.extend([location_pattern, location_pattern])
        
        if recency_days:
            cutoff_date = (datetime.now() - timedelta(days=recency_days)).strftime('%Y-%m-%d')
            query += ' AND m.last_active_date >= ?'
            params.append(cutoff_date)
        
        query += ' GROUP BY m.member_id'
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        
        results = []
        for row in cursor.fetchall():
            result = dict(row)
            result['skills'] = result['skills'].split(',') if result['skills'] else []
            
            # Filter by required skills if specified
            if required_skills:
                member_skills_lower = [s.lower() for s in result['skills']]
                if not all(skill.lower() in member_skills_lower for skill in required_skills):
                    continue
            
            # Calculate ranking score
            result['ranking_score'] = self._calculate_ranking_score(
                result['confidence_score'],
                result['days_since_active'],
                len(result['skills'])
            )
            
            results.append(result)
        
        # Sort by ranking score
        results.sort(key=lambda x: x['ranking_score'], reverse=True)
        
        return results
    
    def query_by_persona(self, persona: str, limit: int = 10) -> List[Dict]:
        """Query members by persona type"""
        query = '''
            SELECT 
                m.member_id,
                m.member_name,
                m.bio_or_comment,
                m.last_active_date,
                p.persona_type,
                p.confidence_score,
                p.reasoning,
                GROUP_CONCAT(DISTINCT s.skill_name) as skills
            FROM members m
            JOIN member_personas p ON m.member_id = p.member_id
            LEFT JOIN member_skills ms ON m.member_id = ms.member_id
            LEFT JOIN skills s ON ms.skill_id = s.skill_id
            WHERE p.is_current = 1
            AND p.persona_type = ?
            GROUP BY m.member_id
            ORDER BY p.confidence_score DESC
            LIMIT ?
        '''
        
        cursor = self.conn.cursor()
        cursor.execute(query, (persona, limit))
        
        results = []
        for row in cursor.fetchall():
            result = dict(row)
            result['skills'] = result['skills'].split(',') if result['skills'] else []
            results.append(result)
        
        return results
    
    def query_by_skills(self, skills: List[str], match_all: bool = True) -> List[Dict]:
        """
        Query members by skills
        
        Args:
            skills: List of skill names
            match_all: If True, member must have all skills; if False, any skill matches
        """
        skills_lower = [s.lower() for s in skills]
        placeholders = ','.join(['?' for _ in skills_lower])
        
        if match_all:
            # Member must have all specified skills
            query = f'''
                SELECT 
                    m.member_id,
                    m.member_name,
                    m.bio_or_comment,
                    m.last_active_date,
                    p.persona_type,
                    p.confidence_score,
                    GROUP_CONCAT(DISTINCT s.skill_name) as skills
                FROM members m
                JOIN member_personas p ON m.member_id = p.member_id
                JOIN member_skills ms ON m.member_id = ms.member_id
                JOIN skills s ON ms.skill_id = s.skill_id
                WHERE p.is_current = 1
                AND s.skill_name IN ({placeholders})
                GROUP BY m.member_id
                HAVING COUNT(DISTINCT s.skill_id) = ?
                ORDER BY p.confidence_score DESC
            '''
            params = skills_lower + [len(skills_lower)]
        else:
            # Member has any of the specified skills
            query = f'''
                SELECT 
                    m.member_id,
                    m.member_name,
                    m.bio_or_comment,
                    m.last_active_date,
                    p.persona_type,
                    p.confidence_score,
                    GROUP_CONCAT(DISTINCT s.skill_name) as skills,
                    COUNT(DISTINCT s.skill_id) as matching_skills
                FROM members m
                JOIN member_personas p ON m.member_id = p.member_id
                JOIN member_skills ms ON m.member_id = ms.member_id
                JOIN skills s ON ms.skill_id = s.skill_id
                WHERE p.is_current = 1
                AND s.skill_name IN ({placeholders})
                GROUP BY m.member_id
                ORDER BY matching_skills DESC, p.confidence_score DESC
            '''
            params = skills_lower
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        
        results = []
        for row in cursor.fetchall():
            result = dict(row)
            result['skills'] = result['skills'].split(',') if result['skills'] else []
            results.append(result)
        
        return results
    
    def query_low_confidence(self, threshold: float = 0.5) -> List[Dict]:
        """Find members with low confidence scores for review"""
        query = '''
            SELECT 
                m.member_id,
                m.member_name,
                m.bio_or_comment,
                p.persona_type,
                p.confidence_score,
                p.reasoning
            FROM members m
            JOIN member_personas p ON m.member_id = p.member_id
            WHERE p.is_current = 1
            AND p.confidence_score < ?
            ORDER BY p.confidence_score ASC
        '''
        
        cursor = self.conn.cursor()
        cursor.execute(query, (threshold,))
        
        results = []
        for row in cursor.fetchall():
            results.append(dict(row))
        
        return results
    
    def get_statistics(self) -> Dict:
        """Get database statistics"""
        cursor = self.conn.cursor()
        
        stats = {}
        
        # Total members
        cursor.execute('SELECT COUNT(*) as count FROM members')
        stats['total_members'] = cursor.fetchone()['count']
        
        # Persona breakdown
        cursor.execute('''
            SELECT persona_type, COUNT(*) as count
            FROM member_personas
            WHERE is_current = 1
            GROUP BY persona_type
            ORDER BY count DESC
        ''')
        stats['persona_distribution'] = {row['persona_type']: row['count'] for row in cursor.fetchall()}
        
        # Average confidence
        cursor.execute('''
            SELECT AVG(confidence_score) as avg_confidence
            FROM member_personas
            WHERE is_current = 1
        ''')
        stats['average_confidence'] = cursor.fetchone()['avg_confidence']
        
        # Top skills
        cursor.execute('''
            SELECT s.skill_name, COUNT(*) as count
            FROM member_skills ms
            JOIN skills s ON ms.skill_id = s.skill_id
            GROUP BY s.skill_name
            ORDER BY count DESC
            LIMIT 10
        ''')
        stats['top_skills'] = {row['skill_name']: row['count'] for row in cursor.fetchall()}
        
        return stats
    
    def _calculate_ranking_score(self, confidence: float, days_since_active: float, 
                                 skill_count: int) -> float:
        """
        Calculate ranking score based on multiple factors
        
        Formula: confidence * recency_factor * skill_factor
        """
        # Recency factor (decays over time)
        if days_since_active is None:
            recency_factor = 0.5
        else:
            recency_factor = max(0.1, 1.0 - (days_since_active / 365.0))
        
        # Skill factor (more skills = higher score, with diminishing returns)
        skill_factor = min(1.0, 0.5 + (skill_count * 0.1))
        
        return confidence * recency_factor * skill_factor
    
    def close(self):
        """Close database connection"""
        self.conn.close()


def main():
    """CLI interface"""
    parser = argparse.ArgumentParser(description='Query CMT Association Volunteer Database')
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Mentors query
    mentor_parser = subparsers.add_parser('mentors', help='Find potential mentors')
    mentor_parser.add_argument('--location', type=str, help='Filter by location')
    mentor_parser.add_argument('--min-confidence', type=float, default=0.0, help='Minimum confidence score')
    mentor_parser.add_argument('--recency-days', type=int, help='Active within N days')
    mentor_parser.add_argument('--skills', nargs='+', help='Required skills')
    
    # Persona query
    persona_parser = subparsers.add_parser('persona', help='Find members by persona')
    persona_parser.add_argument('type', type=str, help='Persona type')
    persona_parser.add_argument('--limit', type=int, default=10, help='Result limit')
    
    # Skills query
    skills_parser = subparsers.add_parser('skills', help='Find members by skills')
    skills_parser.add_argument('skills', nargs='+', help='Skill names')
    skills_parser.add_argument('--match-all', action='store_true', help='Require all skills')
    
    # Low confidence
    low_conf_parser = subparsers.add_parser('low-confidence', help='Find low confidence entries')
    low_conf_parser.add_argument('--threshold', type=float, default=0.5, help='Confidence threshold')
    
    # Statistics
    subparsers.add_parser('stats', help='Show database statistics')
    
    args = parser.parse_args()
    
    engine = VolunteerQueryEngine()
    
    try:
        if args.command == 'mentors':
            results = engine.query_mentors(
                location=args.location,
                min_confidence=args.min_confidence,
                recency_days=args.recency_days,
                required_skills=args.skills
            )
            print(json.dumps(results, indent=2, default=str))
            
        elif args.command == 'persona':
            results = engine.query_by_persona(args.type, args.limit)
            print(json.dumps(results, indent=2, default=str))
            
        elif args.command == 'skills':
            results = engine.query_by_skills(args.skills, args.match_all)
            print(json.dumps(results, indent=2, default=str))
            
        elif args.command == 'low-confidence':
            results = engine.query_low_confidence(args.threshold)
            print(json.dumps(results, indent=2, default=str))
            
        elif args.command == 'stats':
            stats = engine.get_statistics()
            print(json.dumps(stats, indent=2, default=str))
            
        else:
            parser.print_help()
            
    finally:
        engine.close()


if __name__ == "__main__":
    main()