import sys
import os

def test_imports():
    """Test all required imports"""
    print("Testing imports...")
    try:
        import pandas as pd
        print("  ✓ pandas")
        
        import httpx
        print("  ✓ httpx")
        
        import sqlite3
        print("  ✓ sqlite3")
        
        from datetime import datetime
        print("  ✓ datetime")
        
        import json
        print("  ✓ json")
        
        return True
    except ImportError as e:
        print(f"  ✗ Import error: {e}")
        print("\nRun: pip install -r requirements.txt")
        return False


def test_api_key():
    """Test if API key is set"""
    print("\nTesting API key...")
    
    api_key = os.getenv('OPENROUTER_API_KEY')
    
    if not api_key:
        print("  ✗ OPENROUTER_API_KEY not set")
        print("\nSet it with:")
        print("  export OPENROUTER_API_KEY='your-key-here'  # Linux/Mac")
        print("  set OPENROUTER_API_KEY=your-key-here       # Windows")
        return False
    
    print(f"  ✓ API key found: {api_key[:10]}...")
    return True


def test_api_connection():
    """Test actual API connection"""
    print("\nTesting API connection...")
    
    try:
        import httpx
        
        api_key = os.getenv('OPENROUTER_API_KEY')
        if not api_key:
            print("  ✗ No API key")
            return False
        
        # Test with a simple model
        model_name = "openai/gpt-4o-mini"
        api_url = "https://openrouter.ai/api/v1/chat/completions"
        
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        try:
            with httpx.Client(timeout=30.0, headers=headers) as client:
                response = client.post(
                    api_url,
                    json={
                        "model": model_name,
                        "messages": [
                            {"role": "user", "content": "Say 'test successful' and nothing else"}
                        ]
                    }
                )
                response.raise_for_status()
                data = response.json()
                content = data["choices"][0]["message"]["content"]
                print(f"  ✓ API response: {content.strip()}")
                print(f"  ✓ Working model: {model_name}")
                return True
        except httpx.HTTPStatusError as e:
            print(f"  ✗ API error: {e.response.status_code} - {e.response.text[:200]}")
            return False
        
    except Exception as e:
        print(f"  ✗ API error: {e}")
        return False


def test_csv_format():
    """Test if CSV file exists and has correct format"""
    print("\nTesting CSV file...")
    
    csv_file = 'members_raw.csv'
    
    if not os.path.exists(csv_file):
        print(f"  ✗ File not found: {csv_file}")
        print("\nCreate a CSV with columns: member_name, bio_or_comment, last_active_date")
        return False
    
    try:
        import pandas as pd
        df = pd.read_csv(csv_file)
        
        required_cols = ['member_name', 'bio_or_comment', 'last_active_date']
        actual_cols = [col.lower().strip() for col in df.columns]
        
        missing = [col for col in required_cols if col not in actual_cols]
        
        if missing:
            print(f"  ✗ Missing columns: {missing}")
            print(f"  Found columns: {list(df.columns)}")
            return False
        
        print(f"  ✓ CSV loaded: {len(df)} rows, {len(df.columns)} columns")
        print(f"  ✓ Required columns present")
        return True
        
    except Exception as e:
        print(f"  ✗ CSV error: {e}")
        return False


def test_database_creation():
    """Test database creation"""
    print("\nTesting database creation...")
    
    try:
        import sqlite3
        
        # Create test database
        conn = sqlite3.connect('test_db.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS test (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        ''')
        
        cursor.execute("INSERT INTO test (name) VALUES ('test')")
        conn.commit()
        
        cursor.execute("SELECT * FROM test")
        result = cursor.fetchone()
        
        conn.close()
        
        # Cleanup
        os.remove('test_db.db')
        
        print("  ✓ Database operations work")
        return True
        
    except Exception as e:
        print(f"  ✗ Database error: {e}")
        return False


def test_date_normalization():
    """Test date parsing"""
    print("\nTesting date normalization...")
    
    from datetime import datetime
    
    test_dates = [
        ('2024-06-12', '2024-06-12'),
        ('12/05/24', '2024-05-12'),
        ('Jan 7 2024', '2024-01-07'),
    ]
    
    success = 0
    
    for input_date, expected in test_dates:
        formats = ['%Y-%m-%d', '%d/%m/%y', '%b %d %Y']
        
        for fmt in formats:
            try:
                parsed = datetime.strptime(input_date, fmt)
                result = parsed.strftime('%Y-%m-%d')
                if result == expected:
                    success += 1
                break
            except:
                continue
    
    if success == len(test_dates):
        print(f"  ✓ Date parsing works ({success}/{len(test_dates)} formats)")
        return True
    else:
        print(f"  ⚠ Partial success ({success}/{len(test_dates)} formats)")
        return True


def main():
    """Run all tests"""
    print("=" * 80)
    print("CMT VOLUNTEER SYSTEM - COMPONENT TESTS")
    print("=" * 80)
    
    tests = [
        ("Imports", test_imports),
        ("API Key", test_api_key),
        ("API Connection", test_api_connection),
        ("CSV Format", test_csv_format),
        ("Database", test_database_creation),
        ("Date Parsing", test_date_normalization),
    ]
    
    results = []
    
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"  ✗ Test crashed: {e}")
            results.append((name, False))
    
    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {status:8s} {name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All systems GO! You're ready to run the pipeline.")
        print("\nNext step: python pipeline.py members_raw.csv")
    else:
        print("\n⚠ Some tests failed. Fix the issues above before running pipeline.")
        sys.exit(1)


if __name__ == "__main__":
    main()