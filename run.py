#!/usr/bin/env python
"""
Data Analysis CLI - Single Unified Entry Point

Simple interface for CSV data analysis with focused, meaningful insights.
Results are displayed on console and saved to analysisResult/analysis_results.txt

Usage:
    python run.py <CSV_SOURCE>

Examples:
    python run.py data/sales.csv                    # Local file
    python run.py "https://example.com/data.csv"    # Remote URL
"""

import sys
import os
from io import StringIO
from src.analyzer import AnalysisEngine


class TeeOutput:
    """Utility to write to both console and file simultaneously."""
    
    def __init__(self, file_path):
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        self.file = open(file_path, 'w', encoding='utf-8')
        self.stdout = sys.stdout
    
    def write(self, message):
        """Write to both console and file."""
        self.stdout.write(message)
        self.file.write(message)
        self.file.flush()
    
    def flush(self):
        """Flush both streams."""
        self.stdout.flush()
        self.file.flush()
    
    def close(self):
        """Close file."""
        self.file.close()


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("\n" + "="*80)
        print("DATA ANALYSIS ENGINE")
        print("="*80)
        print("\nUsage: python run.py <CSV_SOURCE>\n")
        print("Examples:")
        print("  python run.py data/sales.csv")
        print("  python run.py https://example.com/data.csv\n")
        print("="*80 + "\n")
        sys.exit(0)
    
    source = sys.argv[1]
    
    # Set up file output (saves to analysisResult/analysis_results.txt)
    output_file = os.path.join("analysisResult", "analysis_results.txt")
    tee = TeeOutput(output_file)
    original_stdout = sys.stdout
    sys.stdout = tee
    
    try:
        print("\n" + "="*80)
        print("UNIFIED DATA ANALYSIS PIPELINE")
        print("="*80 + "\n")
        
        # Initialize and process
        engine = AnalysisEngine()
        
        print("Loading data...")
        engine.load_csv(source)
        
        print(f"Loaded {len(engine.data)} records with {len(engine.headers)} columns")
        print(f"  • Numeric columns: {len(engine.numeric_cols)}")
        print(f"  • Categorical columns: {len(engine.categorical_cols)}\n")
        
        # Run focused analysis (meaningful insights only)
        engine.analyze_focused()
        
        print("\n[OK] Analysis complete!")
        print(f"[OK] Results saved to '{output_file}'\n")
        
    except FileNotFoundError as e:
        print(f"\n[ERROR] {e}\n")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] {e}\n")
        sys.exit(1)
    finally:
        # Restore stdout and close file
        sys.stdout = original_stdout
        tee.close()


if __name__ == "__main__":
    main()
