"""
Data Loading Module

Handles CSV reading from files and URLs.
"""

import csv
import os
import requests
from typing import List, Dict, Any
from collections import defaultdict


class DataLoader:
    """Load and parse CSV data."""

    def __init__(self):
        self.headers: List[str] = []
        self.data: List[Dict[str, Any]] = []
        self.numeric_cols: List[str] = []
        self.categorical_cols: List[str] = []

    def load(self, source: str) -> None:
        """Load CSV from file path or URL."""
        if source.startswith("http://") or source.startswith("https://"):
            self._load_from_url(source)
        else:
            self._load_from_file(source)

    def _load_from_file(self, file_path: str) -> None:
        """Load CSV from local file system with automatic encoding detection."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Try multiple encodings in order of likelihood
        encodings = ["utf-8", "latin-1", "iso-8859-1", "cp1252", "utf-16"]
        
        for encoding in encodings:
            try:
                with open(file_path, "r", encoding=encoding) as f:
                    self._process_stream(f)
                return  # Successfully loaded
            except (UnicodeDecodeError, UnicodeError):
                continue  # Try next encoding
        
        # If all encodings fail, raise error
        raise ValueError(
            f"Could not decode file with any supported encoding: {encodings}"
        )

    def _load_from_url(self, url: str) -> None:
        """Download and load CSV from URL."""
        try:
            response = requests.get(url, timeout=10, stream=True)
            response.raise_for_status()
            
            cache_dir = "data"
            os.makedirs(cache_dir, exist_ok=True)
            cache_file = os.path.join(cache_dir, "cache.csv")
            
            with open(cache_file, "w", newline="", encoding="utf-8") as f:
                f.write(response.text)
            
            with open(cache_file, "r", encoding="utf-8") as f:
                self._process_stream(f)
                
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to download CSV: {e}")

    def _process_stream(self, stream) -> None:
        """Parse CSV stream and infer column types."""
        reader = csv.DictReader(stream)
        self.headers = reader.fieldnames or []
        
        if not self.headers:
            raise ValueError("CSV has no headers")
        
        type_confidence = defaultdict(lambda: {"numeric": 0, "total": 0})
        
        for row in reader:
            self.data.append(row)
            
            for col in self.headers:
                value = row.get(col, "")
                type_confidence[col]["total"] += 1
                
                if value and str(value).strip():
                    try:
                        float(value)
                        type_confidence[col]["numeric"] += 1
                    except (ValueError, TypeError):
                        pass
        
        # Classify columns: numeric if ≥80% of values are numeric
        self.numeric_cols = [
            col for col in self.headers
            if type_confidence[col]["numeric"] / max(type_confidence[col]["total"], 1) >= 0.8
        ]
        self.categorical_cols = [
            col for col in self.headers if col not in self.numeric_cols
        ]
