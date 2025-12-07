# Data Analysis Engine

Production-ready CSV analysis tool with functional programming paradigm, automatic type detection, and comprehensive testing.

## Setup

### Prerequisites
- Python 3.8+

### Installation
```bash
pip install -r requirements.txt
```

### Running Analysis
```bash
python run.py data/sales_data.csv
```

Output is displayed on console AND automatically saved to `analysisResult/analysis_results.txt`

## Sample Output

```
============================================================
UNIFIED DATA ANALYSIS PIPELINE
============================================================

Loading data...
Loaded 78 records with 11 columns
  ò Numeric columns: 5
  ò Categorical columns: 6

============================================================
KEY INSIGHTS
============================================================

[NUMERIC METRICS SUMMARY]
SALES:
  Count: 78
  Sum: $154,070.00
  Average: $1,975.26
  Min: $580.00
  Max: $4,300.00

[TOP 10 REGION BY SALES]
  East: $61,200.00
  West: $59,660.00
  South: $20,460.00
  North: $12,750.00

[DISTRIBUTION: REGION]
  East: 30 records (38.5%)
  West: 30 records (38.5%)
  North: 9 records (11.5%)
  South: 9 records (11.5%)

[SALES BY REGION]
  East: $61,200.00
  West: $59,660.00
  North: $12,750.00
  South: $20,460.00
```

## Project Structure

```
src/
├── analyzer.py              # Engine orchestrator
├── data_loader.py           # CSV loading & type detection
├── data_aggregator.py       # Functional aggregation & grouping
├── data_analyzer.py         # Functional analysis & statistics
└── report_generator.py      # Output formatting

test/
└── test_analyzer.py         # 11 unit tests (all passing)
```

## Testing

```bash
python -m unittest test.test_analyzer -v
```

Result: 11/11 tests passing

## Features

- **Functional Programming**: Pure functions, immutable transformations, lambda expressions
- **Stream Operations**: Filter, map, reduce patterns for data processing
- **Automatic Type Detection**: Numeric vs categorical column inference
- **Comprehensive Testing**: 11 unit tests covering all core functionality
- **Clean Architecture**: Single responsibility, modular design
- **Fast & Scalable**: O(n) algorithms, handles large datasets

## Requirements

- Python 3.8+
- requests (for URL support)
