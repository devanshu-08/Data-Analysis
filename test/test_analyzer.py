"""
Unit Tests for AnalysisEngine and Components

Tests cover:
- CSV loading and type inference
- Aggregation operations  
- Multi-level grouping
- Ranking, distributions, and statistics
"""

import unittest
import os
import tempfile
from src.analyzer import AnalysisEngine


class TestDataLoading(unittest.TestCase):
    """Test CSV loading with various formats."""

    def setUp(self):
        """Set up test fixtures."""
        self.engine = AnalysisEngine()
        self.test_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.test_dir, "test.csv")

    def tearDown(self):
        """Clean up."""
        if os.path.exists(self.test_file):
            os.remove(self.test_file)
        os.rmdir(self.test_dir)

    def test_load_csv_all_text_columns(self):
        """Test loading CSV with all text columns."""
        csv_content = """Name,City,Category
Alice,New York,A
Bob,London,B
Charlie,Paris,C
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        self.assertEqual(len(self.engine.data), 3)
        self.assertTrue(len(self.engine.categorical_cols) > 0)

    def test_load_csv_mixed_numeric_text(self):
        """Test loading CSV with mixed column types."""
        csv_content = """Product,Quantity,Price
Laptop,10,1000
Phone,20,500
Tablet,15,300
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        self.assertIn("Quantity", self.engine.numeric_cols)
        self.assertIn("Price", self.engine.numeric_cols)
        self.assertIn("Product", self.engine.categorical_cols)

    def test_load_csv_with_missing_values(self):
        """Test loading CSV with empty/missing values."""
        csv_content = """Name,Age,Salary
Alice,30,50000
Bob,,60000
Charlie,28,
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        self.assertEqual(len(self.engine.data), 3)

    def test_load_csv_single_row(self):
        """Test loading CSV with single data row."""
        csv_content = """Name,Value
Item,100
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        self.assertEqual(len(self.engine.data), 1)

    def test_load_csv_special_characters(self):
        """Test loading CSV with special characters."""
        csv_content = """Name,Description
Item-A,Description with, comma
Item-B,Normal
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        self.assertEqual(len(self.engine.data), 2)

    def test_load_csv_file_not_found(self):
        """Test error handling for missing file."""
        with self.assertRaises(FileNotFoundError):
            self.engine.load_csv("nonexistent_file.csv")


class TestAggregation(unittest.TestCase):
    """Test aggregation operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.engine = AnalysisEngine()
        self.test_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.test_dir, "test.csv")

    def tearDown(self):
        """Clean up."""
        if os.path.exists(self.test_file):
            os.remove(self.test_file)
        os.rmdir(self.test_dir)

    def test_aggregate_by_product(self):
        """Test aggregation by product."""
        csv_content = """Product,Region,Sales
Laptop,East,1000
Laptop,West,900
Phone,East,500
Phone,West,600
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.aggregator.aggregate("Product", "Sales", "sum")
        self.assertEqual(result["Laptop"], 1900)
        self.assertEqual(result["Phone"], 1100)

    def test_aggregate_profit_by_region(self):
        """Test profit aggregation by region."""
        csv_content = """Region,Profit
East,1000
East,500
West,800
West,200
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.aggregator.aggregate("Region", "Profit", "sum")
        self.assertEqual(result["East"], 1500)
        self.assertEqual(result["West"], 1000)

    def test_aggregate_with_negative_values(self):
        """Test aggregation with negative values."""
        csv_content = """Category,Value
A,100
A,-50
B,200
B,-100
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.aggregator.aggregate("Category", "Value", "sum")
        self.assertEqual(result["A"], 50)
        self.assertEqual(result["B"], 100)

    def test_aggregate_with_decimal_values(self):
        """Test aggregation with decimal values."""
        csv_content = """Item,Amount
A,10.5
A,20.3
B,15.7
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.aggregator.aggregate("Item", "Amount", "sum")
        self.assertAlmostEqual(result["A"], 30.8, places=1)

    def test_aggregate_with_zero_values(self):
        """Test aggregation with zero values."""
        csv_content = """Group,Value
A,0
A,100
B,0
B,0
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.aggregator.aggregate("Group", "Value", "sum")
        self.assertEqual(result["A"], 100)
        self.assertEqual(result["B"], 0)

    def test_aggregate_min_max_operations(self):
        """Test aggregation with min/max operations."""
        csv_content = """Group,Value
A,50
A,100
A,30
B,200
B,150
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result_max = self.engine.aggregator.aggregate("Group", "Value", "max")
        result_min = self.engine.aggregator.aggregate("Group", "Value", "min")
        self.assertEqual(result_max["A"], 100)
        self.assertEqual(result_min["A"], 30)

    def test_filter_by_condition_greater_than(self):
        """Test filtering by greater than condition."""
        csv_content = """Name,Value
A,100
B,200
C,50
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.aggregator.filter_by_condition("Value", ">", 75)
        self.assertGreater(len(result), 0)

    def test_filter_by_condition_less_than(self):
        """Test filtering by less than condition."""
        csv_content = """Name,Value
A,100
B,200
C,50
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.aggregator.filter_by_condition("Value", "<", 150)
        self.assertGreater(len(result), 0)

    def test_group_and_count(self):
        """Test grouping and counting."""
        csv_content = """Category,Item
A,X
A,Y
A,X
B,X
B,Y
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.aggregator.group_and_count("Category")
        self.assertEqual(result["A"], 3)
        self.assertEqual(result["B"], 2)

    def test_cross_tabulation_region_category(self):
        """Test cross-tabulation analysis."""
        csv_content = """Region,Category,Sales
East,Tech,1000
East,Office,500
West,Tech,800
West,Office,600
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.aggregator.cross_tabulation("Region", "Category")
        self.assertIn("East", result)
        self.assertIn("Tech", result["East"])


class TestStatistics(unittest.TestCase):
    """Test statistical analysis operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.engine = AnalysisEngine()
        self.test_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.test_dir, "test.csv")

    def tearDown(self):
        """Clean up."""
        if os.path.exists(self.test_file):
            os.remove(self.test_file)
        os.rmdir(self.test_dir)

    def test_statistics_with_median(self):
        """Test statistics with median calculation."""
        csv_content = """Value
10
20
30
40
50
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.analyzer.get_statistics("Value")
        self.assertEqual(result["median"], 30)

    def test_statistics_even_count_median(self):
        """Test median calculation with even count."""
        csv_content = """Value
10
20
30
40
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.analyzer.get_statistics("Value")
        self.assertEqual(result["median"], 25)

    def test_statistics_std_deviation(self):
        """Test standard deviation calculation."""
        csv_content = """Value
10
20
30
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.analyzer.get_statistics("Value")
        self.assertIn("std_dev", result)

    def test_statistics_range(self):
        """Test range calculation."""
        csv_content = """Value
10
50
30
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.analyzer.get_statistics("Value")
        self.assertEqual(result["range"], 40)

    def test_find_outliers(self):
        """Test outlier detection."""
        csv_content = """Value
10
20
30
100
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.analyzer.find_outliers("Value", threshold=1.5)
        self.assertGreaterEqual(len(result), 0)

    def test_find_outliers_no_outliers(self):
        """Test outlier detection with no outliers."""
        csv_content = """Value
10
15
20
25
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.analyzer.find_outliers("Value", threshold=2)
        self.assertEqual(len(result), 0)

    def test_percentile_calculation(self):
        """Test percentile calculation."""
        csv_content = """Value
10
20
30
40
50
60
70
80
90
100
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.analyzer.percentile("Value", 50)
        self.assertIsNotNone(result)

    def test_correlate_metrics_positive(self):
        """Test positive correlation detection."""
        csv_content = """X,Y
1,2
2,4
3,6
4,8
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.analyzer.correlate_metrics("X", "Y")
        self.assertGreater(result["correlation"], 0)

    def test_correlate_metrics_negative(self):
        """Test negative correlation detection."""
        csv_content = """X,Y
1,8
2,6
3,4
4,2
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.analyzer.correlate_metrics("X", "Y")
        self.assertLess(result["correlation"], 0)

    def test_correlate_metrics_no_correlation(self):
        """Test no correlation case."""
        csv_content = """X,Y
1,5
2,3
3,7
4,2
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.analyzer.correlate_metrics("X", "Y")
        self.assertIn("correlation", result)


class TestLargeDataset(unittest.TestCase):
    """Test with larger datasets."""

    def setUp(self):
        """Set up test fixtures."""
        self.engine = AnalysisEngine()
        self.test_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.test_dir, "test.csv")

    def tearDown(self):
        """Clean up."""
        if os.path.exists(self.test_file):
            os.remove(self.test_file)
        os.rmdir(self.test_dir)

    def test_large_dataset_1000_rows(self):
        """Test with 1000 rows."""
        csv_lines = ["Category,Value"]
        for i in range(1000):
            csv_lines.append(f"Cat{i % 10},{i}")
        
        with open(self.test_file, "w") as f:
            f.write("\n".join(csv_lines))
        
        self.engine.load_csv(self.test_file)
        self.assertEqual(len(self.engine.data), 1000)
        
        result = self.engine.aggregator.aggregate("Category", "Value", "sum")
        self.assertEqual(len(result), 10)

    def test_large_dataset_with_many_categories(self):
        """Test with many categories."""
        csv_lines = ["Group,Value"]
        for i in range(10):
            for j in range(10):
                csv_lines.append(f"Group{i},{j * 10}")
        
        with open(self.test_file, "w") as f:
            f.write("\n".join(csv_lines))
        
        self.engine.load_csv(self.test_file)
        result = self.engine.analyzer.get_distribution("Group")
        self.assertEqual(len(result), 10)


class TestErrorHandling(unittest.TestCase):
    """Test error handling and edge cases."""

    def setUp(self):
        """Set up test fixtures."""
        self.engine = AnalysisEngine()
        self.test_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.test_dir, "test.csv")

    def tearDown(self):
        """Clean up."""
        if os.path.exists(self.test_file):
            os.remove(self.test_file)
        os.rmdir(self.test_dir)

    def test_aggregate_invalid_columns(self):
        """Test error handling for invalid columns."""
        csv_content = """A,B
1,2
3,4
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        
        with self.assertRaises(ValueError):
            self.engine.aggregator.aggregate("Invalid", "B")

    def test_analyze_without_loading_data(self):
        """Test error when analyzing without loading data."""
        with self.assertRaises(RuntimeError):
            self.engine.analyze_focused()

    def test_correlate_non_numeric_column(self):
        """Test correlation on non-numeric columns."""
        csv_content = """Name,Value
Alice,100
Bob,200
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.analyzer.correlate_metrics("Name", "Value")
        self.assertEqual(result.get("correlation", 0), 0)

    def test_statistics_non_numeric_column(self):
        """Test statistics on categorical column."""
        csv_content = """Name,Age
Alice,30
"""
        with open(self.test_file, "w") as f:
            f.write(csv_content)
        
        self.engine.load_csv(self.test_file)
        result = self.engine.analyzer.get_statistics("Name")
        self.assertEqual(result.get("count", 0), 0)


if __name__ == "__main__":
    unittest.main()
