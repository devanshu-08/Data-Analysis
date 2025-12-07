"""
Data Analysis Module - Functional Programming Approach

Handles ranking, distributions, and statistical calculations using:
- Stream operations (filter, map, reduce)
- Functional composition for analysis
"""

from typing import Dict, List, Tuple, Any
from functools import reduce
from statistics import mean, median, stdev


class DataAnalyzer:
    """Perform analysis operations using functional programming paradigms."""

    def __init__(self, data: List[Dict[str, Any]], headers: List[str], numeric_cols: List[str]):
        self.data = data
        self.headers = headers
        self.numeric_cols = numeric_cols

    @staticmethod
    def _is_valid_number(val: Any) -> bool:
        """Check if value can be converted to float."""
        try:
            float(val)
            return True
        except (ValueError, TypeError):
            return False

    def rank(self, rank_col: str, value_col: str, limit: int = 10, operation: str = "sum") -> List[Tuple[str, float]]:
        """
        Rank items by metric value using functional stream operations.
        
        Functional approach:
        - Filter: Valid numeric rows
        - Map: Extract (item, value) tuples  
        - Reduce: Aggregate by item using functional accumulation
        - Sort: Using lambda comparator
        """
        # Stream 1: Filter valid rows and map to (key, value) tuples
        stream = filter(
            lambda item: item is not None,
            map(
                lambda row: (
                    row.get(rank_col, "Unknown"),
                    float(row.get(value_col, 0))
                ) if self._is_valid_number(row.get(value_col)) else None,
                self.data
            )
        )
        
        # Stream 2: Reduce to aggregate by item
        aggregated = reduce(
            lambda acc, item: {**acc, item[0]: acc.get(item[0], []) + [item[1]]},
            stream,
            {}
        )
        
        # Stream 3: Apply operation with lambda
        ops = {
            "sum": lambda v: sum(v),
            "avg": lambda v: sum(v) / len(v) if v else 0,
            "count": lambda v: len(v)
        }
        
        op_func = ops.get(operation, ops["sum"])
        results = {key: op_func(values) for key, values in aggregated.items()}
        
        # Stream 4: Sort using lambda and limit
        return sorted(results.items(), key=lambda x: x[1], reverse=True)[:limit]

    def get_distribution(self, col: str, top_n: int = 10) -> Dict[str, int]:
        """
        Get value counts for a column using functional reduce.
        
        Functional approach:
        - Filter: Valid non-empty values
        - Reduce: Count occurrences using lambda accumulator
        - Sort: Using lambda comparator
        """
        # Stream 1: Filter valid values
        valid_values = filter(
            lambda val: val and str(val).strip(),
            map(lambda row: row.get(col, "Unknown"), self.data)
        )
        
        # Stream 2: Reduce to count occurrences using lambda
        distribution = reduce(
            lambda acc, val: {**acc, val: acc.get(val, 0) + 1},
            valid_values,
            {}
        )
        
        # Stream 3: Sort and limit using lambda
        sorted_dist = sorted(distribution.items(), key=lambda x: x[1], reverse=True)
        return dict(sorted_dist[:top_n])

    def get_statistics(self, col: str) -> Dict[str, Any]:
        """
        Calculate statistics for a numeric column using functional operations.
        
        Functional approach:
        - Filter: Valid numeric values
        - Map: Convert to float
        - Reduce: Calculate aggregates
        - Lambda: Statistical functions
        """
        # Stream 1: Filter valid numeric values and convert
        numeric_stream = map(
            float,
            filter(
                lambda val: self._is_valid_number(val),
                map(lambda row: row.get(col, 0), self.data)
            )
        )
        
        values = list(numeric_stream)
        
        if not values:
            return {
                "count": 0, "sum": 0, "avg": 0, "min": 0, "max": 0,
                "median": 0, "std_dev": 0, "range": 0
            }
        
        # Use reduce to calculate sum in one functional pass
        total = reduce(lambda acc, x: acc + x, values, 0)
        
        # Lambda functions for statistical calculations
        count = len(values)
        avg = total / count
        min_val = min(values)
        max_val = max(values)
        median_val = median(values)
        std_dev = stdev(values) if count > 1 else 0
        range_val = max_val - min_val
        
        return {
            "count": count,
            "sum": total,
            "avg": avg,
            "min": min_val,
            "max": max_val,
            "median": median_val,
            "std_dev": std_dev,
            "range": range_val
        }

    def correlate_metrics(self, metric_col1: str, metric_col2: str) -> Dict[str, Any]:
        """
        Calculate Pearson correlation using functional stream operations.
        
        Functional approach:
        - Filter: Valid pairs of numeric values
        - Reduce: Calculate sums needed for correlation
        - Lambda: Correlation coefficient calculation
        """
        # Stream 1: Filter valid pairs and map to tuples
        pairs = list(filter(
            lambda pair: pair is not None,
            map(
                lambda row: (
                    float(row.get(metric_col1, 0)),
                    float(row.get(metric_col2, 0))
                ) if (self._is_valid_number(row.get(metric_col1)) and 
                      self._is_valid_number(row.get(metric_col2))) else None,
                self.data
            )
        ))
        
        if len(pairs) < 2:
            return {"correlation": 0, "strength": "none", "direction": "none"}
        
        # Stream 2: Reduce to calculate required sums
        n = len(pairs)
        stats = reduce(
            lambda acc, pair: {
                "sum_x": acc["sum_x"] + pair[0],
                "sum_y": acc["sum_y"] + pair[1],
                "sum_xy": acc["sum_xy"] + pair[0] * pair[1],
                "sum_x2": acc["sum_x2"] + pair[0] ** 2,
                "sum_y2": acc["sum_y2"] + pair[1] ** 2
            },
            pairs,
            {"sum_x": 0, "sum_y": 0, "sum_xy": 0, "sum_x2": 0, "sum_y2": 0}
        )
        
        # Lambda: Calculate Pearson correlation coefficient
        numerator = n * stats["sum_xy"] - stats["sum_x"] * stats["sum_y"]
        denominator = ((n * stats["sum_x2"] - stats["sum_x"] ** 2) * 
                      (n * stats["sum_y2"] - stats["sum_y"] ** 2)) ** 0.5
        
        correlation = numerator / denominator if denominator != 0 else 0
        
        # Lambda: Classify strength and direction
        strength = (
            "perfect" if abs(correlation) >= 0.9 else
            "strong" if abs(correlation) >= 0.7 else
            "moderate" if abs(correlation) >= 0.5 else
            "weak" if abs(correlation) > 0 else
            "none"
        )
        
        direction = "positive" if correlation > 0 else "negative" if correlation < 0 else "none"
        
        return {
            "correlation": round(correlation, 4),
            "strength": strength,
            "direction": direction
        }

    def find_outliers(self, col: str, threshold: float = 2.0) -> List[float]:
        """
        Find outliers using 2-sigma rule with functional stream operations.
        
        Functional approach:
        - Filter: Valid numeric values
        - Map: Convert to float
        - Calculate mean/stdev using reduce
        - Filter: Identify outliers using lambda predicate
        """
        # Stream 1: Extract and filter numeric values
        values = list(filter(
            lambda val: self._is_valid_number(val),
            map(lambda row: row.get(col, 0), self.data)
        ))
        
        if len(values) < 2:
            return []
        
        # Convert to float
        values = list(map(float, values))
        
        # Calculate mean using reduce
        mean_val = reduce(lambda acc, x: acc + x, values, 0) / len(values)
        
        # Calculate stdev using map and reduce
        variance = reduce(
            lambda acc, x: acc + (x - mean_val) ** 2,
            values,
            0
        ) / (len(values) - 1)
        stdev_val = variance ** 0.5
        
        # Stream 2: Filter outliers using lambda predicate
        outlier_threshold = threshold * stdev_val
        outliers = list(filter(
            lambda x: abs(x - mean_val) > outlier_threshold,
            values
        ))
        
        return outliers

    def percentile(self, col: str, p: float = 50) -> float:
        """
        Calculate percentile value using functional stream operations.
        
        Functional approach:
        - Filter: Valid numeric values
        - Map: Convert to float and sort
        - Lambda: Extract percentile index
        """
        # Stream 1: Filter and sort values using lambda
        values = sorted(filter(
            lambda val: self._is_valid_number(val),
            map(lambda row: float(row.get(col, 0)), self.data)
        ))
        
        if not values:
            return 0
        
        # Lambda: Calculate percentile index
        index = int((p / 100) * (len(values) - 1))
        return values[index]
