"""
Data Aggregation Module - Functional Programming Approach

Handles grouping, filtering, and combining data using functional paradigms:
- Stream operations (filter, map, reduce)
- Data aggregation using functional composition
- Pure functions and immutable data transformations
"""

from typing import Dict, List, Any, Callable, Tuple
from functools import reduce
from operator import itemgetter


class DataAggregator:
    """Perform aggregation operations using functional programming paradigms."""

    def __init__(self, data: List[Dict[str, Any]], headers: List[str]):
        self.data = data
        self.headers = headers

    @staticmethod
    def _is_valid_number(val: Any) -> bool:
        """Check if value can be converted to float."""
        try:
            float(val)
            return True
        except (ValueError, TypeError):
            return False

    def aggregate(self, group_col: str, value_col: str, operation: str = "sum") -> Dict[str, float]:
        """
        Aggregate numeric values by grouping column using stream operations.
        
        Functional approach:
        - Filter: Extract valid numeric values
        - Map: Transform to (key, value) tuples
        - Reduce: Apply aggregation function to each group
        """
        if group_col not in self.headers or value_col not in self.headers:
            raise ValueError(f"Invalid columns: {group_col}, {value_col}")
        
        # Stream 1: Filter valid rows and map to (key, value) tuples
        stream = filter(
            lambda item: item is not None,
            map(
                lambda row: (
                    row.get(group_col, "Unknown"),
                    float(row.get(value_col, 0))
                ) if self._is_valid_number(row.get(value_col)) else None,
                self.data
            )
        )
        
        # Stream 2: Group using reduce pattern
        grouped = reduce(
            lambda acc, item: {**acc, item[0]: acc.get(item[0], []) + [item[1]]},
            stream,
            {}
        )
        
        # Stream 3: Define aggregation lambda functions
        ops = {
            "sum": lambda v: sum(v),
            "avg": lambda v: sum(v) / len(v) if v else 0,
            "min": lambda v: min(v) if v else 0,
            "max": lambda v: max(v) if v else 0,
            "count": lambda v: len(v)
        }
        
        # Stream 4: Map aggregation function over groups
        agg_func = ops.get(operation, ops["sum"])
        return {key: agg_func(values) for key, values in grouped.items()}

    def group_by_multi(self, group_cols: List[str], value_col: str, operation: str = "sum") -> Dict:
        """
        Multi-level grouping using functional reduce and composition.
        
        Functional approach:
        - Filter valid rows
        - Map to (nested_keys, value) tuples
        - Reduce to build nested dictionary
        """
        if not group_cols or value_col not in self.headers:
            raise ValueError("Invalid columns specified")
        
        # Stream: Filter valid rows
        valid_rows = filter(
            lambda row: self._is_valid_number(row.get(value_col)),
            self.data
        )
        
        # Map: Transform to (key_tuple, value) pairs
        key_value_pairs = map(
            lambda row: (
                tuple(row.get(col, "Unknown") for col in group_cols),
                float(row.get(value_col, 0))
            ),
            valid_rows
        )
        
        # Reduce: Build nested dictionary structure
        def build_nested(acc: Dict, item: Tuple) -> Dict:
            """Build nested dict recursively using functional reduce."""
            keys, val = item
            current = acc
            for key in keys[:-1]:
                current = current.setdefault(key, {})
            last_key = keys[-1]
            current[last_key] = current.get(last_key, 0) + val
            return acc
        
        return reduce(build_nested, key_value_pairs, {})

    def compare_metrics(self, group_col: str, metric_cols: List[str]) -> Dict[str, Dict[str, float]]:
        """
        Calculate average metrics for each group using functional reduce.
        
        Functional approach:
        - Reduce over rows to accumulate metrics by group
        - Map lambda to calculate averages
        """
        def accumulate_metrics(acc: Dict, row: Dict) -> Dict:
            """Reduce function: accumulate metrics per group."""
            group_key = row.get(group_col, "Unknown")
            if group_key not in acc:
                acc[group_key] = {col: [] for col in metric_cols}
            for metric_col in metric_cols:
                if self._is_valid_number(row.get(metric_col)):
                    acc[group_key][metric_col].append(float(row.get(metric_col, 0)))
            return acc
        
        # Reduce: Accumulate metrics
        grouped_metrics = reduce(accumulate_metrics, self.data, {})
        
        # Map: Calculate averages using lambda
        calc_avg = lambda vals: sum(vals) / len(vals) if vals else 0
        return {
            group_key: {col: calc_avg(vals) for col, vals in metrics.items()}
            for group_key, metrics in grouped_metrics.items()
        }

    def filter_by_condition(self, col: str, operator: str, value: float) -> List[Dict[str, Any]]:
        """
        Filter rows based on numeric condition using functional stream.
        
        Functional approach:
        - Define operators as lambda functions
        - Filter stream based on lambda predicate
        """
        # Define operators as lambda functions
        ops = {
            '>': lambda x, y: x > y,
            '<': lambda x, y: x < y,
            '>=': lambda x, y: x >= y,
            '<=': lambda x, y: x <= y,
            '==': lambda x, y: x == y,
            '!=': lambda x, y: x != y
        }
        
        comparator = ops.get(operator, ops['=='])
        
        # Stream: Filter rows matching lambda predicate
        return list(filter(
            lambda row: comparator(
                float(row.get(col, 0)),
                value
            ) if self._is_valid_number(row.get(col)) else False,
            self.data
        ))

    def group_and_count(self, group_col: str) -> Dict[str, int]:
        """
        Count records for each group using functional reduce.
        
        Functional approach:
        - Reduce with lambda to accumulate counts
        """
        return reduce(
            lambda acc, row: {
                **acc,
                row.get(group_col, "Unknown"): acc.get(row.get(group_col, "Unknown"), 0) + 1
            },
            self.data,
            {}
        )

    def cross_tabulation(self, col1: str, col2: str) -> Dict[str, Dict[str, int]]:
        """
        Create cross-tabulation between two categorical columns using functional reduce.
        
        Functional approach:
        - Reduce to build 2D frequency table
        - Lambda functions for key extraction and nested dict building
        """
        def build_crosstab(acc: Dict, row: Dict) -> Dict:
            """Reduce function: build cross-tabulation."""
            key1 = row.get(col1, "Unknown")
            key2 = row.get(col2, "Unknown")
            if key1 not in acc:
                acc[key1] = {}
            acc[key1][key2] = acc[key1].get(key2, 0) + 1
            return acc
        
        return reduce(build_crosstab, self.data, {})
