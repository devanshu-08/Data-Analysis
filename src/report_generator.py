"""
Report Generation Module

Handles formatting and displaying analysis results.
"""

from typing import Dict, List, Tuple, Any


class ReportGenerator:
    """Format and display analysis results."""

    @staticmethod
    def print_header(title: str) -> None:
        """Print a formatted section header."""
        print("\n" + "="*80)
        print(title)
        print("="*80)

    @staticmethod
    def print_subheader(title: str) -> None:
        """Print a formatted subsection header."""
        print(f"{title}")
        print("-" * 40)

    @staticmethod
    def print_summary(total_records: int, total_cols: int, numeric_cols: List[str], categorical_cols: List[str]) -> None:
        """Print dataset summary."""
        print(f"Dataset: {total_records} records, {total_cols} columns")
        print(f"Numeric columns: {numeric_cols}")
        print(f"Categorical columns: {categorical_cols}\n")

    @staticmethod
    def print_aggregation(title: str, results: Dict[str, float]) -> None:
        """Print aggregation results."""
        ReportGenerator.print_subheader(f"Aggregation: {title}")
        for key, value in results.items():
            print(f"  {key}: ${value:,.2f}")
        print()

    @staticmethod
    def print_multi_level(title: str, results: Dict) -> None:
        """Print multi-level grouped results."""
        ReportGenerator.print_subheader(f"Multi-level grouping: {title}")
        for group1, group2_dict in results.items():
            print(f"  {group1}:")
            for group2, value in sorted(group2_dict.items(), key=lambda x: x[1], reverse=True):
                print(f"    {group2}: ${value:,.2f}")
        print()

    @staticmethod
    def print_ranking(title: str, results: List[Tuple[str, float]]) -> None:
        """Print ranked results."""
        ReportGenerator.print_subheader(f"Top items by {title}")
        for idx, (item, value) in enumerate(results, 1):
            print(f"  {idx}. {item}: ${value:,.2f}")
        print()

    @staticmethod
    def print_comparison(title: str, results: Dict[str, Dict[str, float]]) -> None:
        """Print metric comparison results."""
        ReportGenerator.print_subheader(f"Metric comparison by {title}")
        for group_key, metrics in results.items():
            print(f"  {group_key}:")
            for metric, value in metrics.items():
                print(f"    Avg {metric}: ${value:,.2f}")
        print()

    @staticmethod
    def print_distribution(title: str, results: Dict[str, int]) -> None:
        """Print value distribution results."""
        ReportGenerator.print_subheader(f"Distribution: {title}")
        for value, count in results.items():
            print(f"  {value}: {count}")
        print()

    @staticmethod
    def print_completion() -> None:
        """Print completion message."""
        print("="*80)
        print("Analysis Complete!")
        print("="*80 + "\n")
