"""
Analysis Engine - Orchestrator

Coordinates data loading, aggregation, analysis, and reporting.
"""

from src.data_loader import DataLoader
from src.data_aggregator import DataAggregator
from src.data_analyzer import DataAnalyzer
from src.report_generator import ReportGenerator


class AnalysisEngine:
    """Orchestrate complete analysis workflow."""

    def __init__(self):
        self.loader = None
        self.aggregator = None
        self.analyzer = None

    def load_csv(self, source: str) -> None:
        """Load CSV data from file or URL."""
        self.loader = DataLoader()
        self.loader.load(source)
        
        # Initialize dependent components
        self.aggregator = DataAggregator(self.loader.data, self.loader.headers)
        self.analyzer = DataAnalyzer(self.loader.data, self.loader.headers, self.loader.numeric_cols)

    @property
    def data(self):
        return self.loader.data if self.loader else []

    @property
    def numeric_cols(self):
        return self.loader.numeric_cols if self.loader else []

    @property
    def categorical_cols(self):
        return self.loader.categorical_cols if self.loader else []

    @property
    def headers(self):
        return self.loader.headers if self.loader else []

    def analyze(self) -> None:
        """Execute complete analysis pipeline."""
        if not self.loader:
            raise RuntimeError("No data loaded. Call load_csv() first.")

        ReportGenerator.print_header("ANALYSIS RESULTS")
        ReportGenerator.print_summary(
            len(self.loader.data),
            len(self.loader.headers),
            self.numeric_cols,
            self.categorical_cols
        )

        try:
            # Aggregation by categorical columns
            if self.categorical_cols and self.numeric_cols:
                for cat_col in self.categorical_cols[:2]:
                    for num_col in self.numeric_cols[:1]:
                        results = self.aggregator.aggregate(cat_col, num_col, "sum")
                        ReportGenerator.print_aggregation(f"{num_col} by {cat_col}", results)

            # Multi-level grouping
            if len(self.categorical_cols) >= 2 and self.numeric_cols:
                col1, col2 = self.categorical_cols[0], self.categorical_cols[1]
                num_col = self.numeric_cols[0]
                results = self.aggregator.group_by_multi([col1, col2], num_col, "sum")
                ReportGenerator.print_multi_level(f"{num_col} by {col1} and {col2}", results)

            # Ranking
            if self.categorical_cols and self.numeric_cols:
                cat_col = self.categorical_cols[0]
                num_col = self.numeric_cols[0]
                results = self.analyzer.rank(cat_col, num_col, limit=5)
                ReportGenerator.print_ranking(num_col, results)

            # Metric comparison
            if self.categorical_cols and len(self.numeric_cols) > 1:
                cat_col = self.categorical_cols[0]
                metric_cols = self.numeric_cols[:3]
                results = self.aggregator.compare_metrics(cat_col, metric_cols)
                ReportGenerator.print_comparison(cat_col, results)

            # Distribution
            if self.categorical_cols:
                for cat_col in self.categorical_cols[:2]:
                    results = self.analyzer.get_distribution(cat_col)
                    ReportGenerator.print_distribution(cat_col, results)

            ReportGenerator.print_completion()

        except Exception as e:
            print(f"Error during analysis: {e}")
            raise

    def analyze_focused(self) -> None:
        """Execute focused analysis with meaningful insights only."""
        if not self.loader:
            raise RuntimeError("No data loaded. Call load_csv() first.")

        ReportGenerator.print_header("KEY INSIGHTS")

        try:
            # 1. Top numeric metrics
            if self.numeric_cols:
                print("\n[NUMERIC METRICS SUMMARY]")
                print("-" * 80)
                for num_col in self.numeric_cols[:5]:  # Top 5 numeric columns
                    stats = self.analyzer.get_statistics(num_col)
                    print(f"\n{num_col.upper()}:")
                    print(f"  Count: {stats.get('count', 'N/A')}")
                    print(f"  Sum: ${stats.get('sum', 'N/A'):,.2f}" if isinstance(stats.get('sum'), (int, float)) else f"  Sum: {stats.get('sum', 'N/A')}")
                    print(f"  Average: ${stats.get('avg', 'N/A'):,.2f}" if isinstance(stats.get('avg'), (int, float)) else f"  Average: {stats.get('avg', 'N/A')}")
                    print(f"  Min: ${stats.get('min', 'N/A'):,.2f}" if isinstance(stats.get('min'), (int, float)) else f"  Min: {stats.get('min', 'N/A')}")
                    print(f"  Max: ${stats.get('max', 'N/A'):,.2f}" if isinstance(stats.get('max'), (int, float)) else f"  Max: {stats.get('max', 'N/A')}")

            # 2. Category-wise breakdown (if category column exists)
            category_col = None
            if 'category' in [col.lower() for col in self.categorical_cols]:
                category_col = next((col for col in self.categorical_cols if col.lower() == 'category'), None)
            elif self.categorical_cols:
                category_col = self.categorical_cols[0]
            
            if category_col and self.numeric_cols:
                print(f"\n\n[{category_col.upper()}-WISE BREAKDOWN]")
                print("-" * 80)
                
                # Show breakdown by category for each numeric column
                for num_col in self.numeric_cols[:3]:  # Show up to 3 numeric metrics
                    print(f"\n{num_col.upper()} by {category_col.upper()}:")
                    results = self.aggregator.aggregate(category_col, num_col, "sum")
                    sorted_results = sorted(results.items(), key=lambda x: x[1] if isinstance(x[1], (int, float)) else 0, reverse=True)
                    
                    dist = self.analyzer.get_distribution(category_col)
                    for category, total in sorted_results[:10]:
                        if isinstance(total, (int, float)):
                            count = dist.get(category, 1)
                            avg = total / count if count > 0 else 0
                            print(f"  {category}: Total: {total:,.2f} | Avg: {avg:,.2f} ({count} records)")
                        else:
                            print(f"  {category}: {total}")

            # 3. Distribution of primary categorical
            if self.categorical_cols:
                cat_col = self.categorical_cols[0]
                print(f"\n\n[TOP 10 VALUES: {cat_col.upper()}]")
                print("-" * 80)
                results = self.analyzer.get_distribution(cat_col, top_n=10)
                for item, count in list(results.items())[:10]:
                    percentage = (count / len(self.loader.data)) * 100
                    print(f"  {item}: {count} records ({percentage:.1f}%)")

            # 4. Top items by numeric metric (if applicable)
            if self.categorical_cols and self.numeric_cols:
                cat_col = self.categorical_cols[0]
                num_col = self.numeric_cols[0]
                print(f"\n\n[TOP 10 {cat_col.upper()} BY AVG {num_col.upper()}]")
                print("-" * 80)
                results = self.analyzer.rank(cat_col, num_col, limit=10)
                for item, value in results:
                    print(f"  {item}: {value:,.2f}" if isinstance(value, (int, float)) else f"  {item}: {value}")

            # 5. Second categorical distribution if available
            if len(self.categorical_cols) >= 2:
                cat_col2 = self.categorical_cols[1]
                print(f"\n\n[TOP 10 VALUES: {cat_col2.upper()}]")
                print("-" * 80)
                results = self.analyzer.get_distribution(cat_col2, top_n=10)
                for item, count in list(results.items())[:10]:
                    percentage = (count / len(self.loader.data)) * 100
                    print(f"  {item}: {count} records ({percentage:.1f}%)")

            ReportGenerator.print_completion()

        except Exception as e:
            print(f"Error during focused analysis: {e}")
            raise