from typing import List, Dict, Any

try:
    from rich.console import Console
    from rich.table import Table
    RICH_AVAILABLE = True
except Exception:
    RICH_AVAILABLE = False


class ConsoleDashboard:
    def __init__(self) -> None:
        self.console = Console() if RICH_AVAILABLE else None

    def render_summary(self, stats: Dict[str, int]) -> None:
        if not RICH_AVAILABLE:
            print("[STATS]", stats)
            return

        table = Table(title="Gem Hunter - Status")
        table.add_column("pending")
        table.add_column("processing")
        table.add_column("done")
        table.add_column("filtered")
        table.add_column("error")
        table.add_column("scored")
        table.add_row(
            str(stats.get("pending", 0)),
            str(stats.get("processing", 0)),
            str(stats.get("done", 0)),
            str(stats.get("filtered", 0)),
            str(stats.get("error", 0)),
            str(stats.get("scored", 0)),
        )
        self.console.print(table)

    def render_top_matches(self, rows: List[Dict[str, Any]]) -> None:
        if not RICH_AVAILABLE:
            print("[TOP MATCHES]", rows[:10])
            return

        table = Table(title="Top dopasowane oferty")
        table.add_column("score")
        table.add_column("title")
        table.add_column("company")
        table.add_column("url")

        for row in rows:
            score = str(row.get("matching_score", {}).get("score", "-"))
            table.add_row(
                score,
                row.get("title", "-"),
                row.get("company", "-"),
                row.get("url", "-"),
            )
        self.console.print(table)
