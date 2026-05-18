"""
Generates HTML data quality reports for stakeholder visibility.
"""
import json
from datetime import datetime


def generate_html_report(results: list, output_path: str = "docs/dq_report.html"):
    """Generate a standalone HTML data quality report."""
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    score = passed / total * 100 if total else 0

    rows = ""
    for r in results:
        color = "#28a745" if r.passed else "#dc3545"
        status = "PASS" if r.passed else "FAIL"
        rows += f"""
        <tr>
            <td style="color:{color};font-weight:bold">{status}</td>
            <td>{r.check_name}</td>
            <td>{r.metric}</td>
            <td>{r.threshold}</td>
            <td>{r.details}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html>
<head><title>DQ Report - {datetime.now().strftime('%Y-%m-%d %H:%M')}</title>
<style>
body {{ font-family: -apple-system, sans-serif; margin: 40px; background: #f8f9fa; }}
h1 {{ color: #212529; }}
.score {{ font-size: 48px; font-weight: bold; color: {"#28a745" if score >= 90 else "#ffc107" if score >= 70 else "#dc3545"}; }}
table {{ border-collapse: collapse; width: 100%; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
th {{ background: #343a40; color: white; padding: 12px; text-align: left; }}
td {{ padding: 10px 12px; border-bottom: 1px solid #dee2e6; }}
tr:hover {{ background: #f1f3f5; }}
</style></head>
<body>
<h1>Data Quality Report</h1>
<p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
<p class="score">{score:.0f}%</p>
<p>{passed}/{total} checks passed</p>
<table>
<tr><th>Status</th><th>Check</th><th>Metric</th><th>Threshold</th><th>Details</th></tr>
{rows}
</table>
</body></html>"""

    with open(output_path, "w") as f:
        f.write(html)
    print(f"DQ Report saved to {output_path}")
