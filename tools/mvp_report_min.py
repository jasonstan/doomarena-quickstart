#!/usr/bin/env python3
"""Generate a minimal HTML report for the MVP demo results."""

from __future__ import annotations

import argparse
import csv
import html
import json
import pathlib
from collections import defaultdict
from typing import Any


def _load_rows(path: pathlib.Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _load_summary(path: pathlib.Path) -> dict[str, dict[str, str]]:
    summary: dict[str, dict[str, str]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            attack_id = row.get("attack_id")
            if attack_id:
                summary[attack_id] = row
    return summary


def _load_run_meta(rows_path: pathlib.Path) -> dict[str, Any]:
    run_meta_path = rows_path.parent / "run.json"
    if not run_meta_path.exists():
        return {}
    with run_meta_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _load_verifiers(rows_path: pathlib.Path) -> list[dict[str, Any]]:
    verifiers_path = rows_path.parent / "verifiers.json"
    if not verifiers_path.exists():
        return []
    try:
        with verifiers_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        return []
    if isinstance(data, list):
        return data
    return []


def _format_percent(value: str | float | None) -> str:
    if value is None:
        return "0%"
    try:
        if isinstance(value, str):
            value = float(value)
        return f"{value * 100:.1f}%"
    except Exception:  # pragma: no cover - defensive conversion
        return "0%"


def _parse_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a minimal HTML report for MVP demo results")
    parser.add_argument("--rows", required=True, type=pathlib.Path)
    parser.add_argument("--summary", required=True, type=pathlib.Path)
    parser.add_argument("--out", required=True, type=pathlib.Path)
    args = parser.parse_args()

    rows = _load_rows(args.rows)
    summary = _load_summary(args.summary)
    run_meta = _load_run_meta(args.rows)
    verifiers = _load_verifiers(args.rows)

    overall = summary.get("overall", {})

    html_parts: list[str] = []
    html_parts.append("<!DOCTYPE html>")
    html_parts.append("<html><head><meta charset='utf-8'>")
    html_parts.append("<title>DoomArena MVP Demo Report</title>")
    html_parts.append(
        "<style>body{font-family:Arial,Helvetica,sans-serif;margin:20px;}table{border-collapse:collapse;width:100%;}"
        "th,td{border:1px solid #ddd;padding:8px;vertical-align:top;}th{background-color:#f4f4f4;}"
        "button{padding:4px 8px;}pre{white-space:pre-wrap;word-break:break-word;margin:8px 0;}"
        ".success-true{color:#0a0;} .success-false{color:#a00;}"
        ".badge{display:inline-block;padding:2px 6px;margin-left:6px;font-size:12px;border-radius:4px;background-color:#eee;color:#555;}"
        ".badge-rate_limit{background-color:#ffe8d5;color:#a55d00;}"
        ".badge-http{background-color:#ffe5e5;color:#a00000;}"
        ".badge-network{background-color:#e5f1ff;color:#0055aa;}"
        ".stats{margin:12px 0;padding:10px;border:1px solid #ddd;border-radius:6px;background-color:#fafafa;}"
        ".methodology{margin:20px 0;}"
        ".methodology-entry{margin-bottom:16px;padding:12px;border:1px solid #ddd;border-radius:6px;background-color:#fdfdfd;}"
        "</style>"
    )
    html_parts.append(
        "<script>function toggle(id){var el=document.getElementById(id);if(!el){return;}"
        "if(el.style.display==='none'){el.style.display='block';}else{el.style.display='none';}}</script>"
    )
    html_parts.append("</head><body>")
    html_parts.append("<h1>DoomArena MVP Demo Report</h1>")

    provider = html.escape(str(run_meta.get("provider", "unknown")))
    model = html.escape(str(run_meta.get("model", "unknown")))
    temperature = html.escape(str(run_meta.get("temperature", "0")))
    trials = html.escape(str(run_meta.get("trials", "0")))
    timestamp = html.escape(str(run_meta.get("timestamp", "")))

    html_parts.append(
        f"<p><strong>Provider:</strong> {provider} &nbsp; <strong>Model:</strong> {model} &nbsp;"
        f"<strong>Temperature:</strong> {temperature} &nbsp; <strong>Trials per case:</strong> {trials}</p>"
    )
    if timestamp:
        html_parts.append(f"<p><strong>Timestamp:</strong> {timestamp}</p>")

    attempts_total = _parse_int(overall.get("attempts_total"))
    attempts_viable = _parse_int(overall.get("attempts_viable"))
    successes_total = _parse_int(overall.get("successes"))
    excluded_total = max(0, attempts_total - attempts_viable)
    overall_asr = _format_percent(overall.get("asr_viable"))

    excluded_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        attempt_status = row.get("attempt_status", "ok")
        count_in_asr = row.get("count_in_asr", True)
        if attempt_status != "ok" or not count_in_asr:
            error_kind = row.get("error_kind") or "other"
            excluded_counts[str(error_kind)] += 1

    breakdown_parts: list[str] = []
    known_labels = {
        "rate_limit": "rate-limit",
        "http": "http",
        "network": "network",
    }
    for key, label in known_labels.items():
        count = excluded_counts.get(key, 0)
        if count:
            breakdown_parts.append(f"{label} {count}")
    for key, count in excluded_counts.items():
        if key in known_labels or not count:
            continue
        breakdown_parts.append(f"{key} {count}")
    excluded_text = ", ".join(breakdown_parts) if breakdown_parts else "none"

    html_parts.append(
        "<div class='stats'>"
        f"<p><strong>Attempts:</strong> {attempts_viable} / {attempts_total}"
        f" &nbsp; • &nbsp; <strong>Excluded:</strong> {excluded_total} ({excluded_text})" \
        "</p>"
        f"<p><strong>Successes:</strong> {successes_total}"
        f" &nbsp; • &nbsp; <strong>ASR (viable):</strong> {overall_asr}</p>"
        "</div>"
    )

    if verifiers:
        html_parts.append("<h2>Methodology</h2>")
        html_parts.append("<div class='methodology'>")
        for entry in sorted(verifiers, key=lambda item: str(item.get("attack_id", ""))):
            attack_id = html.escape(str(entry.get("attack_id", "")))
            nl_desc = html.escape(str(entry.get("nl", "")))
            rule_type = html.escape(str(entry.get("type", "")))
            threshold = html.escape(str(entry.get("threshold", "")))
            patterns_raw = entry.get("patterns", [])
            if not isinstance(patterns_raw, list):
                patterns_raw = [patterns_raw]
            patterns_display = html.escape(json.dumps(patterns_raw, ensure_ascii=False))
            explanation = html.escape(str(entry.get("explanation", "")))
            html_parts.append("<div class='methodology-entry'>")
            html_parts.append(f"<p><strong>Attack ID:</strong> {attack_id}</p>")
            html_parts.append(
                f"<p><strong>Natural-language description:</strong> {nl_desc}</p>"
            )
            html_parts.append(f"<p><strong>Rule:</strong> {rule_type}</p>")
            html_parts.append(f"<p><strong>Threshold:</strong> {threshold}</p>")
            html_parts.append(
                f"<p><strong>Patterns:</strong> <code>{patterns_display}</code></p>"
            )
            html_parts.append(
                f"<p><strong>In plain English:</strong> {explanation}</p>"
            )
            html_parts.append("</div>")
        html_parts.append("</div>")

    html_parts.append(
        "<table><thead><tr><th>Attempt</th><th>Attack ID</th><th>Success</th><th>Input</th><th>Output</th><th>Why success?</th></tr></thead><tbody>"
    )
    for index, row in enumerate(rows):
        attempt = html.escape(str(row.get("trial_id", index)))
        attack_id = html.escape(str(row.get("attack_id", "")))
        success_value = bool(row.get("success"))
        success_label = "✅" if success_value else "❌"
        success_class = "success-true" if success_value else "success-false"
        input_content = html.escape(row.get("input_text", ""))
        output_content = html.escape(row.get("output_text", ""))
        error_text = html.escape(row.get("error", ""))
        attempt_status = row.get("attempt_status", "ok")
        count_in_asr = row.get("count_in_asr", True)
        input_id = f"input-{index}"
        output_id = f"output-{index}"
        input_cell = (
            f"<button type='button' onclick=\"toggle('{input_id}')\">Toggle</button>"
            f"<div id='{input_id}' style='display:none;'><pre>{input_content}</pre></div>"
        )
        if error_text:
            output_body = f"<pre>{output_content}</pre><pre>Error: {error_text}</pre>"
        else:
            output_body = f"<pre>{output_content}</pre>"
        output_cell = (
            f"<button type='button' onclick=\"toggle('{output_id}')\">Toggle</button>"
            f"<div id='{output_id}' style='display:none;'>{output_body}</div>"
        )
        badge_html = ""
        if attempt_status != "ok" or not count_in_asr:
            badge_kind = str(row.get("error_kind") or "other")
            badge_label_map = {
                "rate_limit": "rate limit",
                "http": "http",
                "network": "network",
            }
            badge_label = badge_label_map.get(badge_kind, "other")
            badge_class = f"badge badge-{html.escape(badge_kind)}"
            badge_html = f"<span class='{badge_class}'>Excluded - {html.escape(badge_label)}</span>"

        verifier_rule_raw = str(row.get("verifier_rule_id", "-")) or "-"
        verifier_threshold_raw = str(row.get("verifier_threshold", "-")) or "-"
        verifier_matched_raw = "true" if row.get("verifier_matched") else "false"
        verifier_pattern_raw = str(row.get("verifier_pattern", ""))
        fallback_text = (
            f"rule={verifier_rule_raw} • threshold={verifier_threshold_raw} "
            f"• matched={verifier_matched_raw} • pattern=\"{verifier_pattern_raw}\""
        )
        why_success = ""
        if attempt_status == "ok" and bool(count_in_asr):
            why_field = row.get("verifier_why")
            if why_field:
                why_success = html.escape(str(why_field))
            else:
                why_success = html.escape(fallback_text)

        html_parts.append(
            "<tr>"
            f"<td>{attempt}{badge_html}</td>"
            f"<td>{attack_id}</td>"
            f"<td class='{success_class}'>{success_label}</td>"
            f"<td>{input_cell}</td>"
            f"<td>{output_cell}</td>"
            f"<td>{why_success}</td>"
            "</tr>"
        )
    html_parts.append("</tbody></table>")

    html_parts.append("</body></html>")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("".join(html_parts), encoding="utf-8")


if __name__ == "__main__":
    main()
