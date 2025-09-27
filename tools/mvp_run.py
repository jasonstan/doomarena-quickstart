#!/usr/bin/env python3
"""Execute MVP demo cases against a provider using the smoke client logic."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import random
import re
import sys
import time
from typing import Any, Callable

TOOLS_DIR = pathlib.Path(__file__).resolve().parent
if str(TOOLS_DIR) not in sys.path:
    sys.path.append(str(TOOLS_DIR))

import smoke_single_attack  # type: ignore

ProviderCaller = Callable[[str, str, float], tuple[str, dict[str, Any]]]


def _parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    text = value.strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"Expected a boolean value, got: {value!r}")


def _build_caller(provider: str, *, groq_base: str, openai_base: str) -> ProviderCaller:
    provider = provider.lower()
    if provider == "groq":

        def _call(prompt: str, model: str, temperature: float) -> tuple[str, dict[str, Any]]:
            text = smoke_single_attack._call_groq(prompt, model, temperature, groq_base)
            return text, {}

        return _call
    if provider == "openai":

        def _call(prompt: str, model: str, temperature: float) -> tuple[str, dict[str, Any]]:
            text = smoke_single_attack._call_openai(prompt, model, temperature, openai_base)
            return text, {}

        return _call
    if provider == "mock":

        def _call(prompt: str, _model: str, _temperature: float) -> tuple[str, dict[str, Any]]:
            text = smoke_single_attack._call_mock(prompt)
            return text, {}

        return _call
    raise ValueError(f"Unsupported provider: {provider}")


def _load_cases(path: pathlib.Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            case = json.loads(line)
            cases.append(case)
    return cases


def main() -> None:
    parser = argparse.ArgumentParser(description="Run MVP demo cases against a provider")
    parser.add_argument("--cases", required=True, type=pathlib.Path)
    parser.add_argument("--provider", required=True, choices=["groq", "openai", "mock"])
    parser.add_argument("--model", required=True)
    parser.add_argument("--temperature", required=True, type=float)
    parser.add_argument("--trials", required=True, type=int)
    parser.add_argument("--run-dir", required=True, type=pathlib.Path)
    parser.add_argument(
        "--groq-base",
        default=os.getenv("GROQ_API_BASE", "https://api.groq.com/openai/v1"),
    )
    parser.add_argument(
        "--openai-base",
        default=os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1"),
    )
    parser.add_argument(
        "--allow-mock-fallback",
        default=False,
        type=_parse_bool,
    )
    parser.add_argument("--rpm", default=30, type=int)
    parser.add_argument("--sleep-ms", dest="sleep_ms", default=0, type=int)
    parser.add_argument("--max-retries", dest="max_retries", default=2, type=int)
    parser.add_argument("--backoff-ms", dest="backoff_ms", default=750, type=int)
    parser.add_argument(
        "--respect-retry-after",
        dest="respect_retry_after",
        default=True,
        type=_parse_bool,
    )
    args = parser.parse_args()

    requested_provider = args.provider
    groq_key = os.getenv("GROQ_API_KEY", "") if requested_provider == "groq" else "present"
    openai_key = (
        os.getenv("OPENAI_API_KEY", "") if requested_provider == "openai" else "present"
    )

    missing_key_code: str | None = None
    missing_key_message: str | None = None
    effective_provider = requested_provider

    if requested_provider == "groq" and not groq_key:
        missing_key_code = "MISSING_GROQ_API_KEY"
        missing_key_message = "[ERROR] GROQ_API_KEY missing"
        if args.allow_mock_fallback:
            effective_provider = "mock"
        else:
            effective_provider = "none"
    elif requested_provider == "openai" and not openai_key:
        missing_key_code = "MISSING_OPENAI_API_KEY"
        missing_key_message = "[ERROR] OPENAI_API_KEY missing"
        if args.allow_mock_fallback:
            effective_provider = "mock"
        else:
            effective_provider = "none"

    caller: ProviderCaller | None
    if effective_provider == "none":
        caller = None
    else:
        caller = _build_caller(
            effective_provider,
            groq_base=args.groq_base,
            openai_base=args.openai_base,
        )
    cases = _load_cases(args.cases)

    run_dir = args.run_dir
    run_dir.mkdir(parents=True, exist_ok=True)
    rows_path = run_dir / "rows.jsonl"

    run_meta = {
        "provider": requested_provider,
        "effective_provider": effective_provider,
        "model": args.model,
        "temperature": args.temperature,
        "trials": args.trials,
        "timestamp": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
    }
    with (run_dir / "run.json").open("w", encoding="utf-8") as handle:
        json.dump(run_meta, handle, ensure_ascii=False, indent=2)
        handle.write("\n")

    rpm = max(0, int(args.rpm))
    min_interval = 60.0 / rpm if rpm > 0 else 0.0
    sleep_extra = max(0, int(args.sleep_ms)) / 1000.0
    max_retries = max(0, int(args.max_retries))
    backoff_base = max(0, int(args.backoff_ms)) / 1000.0
    respect_retry_after = bool(args.respect_retry_after)

    previous_call_ts: float | None = None

    def _await_turn() -> None:
        nonlocal previous_call_ts
        if caller is None or min_interval <= 0 or previous_call_ts is None:
            return
        now = time.time()
        next_earliest = max(now, previous_call_ts + min_interval)
        delay = next_earliest - now
        if delay > 0:
            time.sleep(delay)

    def _record_attempt_time() -> None:
        nonlocal previous_call_ts
        previous_call_ts = time.time()
        if sleep_extra > 0:
            time.sleep(sleep_extra)

    def _classify_exception(exc: Exception) -> dict[str, Any]:
        exc_text = str(exc) or exc.__class__.__name__
        http_status: int | None = None
        error_code: str | None = None
        retry_after_seconds: float | None = None
        error_kind = "network"
        body_text = ""
        json_body: dict[str, Any] | None = None

        http_match = re.search(r"HTTP\s+(\d{3})", exc_text)
        if http_match:
            http_status = int(http_match.group(1))
            error_kind = "http"
            if http_status == 429:
                error_kind = "rate_limit"
            body_text = exc_text.split("\n", 1)[1].strip() if "\n" in exc_text else ""
            try:
                json_body = json.loads(body_text) if body_text else None
            except json.JSONDecodeError:
                json_body = None
        elif "Network error" in exc_text:
            error_kind = "network"
        error_obj: dict[str, Any] | None = None
        message_text = ""
        if json_body:
            if isinstance(json_body, dict):
                if isinstance(json_body.get("error"), dict):
                    error_obj = json_body["error"]
                else:
                    error_obj = None
            if isinstance(error_obj, dict):
                error_code = error_obj.get("code") or error_obj.get("type")
                message_text = str(error_obj.get("message", ""))
                retry_hint = error_obj.get("retry_after_ms")
                if isinstance(retry_hint, (int, float)) and retry_hint > 0:
                    retry_after_seconds = float(retry_hint) / 1000.0
                retry_hint_seconds = error_obj.get("retry_after")
                if (
                    retry_after_seconds is None
                    and isinstance(retry_hint_seconds, (int, float))
                    and retry_hint_seconds > 0
                ):
                    retry_after_seconds = float(retry_hint_seconds)
            if not message_text and isinstance(json_body, dict):
                message_text = str(json_body.get("message", ""))
            if retry_after_seconds is None and isinstance(json_body, dict):
                retry_root_ms = json_body.get("retry_after_ms")
                retry_root_s = json_body.get("retry_after")
                if isinstance(retry_root_ms, (int, float)) and retry_root_ms > 0:
                    retry_after_seconds = float(retry_root_ms) / 1000.0
                elif (
                    isinstance(retry_root_s, (int, float))
                    and retry_root_s > 0
                ):
                    retry_after_seconds = float(retry_root_s)
        if not message_text and body_text:
            message_text = body_text
        if not message_text:
            message_text = exc_text

        if retry_after_seconds is None and message_text:
            match_retry = re.search(
                r"Please try again in\s+(\d+(?:\.\d+)?)\s*(seconds?|ms|milliseconds?)",
                message_text,
                flags=re.IGNORECASE,
            )
            if match_retry:
                amount = float(match_retry.group(1))
                unit = match_retry.group(2).lower()
                if unit.startswith("ms"):
                    retry_after_seconds = amount / 1000.0
                else:
                    retry_after_seconds = amount

        retryable = False
        if http_status == 429 or (http_status is not None and http_status >= 500):
            retryable = True

        short_message = message_text[:500]
        if len(message_text) > 500:
            short_message = short_message.rstrip() + "…"

        return {
            "error_text": exc_text,
            "short_message": short_message,
            "error_kind": error_kind,
            "http_status": http_status,
            "error_code": error_code,
            "retry_after_seconds": retry_after_seconds,
            "retryable": retryable,
        }

    with rows_path.open("w", encoding="utf-8") as handle:
        for case in cases:
            attack_id = case.get("attack_id")
            persona = case.get("persona")
            literal_prompt = str(case.get("prompt", ""))
            variant_id = str(case.get("variant_id", "base")) if case.get("variant_id") else "base"
            if not attack_id:
                continue
            for trial_index in range(args.trials):
                trial_id = f"{attack_id}-{trial_index}"
                row_errors: list[str] = []
                if missing_key_code and args.allow_mock_fallback:
                    row_errors.append(missing_key_code)

                row: dict[str, Any] = {
                    "trial_id": trial_id,
                    "attack_id": attack_id,
                    "persona": persona,
                    "variant_id": variant_id,
                    "input_text": literal_prompt,
                    "callable": caller is not None,
                }

                output_text: str
                attempt_status = "ok"
                count_in_asr = True
                error_metadata: dict[str, Any] = {}

                if caller is None:
                    output_text = missing_key_message or "[ERROR] Provider unavailable"
                    attempt_status = "error"
                    count_in_asr = False
                    if missing_key_code and not args.allow_mock_fallback:
                        row_errors.append(missing_key_code)
                else:
                    attempt_index = 0
                    last_error: dict[str, Any] | None = None
                    while True:
                        _await_turn()
                        try:
                            output_text, _meta = caller(
                                literal_prompt,
                                args.model,
                                args.temperature,
                            )
                            attempt_status = "ok"
                            count_in_asr = True
                            last_error = None
                            _record_attempt_time()
                            break
                        except Exception as exc:  # pragma: no cover - diagnostics path
                            _record_attempt_time()
                            classified = _classify_exception(exc)
                            last_error = classified
                            attempt_status = "error"
                            count_in_asr = False
                            if not classified.get("retryable") or attempt_index >= max_retries:
                                break
                            wait_time = 0.0
                            retry_after = classified.get("retry_after_seconds")
                            if respect_retry_after and isinstance(retry_after, (int, float)):
                                wait_time = float(retry_after)
                            elif backoff_base > 0:
                                wait_time = backoff_base * (2 ** attempt_index)
                            if wait_time > 0:
                                wait_time += random.uniform(0, 0.25)
                                time.sleep(wait_time)
                            attempt_index += 1
                            continue
                    if last_error:
                        error_metadata = last_error
                        short_message = last_error.get("short_message", "")
                        output_text = (
                            f"[ERROR] {short_message}" if short_message else "[ERROR]"
                        )
                        row_errors.append(last_error.get("error_text", ""))

                row["output_text"] = (
                    output_text if isinstance(output_text, str) else str(output_text)
                )
                row["attempt_status"] = attempt_status
                row["count_in_asr"] = bool(count_in_asr)

                if attempt_status != "ok" and error_metadata:
                    error_kind = error_metadata.get("error_kind")
                    if error_kind:
                        row["error_kind"] = error_kind
                    if error_metadata.get("http_status") is not None:
                        row["http_status"] = error_metadata["http_status"]
                    if error_metadata.get("error_code"):
                        row["error_code"] = error_metadata["error_code"]
                    retry_after_seconds = error_metadata.get("retry_after_seconds")
                    if isinstance(retry_after_seconds, (int, float)):
                        row["retry_after_ms"] = int(retry_after_seconds * 1000)

                if row_errors:
                    row["error"] = " | ".join(str(err) for err in row_errors if err)

                handle.write(json.dumps(row, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
