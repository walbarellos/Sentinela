from __future__ import annotations

import subprocess

import requests

STATUS_MARKER = "__SENTINELA_HTTP_STATUS__:"


def _should_fallback(exc: requests.RequestException) -> bool:
    text = str(exc)
    return any(
        token in text
        for token in (
            "Response ended prematurely",
            "ChunkedEncodingError",
            "IncompleteRead",
        )
    )


def _http_error(url: str, status_code: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    response.url = url
    return requests.HTTPError(f"{status_code} error while fetching {url}", response=response)


def fetch_html(session: requests.Session, url: str, *, timeout: int = 30) -> str:
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "identity",
        "Connection": "close",
    }
    try:
        response = session.get(url, timeout=timeout, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.HTTPError:
        raise
    except requests.RequestException as exc:
        if not _should_fallback(exc):
            raise

    result = subprocess.run(
        [
            "curl",
            "-sS",
            "--http1.0",
            "--location",
            "--write-out",
            f"\n{STATUS_MARKER}%{{http_code}}",
            url,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    stdout = result.stdout or ""
    if STATUS_MARKER not in stdout:
        raise requests.RequestException(
            f"curl fallback failed for {url}: {result.stderr.strip() or result.returncode}"
        )

    body, _, status_text = stdout.rpartition(f"\n{STATUS_MARKER}")
    try:
        status_code = int(status_text.strip())
    except ValueError as exc:
        raise requests.RequestException(f"invalid fallback status for {url}: {status_text}") from exc

    if status_code >= 400:
        raise _http_error(url, status_code)
    if not body.strip():
        raise requests.RequestException(f"empty response body for {url}")
    return body
