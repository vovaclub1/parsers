from pathlib import Path


KNOWN_LEAKED_FRAGMENTS = (
    "jGxDRQQi:iqTupHAT",
    "7148d2c979f97de424b7e4ea249776947ed8c307731493b9ae593a895d17c141",
)


def test_known_proxy_and_relay_credentials_are_not_in_repository():
    root = Path(__file__).resolve().parents[1]
    offenders = []
    for path in root.rglob("*"):
        if (
            not path.is_file()
            or ".git" in path.parts
            or "__pycache__" in path.parts
            or ".venv" in path.parts
            or path.suffix in {".pyc", ".pyo"}
        ):
            continue
        if path == Path(__file__).resolve():
            continue
        try:
            text = path.read_text(errors="ignore")
        except OSError:
            continue
        for fragment in KNOWN_LEAKED_FRAGMENTS:
            if fragment in text:
                offenders.append(str(path.relative_to(root)))
                break

    assert offenders == []
