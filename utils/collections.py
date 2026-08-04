def dedupe(items: list[str]) -> list[str]:
    return list(dict.fromkeys(items))
