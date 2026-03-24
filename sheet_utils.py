from .helpers import norm_key

def build_header_map(values: list[list[str]]) -> dict[str, int]:
    if not values:
        return {}
    header = values[0]
    return {norm_key(h): i for i, h in enumerate(header) if (h or "").strip()}

def cell(row: list, hmap: dict[str, int], *names: str):
    for n in names:
        k = norm_key(n)
        if k in hmap:
            idx = hmap[k]
            if idx < len(row):
                return row[idx]
    return ""

def row_cell(row: list, hmap: dict[str, int], *names: str):
    return cell(row, hmap, *names)
