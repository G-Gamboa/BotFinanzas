import re
import unicodedata
from datetime import date, datetime, timedelta

def norm(s: str) -> str:
    s = str(s).strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return s

def norm_key(s: str) -> str:
    return norm(s)

def pick(row: dict, *candidates: str):
    nrow = {norm(k): v for k, v in row.items()}
    for c in candidates:
        key = norm(c)
        if key in nrow:
            return nrow[key]
    return None

def to_float(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()
    s = re.sub(r"[^0-9.,\-]", "", s)
    if not s:
        return 0.0

    if "." in s and "," in s:
        s = s.replace(".", "")
        s = s.replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return 0.0

def parse_money_text(txt: str) -> float:
    raw = re.sub(r"[^0-9.,\-]", "", txt.strip())
    if "." in raw and "," in raw:
        raw = raw.replace(".", "")
        raw = raw.replace(",", ".")
    elif "," in raw:
        raw = raw.replace(",", ".")
    return float(raw) if raw else 0.0

def parse_fecha(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    s = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None

def ensure_fecha_text(value: str) -> str:
    f = parse_fecha(value)
    if not f:
        raise ValueError("Fecha inválida. Usa YYYY-MM-DD.")
    return f.strftime("%Y-%m-%d")

def is_positive_amount(value) -> bool:
    try:
        return float(value) > 0
    except Exception:
        return False

def parse_positive_int_text(txt: str) -> int:
    val = int(float(txt.strip()))
    if val < 0:
        raise ValueError("Debe ser un número entero igual o mayor a 0.")
    return val

def month_range(today: date):
    start = today.replace(day=1)
    if start.month == 12:
        next_month = date(start.year + 1, 1, 1)
    else:
        next_month = date(start.year, start.month + 1, 1)
    return start, next_month

def week_range(today: date):
    start = today - timedelta(days=today.weekday())
    return start, start + timedelta(days=7)

def format_money_q(value: float) -> str:
    return f"Q {value:,.2f}"

def format_money_usd(value: float) -> str:
    return f"${value:,.2f}"

def cuentas_permitidas_egreso(cuentas, inv_accounts):
    inv_set = {c.strip().lower() for c in inv_accounts}
    return [c for c in cuentas if c.strip().lower() not in inv_set]