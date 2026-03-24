from config import CUENTAS, INV_CUENTAS_DEFAULT

from helpers import norm_key

def col_clean(values):
    out = []
    for v in values[1:]:
        v = (v or "").strip()
        if v:
            out.append(v)
    return out

def sort_special(items: list[str], first: str | None = None, last: str | None = None) -> list[str]:
    clean = [(x or "").strip() for x in items if (x or "").strip()]
    seen = set()
    clean2 = []
    for x in clean:
        k = x.lower()
        if k not in seen:
            seen.add(k)
            clean2.append(x)

    first_item = None
    last_item = None

    if first:
        for x in clean2:
            if x.lower() == first.lower():
                first_item = x
                break
        clean2 = [x for x in clean2 if x.lower() != first.lower()]

    if last:
        for x in clean2:
            if x.lower() == last.lower():
                last_item = x
                break
        clean2 = [x for x in clean2 if x.lower() != last.lower()]

    clean2_sorted = sorted(clean2, key=lambda s: s.lower())

    out = []
    if first_item:
        out.append(first_item)
    out.extend(clean2_sorted)
    if last_item:
        out.append(last_item)
    return out

def load_catalogos(sh):
    from .config import SHEET_CATEGORIAS
    ws = sh.worksheet(SHEET_CATEGORIAS)

    fuentes_ing = col_clean(ws.col_values(1))
    categ_ing = col_clean(ws.col_values(2))
    metodos = col_clean(ws.col_values(3))
    bancos = col_clean(ws.col_values(4))
    categ_egr = col_clean(ws.col_values(5))
    cuentas = col_clean(ws.col_values(6))
    personas = col_clean(ws.col_values(7))

    return {
        "FUENTES_ING": sort_special(fuentes_ing, last="Otros"),
        "CATEG_ING": sort_special(categ_ing, last="Otros"),
        "METODOS": sort_special(metodos, last="Otros"),
        "BANCOS": sort_special(bancos, last="Otros"),
        "CATEG_EGR": sort_special(categ_egr, last="Otros"),
        "CUENTAS": [x for x in sort_special(cuentas, first="Efectivo") if x.lower() != "otros"],
        "PERSONAS_PRESTAMO": sort_special(personas, last="Otros"),
    }

def get_catalogos(context):
    cats = context.user_data.get("catalogos")
    if isinstance(cats, dict) and cats:
        return cats
    return None

def canon_cuenta(raw: str, cuentas_catalogo: list[str]) -> str:
    r = (raw or "").strip()
    if not r:
        return ""
    mapa = {norm_key(c): c for c in (cuentas_catalogo or []) if (c or "").strip()}
    return mapa.get(norm_key(r), r)

def get_accounts_by_role(context):
    cats = get_catalogos(context) or {}
    cuentas = cats.get("CUENTAS", CUENTAS)
    inv_accounts = [c for c in cuentas if norm_key(c) in {norm_key(x) for x in INV_CUENTAS_DEFAULT}]
    patrimonial_accounts = [c for c in cuentas if norm_key(c) in {"ahorro", "prestamos"}]
    liquid_accounts = [c for c in cuentas if c not in inv_accounts and c not in patrimonial_accounts]
    return liquid_accounts, patrimonial_accounts, inv_accounts

def get_investment_accounts_from_catalog(cuentas_catalogo: list[str]) -> list[str]:
    invset = {norm_key(x) for x in INV_CUENTAS_DEFAULT}
    return [c for c in cuentas_catalogo if norm_key(c) in invset]
