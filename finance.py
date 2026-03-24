from collections import defaultdict
from datetime import datetime, timedelta

from .catalogs import canon_cuenta, col_clean, get_investment_accounts_from_catalog
from .config import (
    BOLSA_NORMAL,
    INV_CUENTAS_DEFAULT,
    SHEET_CATEGORIAS,
    SHEET_DEUDAS,
    SHEET_EGRESOS,
    SHEET_INGRESOS,
    SHEET_MOVIMIENTOS,
    TZ,
    USD_TO_GTQ,
)
from .helpers import month_range, norm_key, parse_fecha, pick, to_float, week_range
from .sheet_utils import build_header_map, cell, row_cell
from .sheets_service import get_sheet_for_user

def build_resumen_mes(gc, uid: int) -> str:
    sh = get_sheet_for_user(gc, uid)
    ws_ing = sh.worksheet(SHEET_INGRESOS)
    ws_egr = sh.worksheet(SHEET_EGRESOS)

    today = datetime.now(TZ).date()
    start, end = month_range(today)

    ing_rows = ws_ing.get_all_records()
    egr_rows = ws_egr.get_all_records()

    total_ing = 0.0
    total_egr = 0.0
    gastos_por_categoria = defaultdict(float)

    for r in ing_rows:
        f = parse_fecha(pick(r, "FECHA", "Fecha"))
        if not f or not (start <= f < end):
            continue
        total_ing += to_float(pick(r, "MONTO", "Monto"))

    for r in egr_rows:
        f = parse_fecha(pick(r, "FECHA", "Fecha"))
        if not f or not (start <= f < end):
            continue
        monto = to_float(pick(r, "MONTO", "Monto"))
        cat = str(pick(r, "CATEGORÍA", "CATEGORIA", "Categoría", "Categoria") or "").strip()
        total_egr += monto
        gastos_por_categoria[cat] += monto

    balance = total_ing - total_egr
    top = sorted(gastos_por_categoria.items(), key=lambda x: x[1], reverse=True)[:6]
    top_txt = "\n".join([f"- {c}: {v:,.2f}" for c, v in top]) if top else "- (sin egresos aún)"

    return (
        f"Resumen del mes ({start} a {end - timedelta(days=1)}):\n"
        f"Ingresos: {total_ing:,.2f}\n"
        f"Egresos: {total_egr:,.2f}\n"
        f"Balance: {balance:,.2f}\n\n"
        f"Top gastos:\n{top_txt}"
    )

def build_resumen_semana(gc, uid: int) -> str:
    sh = get_sheet_for_user(gc, uid)
    ws_ing = sh.worksheet(SHEET_INGRESOS)
    ws_egr = sh.worksheet(SHEET_EGRESOS)

    today = datetime.now(TZ).date()
    start, end = week_range(today)

    ing_rows = ws_ing.get_all_records()
    egr_rows = ws_egr.get_all_records()

    total_ing = 0.0
    total_egr = 0.0
    gastos_por_categoria = defaultdict(float)

    for r in ing_rows:
        f = parse_fecha(pick(r, "FECHA", "Fecha"))
        if not f or not (start <= f < end):
            continue
        total_ing += to_float(pick(r, "MONTO", "Monto"))

    for r in egr_rows:
        f = parse_fecha(pick(r, "FECHA", "Fecha"))
        if not f or not (start <= f < end):
            continue
        monto = to_float(pick(r, "MONTO", "Monto"))
        cat = str(pick(r, "CATEGORÍA", "CATEGORIA", "Categoría", "Categoria") or "").strip()
        total_egr += monto
        gastos_por_categoria[cat] += monto

    balance = total_ing - total_egr
    top = sorted(gastos_por_categoria.items(), key=lambda x: x[1], reverse=True)[:6]
    top_txt = "\n".join([f"- {c}: {v:,.2f}" for c, v in top]) if top else "- (sin egresos aún)"

    return (
        f"Resumen semanal ({start} a {end - timedelta(days=1)}):\n"
        f"Ingresos: {total_ing:,.2f}\n"
        f"Egresos: {total_egr:,.2f}\n"
        f"Balance: {balance:,.2f}\n\n"
        f"Top gastos:\n{top_txt}"
    )

def build_saldos_dinamicos(
    gc,
    uid: int,
    cuentas: list[str],
    *,
    inv_cuentas: set[str] = None,
    ahorro_cuenta: str = "Ahorro",
    prestamos_cuenta: str = "Préstamos",
) -> dict[str, float]:
    if inv_cuentas is None:
        inv_cuentas = INV_CUENTAS_DEFAULT

    inv_cuentas_n = {norm_key(x) for x in inv_cuentas}
    ahorro_cuenta_n = norm_key(ahorro_cuenta)
    prestamos_cuenta_n = norm_key(prestamos_cuenta)

    sh = get_sheet_for_user(gc, uid)
    ws_ing = sh.worksheet(SHEET_INGRESOS)
    ws_egr = sh.worksheet(SHEET_EGRESOS)
    ws_mov = sh.worksheet(SHEET_MOVIMIENTOS)
    ws_cat = sh.worksheet(SHEET_CATEGORIAS)

    cuentas_catalogo = col_clean(ws_cat.col_values(6))

    def is_excluded_account(acc: str) -> bool:
        k = norm_key(acc)
        return (k in inv_cuentas_n) or (k == ahorro_cuenta_n) or (k == prestamos_cuenta_n)

    saldos = defaultdict(float)

    ing_vals = ws_ing.get("A1:G")
    ing_h = build_header_map(ing_vals)
    for row in ing_vals[1:]:
        if not any((c or "").strip() for c in row):
            continue
        categoria = str(cell(row, ing_h, "CATEGORÍA", "CATEGORIA", "Categoria") or "").strip().lower()
        if categoria in {"inversiones", "prestamos"}:
            continue

        metodo = str(cell(row, ing_h, "MÉTODO", "METODO", "Metodo") or "").strip()
        banco = str(cell(row, ing_h, "BANCO", "Banco") or "").strip()
        cuenta = banco if norm_key(metodo) == "transferencia" else metodo
        cuenta = canon_cuenta(cuenta, cuentas_catalogo)
        if not cuenta or is_excluded_account(cuenta):
            continue
        saldos[cuenta] += to_float(cell(row, ing_h, "MONTO", "Monto"))

    egr_vals = ws_egr.get("A1:F")
    egr_h = build_header_map(egr_vals)
    for row in egr_vals[1:]:
        if not any((c or "").strip() for c in row):
            continue
        metodo = str(cell(row, egr_h, "MÉTODO", "METODO", "Metodo") or "").strip()
        banco = str(cell(row, egr_h, "BANCO", "Banco") or "").strip()
        cuenta = banco if norm_key(metodo) == "transferencia" else metodo
        cuenta = canon_cuenta(cuenta, cuentas_catalogo)
        if not cuenta or is_excluded_account(cuenta):
            continue
        saldos[cuenta] -= to_float(cell(row, egr_h, "MONTO", "Monto"))

    mov_vals = ws_mov.get("A1:I")
    mov_h = build_header_map(mov_vals)
    for row in mov_vals[1:]:
        if not any((c or "").strip() for c in row):
            continue
        bolsa_rem = str(cell(row, mov_h, "BOLSA_REMITENTE") or "").strip() or BOLSA_NORMAL
        rem_raw = str(cell(row, mov_h, "REMITENTE") or "").strip()
        bolsa_des = str(cell(row, mov_h, "BOLSA_DESTINO") or "").strip() or BOLSA_NORMAL
        des_raw = str(cell(row, mov_h, "DESTINO") or "").strip()
        rem = canon_cuenta(rem_raw, cuentas_catalogo)
        des = canon_cuenta(des_raw, cuentas_catalogo)
        out_amt = to_float(cell(row, mov_h, "MONTO", "Monto"))
        md = to_float(cell(row, mov_h, "MONTO_DESTINO", "Monto_destino"))
        in_amt = md if abs(md) > 1e-9 else out_amt

        if norm_key(bolsa_rem) == norm_key(BOLSA_NORMAL) and rem and not is_excluded_account(rem):
            saldos[rem] -= out_amt
        if norm_key(bolsa_des) == norm_key(BOLSA_NORMAL) and des and not is_excluded_account(des):
            saldos[des] += in_amt

    for c in cuentas:
        cc = canon_cuenta(c, cuentas_catalogo)
        if not cc or is_excluded_account(cc):
            continue
        saldos[cc] += 0.0

    return dict(saldos)

def build_networth(
    gc,
    uid: int,
    usd_to_gtq: float = None,
    inv_cuentas: set[str] = None,
    ahorro_cuenta: str = "Ahorro",
    prestamos_cuenta: str = "Préstamos",
) -> dict:
    if usd_to_gtq is None:
        usd_to_gtq = USD_TO_GTQ
    if inv_cuentas is None:
        inv_cuentas = INV_CUENTAS_DEFAULT

    inv_set = {norm_key(x) for x in inv_cuentas}
    ahorro_n = norm_key(ahorro_cuenta)
    prestamos_n = norm_key(prestamos_cuenta)

    sh = get_sheet_for_user(gc, uid)
    ws_ing = sh.worksheet(SHEET_INGRESOS)
    ws_mov = sh.worksheet(SHEET_MOVIMIENTOS)
    ws_cat = sh.worksheet(SHEET_CATEGORIAS)

    cuentas_catalogo = col_clean(ws_cat.col_values(6))
    liquid_accounts = [c for c in cuentas_catalogo if norm_key(c) not in inv_set | {ahorro_n, prestamos_n}]
    liquid_map = build_saldos_dinamicos(gc, uid, liquid_accounts)

    ahorro_map = defaultdict(float)
    prestamos_map = defaultdict(float)
    inv_map = defaultdict(float)

    ing_vals = ws_ing.get("A1:G")
    ing_h = build_header_map(ing_vals)
    for row in ing_vals[1:]:
        if not any((c or "").strip() for c in row):
            continue

        categoria = str(cell(row, ing_h, "CATEGORÍA", "CATEGORIA", "Categoria") or "").strip().lower()
        monto = to_float(cell(row, ing_h, "MONTO", "Monto"))
        metodo = canon_cuenta(str(cell(row, ing_h, "MÉTODO", "METODO", "Metodo") or "").strip(), cuentas_catalogo)

        if categoria == "inversiones" and norm_key(metodo) in inv_set:
            inv_map[metodo] += monto
        elif categoria == "prestamos":
            prestamos_map["General"] += monto

    mov_vals = ws_mov.get("A1:I")
    mov_h = build_header_map(mov_vals)
    for row in mov_vals[1:]:
        if not any((c or "").strip() for c in row):
            continue

        bolsa_rem = str(cell(row, mov_h, "BOLSA_REMITENTE") or "").strip() or BOLSA_NORMAL
        rem = canon_cuenta(str(cell(row, mov_h, "REMITENTE") or "").strip(), cuentas_catalogo)
        bolsa_des = str(cell(row, mov_h, "BOLSA_DESTINO") or "").strip() or BOLSA_NORMAL
        des = canon_cuenta(str(cell(row, mov_h, "DESTINO") or "").strip(), cuentas_catalogo)
        persona = str(cell(row, mov_h, "PERSONA_PRESTAMO", "PERSONAS_PRESTAMO", "PERSONA PRESTAMO") or "").strip() or "General"
        monto = to_float(cell(row, mov_h, "MONTO", "Monto"))
        monto_dest = to_float(cell(row, mov_h, "MONTO_DESTINO", "Monto_destino"))
        entrada = monto_dest if abs(monto_dest) > 1e-9 else monto

        br = norm_key(bolsa_rem)
        bd = norm_key(bolsa_des)

        if bd == ahorro_n:
            ahorro_map[des or "Sin cuenta"] += entrada
        if br == ahorro_n:
            ahorro_map[rem or "Sin cuenta"] -= monto

        if bd == prestamos_n:
            prestamos_map[persona] += entrada
        if br == prestamos_n:
            prestamos_map[persona] -= monto

        if bd == norm_key("Inversion"):
            inv_map[des or "Sin cuenta"] += entrada
        if br == norm_key("Inversion"):
            inv_map[rem or "Sin cuenta"] -= monto

    for c in get_investment_accounts_from_catalog(cuentas_catalogo):
        inv_map[c] += 0.0

    liquidez_gtq = sum(liquid_map.values())
    ahorro_gtq = sum(ahorro_map.values())
    prestamos_gtq = sum(prestamos_map.values())
    inv_total_usd = sum(inv_map.values())
    total_gtq = liquidez_gtq + ahorro_gtq + prestamos_gtq + (inv_total_usd * usd_to_gtq)

    return {
        "liquid_map": dict(liquid_map),
        "liquidez_gtq": liquidez_gtq,
        "ahorro_map": dict(ahorro_map),
        "ahorro_gtq": ahorro_gtq,
        "prestamos_map": dict(prestamos_map),
        "prestamos_gtq": prestamos_gtq,
        "inv_map": dict(inv_map),
        "inv_total_usd": inv_total_usd,
        "total_gtq": total_gtq,
        "tc": usd_to_gtq,
    }

def build_deudas(gc, uid: int) -> list[dict]:
    sh = get_sheet_for_user(gc, uid)
    ws = sh.worksheet(SHEET_DEUDAS)

    vals = ws.get("A1:I")
    hmap = build_header_map(vals)

    deudas = []

    for sheet_row_num, row in enumerate(vals[1:], start=2):
        if not any((c or "").strip() for c in row):
            continue

        nombre = str(row_cell(row, hmap, "NOMBRE") or "").strip()
        acreedor = str(row_cell(row, hmap, "A QUIÉN LE DEBO", "A QUIEN LE DEBO") or "").strip()
        fecha_pago = str(row_cell(row, hmap, "FECHA DE PAGO") or "").strip()
        cuota = to_float(row_cell(row, hmap, "CUOTA"))
        meses = int(to_float(row_cell(row, hmap, "MESES")))
        pagados = int(to_float(row_cell(row, hmap, "PAGADOS")))
        pendientes = int(to_float(row_cell(row, hmap, "PENDIENTES")))
        saldo = to_float(row_cell(row, hmap, "SALDO"))
        estado = str(row_cell(row, hmap, "ESTADO") or "").strip()

        if pendientes <= 0 and meses > pagados:
            pendientes = max(meses - pagados, 0)
        if saldo <= 0 and cuota > 0 and pendientes > 0:
            saldo = cuota * pendientes
        if not estado:
            estado = "Pagada" if pendientes <= 0 else "Activa"

        deudas.append({
            "row": sheet_row_num,
            "nombre": nombre,
            "acreedor": acreedor,
            "fecha_pago": fecha_pago,
            "cuota": cuota,
            "meses": meses,
            "pagados": pagados,
            "pendientes": pendientes,
            "saldo": saldo,
            "estado": estado,
        })

    return deudas

def build_total_deudas(gc, uid: int) -> float:
    deudas = build_deudas(gc, uid)
    return sum(d["saldo"] for d in deudas if d["estado"].lower() == "activa")
