import re
import json
import os
import unicodedata
from collections import defaultdict
from datetime import datetime, timedelta, date, time as dtime
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.environ["BOT_TOKEN"]
USER_SHEETS = json.loads(os.environ["USER_SHEETS"])
SERVICE_ACCOUNT_INFO = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

SHEET_INGRESOS = "Ingresos"
SHEET_EGRESOS = "Egresos"
SHEET_MOVIMIENTOS = "Movimientos"
SHEET_RESUMEN = "Resumen"
SHEET_CATEGORIAS = "Categorías"

USD_TO_GTQ = 7.7
TZ = ZoneInfo("America/Guatemala")

INV_CUENTAS_DEFAULT = {"Ugly", "Binance", "Osmo", "Hapi"}
BOLSA_NORMAL = "Normal"

# Fallbacks por si falta hoja Categorías
FUENTES_ING = ["Trabajo", "Freelance", "Negocios", "Otros"]
CATEG_ING = ["Salario", "Proyecto", "Ventas", "Inversiones", "Intereses", "Préstamos", "Otros"]
METODOS = ["Efectivo", "Transferencia"]
BANCOS = ["BI", "Banrural", "Nexa", "Zigi", "GyT"]
CATEG_EGR = [
    "Agua", "Internet", "Transporte", "Comida", "Casa", "Chatarra", "Supermercado",
    "Estudios", "Mercado", "Entretenimiento", "Salud", "Ropa", "Zapatos",
    "Suscripciones", "Salidas", "Regalos", "Otros"
]
CUENTAS = ["Efectivo", "BI", "Banrural", "Nexa", "Zigi", "GyT", "Ahorro", "Préstamos"]
PERSONAS_PRESTAMO = []

# =========================
# HELPERS GENERALES
# =========================
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

def get_sheet_for_user(gc, uid: int):
    uid_str = str(uid)
    sheet_id = USER_SHEETS.get(uid_str)
    if not sheet_id:
        raise RuntimeError("Tu usuario no tiene Sheet configurado.")
    return gc.open_by_key(sheet_id)

def gs_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=scopes)
    return gspread.authorize(creds)

# =========================
# HELPERS DE CATÁLOGO
# =========================
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

def get_catalogos(context: ContextTypes.DEFAULT_TYPE):
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

def get_accounts_by_role(context: ContextTypes.DEFAULT_TYPE):
    cats = get_catalogos(context) or {}
    cuentas = cats.get("CUENTAS", CUENTAS)
    inv_accounts = [c for c in cuentas if norm_key(c) in {norm_key(x) for x in INV_CUENTAS_DEFAULT}]
    patrimonial_accounts = [c for c in cuentas if norm_key(c) in {"ahorro", "prestamos"}]
    liquid_accounts = [c for c in cuentas if c not in inv_accounts and c not in patrimonial_accounts]
    return liquid_accounts, patrimonial_accounts, inv_accounts

def get_investment_accounts_from_catalog(cuentas_catalogo: list[str]) -> list[str]:
    invset = {norm_key(x) for x in INV_CUENTAS_DEFAULT}
    return [c for c in cuentas_catalogo if norm_key(c) in invset]

# =========================
# HELPERS SHEETS RÁPIDOS
# =========================
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

# =========================
# UI
# =========================
def kb_list(items, prefix: str, cols: int = 2):
    rows, row = [], []
    for i, it in enumerate(items):
        row.append(InlineKeyboardButton(it, callback_data=f"{prefix}:{it}"))
        if (i + 1) % cols == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Cancelar", callback_data="CANCEL")])
    return InlineKeyboardMarkup(rows)

def kb_main():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Ingreso", callback_data="TYPE:ING"),
            InlineKeyboardButton("Egreso", callback_data="TYPE:EGR"),
        ],
        [InlineKeyboardButton("Movimiento", callback_data="TYPE:MOV")],
        [InlineKeyboardButton("Cancelar", callback_data="CANCEL")],
    ])

def kb_date():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Hoy", callback_data="DATE:HOY"),
            InlineKeyboardButton("Ayer", callback_data="DATE:AYER"),
        ],
        [InlineKeyboardButton("Otra fecha", callback_data="DATE:OTRA")],
        [InlineKeyboardButton("Cancelar", callback_data="CANCEL")],
    ])

def kb_confirm():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Guardar", callback_data="CONFIRM:SAVE"),
            InlineKeyboardButton("Editar", callback_data="CONFIRM:EDIT"),
        ],
        [InlineKeyboardButton("Cancelar", callback_data="CANCEL")],
    ])

def kb_mov_type():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Normal", callback_data="MVT:NORMAL"),
            InlineKeyboardButton("Ahorro", callback_data="MVT:AHORRO"),
        ],
        [
            InlineKeyboardButton("Inversión", callback_data="MVT:INVERSION"),
            InlineKeyboardButton("Préstamos", callback_data="MVT:PRESTAMO"),
        ],
        [InlineKeyboardButton("Cancelar", callback_data="CANCEL")],
    ])

def kb_mov_direction(movtype: str):
    movtype = norm_key(movtype)
    if movtype == "ahorro":
        opts = [
            [InlineKeyboardButton("Guardar", callback_data="MDIR:GUARDAR")],
            [InlineKeyboardButton("Retirar", callback_data="MDIR:RETIRAR")],
        ]
    elif movtype == "inversion":
        opts = [
            [InlineKeyboardButton("Invertir", callback_data="MDIR:INVERTIR")],
            [InlineKeyboardButton("Retirar", callback_data="MDIR:RETIRAR_INV")],
        ]
    elif movtype == "prestamos":
        opts = [
            [InlineKeyboardButton("Dar", callback_data="MDIR:DAR")],
            [InlineKeyboardButton("Cobrar", callback_data="MDIR:COBRAR")],
        ]
    else:
        opts = []
    opts.append([InlineKeyboardButton("Cancelar", callback_data="CANCEL")])
    return InlineKeyboardMarkup(opts)

# =========================
# STATE
# =========================
def st_get(context: ContextTypes.DEFAULT_TYPE):
    if "flow" not in context.user_data:
        context.user_data["flow"] = {"step": None, "data": {}}
    return context.user_data["flow"]

def st_reset(context: ContextTypes.DEFAULT_TYPE):
    context.user_data["flow"] = {"step": None, "data": {}}

# =========================
# AUTH
# =========================
def allowed(update: Update) -> bool:
    if update.message and update.message.text == "/whoami":
        return True
    uid = str(update.effective_user.id) if update.effective_user else ""
    return uid in USER_SHEETS

# =========================
# RESÚMENES DE TEXTO
# =========================
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

def is_last_day_of_month(d: date) -> bool:
    return (d + timedelta(days=1)).day == 1

async def job_resumen_semanal(context: ContextTypes.DEFAULT_TYPE):
    gc = context.application.bot_data["gc"]
    bot = context.bot
    for uid_str in USER_SHEETS.keys():
        uid = int(uid_str)
        try:
            txt = build_resumen_semana(gc, uid)
            await bot.send_message(chat_id=uid, text=txt)
        except Exception:
            pass

async def job_resumen_fin_de_mes(context: ContextTypes.DEFAULT_TYPE):
    hoy = datetime.now(TZ).date()
    if not is_last_day_of_month(hoy):
        return
    gc = context.application.bot_data["gc"]
    bot = context.bot
    for uid_str in USER_SHEETS.keys():
        uid = int(uid_str)
        try:
            txt = build_resumen_mes(gc, uid)
            await bot.send_message(chat_id=uid, text=f"Fin de mes:\n\n{txt}")
        except Exception:
            pass

# =========================
# RENDER SUMMARY
# =========================
def render_summary(data):
    if data["tipo"] == "ING":
        return (
            "Resumen:\n"
            f"Fecha: {data.get('fecha','')}\n"
            f"Fuente: {data.get('fuente','')}\n"
            f"Categoría: {data.get('categoria','')}\n"
            f"Monto: {data.get('monto','')}\n"
            f"Método: {data.get('metodo','')}\n"
            f"Banco: {data.get('banco','')}\n"
            f"Nota: {data.get('nota','')}"
        )
    elif data["tipo"] == "MOV":
        md = data.get("monto_destino", 0)
        md_txt = "(igual)" if not md else str(md)
        return (
            "Resumen movimiento:\n"
            f"Fecha: {data.get('fecha','')}\n"
            f"Tipo: {data.get('mov_type','')}\n"
            f"Bolsa sale: {data.get('bolsa_remitente','')}\n"
            f"Cuenta sale: {data.get('remitente','')}\n"
            f"Bolsa entra: {data.get('bolsa_destino','')}\n"
            f"Cuenta entra: {data.get('destino','')}\n"
            f"Persona préstamo: {data.get('persona_prestamo','')}\n"
            f"Monto sale: {data.get('monto','')}\n"
            f"Monto entra: {md_txt}\n"
            f"Nota: {data.get('nota','')}"
        )
    else:
        return (
            "Resumen:\n"
            f"Fecha: {data.get('fecha','')}\n"
            f"Categoría: {data.get('categoria','')}\n"
            f"Monto: {data.get('monto','')}\n"
            f"Método: {data.get('metodo','')}\n"
            f"Banco: {data.get('banco','')}\n"
            f"Nota: {data.get('nota','')}"
        )

# =========================
# CÁLCULOS
# =========================
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

def render_inversiones(inv_map: dict[str, float], show_zeros: bool = False) -> str:
    items = sorted(inv_map.items(), key=lambda x: x[1], reverse=True)
    if not show_zeros:
        items = [(c, v) for c, v in items if abs(v) > 1e-9]
    if not items:
        return "  - (sin inversiones aún)"
    return "\n".join([f"  - {c}: {format_money_usd(v)}" for c, v in items])

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

def render_q_breakdown(title: str, data: dict[str, float], show_zeros: bool = False) -> str:
    items = sorted(data.items(), key=lambda x: x[1], reverse=True)
    if not show_zeros:
        items = [(k, v) for k, v in items if abs(v) > 1e-9]
    lines = [title]
    if not items:
        lines.append("  - (sin datos aún)")
    else:
        for k, v in items:
            lines.append(f"  - {k}: {format_money_q(v)}")
    return "\n".join(lines)

# =========================
# COMMANDS
# =========================
async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Tu user_id es: {update.effective_user.id}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    gc = context.application.bot_data["gc"]
    sh = get_sheet_for_user(gc, update.effective_user.id)
    cats = load_catalogos(sh)
    context.user_data["catalogos"] = cats
    context.user_data["cuentas"] = cats.get("CUENTAS") or CUENTAS
    st_reset(context)
    await update.message.reply_text("¿Qué quieres registrar?", reply_markup=kb_main())

async def nuevo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)

async def cancelar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st_reset(context)
    await update.message.reply_text("Cancelado. Usa /nuevo para iniciar.")

async def resumen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    gc = context.application.bot_data["gc"]
    try:
        txt = build_resumen_mes(gc, update.effective_user.id)
        await update.message.reply_text(txt)
    except Exception as e:
        await update.message.reply_text(f"No pude generar el resumen. Error: {e}")

async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    gc = context.application.bot_data["gc"]
    try:
        sh = get_sheet_for_user(gc, update.effective_user.id)
        ws = sh.worksheet(SHEET_RESUMEN)

        cuentas = ws.col_values(1)
        saldos = ws.col_values(4)
        n = min(len(cuentas), len(saldos))
        pares = []
        for i in range(n):
            cta = (cuentas[i] or "").strip()
            sal = (saldos[i] or "").strip()
            if not cta and not sal:
                continue
            if i == 0 and cta.lower() in ("cuenta", "cuentas") and sal.lower() in ("saldo", "saldos"):
                continue
            if cta:
                pares.append((cta, sal))

        if not pares:
            await update.message.reply_text("No encontré datos en Resumen (col A y D).")
            return

        w_cta = max(len("Cuenta"), max(len(c) for c, _ in pares))
        w_sal = max(len("Saldo"), max(len(s) for _, s in pares))
        top = f"┌{'─'*(w_cta+2)}┬{'─'*(w_sal+2)}┐"
        hdr = f"│ {'Cuenta'.ljust(w_cta)} │ {'Saldo'.ljust(w_sal)} │"
        mid = f"├{'─'*(w_cta+2)}┼{'─'*(w_sal+2)}┤"
        rows = [f"│ {c.ljust(w_cta)} │ {s.ljust(w_sal)} │" for c, s in pares]
        bot = f"└{'─'*(w_cta+2)}┴{'─'*(w_sal+2)}┘"
        table = "\n".join([top, hdr, mid, *rows, bot])

        await update.message.reply_text(f"<b>Balance</b>\n<pre>{table}</pre>", parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"No pude leer la hoja 'Resumen'. Error: {e}")

async def saldos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    gc = context.application.bot_data["gc"]

    if "catalogos" not in context.user_data or "cuentas" not in context.user_data:
        sh = get_sheet_for_user(gc, update.effective_user.id)
        cats = load_catalogos(sh)
        context.user_data["catalogos"] = cats
        context.user_data["cuentas"] = cats.get("CUENTAS") or CUENTAS

    cuentas = context.user_data.get("cuentas", CUENTAS)

    try:
        saldos_map = build_saldos_dinamicos(gc, update.effective_user.id, cuentas)
        items = sorted(saldos_map.items(), key=lambda x: x[1], reverse=True)
        pares = [(c, format_money_q(v)) for c, v in items if c and abs(v) > 0.000001]

        if not pares:
            await update.message.reply_text("No hay movimientos suficientes para calcular saldos aún.")
            return

        w_cta = max(len("Cuenta"), max(len(c) for c, _ in pares))
        w_sal = max(len("Saldo"), max(len(s) for _, s in pares))
        top = f"┌{'─'*(w_cta+2)}┬{'─'*(w_sal+2)}┐"
        hdr = f"│ {'Cuenta'.ljust(w_cta)} │ {'Saldo'.rjust(w_sal)} │"
        mid = f"├{'─'*(w_cta+2)}┼{'─'*(w_sal+2)}┤"
        rows = [f"│ {c.ljust(w_cta)} │ {s.rjust(w_sal)} │" for c, s in pares]
        bot = f"└{'─'*(w_cta+2)}┴{'─'*(w_sal+2)}┘"
        table = "\n".join([top, hdr, mid, *rows, bot])

        await update.message.reply_text(f"<b>Saldos</b>\n<pre>{table}</pre>", parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"No pude calcular saldos. Error: {e}")

async def networth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    gc = context.application.bot_data["gc"]
    try:
        info = build_networth(gc, update.effective_user.id)
        txt = (
            "<b>Net Worth</b>\n\n"
            f"{render_q_breakdown('Liquidez', info['liquid_map'])}\n\n"
            f"{render_q_breakdown('Ahorro', info['ahorro_map'])}\n\n"
            f"{render_q_breakdown('Préstamos', info['prestamos_map'])}\n\n"
            f"Inversiones\n{render_inversiones(info['inv_map'])}\n\n"
            f"<b>Total patrimonial (GTQ):</b> {format_money_q(info['total_gtq'])}\n"
            f"<i>TC usado: {info['tc']}</i>"
        )
        await update.message.reply_text(txt, parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"No pude calcular net worth. Error: {e}")

async def ahorro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await networth(update, context)

# =========================
# CALLBACKS
# =========================
async def on_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return

    q = update.callback_query
    await q.answer()
    cb = q.data

    if cb == "CANCEL":
        st_reset(context)
        await q.edit_message_text("Cancelado.")
        return

    st = st_get(context)
    data = st["data"]

    if cb.startswith("TYPE:"):
        data.clear()
        data["tipo"] = cb.split(":")[1]
        st["step"] = "date"
        await q.edit_message_text("Fecha:", reply_markup=kb_date())
        return

    if cb.startswith("DATE:"):
        opt = cb.split(":")[1]
        if opt == "HOY":
            data["fecha"] = datetime.now(TZ).strftime("%Y-%m-%d")
        elif opt == "AYER":
            data["fecha"] = (datetime.now(TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            st["step"] = "wait_date"
            await q.edit_message_text("Escribe la fecha YYYY-MM-DD")
            return

        if data["tipo"] == "ING":
            st["step"] = "fuente"
            cats = get_catalogos(context)
            fuentes = cats["FUENTES_ING"] if cats else FUENTES_ING
            await q.edit_message_text("Fuente:", reply_markup=kb_list(fuentes, "SRC"))
        elif data["tipo"] == "EGR":
            st["step"] = "categoria"
            cats = get_catalogos(context)
            categ_egr = cats["CATEG_EGR"] if cats else CATEG_EGR
            await q.edit_message_text("Categoría:", reply_markup=kb_list(categ_egr, "CAT"))
        elif data["tipo"] == "MOV":
            st["step"] = "mov_type"
            await q.edit_message_text("Tipo de movimiento:", reply_markup=kb_mov_type())
        return

    if cb.startswith("MVT:"):
        mov_type = cb.split(":")[1]
        data["mov_type"] = mov_type
        if mov_type == "NORMAL":
            data["bolsa_remitente"] = BOLSA_NORMAL
            data["bolsa_destino"] = BOLSA_NORMAL
            st["step"] = "mov_from"
            liquid, _, _ = get_accounts_by_role(context)
            await q.edit_message_text("Cuenta de dónde sale:", reply_markup=kb_list(liquid, "FROM"))
        else:
            st["step"] = "mov_dir"
            await q.edit_message_text("Dirección:", reply_markup=kb_mov_direction(mov_type))
        return

    if cb.startswith("MDIR:"):
        direction = cb.split(":")[1]
        data["mov_direction"] = direction
        liquid, _, inv = get_accounts_by_role(context)
        cats = get_catalogos(context) or {}
        personas = cats.get("PERSONAS_PRESTAMO", PERSONAS_PRESTAMO)

        mov_type = data.get("mov_type")
        if mov_type == "AHORRO":
            st["step"] = "ahorro_account"
            await q.edit_message_text("Cuenta física:", reply_markup=kb_list(liquid, "ACC"))
        elif mov_type == "INVERSION":
            if direction == "INVERTIR":
                st["step"] = "inv_from"
                await q.edit_message_text("Cuenta de dónde sale (GTQ):", reply_markup=kb_list(liquid, "FROM"))
            else:
                st["step"] = "inv_account"
                await q.edit_message_text("Cuenta inversión (USD):", reply_markup=kb_list(inv, "INVACC"))
        elif mov_type == "PRESTAMO":
            if direction == "DAR":
                st["step"] = "loan_account"
                await q.edit_message_text("Cuenta de dónde sale:", reply_markup=kb_list(liquid, "ACC"))
            else:
                st["step"] = "loan_person"
                await q.edit_message_text("¿Quién te paga?", reply_markup=kb_list(personas, "PERS"))
        return

    if cb.startswith("SRC:"):
        data["fuente"] = cb.split(":", 1)[1]
        st["step"] = "categoria"
        cats = get_catalogos(context)
        categ_ing = cats["CATEG_ING"] if cats else CATEG_ING
        await q.edit_message_text("Categoría:", reply_markup=kb_list(categ_ing, "CAT"))
        return

    if cb.startswith("CAT:"):
        data["categoria"] = cb.split(":", 1)[1]
        st["step"] = "monto"
        await q.edit_message_text("Monto:")
        return

    if cb.startswith("PAY:"):
        data["metodo"] = cb.split(":", 1)[1]
        if data["metodo"] == "Transferencia":
            st["step"] = "banco"
            cats = get_catalogos(context)
            bancos = cats["BANCOS"] if cats else BANCOS
            await q.edit_message_text("Banco:", reply_markup=kb_list(bancos, "BANK"))
        else:
            data["banco"] = ""
            st["step"] = "nota"
            await q.edit_message_text("Nota (o -):")
        return

    if cb.startswith("BANK:"):
        data["banco"] = cb.split(":", 1)[1]
        st["step"] = "nota"
        await q.edit_message_text("Nota (o -):")
        return

    if cb.startswith("FROM:"):
        data["remitente"] = cb.split(":", 1)[1]
        if data.get("mov_type") == "INVERSION" and data.get("mov_direction") == "INVERTIR":
            st["step"] = "inv_to_account"
            _, _, inv = get_accounts_by_role(context)
            await q.edit_message_text("Cuenta inversión (USD):", reply_markup=kb_list(inv, "INVTOACC"))
        else:
            st["step"] = "mov_to"
            liquid, _, _ = get_accounts_by_role(context)
            await q.edit_message_text("Cuenta a dónde entra:", reply_markup=kb_list(liquid, "TO"))
        return

    if cb.startswith("TO:"):
        data["destino"] = cb.split(":", 1)[1]
        if data.get("destino") == data.get("remitente") and data.get("mov_type") == "NORMAL":
            await q.edit_message_text("Destino no puede ser igual al remitente.", reply_markup=kb_list(get_accounts_by_role(context)[0], "TO"))
            return
        st["step"] = "monto"
        await q.edit_message_text("Monto:")
        return

    if cb.startswith("ACC:"):
        account = cb.split(":", 1)[1]
        mov_type = data.get("mov_type")
        direction = data.get("mov_direction")

        if mov_type == "AHORRO":
            if direction == "GUARDAR":
                data["bolsa_remitente"] = BOLSA_NORMAL
                data["remitente"] = account
                data["bolsa_destino"] = "Ahorro"
                data["destino"] = account
            else:
                data["bolsa_remitente"] = "Ahorro"
                data["remitente"] = account
                data["bolsa_destino"] = BOLSA_NORMAL
                data["destino"] = account
            st["step"] = "monto"
            await q.edit_message_text("Monto:")
            return

        if mov_type == "PRESTAMO":
            data["loan_account"] = account
            personas = (get_catalogos(context) or {}).get("PERSONAS_PRESTAMO", PERSONAS_PRESTAMO)
            st["step"] = "loan_person"
            prompt = "¿A quién le prestas?" if direction == "DAR" else "¿Quién te paga?"
            await q.edit_message_text(prompt, reply_markup=kb_list(personas, "PERS"))
            return

    if cb.startswith("INVACC:"):
        inv_account = cb.split(":", 1)[1]
        direction = data.get("mov_direction")
        if direction == "RETIRAR_INV":
            data["bolsa_remitente"] = "Inversion"
            data["remitente"] = inv_account
            st["step"] = "inv_to"
            liquid, _, _ = get_accounts_by_role(context)
            await q.edit_message_text("Cuenta a dónde entra (GTQ):", reply_markup=kb_list(liquid, "INVTO"))
        return

    if cb.startswith("INVTOACC:"):
        data["bolsa_remitente"] = BOLSA_NORMAL
        data["bolsa_destino"] = "Inversion"
        data["destino"] = cb.split(":", 1)[1]
        st["step"] = "monto"
        await q.edit_message_text("Monto:")
        return

    if cb.startswith("INVTO:"):
        data["bolsa_destino"] = BOLSA_NORMAL
        data["destino"] = cb.split(":", 1)[1]
        st["step"] = "monto"
        await q.edit_message_text("Monto:")
        return

    if cb.startswith("PERS:"):
        person = cb.split(":", 1)[1]
        data["persona_prestamo"] = person
        mov_type = data.get("mov_type")
        direction = data.get("mov_direction")
        if mov_type == "PRESTAMO":
            account = data.get("loan_account", "")
            if direction == "DAR":
                data["bolsa_remitente"] = BOLSA_NORMAL
                data["remitente"] = account
                data["bolsa_destino"] = "Prestamo"
                data["destino"] = account
            else:
                liquid, _, _ = get_accounts_by_role(context)
                if not data.get("destino"):
                    st["step"] = "loan_collect_account"
                    await q.edit_message_text("Cuenta a dónde entra:", reply_markup=kb_list(liquid, "COLLACC"))
                    return
            st["step"] = "monto"
            await q.edit_message_text("Monto:")
        return

    if cb.startswith("COLLACC:"):
        account = cb.split(":", 1)[1]
        data["bolsa_remitente"] = "Prestamo"
        data["remitente"] = account
        data["bolsa_destino"] = BOLSA_NORMAL
        data["destino"] = account
        st["step"] = "monto"
        await q.edit_message_text("Monto:")
        return

    if cb == "CONFIRM:SAVE":
        await save_to_sheets(context, data, update.effective_user.id)
        st_reset(context)
        await q.edit_message_text("Guardado correctamente.")
        return

# =========================
# TEXT INPUT
# =========================
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return

    st = st_get(context)
    step = st["step"]
    data = st["data"]
    txt = update.message.text.strip()

    if step == "wait_date":
        data["fecha"] = txt
        cats = get_catalogos(context)

        if data["tipo"] == "ING":
            st["step"] = "fuente"
            fuentes = cats["FUENTES_ING"] if cats else FUENTES_ING
            await update.message.reply_text("Fuente:", reply_markup=kb_list(fuentes, "SRC"))
        elif data["tipo"] == "EGR":
            st["step"] = "categoria"
            categ_egr = cats["CATEG_EGR"] if cats else CATEG_EGR
            await update.message.reply_text("Categoría:", reply_markup=kb_list(categ_egr, "CAT"))
        elif data["tipo"] == "MOV":
            st["step"] = "mov_type"
            await update.message.reply_text("Tipo de movimiento:", reply_markup=kb_mov_type())
        return

    if step == "monto":
        data["monto"] = parse_money_text(txt)
        if data["tipo"] == "MOV":
            st["step"] = "monto_destino"
            await update.message.reply_text("Monto destino (si es el mismo, escribe 0):")
        else:
            st["step"] = "metodo"
            cats = get_catalogos(context)
            metodos = cats["METODOS"] if cats else METODOS
            await update.message.reply_text("Método:", reply_markup=kb_list(metodos, "PAY"))
        return

    if step == "monto_destino":
        try:
            v = parse_money_text(txt)
        except Exception:
            v = 0.0
        data["monto_destino"] = 0.0 if abs(v) < 0.000001 else v
        st["step"] = "nota"
        await update.message.reply_text("Nota (o -):")
        return

    if step == "nota":
        data["nota"] = "" if txt == "-" else txt
        st["step"] = "confirm"
        await update.message.reply_text(render_summary(data), reply_markup=kb_confirm())

# =========================
# SAVE
# =========================
async def save_to_sheets(context: ContextTypes.DEFAULT_TYPE, data, uid: int):
    gc = context.application.bot_data["gc"]
    uid_str = str(uid)
    sheet_id = USER_SHEETS.get(uid_str)
    if not sheet_id:
        raise RuntimeError("Tu usuario no tiene Sheet configurado.")

    sh = gc.open_by_key(sheet_id)

    if data["tipo"] == "ING":
        ws = sh.worksheet(SHEET_INGRESOS)
        ws.append_row([
            data["fecha"], data["fuente"], data["categoria"],
            data["monto"], data["metodo"], data["banco"], data["nota"]
        ], value_input_option="USER_ENTERED")

    elif data["tipo"] == "MOV":
        ws = sh.worksheet(SHEET_MOVIMIENTOS)
        ws.append_row([
            data["fecha"],
            data.get("bolsa_remitente", BOLSA_NORMAL),
            data.get("remitente", ""),
            data.get("bolsa_destino", BOLSA_NORMAL),
            data.get("destino", ""),
            data.get("persona_prestamo", ""),
            data["monto"],
            data.get("monto_destino", 0),
            data.get("nota", ""),
        ], value_input_option="USER_ENTERED")

    else:
        ws = sh.worksheet(SHEET_EGRESOS)
        ws.append_row([
            data["fecha"], data["categoria"],
            data["monto"], data["metodo"], data["banco"], data["nota"]
        ], value_input_option="USER_ENTERED")

# =========================
# MAIN
# =========================
def main():
    gc = gs_client()
    app = Application.builder().token(BOT_TOKEN).build()
    app.bot_data["gc"] = gc

    app.job_queue.run_daily(
        job_resumen_semanal,
        time=dtime(hour=21, minute=0, tzinfo=TZ),
        days=(6,),
        name="resumen_semanal_dom_2100",
    )
    app.job_queue.run_daily(
        job_resumen_fin_de_mes,
        time=dtime(hour=21, minute=0, tzinfo=TZ),
        name="resumen_fin_de_mes_ultimo_dia_2100",
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("nuevo", nuevo))
    app.add_handler(CommandHandler("cancelar", cancelar))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("resumen", resumen))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(CommandHandler("saldos", saldos))
    app.add_handler(CommandHandler("ahorro", ahorro))
    app.add_handler(CommandHandler("networth", networth))

    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    print("Bot finanzas encendido...")
    app.run_polling()

if __name__ == "__main__":
    main()
