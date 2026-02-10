import re
import json
import os
from datetime import datetime, timedelta
from datetime import date, time as dtime
from zoneinfo import ZoneInfo
from collections import defaultdict
import unicodedata
from datetime import date as _date

import gspread
from google.oauth2.service_account import Credentials
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ContextTypes, filters
)

# =========================
# CONFIG (RAILWAY)
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


# =========================
# CATÁLOGOS (BOTONES)
# =========================
FUENTES_ING = ["Trabajo", "Freelance", "Negocios"]
CATEG_ING = ["Salario", "Proyecto", "Inversiones", "Ventas", "Otros"]
METODOS = ["Efectivo", "Transferencia"]
BANCOS = ["BI", "Banrural", "Nexa", "Zigi","GyT"]

CATEG_EGR = [
    "Agua", "Internet", "Transporte", "Comida","Casa", "Chatarra", "Supermercado","Estudios",
    "Mercado", "Entretenimiento", "Salud", "Ahorro", "Ropa", "Zapatos","Suscripciones","Salidas","Regalos","Otros"
]

CUENTAS = ["Efectivo", "BI", "Banrural", "Nexa", "Zigi", "GyT"]

# =========================
# HELPERS UI
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
            InlineKeyboardButton("Egreso", callback_data="TYPE:EGR")
        ],
        [InlineKeyboardButton("Movimiento", callback_data="TYPE:MOV")],
        [InlineKeyboardButton("Cancelar", callback_data="CANCEL")]
    ])


def kb_date():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Hoy", callback_data="DATE:HOY"),
            InlineKeyboardButton("Ayer", callback_data="DATE:AYER")
        ],
        [InlineKeyboardButton("Otra fecha", callback_data="DATE:OTRA")],
        [InlineKeyboardButton("Cancelar", callback_data="CANCEL")]
    ])

def kb_confirm():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Guardar", callback_data="CONFIRM:SAVE"),
            InlineKeyboardButton("Editar", callback_data="CONFIRM:EDIT")
        ],
        [InlineKeyboardButton("Cancelar", callback_data="CANCEL")]
    ])

TZ = ZoneInfo("America/Guatemala")

def get_sheet_for_user(gc, uid: int):
    uid_str = str(uid)
    sheet_id = USER_SHEETS.get(uid_str)
    if not sheet_id:
        raise RuntimeError("Tu usuario no tiene Sheet configurado.")
    return gc.open_by_key(sheet_id)

def col_clean(values):
    # values incluye encabezado en fila 1, por eso usamos [1:]
    out = []
    for v in values[1:]:
        v = (v or "").strip()
        if v:
            out.append(v)
    return out

def sort_special(items: list[str], first: str | None = None, last: str | None = None) -> list[str]:
    clean = [(x or "").strip() for x in items if (x or "").strip()]
    # quitar duplicados manteniendo orden
    seen = set()
    clean2 = []
    for x in clean:
        k = x.lower()
        if k not in seen:
            seen.add(k)
            clean2.append(x)

    # separar first/last
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

    fuentes_ing = col_clean(ws.col_values(1))   # A
    categ_ing   = col_clean(ws.col_values(2))   # B
    metodos     = col_clean(ws.col_values(3))   # C
    bancos      = col_clean(ws.col_values(4))   # D
    categ_egr   = col_clean(ws.col_values(5))   # E
    cuentas     = col_clean(ws.col_values(6))   # F 

    return {
        "FUENTES_ING": sort_special(fuentes_ing, last="Otros"),
        "CATEG_ING":   sort_special(categ_ing,   last="Otros"),
        "METODOS":     sort_special(metodos,     last="Otros"),
        "BANCOS":      sort_special(bancos,      last="Otros"),
        "CATEG_EGR":   sort_special(categ_egr,   last="Otros"),
       "CUENTAS": [x for x in sort_special(cuentas, first="Efectivo") if x.lower() != "otros"], 
    }


def sort_with_priorities(items):
    def key(x):
        v = x.strip().lower()

        if v == "efectivo":
            return (0, "")      
        if v == "otros":
            return (2, "")      

        return (1, v)          

    return sorted(items, key=key)



def build_cuentas_from_catalogos(cats: dict) -> list[str]:
    """
    Construye la lista de CUENTAS a partir de:
    - Bancos
    - Métodos (excepto 'Transferencia')
    """
    cuentas = set()

    # Bancos siempre cuentan
    for b in cats.get("BANCOS", []):
        if b:
            cuentas.add(b)

    # Métodos, excepto Transferencia
    for m in cats.get("METODOS", []):
        if m and m.lower() != "transferencia":
            cuentas.add(m)

    # Orden alfabético para UX consistente
    return sorted(cuentas)


def get_catalogos(context: ContextTypes.DEFAULT_TYPE):
    """
    Devuelve el diccionario de catálogos ya cargado en context.user_data.
    Si no existe (o está vacío), devuelve None.
    """
    cats = context.user_data.get("catalogos")
    if isinstance(cats, dict) and cats:
        return cats
    return None

def norm_key(s: str) -> str:
    return (s or "").strip().lower()

def canon_cuenta(raw: str, cuentas_catalogo: list[str]) -> str:
    r = (raw or "").strip()
    if not r:
        return ""

    mapa = {norm_key(c): c for c in (cuentas_catalogo or []) if (c or "").strip()}
    return mapa.get(norm_key(r), r)  # si no existe, deja el original

def cuenta_from_ing_egr(row: dict, cuentas_catalogo: list[str]) -> str:
    metodo = str(pick(row, "MÉTODO","METODO","Metodo","Método") or "").strip()
    banco  = str(pick(row, "BANCO","Banco") or "").strip()

    if norm_key(metodo) == "transferencia":
        return canon_cuenta(banco, cuentas_catalogo)  # ✅ banco real
    return canon_cuenta(metodo, cuentas_catalogo)     # ✅ método como cuenta


def parse_fecha(value):
    if value is None:
        return None

    # Si ya viene como date/datetime (a veces pasa)
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, _date):
        return value

    s = str(value).strip()

    # intenta formatos comunes
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    return None

def to_float(value) -> float:
    if value is None:
        return 0.0

    # Si Sheets ya devuelve número (a veces pasa)
    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip()

    # Elimina moneda (Q), espacios y cualquier cosa rara
    # Deja solo dígitos, punto, coma y signo menos
    s = re.sub(r"[^0-9.,\-]", "", s)

    if not s:
        return 0.0

    # Formato Guatemala: 7.000,00
    if "." in s and "," in s:
        s = s.replace(".", "")   # quita separadores de miles
        s = s.replace(",", ".")  # coma decimal → punto

    # Otros casos: solo coma → decimal
    elif "," in s:
        s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return 0.0


def month_range(today: date):
    start = today.replace(day=1)
    # siguiente mes:
    if start.month == 12:
        next_month = date(start.year + 1, 1, 1)
    else:
        next_month = date(start.year, start.month + 1, 1)
    end = next_month  # exclusivo
    return start, end

def week_range(today: date):
    # semana lunes-domingo
    start = today - timedelta(days=today.weekday())
    end = start + timedelta(days=7)
    return start, end

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
    total_ahorro = 0.0
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
        if cat.lower() == "ahorro":
            total_ahorro += monto
        else:
            gastos_por_categoria[cat] += monto

    balance = total_ing - total_egr
    top = sorted(gastos_por_categoria.items(), key=lambda x: x[1], reverse=True)[:6]
    top_txt = "\n".join([f"- {c}: {v:,.2f}" for c, v in top]) if top else "- (sin egresos aún)"

    return (
        f"Resumen del mes ({start} a {end - timedelta(days=1)}):\n"
        f"Ingresos: {total_ing:,.2f}\n"
        f"Egresos: {total_egr:,.2f}\n"
        f"Ahorro (cat. Ahorro): {total_ahorro:,.2f}\n"
        f"Balance: {balance:,.2f}\n\n"
        f"Top gastos (sin Ahorro):\n{top_txt}"
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
        if cat.lower() != "ahorro":
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

from datetime import date, time as dtime

async def job_resumen_semanal(context: ContextTypes.DEFAULT_TYPE):
    gc = context.application.bot_data["gc"]
    bot = context.bot

    for uid_str in USER_SHEETS.keys():
        uid = int(uid_str)
        try:
            txt = build_resumen_semana(gc, uid)
            await bot.send_message(chat_id=uid, text=txt)
        except Exception:
            # si el usuario nunca inició chat con el bot o lo bloqueó, falla; lo ignoramos
            pass


def is_last_day_of_month(d: date) -> bool:
    # Si mañana es día 1, hoy es el último día del mes
    return (d + timedelta(days=1)).day == 1


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
            f"Remitente: {data.get('remitente','')}\n"
            f"Destino: {data.get('destino','')}\n"
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
    

def norm(s: str) -> str:
    s = str(s).strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return s

def pick(row: dict, *candidates: str):
    # Busca por nombre normalizado (sin tildes, sin espacios, lowercase)
    nrow = {norm(k): v for k, v in row.items()}
    for c in candidates:
        key = norm(c)
        if key in nrow:
            return nrow[key]
    return None

# =========================
# STATE
# =========================
def st_get(context: ContextTypes.DEFAULT_TYPE):
    if "flow" not in context.user_data:
        context.user_data["flow"] = {"step": None, "data": {}}
    return context.user_data["flow"]

def st_reset(context: ContextTypes.DEFAULT_TYPE):
    context.user_data["flow"] = {"step": None, "data": {}}

def allowed(update: Update) -> bool:
    # permitir /whoami a cualquiera
    if update.message and update.message.text == "/whoami":
        return True

    uid = str(update.effective_user.id) if update.effective_user else ""
    return uid in USER_SHEETS


# =========================
# GOOGLE SHEETS
# =========================
def gs_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        SERVICE_ACCOUNT_INFO,
        scopes=scopes
    )
    return gspread.authorize(creds)

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
    context.user_data["cuentas"] = cats.get("CUENTAS") or build_cuentas_from_catalogos(cats)


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

        cuentas = ws.col_values(1)  # A
        saldos  = ws.col_values(4)  # D

        n = min(len(cuentas), len(saldos))

        pares = []
        for i in range(n):
            cta = (cuentas[i] or "").strip()
            sal = (saldos[i] or "").strip()

            if not cta and not sal:
                continue

            # saltar encabezados típicos
            if i == 0 and cta.lower() in ("cuenta", "cuentas") and sal.lower() in ("saldo", "saldos"):
                continue

            if cta:
                pares.append((cta, sal))

        if not pares:
            await update.message.reply_text("No encontré datos en Resumen (col A y D).")
            return

        # ancho de columnas (con mínimos para que se vea bonito)
        w_cta = max(len("Cuenta"), max(len(c) for c, _ in pares))
        w_sal = max(len("Saldo"),  max(len(s) for _, s in pares))

        top = f"┌{'─'*(w_cta+2)}┬{'─'*(w_sal+2)}┐"
        hdr = f"│ {'Cuenta'.ljust(w_cta)} │ {'Saldo'.ljust(w_sal)} │"
        mid = f"├{'─'*(w_cta+2)}┼{'─'*(w_sal+2)}┤"
        rows = [
            f"│ {c.ljust(w_cta)} │ {s.ljust(w_sal)} │"
            for c, s in pares
        ]
        bot = f"└{'─'*(w_cta+2)}┴{'─'*(w_sal+2)}┘"

        table = "\n".join([top, hdr, mid, *rows, bot])

        await update.message.reply_text(
            f"<b>Balance</b>\n<pre>{table}</pre>",
            parse_mode="HTML"
        )

    except Exception as e:
        await update.message.reply_text(f"No pude leer la hoja 'Resumen'. Error: {e}")


def resolve_cuenta_from_row(row: dict, cats: dict | None, tipo: str) -> str:
    """
    Determina a qué 'cuenta' impacta una fila de Ingresos/Egresos.
    tipo: 'ING' o 'EGR'
    """
    metodo = str(pick(row, "MÉTODO", "METODO", "Metodo", "Método") or "").strip()
    banco  = str(pick(row, "BANCO", "Banco") or "").strip()

    # Normaliza
    m_low = metodo.lower()

    if m_low == "transferencia":
        return banco or "Transferencia"
    # Efectivo y wallets/métodos cuentan como cuenta en opción 1
    return metodo or "(sin método)"

def format_money_q(value: float) -> str:
    # Formato estilo Q con miles y 2 decimales (salida visual)
    # Ej: 1234.5 -> Q 1,234.50
    return f"Q {value:,.2f}"

from collections import defaultdict

def build_saldos_dinamicos(
    gc,
    uid: int,
    cuentas: list[str],
    *,
    inv_cuentas: set[str] = None,
    ahorro_cuenta: str = "Ahorro",
) -> dict[str, float]:
    """
    Saldos líquidos por cuenta (GTQ):
    - Incluye: Efectivo, Bancos, etc.
    - Excluye: Ahorro y cuentas de inversión (Ugly/Binance/Osmo/Hapi)
    - Ingresos/Egresos:
        - si MÉTODO = Transferencia -> cuenta = BANCO (GTQ)
        - si no -> cuenta = MÉTODO
        - Excluye categoría "Inversiones" en Ingresos (porque eso va a inversiones)
    - Movimientos:
        - Sale del REMITENTE con MONTO
        - Entra al DESTINO con MONTO_DESTINO si existe (>0) sino MONTO
        - (Respeta tu convención de monedas; para saldos líquidos solo aplica
           cuando el destino/remitente es cuenta GTQ)
    Optimización:
    - Evita get_all_records(); usa ws.get("A1:...") con mapeo de headers.
    """

    if inv_cuentas is None:
        inv_cuentas = {"Ugly", "Binance", "Osmo", "Hapi"}

    inv_cuentas_n = {norm_key(x) for x in inv_cuentas}
    ahorro_cuenta_n = norm_key(ahorro_cuenta)

    sh = get_sheet_for_user(gc, uid)
    ws_ing = sh.worksheet(SHEET_INGRESOS)
    ws_egr = sh.worksheet(SHEET_EGRESOS)
    ws_mov = sh.worksheet(SHEET_MOVIMIENTOS)
    ws_cat = sh.worksheet(SHEET_CATEGORIAS)

    # Catálogo oficial de CUENTAS (col F)
    cuentas_catalogo = col_clean(ws_cat.col_values(6))

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

    def is_excluded_account(acc: str) -> bool:
        k = norm_key(acc)
        return (k == ahorro_cuenta_n) or (k in inv_cuentas_n)

    def resolve_cuenta_ing_egr(row: list, hmap: dict[str, int]) -> str:
        """
        Método/Transferencia/Banco -> cuenta canónica.
        """
        metodo = str(cell(row, hmap, "MÉTODO", "METODO", "Metodo") or "").strip()
        banco  = str(cell(row, hmap, "BANCO", "Banco") or "").strip()

        if norm_key(metodo) == "transferencia":
            cuenta = banco
        else:
            cuenta = metodo

        cuenta = canon_cuenta(cuenta, cuentas_catalogo)
        return cuenta

    def mov_monto_origen(row: list, hmap: dict[str, int]) -> float:
        return to_float(cell(row, hmap, "MONTO", "Monto"))

    def mov_monto_destino(row: list, hmap: dict[str, int]) -> float:
        md = to_float(cell(row, hmap, "MONTO_DESTINO", "Monto_destino"))
        if abs(md) > 1e-9:
            return md
        return mov_monto_origen(row, hmap)

    saldos = defaultdict(float)

    # =========================
    # 1) INGRESOS (GTQ líquidos)
    # =========================
    ing_vals = ws_ing.get("A1:G")
    ing_h = build_header_map(ing_vals)

    for row in ing_vals[1:]:
        if not any((c or "").strip() for c in row):
            continue

        categoria = str(cell(row, ing_h, "CATEGORÍA", "CATEGORIA", "Categoria") or "").strip().lower()
        if categoria == "inversiones":
            continue  # no entra a saldos líquidos

        cuenta = resolve_cuenta_ing_egr(row, ing_h)
        if not cuenta:
            continue

        # Excluir Ahorro e inversiones como cuentas dentro de /saldos
        if is_excluded_account(cuenta):
            continue

        monto = to_float(cell(row, ing_h, "MONTO", "Monto"))
        saldos[cuenta] += monto

    # =========================
    # 2) EGRESOS (GTQ líquidos)
    # =========================
    egr_vals = ws_egr.get("A1:F")
    egr_h = build_header_map(egr_vals)

    for row in egr_vals[1:]:
        if not any((c or "").strip() for c in row):
            continue

        # Ya NO usas categoría Ahorro aquí, pero igual no estorba tener el filtro si aparece:
        categoria = str(cell(row, egr_h, "CATEGORÍA", "CATEGORIA", "Categoria") or "").strip().lower()
        if categoria == "ahorro":
            continue

        cuenta = resolve_cuenta_ing_egr(row, egr_h)
        if not cuenta:
            continue

        if is_excluded_account(cuenta):
            continue

        monto = to_float(cell(row, egr_h, "MONTO", "Monto"))
        saldos[cuenta] -= monto

    # =========================
    # 3) MOVIMIENTOS (GTQ líquidos)
    # =========================
    mov_vals = ws_mov.get("A1:G")
    mov_h = build_header_map(mov_vals)

    for row in mov_vals[1:]:
        if not any((c or "").strip() for c in row):
            continue

        rem_raw = str(cell(row, mov_h, "REMITENTE", "Remitente") or "").strip()
        des_raw = str(cell(row, mov_h, "DESTINO", "Destino") or "").strip()
        if not rem_raw or not des_raw:
            continue

        rem = canon_cuenta(rem_raw, cuentas_catalogo)
        des = canon_cuenta(des_raw, cuentas_catalogo)
        if not rem or not des:
            continue

        # Tu convención:
        # - salida del remitente = MONTO
        # - entrada del destino = MONTO_DESTINO (si existe) sino MONTO
        out_amt = mov_monto_origen(row, mov_h)
        in_amt  = mov_monto_destino(row, mov_h)

        # Aplicar solo a cuentas líquidas (no ahorro ni inversiones)
        if not is_excluded_account(rem):
            saldos[rem] -= out_amt

        if not is_excluded_account(des):
            saldos[des] += in_amt

    # =========================
    # 4) Asegurar cuentas del catálogo (0) para que existan
    # =========================
    for c in cuentas:
        cc = canon_cuenta(c, cuentas_catalogo)
        if not cc or is_excluded_account(cc):
            continue
        saldos[cc] += 0.0

    return dict(saldos)



async def ahorro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return

    gc = context.application.bot_data["gc"]

    try:
        ahorro_gtq, inv_usd, total_gtq = build_ahorro_inversiones(
            gc, update.effective_user.id
        )

        msg = (
            "<b>Ahorro / Inversiones</b>\n"
            f"- Ahorro acumulado: {format_money_q(ahorro_gtq)}\n"
            f"- Inversiones acumuladas: ${inv_usd:,.2f} USD\n"
            f"\n<b>Total patrimonial (GTQ):</b> {format_money_q(total_gtq)}\n"
            f"<i>TC usado: {USD_TO_GTQ}</i>"
        )

        await update.message.reply_text(msg, parse_mode="HTML")

    except Exception as e:
        await update.message.reply_text(f"No pude calcular ahorro. Error: {e}")


def build_ahorro_inversiones(
    gc,
    uid: int,
    usd_to_gtq: float = None,
    inv_cuentas: set[str] = None,
    ahorro_cuenta: str = "Ahorro",
) -> tuple[float, float, float]:
    """
    Saldo actual:
    - ahorro_gtq: SOLO desde MOVIMIENTOS (GTQ)
    - inversiones_usd: INGRESOS categoría "Inversiones" (USD) + MOVIMIENTOS (USD)
    - total_gtq = ahorro_gtq + inversiones_usd * usd_to_gtq

    Convención de Movimientos:
    - Salida del remitente: MONTO
    - Entrada al destino: MONTO_DESTINO (si existe y >0), si no MONTO
    - Si remitente es inversión -> MONTO está en USD
    - Si destino es inversión -> MONTO_DESTINO está en USD
    - Si destino es GTQ -> MONTO_DESTINO está en GTQ
    """

    if usd_to_gtq is None:
        usd_to_gtq = USD_TO_GTQ

    if inv_cuentas is None:
        inv_cuentas = {"Ugly", "Binance", "Osmo", "Hapi"}

    inv_cuentas_n = {norm_key(x) for x in inv_cuentas}
    ahorro_cuenta_n = norm_key(ahorro_cuenta)

    sh = get_sheet_for_user(gc, uid)
    ws_ing = sh.worksheet(SHEET_INGRESOS)
    ws_mov = sh.worksheet(SHEET_MOVIMIENTOS)
    ws_cat = sh.worksheet(SHEET_CATEGORIAS)

    # Catálogo canónico de cuentas (col F)
    cuentas_catalogo = col_clean(ws_cat.col_values(6))

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

    ahorro_gtq = 0.0
    inversiones_usd = 0.0

    # =========================
    # 1) Inversiones base desde INGRESOS (USD)
    # =========================
    ing_vals = ws_ing.get("A1:G")
    ing_h = build_header_map(ing_vals)

    for row in ing_vals[1:]:
        if not any((c or "").strip() for c in row):
            continue
        categoria = str(cell(row, ing_h, "CATEGORÍA", "CATEGORIA", "Categoria") or "").strip().lower()
        if categoria == "inversiones":
            inversiones_usd += to_float(cell(row, ing_h, "MONTO", "Monto"))

    # =========================
    # 2) Movimientos: saldo actual
    # =========================
    mov_vals = ws_mov.get("A1:G")  # por si existe MONTO_DESTINO
    mov_h = build_header_map(mov_vals)

    for row in mov_vals[1:]:
        if not any((c or "").strip() for c in row):
            continue

        rem_raw = str(cell(row, mov_h, "REMITENTE", "Remitente") or "").strip()
        des_raw = str(cell(row, mov_h, "DESTINO", "Destino") or "").strip()
        if not rem_raw or not des_raw:
            continue

        rem = canon_cuenta(rem_raw, cuentas_catalogo)
        des = canon_cuenta(des_raw, cuentas_catalogo)
        rem_n = norm_key(rem)
        des_n = norm_key(des)

        monto_origen = to_float(cell(row, mov_h, "MONTO", "Monto"))  # salida del remitente
        monto_destino = to_float(cell(row, mov_h, "MONTO_DESTINO", "Monto_destino"))  # entrada al destino

        entrada_destino = monto_destino if abs(monto_destino) > 1e-9 else monto_origen

        # ---- AHORRO (GTQ) ----
        # Ahorro es cuenta GTQ: entra con entrada_destino (GTQ), sale con monto_origen (GTQ)
        if des_n == ahorro_cuenta_n:
            ahorro_gtq += entrada_destino
        if rem_n == ahorro_cuenta_n:
            ahorro_gtq -= monto_origen

        # ---- INVERSIONES (USD) ----
        # Inversión entra con entrada_destino (USD), sale con monto_origen (USD)
        if des_n in inv_cuentas_n:
            inversiones_usd += entrada_destino
        if rem_n in inv_cuentas_n:
            inversiones_usd -= monto_origen

    total_gtq = ahorro_gtq + (inversiones_usd * usd_to_gtq)
    return ahorro_gtq, inversiones_usd, total_gtq




async def saldos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return

    gc = context.application.bot_data["gc"]

    # Aseguramos que catálogos/cuentas estén cargados para este usuario
    if "catalogos" not in context.user_data or "cuentas" not in context.user_data:
        sh = get_sheet_for_user(gc, update.effective_user.id)
        cats = load_catalogos(sh)
        context.user_data["catalogos"] = cats
        context.user_data["cuentas"] = cats.get("CUENTAS") or build_cuentas_from_catalogos(cats)


    cuentas = context.user_data.get("cuentas", CUENTAS)

    try:
        saldos_map = build_saldos_dinamicos(gc, update.effective_user.id, cuentas)

        # Ordenar por saldo desc
        items = sorted(saldos_map.items(), key=lambda x: x[1], reverse=True)

        # Preparar tabla bonita (monoespaciada)
        pares = [(c, format_money_q(v)) for c, v in items if c and abs(v) > 0.000001]

        if not pares:
            await update.message.reply_text("No hay movimientos suficientes para calcular saldos aún.")
            return

        w_cta = max(len("Cuenta"), max(len(c) for c, _ in pares))
        w_sal = max(len("Saldo"),  max(len(s) for _, s in pares))

        top = f"┌{'─'*(w_cta+2)}┬{'─'*(w_sal+2)}┐"
        hdr = f"│ {'Cuenta'.ljust(w_cta)} │ {'Saldo'.rjust(w_sal)} │"
        mid = f"├{'─'*(w_cta+2)}┼{'─'*(w_sal+2)}┤"
        rows = [
            f"│ {c.ljust(w_cta)} │ {s.rjust(w_sal)} │"
            for c, s in pares
        ]
        bot = f"└{'─'*(w_cta+2)}┴{'─'*(w_sal+2)}┘"

        table = "\n".join([top, hdr, mid, *rows, bot])

        await update.message.reply_text(
            f"<b>Saldos</b>\n<pre>{table}</pre>",
            parse_mode="HTML"
        )

    except Exception as e:
        await update.message.reply_text(f"No pude calcular saldos. Error: {e}")


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
            st["step"] = "remitente"
            cats = get_catalogos(context)
            cuentas = cats["CUENTAS"] if cats else CUENTAS
            await q.edit_message_text("Remitente (de dónde sale):", reply_markup=kb_list(cuentas, "FROM"))

        return

    if cb.startswith("SRC:"):
        data["fuente"] = cb.split(":")[1]
        st["step"] = "categoria"
        cats = get_catalogos(context)
        categ_ing = cats["CATEG_ING"] if cats else CATEG_ING
        await q.edit_message_text("Categoría:", reply_markup=kb_list(categ_ing, "CAT"))

        return

    if cb.startswith("CAT:"):
        data["categoria"] = cb.split(":")[1]
        st["step"] = "monto"
        await q.edit_message_text("Monto:")
        return

    if cb.startswith("PAY:"):
        data["metodo"] = cb.split(":")[1]
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
        data["banco"] = cb.split(":")[1]
        st["step"] = "nota"
        await q.edit_message_text("Nota (o -):")
        return

    if cb == "CONFIRM:SAVE":
        await save_to_sheets(context, data, update.effective_user.id)
        st_reset(context)
        await q.edit_message_text("Guardado correctamente.")
        return
    
    if cb.startswith("FROM:"):
        data["remitente"] = cb.split(":", 1)[1]
        st["step"] = "destino"
        cats = get_catalogos(context)
        cuentas = cats["CUENTAS"] if cats else CUENTAS
        await q.edit_message_text("Destino (a dónde entra):", reply_markup=kb_list(cuentas, "TO"))
        return

    if cb.startswith("TO:"):
        data["destino"] = cb.split(":", 1)[1]
        if data.get("destino") == data.get("remitente"):
            await q.edit_message_text("Destino no puede ser igual al remitente. Elige otro:", reply_markup=kb_list(CUENTAS, "TO"))
            return
        st["step"] = "monto"
        await q.edit_message_text("Monto:")
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
            st["step"] = "remitente"
            cuentas = context.user_data.get("cuentas", CUENTAS)
            await update.message.reply_text(
                "Remitente (de dónde sale):",
                reply_markup=kb_list(cuentas, "FROM")
            )

        return


    if step == "monto":
        raw = txt.strip()

        # quitar moneda y espacios
        raw = re.sub(r"[^0-9.,\-]", "", raw)

        # Caso Guatemala: 1.234,56
        if "." in raw and "," in raw:
            raw = raw.replace(".", "")
            raw = raw.replace(",", ".")
        # Caso decimal con punto: 150.5
        elif "." in raw:
            raw = raw.replace(",", "")
        # Caso decimal con coma: 150,5
        elif "," in raw:
            raw = raw.replace(",", ".")

        data["monto"] = float(raw)

        if data["tipo"] == "MOV":
            # ✅ NUEVO: pedir MONTO_DESTINO opcional
            st["step"] = "monto_destino"
            await update.message.reply_text(
                "Monto destino (si es el mismo, escribe 0):"
            )
        else:
            st["step"] = "metodo"
            cats = get_catalogos(context)
            metodos = cats["METODOS"] if cats else METODOS
            await update.message.reply_text("Método:", reply_markup=kb_list(metodos, "PAY"))

        return


    if step == "monto_destino":
        raw = txt.strip()

        raw = re.sub(r"[^0-9.,\-]", "", raw)

        if "." in raw and "," in raw:
            raw = raw.replace(".", "")
            raw = raw.replace(",", ".")
        elif "." in raw:
            raw = raw.replace(",", "")
        elif "," in raw:
            raw = raw.replace(",", ".")

        try:
            v = float(raw)
        except:
            v = 0.0

        # regla: 0 => mismo monto
        data["monto_destino"] = 0.0 if abs(v) < 0.000001 else v

        st["step"] = "nota"
        await update.message.reply_text("Nota (o -):")
        return



    if step == "nota":
        data["nota"] = "" if txt == "-" else txt
        st["step"] = "confirm"
        await update.message.reply_text(
            render_summary(data),
            reply_markup=kb_confirm()
        )

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
            data["remitente"],
            data["destino"],
            data["monto"],
            data.get("monto_destino", 0),  
            data.get("nota", "")
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

    # 1) Semanal: domingos a las 21:00 Guatemala
    app.job_queue.run_daily(
        job_resumen_semanal,
        time=dtime(hour=21, minute=0, tzinfo=TZ),
        days=(6,),  # 0=Lun ... 6=Dom
        name="resumen_semanal_dom_2100"
    )

    # 2) Fin de mes: chequeo diario a las 21:00, solo envía si hoy es último día
    app.job_queue.run_daily(
        job_resumen_fin_de_mes,
        time=dtime(hour=21, minute=0, tzinfo=TZ),
        name="resumen_fin_de_mes_ultimo_dia_2100"
    )



    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("nuevo", nuevo))
    app.add_handler(CommandHandler("cancelar", cancelar))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("resumen", resumen))
    app.add_handler(CommandHandler("balance", balance))
    app.add_handler(CommandHandler("saldos", saldos))
    app.add_handler(CommandHandler("ahorro", ahorro))


    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    print("Bot finanzas encendido...")
    app.run_polling()


if __name__ == "__main__":
    main()

