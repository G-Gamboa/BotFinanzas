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

def load_catalogos(sh):
    ws = sh.worksheet(SHEET_CATEGORIAS)

    fuentes_ing = col_clean(ws.col_values(1))     # A
    categ_ing   = col_clean(ws.col_values(2))     # B
    metodos     = col_clean(ws.col_values(3))     # C
    bancos      = col_clean(ws.col_values(4))     # D
    categ_egr   = col_clean(ws.col_values(5))     # E

    return {
        "FUENTES_ING": fuentes_ing,
        "CATEG_ING": categ_ing,
        "METODOS": metodos,
        "BANCOS": bancos,
        "CATEG_EGR": sorted(categ_egr),
    }

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
        return (
            "Resumen movimiento:\n"
            f"Fecha: {data.get('fecha','')}\n"
            f"Remitente: {data.get('remitente','')}\n"
            f"Destino: {data.get('destino','')}\n"
            f"Monto: {data.get('monto','')}\n"
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
    context.user_data["cuentas"] = build_cuentas_from_catalogos(cats)

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

def build_saldos_dinamicos(gc, uid: int, cuentas: list[str]) -> dict[str, float]:
    sh = get_sheet_for_user(gc, uid)

    ws_ing = sh.worksheet(SHEET_INGRESOS)
    ws_egr = sh.worksheet(SHEET_EGRESOS)
    ws_mov = sh.worksheet(SHEET_MOVIMIENTOS)

    ing_rows = ws_ing.get_all_records()
    egr_rows = ws_egr.get_all_records()
    mov_rows = ws_mov.get_all_records()

    saldos = defaultdict(float)

    # Ingresos: suma
    for r in ing_rows:
        cuenta = resolve_cuenta_from_row(r, None, "ING")
        monto = to_float(pick(r, "MONTO", "Monto"))
        if cuenta:
            saldos[cuenta] += monto

    # Egresos: resta
    for r in egr_rows:
        cuenta = resolve_cuenta_from_row(r, None, "EGR")
        monto = to_float(pick(r, "MONTO", "Monto"))
        if cuenta:
            saldos[cuenta] -= monto

    # Movimientos: - remitente, + destino
    for r in mov_rows:
        rem = str(pick(r, "REMITENTE", "Remitente") or "").strip()
        des = str(pick(r, "DESTINO", "Destino") or "").strip()
        monto = to_float(pick(r, "MONTO", "Monto"))

        if rem:
            saldos[rem] -= monto
        if des:
            saldos[des] += monto

    # Asegurar que existan todas las cuentas conocidas (aunque estén en 0)
    for c in cuentas:
        saldos[c] += 0.0

    return saldos

async def saldos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return

    gc = context.application.bot_data["gc"]

    # Aseguramos que catálogos/cuentas estén cargados para este usuario
    if "catalogos" not in context.user_data or "cuentas" not in context.user_data:
        sh = get_sheet_for_user(gc, update.effective_user.id)
        cats = load_catalogos(sh)
        context.user_data["catalogos"] = cats
        context.user_data["cuentas"] = build_cuentas_from_catalogos(cats)

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
            cuentas = context.user_data.get("cuentas", CUENTAS)
            await q.edit_message_text(
                "Remitente (de dónde sale):",
                reply_markup=kb_list(cuentas, "FROM")
            )

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
        cuentas = context.user_data.get("cuentas", CUENTAS)
        await q.edit_message_text(
            "Destino (a dónde entra):",
            reply_markup=kb_list(cuentas, "TO")
        )

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
            st["step"] = "nota"
            await update.message.reply_text("Nota (o -):")
        else:
            st["step"] = "metodo"
            cats = get_catalogos(context)
            metodos = cats["METODOS"] if cats else METODOS
            await update.message.reply_text("Método:", reply_markup=kb_list(metodos, "PAY"))

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




    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    print("Bot finanzas encendido...")
    app.run_polling()


if __name__ == "__main__":
    main()

