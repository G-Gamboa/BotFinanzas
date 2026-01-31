import re
from datetime import datetime, timedelta

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
# CONFIG
# =========================
import json
import os
from google.oauth2.service_account import Credentials

SERVICE_ACCOUNT_INFO = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

def gs_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        SERVICE_ACCOUNT_INFO,
        scopes=scopes
    )
    return gspread.authorize(creds)

SHEET_INGRESOS = "Ingresos"
SHEET_EGRESOS = "Egresos"

# PON AQUÍ TU USER ID cuando lo veas con /whoami
ALLOWED_USER_IDS = {}  # Ej: {123456789}

# =========================
# CATÁLOGOS (BOTONES)
# =========================
FUENTES_ING = ["Trabajo", "Freelance", "Negocios"]
CATEG_ING = ["Salario", "Proyecto", "Inversiones", "Ventas", "Otros"]
METODOS = ["Efectivo", "Transferencia", "Osmo", "Ugly"]
BANCOS = ["Bi", "Banrural", "Nexa", "Zigi"]

CATEG_EGR = [
    "Agua", "Internet", "Transporte", "Comida casa", "Chatarra",
    "Mercado", "Entretenimiento", "Salud", "Ahorro", "Ropa", "Zapatos"
]

# =========================
# HELPERS UI
# =========================
def kb_list(items, prefix: str, cols: int = 2):
    rows, row = [], []
    for i, it in enumerate(items):
        row.append(InlineKeyboardButton(it, callback_data=f"{prefix}:{it}"))
        if (i + 1) % cols == 0:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Cancelar", callback_data="CANCEL")])
    return InlineKeyboardMarkup(rows)

def kb_main():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Ingreso", callback_data="TYPE:ING"),
         InlineKeyboardButton("Egreso", callback_data="TYPE:EGR")],
        [InlineKeyboardButton("Cancelar", callback_data="CANCEL")]
    ])

def kb_date():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Hoy", callback_data="DATE:HOY"),
         InlineKeyboardButton("Ayer", callback_data="DATE:AYER")],
        [InlineKeyboardButton("Otra fecha", callback_data="DATE:OTRA")],
        [InlineKeyboardButton("Cancelar", callback_data="CANCEL")]
    ])

def kb_confirm():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Guardar", callback_data="CONFIRM:SAVE"),
         InlineKeyboardButton("Editar", callback_data="CONFIRM:EDIT")],
        [InlineKeyboardButton("Cancelar", callback_data="CANCEL")]
    ])

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
    uid = update.effective_user.id if update.effective_user else None
    # si no has configurado ALLOWED_USER_IDS aún, permite para que puedas usar /whoami
    if ALLOWED_USER_IDS == {0}:
        return True
    return uid in ALLOWED_USER_IDS

# =========================
# GOOGLE SHEETS INIT
# =========================
def gs_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    return gspread.authorize(creds)

# =========================
# COMMANDS
# =========================
async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    await update.message.reply_text(f"Tu user_id es: {uid}\nPonlo en ALLOWED_USER_IDS para bloquear a otros.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    st_reset(context)
    st = st_get(context)
    st["step"] = "choose_type"
    await update.message.reply_text("¿Qué quieres registrar?", reply_markup=kb_main())

async def nuevo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)

async def cancelar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return
    st_reset(context)
    await update.message.reply_text("Cancelado. Usa /nuevo para iniciar.")

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
        await q.edit_message_text("Cancelado. Usa /nuevo cuando quieras.")
        return

    st = st_get(context)
    data = st["data"]

    # Tipo
    if cb.startswith("TYPE:"):
        tipo = cb.split(":", 1)[1]
        data.clear()
        data["tipo"] = tipo  # ING / EGR
        st["step"] = "choose_date"
        await q.edit_message_text("Fecha:", reply_markup=kb_date())
        return

    # Fecha
    if cb.startswith("DATE:"):
        opt = cb.split(":", 1)[1]
        if opt == "HOY":
            data["fecha"] = datetime.now().strftime("%Y-%m-%d")
        elif opt == "AYER":
            data["fecha"] = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            st["step"] = "wait_date_text"
            await q.edit_message_text("Escribe la fecha en formato YYYY-MM-DD (ej: 2026-01-30).")
            return

        # siguiente según tipo
        if data["tipo"] == "ING":
            st["step"] = "choose_fuente"
            await q.edit_message_text("Ingreso: elige la FUENTE:", reply_markup=kb_list(FUENTES_ING, "SRC"))
        else:
            st["step"] = "choose_categoria"
            await q.edit_message_text("Egreso: elige la CATEGORÍA:", reply_markup=kb_list(CATEG_EGR, "CAT"))
        return

    # Ingreso: fuente
    if cb.startswith("SRC:"):
        data["fuente"] = cb.split(":", 1)[1]
        st["step"] = "choose_categoria"
        await q.edit_message_text("Ingreso: elige la CATEGORÍA:", reply_markup=kb_list(CATEG_ING, "CAT"))
        return

    # Categoría (aplica a ambos)
    if cb.startswith("CAT:"):
        data["categoria"] = cb.split(":", 1)[1]
        st["step"] = "wait_monto"
        await q.edit_message_text("Escribe el MONTO (ej: 125 o 125.50).")
        return

    # Método
    if cb.startswith("PAY:"):
        data["metodo"] = cb.split(":", 1)[1]
        if data["metodo"] == "Transferencia":
            st["step"] = "choose_banco"
            await q.edit_message_text("Elige el BANCO:", reply_markup=kb_list(BANCOS, "BANK"))
        else:
            data["banco"] = ""
            st["step"] = "wait_nota"
            await q.edit_message_text("Nota (opcional). Escribe texto o escribe: - para dejar vacío.")
        return

    # Banco
    if cb.startswith("BANK:"):
        data["banco"] = cb.split(":", 1)[1]
        st["step"] = "wait_nota"
        await q.edit_message_text("Nota (opcional). Escribe texto o escribe: - para dejar vacío.")
        return

    # Confirmación
    if cb == "CONFIRM:EDIT":
        # reiniciar flujo conservando tipo
        tipo = data.get("tipo")
        st_reset(context)
        st = st_get(context)
        st["data"]["tipo"] = tipo
        st["step"] = "choose_date"
        await q.edit_message_text("Ok, editemos. Fecha:", reply_markup=kb_date())
        return

    if cb == "CONFIRM:SAVE":
        try:
            await save_to_sheets(context, data)
        except Exception as e:
            st_reset(context)
            await q.edit_message_text(f"No se pudo guardar. Error: {e}")
            return

        resumen = render_summary(data)
        st_reset(context)
        await q.edit_message_text("Guardado.\n\n" + resumen)
        return

# =========================
# TEXT INPUT (fecha/monto/nota)
# =========================
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not allowed(update):
        return

    st = st_get(context)
    step = st["step"]
    data = st["data"]
    text = (update.message.text or "").strip()

    if step == "wait_date_text":
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            await update.message.reply_text("Formato inválido. Usa YYYY-MM-DD (ej: 2026-01-30).")
            return
        try:
            datetime.strptime(text, "%Y-%m-%d")
        except ValueError:
            await update.message.reply_text("Fecha inválida (revisa día/mes).")
            return

        data["fecha"] = text
        if data.get("tipo") == "ING":
            st["step"] = "choose_fuente"
            await update.message.reply_text("Ingreso: elige la FUENTE:", reply_markup=kb_list(FUENTES_ING, "SRC"))
        else:
            st["step"] = "choose_categoria"
            await update.message.reply_text("Egreso: elige la CATEGORÍA:", reply_markup=kb_list(CATEG_EGR, "CAT"))
        return

    if step == "wait_monto":
        m = text.replace(",", ".")
        try:
            monto = float(m)
            if monto <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("Monto inválido. Ejemplos válidos: 75 | 125.50")
            return

        data["monto"] = monto
        st["step"] = "choose_metodo"
        await update.message.reply_text("Elige el MÉTODO:", reply_markup=kb_list(METODOS, "PAY"))
        return

    if step == "wait_nota":
        data["nota"] = "" if text == "-" else text
        st["step"] = "confirm"
        await update.message.reply_text(render_summary(data), reply_markup=kb_confirm())
        return

    await update.message.reply_text("Usa /nuevo para iniciar un registro.")

# =========================
# RENDER + SAVE
# =========================
def render_summary(data):
    if data.get("tipo") == "ING":
        return (
            "Resumen:\n"
            f"Tipo: Ingreso\n"
            f"Fecha: {data.get('fecha','')}\n"
            f"Fuente: {data.get('fuente','')}\n"
            f"Categoría: {data.get('categoria','')}\n"
            f"Monto: {data.get('monto','')}\n"
            f"Método: {data.get('metodo','')}\n"
            f"Banco: {data.get('banco','')}\n"
            f"Nota: {data.get('nota','')}"
        )
    else:
        return (
            "Resumen:\n"
            f"Tipo: Egreso\n"
            f"Fecha: {data.get('fecha','')}\n"
            f"Categoría: {data.get('categoria','')}\n"
            f"Monto: {data.get('monto','')}\n"
            f"Método: {data.get('metodo','')}\n"
            f"Banco: {data.get('banco','')}\n"
            f"Nota: {data.get('nota','')}"
        )

async def save_to_sheets(context: ContextTypes.DEFAULT_TYPE, data):
    sh = context.application.bot_data["sh"]

    if data.get("tipo") == "ING":
        ws = sh.worksheet(SHEET_INGRESOS)
        # FECHA FUENTE CATEGORÍA MONTO MÉTODO BANCO NOTA
        row = [
            data.get("fecha", ""),
            data.get("fuente", ""),
            data.get("categoria", ""),
            data.get("monto", ""),
            data.get("metodo", ""),
            data.get("banco", ""),
            data.get("nota", "")
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")
    else:
        ws = sh.worksheet(SHEET_EGRESOS)
        # FECHA CATEGORÍA MONTO MÉTODO BANCO NOTA
        row = [
            data.get("fecha", ""),
            data.get("categoria", ""),
            data.get("monto", ""),
            data.get("metodo", ""),
            data.get("banco", ""),
            data.get("nota", "")
        ]
        ws.append_row(row, value_input_option="USER_ENTERED")

# =========================
# MAIN
# =========================
def main():
    gc = gs_client()
    sh = gc.open_by_key(SHEET_ID)

    app = Application.builder().token(BOT_TOKEN).build()
    app.bot_data["sh"] = sh

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("nuevo", nuevo))
    app.add_handler(CommandHandler("cancelar", cancelar))
    app.add_handler(CommandHandler("whoami", whoami))

    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    print("Bot finanzas encendido...")
    app.run_polling()

if __name__ == "__main__":
    main()
