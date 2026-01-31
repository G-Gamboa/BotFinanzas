import re
import json
import os
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
# CONFIG (RAILWAY)
# =========================
BOT_TOKEN = os.environ["BOT_TOKEN"]
SHEET_ID = os.environ["SHEET_ID"]

SERVICE_ACCOUNT_INFO = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

SHEET_INGRESOS = "Ingresos"
SHEET_EGRESOS = "Egresos"

# 👇 TU USER ID (déjalo así)
ALLOWED_USER_IDS = {1282471582}

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
    return uid in ALLOWED_USER_IDS

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
    st_reset(context)
    await update.message.reply_text(
        "¿Qué quieres registrar?",
        reply_markup=kb_main()
    )

async def nuevo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)

async def cancelar(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            data["fecha"] = datetime.now().strftime("%Y-%m-%d")
        elif opt == "AYER":
            data["fecha"] = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            st["step"] = "wait_date"
            await q.edit_message_text("Escribe la fecha YYYY-MM-DD")
            return

        if data["tipo"] == "ING":
            st["step"] = "fuente"
            await q.edit_message_text("Fuente:", reply_markup=kb_list(FUENTES_ING, "SRC"))
        else:
            st["step"] = "categoria"
            await q.edit_message_text("Categoría:", reply_markup=kb_list(CATEG_EGR, "CAT"))
        return

    if cb.startswith("SRC:"):
        data["fuente"] = cb.split(":")[1]
        st["step"] = "categoria"
        await q.edit_message_text("Categoría:", reply_markup=kb_list(CATEG_ING, "CAT"))
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
            await q.edit_message_text("Banco:", reply_markup=kb_list(BANCOS, "BANK"))
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
        await save_to_sheets(context, data)
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
        if data["tipo"] == "ING":
            st["step"] = "fuente"
            await update.message.reply_text("Fuente:", reply_markup=kb_list(FUENTES_ING, "SRC"))
        else:
            st["step"] = "categoria"
            await update.message.reply_text("Categoría:", reply_markup=kb_list(CATEG_EGR, "CAT"))
        return

    if step == "monto":
        data["monto"] = float(txt.replace(",", "."))
        st["step"] = "metodo"
        await update.message.reply_text("Método:", reply_markup=kb_list(METODOS, "PAY"))
        return

    if step == "nota":
        data["nota"] = "" if txt == "-" else txt
        st["step"] = "confirm"
        await update.message.reply_text(
            "Confirmar",
            reply_markup=kb_confirm()
        )

# =========================
# SAVE
# =========================
async def save_to_sheets(context: ContextTypes.DEFAULT_TYPE, data):
    sh = context.application.bot_data["sh"]

    if data["tipo"] == "ING":
        ws = sh.worksheet(SHEET_INGRESOS)
        ws.append_row([
            data["fecha"], data["fuente"], data["categoria"],
            data["monto"], data["metodo"], data["banco"], data["nota"]
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
