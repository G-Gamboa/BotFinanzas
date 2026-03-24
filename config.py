import json
import os
from zoneinfo import ZoneInfo

BOT_TOKEN = os.environ["BOT_TOKEN"]
USER_SHEETS = json.loads(os.environ["USER_SHEETS"])
SERVICE_ACCOUNT_INFO = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])

SHEET_INGRESOS = "Ingresos"
SHEET_EGRESOS = "Egresos"
SHEET_MOVIMIENTOS = "Movimientos"
SHEET_RESUMEN = "Resumen"
SHEET_CATEGORIAS = "Categorías"
SHEET_DEUDAS = "Deudas"

USD_TO_GTQ = 7.7
TZ = ZoneInfo("America/Guatemala")

INV_CUENTAS_DEFAULT = {"Ugly", "Binance", "Osmo", "Hapi"}
BOLSA_NORMAL = "Normal"

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
