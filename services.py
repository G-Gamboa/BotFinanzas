from datetime import datetime

from config import (
    BANCOS,
    BOLSA_NORMAL,
    SHEET_DEUDAS,
    SHEET_EGRESOS,
    SHEET_INGRESOS,
    SHEET_MOVIMIENTOS,
    TZ,
    USER_SHEETS,
)
from helpers import format_money_q, to_float
from sheets_service import get_sheet_for_user
from validators import validate_flow_data

async def save_to_sheets(context, data, uid: int):
    gc = context.application.bot_data["gc"]
    uid_str = str(uid)
    sheet_id = USER_SHEETS.get(uid_str)
    if not sheet_id:
        raise RuntimeError("Tu usuario no tiene Sheet configurado.")

    validate_flow_data(data)
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

    elif data["tipo"] == "DEUDA":
        ws = sh.worksheet(SHEET_DEUDAS)
        ws.append_row([
            data["deuda_nombre"],
            data["deuda_acreedor"],
            data["deuda_fecha_pago"],
            data["deuda_cuota"],
            data["deuda_meses"],
            data["deuda_pagados"],
            data["deuda_pendientes"],
            data["deuda_saldo"],
            data["deuda_estado"],
        ], value_input_option="USER_ENTERED")

    else:
        ws = sh.worksheet(SHEET_EGRESOS)
        ws.append_row([
            data["fecha"], data["categoria"],
            data["monto"], data["metodo"], data["banco"], data["nota"]
        ], value_input_option="USER_ENTERED")

def sumar_un_pago_deuda(sh, row_num: int):
    ws = sh.worksheet(SHEET_DEUDAS)
    pagados_actual = int(to_float(ws.cell(row_num, 6).value))
    ws.update_cell(row_num, 6, pagados_actual + 1)

def registrar_egreso_deuda(sh, fecha: str, cuenta_pago: str, monto: float, nombre_deuda: str):
    ws = sh.worksheet(SHEET_EGRESOS)

    if cuenta_pago.strip().lower() in {"bi", "banrural", "nexa", "zigi", "gyt"}:
        metodo = "Transferencia"
        banco = cuenta_pago
    else:
        metodo = cuenta_pago
        banco = ""

    ws.append_row([
        fecha,
        "Deuda",
        monto,
        metodo,
        banco,
        f"Pago de deuda: {nombre_deuda}"
    ], value_input_option="USER_ENTERED")

async def ejecutar_pago_deuda(context, uid: int, data: dict):
    from .finance import build_deudas

    gc = context.application.bot_data["gc"]
    sh = get_sheet_for_user(gc, uid)

    fecha = datetime.now(TZ).strftime("%Y-%m-%d")
    row_num = data["deuda_row"]
    nombre_deuda = data["deuda_nombre"]
    cuota = float(data["deuda_cuota"])
    cuenta_pago = data["cuenta_pago"]

    deuda_actual = next((d for d in build_deudas(gc, uid) if d["row"] == row_num), None)
    if not deuda_actual:
        raise ValueError("No encontré la deuda seleccionada.")
    if deuda_actual["estado"].lower() != "activa" or deuda_actual["pendientes"] <= 0:
        raise ValueError("Esa deuda ya está pagada.")

    sumar_un_pago_deuda(sh, row_num)
    registrar_egreso_deuda(sh, fecha, cuenta_pago, cuota, nombre_deuda)
