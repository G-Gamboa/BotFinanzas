from telegram import InlineKeyboardButton, InlineKeyboardMarkup

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
            InlineKeyboardButton("Préstamo", callback_data="MVT:PRESTAMO"),
        ],
        [InlineKeyboardButton("Cancelar", callback_data="CANCEL")],
    ])

def kb_mov_direction(movtype: str):
    movtype = movtype.strip().lower()
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
    elif movtype == "prestamo":
        opts = [
            [InlineKeyboardButton("Dar", callback_data="MDIR:DAR")],
            [InlineKeyboardButton("Cobrar", callback_data="MDIR:COBRAR")],
        ]
    else:
        opts = []
    opts.append([InlineKeyboardButton("Cancelar", callback_data="CANCEL")])
    return InlineKeyboardMarkup(opts)

def kb_deudas_activas(items: list[dict]):
    rows = []
    for d in items:
        rows.append([InlineKeyboardButton(
            f"{d['nombre']} | {d['cuota']:,.2f}",
            callback_data=f"DEUDA:{d['row']}"
        )])
    rows.append([InlineKeyboardButton("Cancelar", callback_data="CANCEL")])
    return InlineKeyboardMarkup(rows)

def kb_cuentas_pago(cuentas: list[str]):
    return kb_list(cuentas, "PAGAR_CTA")
