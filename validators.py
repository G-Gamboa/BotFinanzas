from .helpers import ensure_fecha_text, is_positive_amount
from .helpers import norm_key

def movimientos_misma_ruta(data: dict) -> bool:
    from .config import BOLSA_NORMAL
    return (
        norm_key(data.get("bolsa_remitente", BOLSA_NORMAL)) == norm_key(data.get("bolsa_destino", BOLSA_NORMAL))
        and norm_key(data.get("remitente", "")) == norm_key(data.get("destino", ""))
        and bool((data.get("remitente", "") or "").strip())
    )

def validate_flow_data(data: dict):
    tipo = data.get("tipo")

    if tipo in {"ING", "EGR", "MOV"}:
        data["fecha"] = ensure_fecha_text(data.get("fecha", ""))

    if tipo == "ING":
        if not is_positive_amount(data.get("monto")):
            raise ValueError("El monto debe ser mayor a 0.")
    elif tipo == "EGR":
        if not is_positive_amount(data.get("monto")):
            raise ValueError("El monto debe ser mayor a 0.")
    elif tipo == "MOV":
        if not is_positive_amount(data.get("monto")):
            raise ValueError("El monto debe ser mayor a 0.")
        monto_dest = data.get("monto_destino", 0)
        if monto_dest not in (None, "") and float(monto_dest) < 0:
            raise ValueError("El monto destino no puede ser negativo.")
        if movimientos_misma_ruta(data):
            raise ValueError("Remitente y destino no pueden ser iguales.")
        if norm_key(data.get("mov_type", "")) == "prestamo" and not (data.get("persona_prestamo", "") or "").strip():
            raise ValueError("El préstamo debe tener una persona asociada.")
    elif tipo == "DEUDA":
        data["deuda_fecha_pago"] = ensure_fecha_text(data.get("deuda_fecha_pago", ""))
        if not (data.get("deuda_nombre", "") or "").strip():
            raise ValueError("La deuda debe tener nombre.")
        if not (data.get("deuda_acreedor", "") or "").strip():
            raise ValueError("Debes indicar a quién le debes.")
        if not is_positive_amount(data.get("deuda_cuota")):
            raise ValueError("La cuota debe ser mayor a 0.")
        meses = int(data.get("deuda_meses", 0))
        pagados = int(data.get("deuda_pagados", 0))
        if meses <= 0:
            raise ValueError("Los meses deben ser mayores a 0.")
        if pagados < 0:
            raise ValueError("Pagados no puede ser negativo.")
        if pagados > meses:
            raise ValueError("Pagados no puede ser mayor que meses.")
        data["deuda_pendientes"] = max(meses - pagados, 0)
        data["deuda_saldo"] = float(data["deuda_cuota"]) * data["deuda_pendientes"]
        data["deuda_estado"] = "Pagada" if data["deuda_pendientes"] <= 0 else "Activa"
