from .helpers import format_money_q

def render_summary(data):
    if data["tipo"] == "DEUDA":
        return (
            "Resumen nueva deuda:\n"
            f"Nombre: {data.get('deuda_nombre','')}\n"
            f"A quién le debo: {data.get('deuda_acreedor','')}\n"
            f"Fecha de pago: {data.get('deuda_fecha_pago','')}\n"
            f"Cuota: {format_money_q(float(data.get('deuda_cuota', 0) or 0))}\n"
            f"Meses: {data.get('deuda_meses', 0)}\n"
            f"Pagados: {data.get('deuda_pagados', 0)}\n"
            f"Pendientes: {data.get('deuda_pendientes', 0)}\n"
            f"Saldo: {format_money_q(float(data.get('deuda_saldo', 0) or 0))}\n"
            f"Estado: {data.get('deuda_estado','')}"
        )
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

def render_lines_q(data: dict[str, float]) -> str:
    items = [(k, v) for k, v in data.items() if abs(v) > 1e-9]
    if not items:
        return "  - (sin datos)"
    return "\n".join([f"  - {k}: {format_money_q(v)}" for k, v in items])

def render_lines_usd(data: dict[str, float]) -> str:
    items = [(k, v) for k, v in data.items() if abs(v) > 1e-9]
    if not items:
        return "  - (sin datos)"
    return "\n".join([f"  - {k}: ${v:,.2f}" for k, v in items])
