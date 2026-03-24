from shared import ensure_catalogs
from .auth import allowed
from .catalogs import get_catalogos, get_accounts_by_role
from .config import BANCOS, CATEG_EGR, CATEG_ING, CUENTAS, FUENTES_ING, METODOS
from .finance import build_deudas, build_networth, build_resumen_mes, build_saldos_dinamicos, build_total_deudas
from .helpers import format_money_q
from .keyboards import kb_deudas_activas, kb_main, kb_cuentas_pago
from .renderers import render_lines_q, render_lines_usd
from .services import ejecutar_pago_deuda
from .state import st_get, st_reset

async def whoami(update, context):
    await update.message.reply_text(f"Tu user_id es: {update.effective_user.id}")

async def start(update, context):
    if not allowed(update):
        return
    await ensure_catalogs(update, context)
    st_reset(context)
    await update.message.reply_text("¿Qué quieres registrar?", reply_markup=kb_main())

async def nuevo(update, context):
    await start(update, context)

async def nueva_deuda(update, context):
    if not allowed(update):
        return
    st_reset(context)
    st = st_get(context)
    st["data"]["tipo"] = "DEUDA"
    st["step"] = "deuda_nombre"
    await update.message.reply_text("Nombre de la deuda:")

async def cancelar(update, context):
    st_reset(context)
    await update.message.reply_text("Cancelado. Usa /nuevo para iniciar.")

async def resumen(update, context):
    if not allowed(update):
        return
    gc = context.application.bot_data["gc"]
    try:
        txt = build_resumen_mes(gc, update.effective_user.id)
        await update.message.reply_text(txt)
    except Exception as e:
        await update.message.reply_text(f"No pude generar el resumen. Error: {e}")

async def saldos(update, context):
    if not allowed(update):
        return
    gc = context.application.bot_data["gc"]
    await ensure_catalogs(update, context)

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

async def networth(update, context):
    if not allowed(update):
        return

    gc = context.application.bot_data["gc"]

    try:
        nw = build_networth(gc, update.effective_user.id)

        msg = (
            "Net Worth\n\n"
            "Liquidez\n"
            f"{render_lines_q(nw['liquid_map'])}\n"
            f"- Total líquido: {format_money_q(nw['liquidez_gtq'])}\n\n"
            "Ahorro\n"
            f"{render_lines_q(nw['ahorro_map'])}\n\n"
            "Préstamos\n"
            f"{render_lines_q(nw['prestamos_map'])}\n\n"
            "Inversiones\n"
            f"{render_lines_usd(nw['inv_map'])}\n\n"
            f"Total patrimonial (GTQ): {format_money_q(nw['total_gtq'])}\n"
            f"TC usado: {nw['tc']}"
        )

        await update.message.reply_text(msg)

    except Exception as e:
        await update.message.reply_text(f"No pude calcular net worth. Error: {e}")

async def ahorro(update, context):
    await networth(update, context)

async def deudas(update, context):
    if not allowed(update):
        return

    gc = context.application.bot_data["gc"]

    try:
        items = build_deudas(gc, update.effective_user.id)

        if not items:
            await update.message.reply_text("No encontré deudas en la hoja Deudas.")
            return

        bloques = []
        for d in items:
            bloques.append(
                f"{d['nombre']}\n"
                f"- A quién le debo: {d['acreedor']}\n"
                f"- Fecha de pago: {d['fecha_pago']}\n"
                f"- Cuota: {format_money_q(d['cuota'])}\n"
                f"- Pagados: {d['pagados']} / {d['meses']}\n"
                f"- Pendientes: {d['pendientes']}\n"
                f"- Saldo: {format_money_q(d['saldo'])}\n"
                f"- Estado: {d['estado']}"
            )

        await update.message.reply_text("Deudas\n\n" + "\n\n".join(bloques))

    except Exception as e:
        await update.message.reply_text(f"No pude leer deudas. Error: {e}")

async def deudas_activas(update, context):
    if not allowed(update):
        return

    gc = context.application.bot_data["gc"]

    try:
        items = build_deudas(gc, update.effective_user.id)
        activas = [d for d in items if d["estado"].lower() == "activa" and d["pendientes"] > 0]

        if not activas:
            await update.message.reply_text("No tienes deudas activas.")
            return

        txt = "Deudas activas\n\n" + "\n".join(
            f"- {d['nombre']} | Vence: {d['fecha_pago']} | Pendientes: {d['pendientes']} | Saldo: {format_money_q(d['saldo'])}"
            for d in activas
        )

        await update.message.reply_text(txt)

    except Exception as e:
        await update.message.reply_text(f"No pude leer deudas activas. Error: {e}")

async def neto(update, context):
    if not allowed(update):
        return

    gc = context.application.bot_data["gc"]

    try:
        nw = build_networth(gc, update.effective_user.id)
        pasivos_gtq = build_total_deudas(gc, update.effective_user.id)
        neto_gtq = nw["total_gtq"] - pasivos_gtq

        msg = (
            "Patrimonio Neto\n\n"
            f"Patrimonio bruto: {format_money_q(nw['total_gtq'])}\n"
            f"Pasivos (deudas): {format_money_q(pasivos_gtq)}\n\n"
            f"Patrimonio neto: {format_money_q(neto_gtq)}"
        )

        await update.message.reply_text(msg)

    except Exception as e:
        await update.message.reply_text(f"No pude calcular patrimonio neto. Error: {e}")

async def pagar(update, context):
    if not allowed(update):
        return

    gc = context.application.bot_data["gc"]

    try:
        items = build_deudas(gc, update.effective_user.id)
        activas = [d for d in items if d["estado"].lower() == "activa" and d["pendientes"] > 0]

        if not activas:
            await update.message.reply_text("No tienes deudas activas para pagar.")
            return

        st_reset(context)
        st = st_get(context)
        st["step"] = "pagar_deuda_select"
        context.user_data["deudas_activas"] = activas

        await update.message.reply_text(
            "Selecciona la deuda que vas a pagar:",
            reply_markup=kb_deudas_activas(activas)
        )

    except Exception as e:
        await update.message.reply_text(f"No pude iniciar el pago de deuda. Error: {e}")
