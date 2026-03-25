from datetime import datetime, timedelta

from auth import allowed
from catalogs import get_accounts_by_role, get_catalogos
from config import BANCOS, BOLSA_NORMAL, CATEG_EGR, CATEG_ING, CUENTAS, FUENTES_ING, METODOS, PERSONAS_PRESTAMO, TZ
from finance import build_deudas
from helpers import ensure_fecha_text, format_money_q, parse_money_text, parse_positive_int_text, cuentas_permitidas_egreso
from keyboards import kb_confirm, kb_cuentas_pago, kb_date, kb_list, kb_mov_direction, kb_mov_type
from renderers import render_summary
from services import ejecutar_pago_deuda, save_to_sheets
from state import st_get, st_reset
from validators import movimientos_misma_ruta, validate_flow_data

async def on_cb(update, context):
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
            st["step"] = "mov_type"
            await q.edit_message_text("Tipo de movimiento:", reply_markup=kb_mov_type())
        return

    if cb.startswith("MVT:"):
        mov_type = cb.split(":")[1]
        data["mov_type"] = mov_type
        if mov_type == "NORMAL":
            data["bolsa_remitente"] = BOLSA_NORMAL
            data["bolsa_destino"] = BOLSA_NORMAL
            st["step"] = "mov_from"
            liquid, _, _ = get_accounts_by_role(context)
            await q.edit_message_text("Cuenta de dónde sale:", reply_markup=kb_list(liquid, "FROM"))
        else:
            st["step"] = "mov_dir"
            await q.edit_message_text("Dirección:", reply_markup=kb_mov_direction(mov_type))
        return

    if cb.startswith("MDIR:"):
        direction = cb.split(":")[1]
        data["mov_direction"] = direction
        liquid, _, inv = get_accounts_by_role(context)
        cats = get_catalogos(context) or {}
        personas = cats.get("PERSONAS_PRESTAMO", PERSONAS_PRESTAMO)

        mov_type = data.get("mov_type")
        if mov_type == "AHORRO":
            st["step"] = "ahorro_account"
            await q.edit_message_text("Cuenta física:", reply_markup=kb_list(liquid, "ACC"))
        elif mov_type == "INVERSION":
            if direction == "INVERTIR":
                st["step"] = "inv_from"
                await q.edit_message_text("Cuenta de dónde sale (GTQ):", reply_markup=kb_list(liquid, "FROM"))
            else:
                st["step"] = "inv_account"
                await q.edit_message_text("Cuenta inversión (USD):", reply_markup=kb_list(inv, "INVACC"))
        elif mov_type == "PRESTAMO":
            if direction == "DAR":
                st["step"] = "loan_account"
                await q.edit_message_text("Cuenta de dónde sale:", reply_markup=kb_list(liquid, "ACC"))
            else:
                st["step"] = "loan_person"
                await q.edit_message_text("¿Quién te paga?", reply_markup=kb_list(personas, "PERS"))
        return

    if cb.startswith("SRC:"):
        data["fuente"] = cb.split(":", 1)[1]
        st["step"] = "categoria"
        cats = get_catalogos(context)
        categ_ing = cats["CATEG_ING"] if cats else CATEG_ING
        await q.edit_message_text("Categoría:", reply_markup=kb_list(categ_ing, "CAT"))
        return

    if cb.startswith("CAT:"):
        data["categoria"] = cb.split(":", 1)[1]

        if data.get("tipo") == "EGR":
            liquid, _, inv = get_accounts_by_role(context)
            cuentas_validas = cuentas_permitidas_egreso(liquid, inv)

            st["step"] = "cuenta_egreso"
            await q.edit_message_text(
                "Cuenta:",
                reply_markup=kb_list(cuentas_validas, "ACC_EGR")
            )
            return

        st["step"] = "monto"
        await q.edit_message_text("Monto:")
        return
    

    if cb.startswith("ACC_EGR:"):
        data["cuenta"] = cb.split(":", 1)[1]

        st["step"] = "monto"
        await q.edit_message_text("Monto:")
        return

    if cb.startswith("PAY:"):
        data["metodo"] = cb.split(":", 1)[1]

        if data.get("tipo") == "EGR":
            cats = get_catalogos(context) or {}
            cuentas = cats.get("CUENTAS", CUENTAS)
            liquid, _, inv = get_accounts_by_role(context)
            cuentas_validas = cuentas_permitidas_egreso(liquid, inv)

            if data["metodo"] == "Transferencia":
                st["step"] = "banco"
                await q.edit_message_text("Cuenta:", reply_markup=kb_list(cuentas_validas, "BANK"))
            else:
                if data["metodo"] not in cuentas_validas:
                    await q.edit_message_text("Cuenta no permitida para egreso.")
                    return
                data["banco"] = ""
                st["step"] = "nota"
                await q.edit_message_text("Nota (o -):")
            return

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
        data["banco"] = cb.split(":", 1)[1]
        st["step"] = "nota"
        await q.edit_message_text("Nota (o -):")
        return

    if cb.startswith("FROM:"):
        data["remitente"] = cb.split(":", 1)[1]
        if data.get("mov_type") == "INVERSION" and data.get("mov_direction") == "INVERTIR":
            st["step"] = "inv_to_account"
            _, _, inv = get_accounts_by_role(context)
            await q.edit_message_text("Cuenta inversión (USD):", reply_markup=kb_list(inv, "INVTOACC"))
        else:
            st["step"] = "mov_to"
            liquid, _, _ = get_accounts_by_role(context)
            await q.edit_message_text("Cuenta a dónde entra:", reply_markup=kb_list(liquid, "TO"))
        return

    if cb.startswith("TO:"):
        data["destino"] = cb.split(":", 1)[1]
        if movimientos_misma_ruta(data):
            await q.edit_message_text("Destino no puede ser igual al remitente.", reply_markup=kb_list(get_accounts_by_role(context)[0], "TO"))
            return
        st["step"] = "monto"
        await q.edit_message_text("Monto:")
        return

    if cb.startswith("ACC:"):
        account = cb.split(":", 1)[1]
        mov_type = data.get("mov_type")
        direction = data.get("mov_direction")

        if mov_type == "AHORRO":
            if direction == "GUARDAR":
                data["bolsa_remitente"] = BOLSA_NORMAL
                data["remitente"] = account
                data["bolsa_destino"] = "Ahorro"
                data["destino"] = account
            else:
                data["bolsa_remitente"] = "Ahorro"
                data["remitente"] = account
                data["bolsa_destino"] = BOLSA_NORMAL
                data["destino"] = account
            st["step"] = "monto"
            await q.edit_message_text("Monto:")
            return

        if mov_type == "PRESTAMO":
            data["loan_account"] = account
            personas = (get_catalogos(context) or {}).get("PERSONAS_PRESTAMO", PERSONAS_PRESTAMO)
            st["step"] = "loan_person"
            prompt = "¿A quién le prestas?" if direction == "DAR" else "¿Quién te paga?"
            await q.edit_message_text(prompt, reply_markup=kb_list(personas, "PERS"))
            return

    if cb.startswith("INVACC:"):
        inv_account = cb.split(":", 1)[1]
        direction = data.get("mov_direction")
        if direction == "RETIRAR_INV":
            data["bolsa_remitente"] = "Inversion"
            data["remitente"] = inv_account
            st["step"] = "inv_to"
            liquid, _, _ = get_accounts_by_role(context)
            await q.edit_message_text("Cuenta a dónde entra (GTQ):", reply_markup=kb_list(liquid, "INVTO"))
        return

    if cb.startswith("INVTOACC:"):
        data["bolsa_remitente"] = BOLSA_NORMAL
        data["bolsa_destino"] = "Inversion"
        data["destino"] = cb.split(":", 1)[1]
        st["step"] = "monto"
        await q.edit_message_text("Monto:")
        return

    if cb.startswith("INVTO:"):
        data["bolsa_destino"] = BOLSA_NORMAL
        data["destino"] = cb.split(":", 1)[1]
        st["step"] = "monto"
        await q.edit_message_text("Monto:")
        return

    if cb.startswith("PERS:"):
        person = cb.split(":", 1)[1]
        data["persona_prestamo"] = person
        mov_type = data.get("mov_type")
        direction = data.get("mov_direction")
        if mov_type == "PRESTAMO":
            account = data.get("loan_account", "")
            if direction == "DAR":
                data["bolsa_remitente"] = BOLSA_NORMAL
                data["remitente"] = account
                data["bolsa_destino"] = "Prestamos"
                data["destino"] = account
            else:
                liquid, _, _ = get_accounts_by_role(context)
                if not data.get("destino"):
                    st["step"] = "loan_collect_account"
                    await q.edit_message_text("Cuenta a dónde entra:", reply_markup=kb_list(liquid, "COLLACC"))
                    return
            st["step"] = "monto"
            await q.edit_message_text("Monto:")
        return

    if cb.startswith("COLLACC:"):
        account = cb.split(":", 1)[1]
        data["bolsa_remitente"] = "Prestamos"
        data["remitente"] = account
        data["bolsa_destino"] = BOLSA_NORMAL
        data["destino"] = account
        st["step"] = "monto"
        await q.edit_message_text("Monto:")
        return

    if cb.startswith("DEUDA:"):
        row_num = int(cb.split(":")[1])

        gc = context.application.bot_data["gc"]
        activas = [d for d in build_deudas(gc, update.effective_user.id) if d["estado"].lower() == "activa" and d["pendientes"] > 0]
        context.user_data["deudas_activas"] = activas
        deuda = next((d for d in activas if d["row"] == row_num), None)

        if not deuda:
            await q.edit_message_text("No encontré esa deuda o ya está pagada.")
            return

        st["data"]["deuda_row"] = deuda["row"]
        st["data"]["deuda_nombre"] = deuda["nombre"]
        st["data"]["deuda_cuota"] = deuda["cuota"]

        cats = get_catalogos(context)
        cuentas = cats["CUENTAS"] if cats else CUENTAS

        excluir = {"ahorro", "prestamos", "ugly", "binance", "osmo", "hapi"}
        cuentas_pago = [c for c in cuentas if c.strip().lower() not in excluir]

        st["step"] = "pagar_deuda_cuenta"
        await q.edit_message_text(
            f"¿Con qué cuenta pagarás {deuda['nombre']} por {format_money_q(deuda['cuota'])}?",
            reply_markup=kb_cuentas_pago(cuentas_pago)
        )
        return

    if cb.startswith("PAGAR_CTA:"):
        cuenta_pago = cb.split(":", 1)[1]

        st["data"]["cuenta_pago"] = cuenta_pago

        try:
            await ejecutar_pago_deuda(context, update.effective_user.id, st["data"])
        except Exception as e:
            st_reset(context)
            await q.edit_message_text(f"No pude registrar el pago. {e}")
            return

        deuda_nombre = st["data"]["deuda_nombre"]
        cuota = st["data"]["deuda_cuota"]

        st_reset(context)
        await q.edit_message_text(
            f"Pago registrado.\n\n"
            f"Deuda: {deuda_nombre}\n"
            f"Cuenta: {cuenta_pago}\n"
            f"Monto: {format_money_q(cuota)}"
        )
        return

    if cb == "CONFIRM:SAVE":
        try:
            validate_flow_data(data)
            await save_to_sheets(context, data, update.effective_user.id)
        except Exception as e:
            await q.edit_message_text(f"No pude guardar. {e}")
            return
        st_reset(context)
        await q.edit_message_text("Guardado correctamente.")
        return

async def on_text(update, context):
    if not allowed(update):
        return

    st = st_get(context)
    step = st["step"]
    data = st["data"]
    txt = update.message.text.strip()

    if step == "wait_date":
        try:
            data["fecha"] = ensure_fecha_text(txt)
        except Exception as e:
            await update.message.reply_text(str(e))
            return
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
            st["step"] = "mov_type"
            await update.message.reply_text("Tipo de movimiento:", reply_markup=kb_mov_type())
        return

    if step == "monto":
        monto = parse_money_text(txt)
        if monto <= 0:
            await update.message.reply_text("El monto debe ser mayor a 0.")
            return
        data["monto"] = monto
        if data["tipo"] == "MOV":
            st["step"] = "monto_destino"
            await update.message.reply_text("Monto destino (si es el mismo, escribe 0):")
        else:
            st["step"] = "metodo"
            cats = get_catalogos(context)
            metodos = cats["METODOS"] if cats else METODOS
            await update.message.reply_text("Método:", reply_markup=kb_list(metodos, "PAY"))
        return

    if step == "monto_destino":
        try:
            v = parse_money_text(txt)
        except Exception:
            v = 0.0
        if v < 0:
            await update.message.reply_text("El monto destino no puede ser negativo.")
            return
        data["monto_destino"] = 0.0 if abs(v) < 0.000001 else v
        st["step"] = "nota"
        await update.message.reply_text("Nota (o -):")
        return

    if step == "nota":
        data["nota"] = "" if txt == "-" else txt
        try:
            validate_flow_data(data)
        except Exception as e:
            await update.message.reply_text(str(e))
            return
        st["step"] = "confirm"
        await update.message.reply_text(render_summary(data), reply_markup=kb_confirm())
        return

    if step == "deuda_nombre":
        if not txt:
            await update.message.reply_text("Escribe el nombre de la deuda.")
            return
        data["deuda_nombre"] = txt
        st["step"] = "deuda_acreedor"
        await update.message.reply_text("¿A quién le debes?")
        return

    if step == "deuda_acreedor":
        if not txt:
            await update.message.reply_text("Debes indicar a quién le debes.")
            return
        data["deuda_acreedor"] = txt
        st["step"] = "deuda_fecha_pago"
        await update.message.reply_text("Fecha de pago (YYYY-MM-DD):")
        return

    if step == "deuda_fecha_pago":
        try:
            data["deuda_fecha_pago"] = ensure_fecha_text(txt)
        except Exception as e:
            await update.message.reply_text(str(e))
            return
        st["step"] = "deuda_cuota"
        await update.message.reply_text("Cuota:")
        return

    if step == "deuda_cuota":
        cuota = parse_money_text(txt)
        if cuota <= 0:
            await update.message.reply_text("La cuota debe ser mayor a 0.")
            return
        data["deuda_cuota"] = cuota
        st["step"] = "deuda_meses"
        await update.message.reply_text("Meses:")
        return

    if step == "deuda_meses":
        try:
            meses = parse_positive_int_text(txt)
        except Exception:
            await update.message.reply_text("Meses debe ser un entero mayor a 0.")
            return
        if meses <= 0:
            await update.message.reply_text("Meses debe ser un entero mayor a 0.")
            return
        data["deuda_meses"] = meses
        st["step"] = "deuda_pagados"
        await update.message.reply_text("Pagados (0 si es nueva):")
        return

    if step == "deuda_pagados":
        try:
            pagados = parse_positive_int_text(txt)
        except Exception:
            await update.message.reply_text("Pagados debe ser un entero igual o mayor a 0.")
            return
        if pagados > int(data.get("deuda_meses", 0)):
            await update.message.reply_text("Pagados no puede ser mayor que meses.")
            return
        data["deuda_pagados"] = pagados
        data["tipo"] = "DEUDA"
        try:
            validate_flow_data(data)
        except Exception as e:
            await update.message.reply_text(str(e))
            return
        st["step"] = "confirm"
        await update.message.reply_text(render_summary(data), reply_markup=kb_confirm())
        return
