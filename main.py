from datetime import time as dtime

from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

from BotFinanzas.config import BOT_TOKEN, TZ
from BotFinanzas.handlers.commands import (
    ahorro,
    cancelar,
    deudas,
    deudas_activas,
    neto,
    networth,
    nueva_deuda,
    nuevo,
    pagar,
    resumen,
    saldos,
    start,
    whoami,
)
from BotFinanzas.handlers.conversation import on_cb, on_text
from BotFinanzas.jobs import job_resumen_fin_de_mes, job_resumen_semanal
from BotFinanzas.sheets_service import gs_client

def main():
    gc = gs_client()
    app = Application.builder().token(BOT_TOKEN).build()
    app.bot_data["gc"] = gc

    app.job_queue.run_daily(
        job_resumen_semanal,
        time=dtime(hour=21, minute=0, tzinfo=TZ),
        days=(6,),
        name="resumen_semanal_dom_2100",
    )
    app.job_queue.run_daily(
        job_resumen_fin_de_mes,
        time=dtime(hour=21, minute=0, tzinfo=TZ),
        name="resumen_fin_de_mes_ultimo_dia_2100",
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("nuevo", nuevo))
    app.add_handler(CommandHandler("nueva_deuda", nueva_deuda))
    app.add_handler(CommandHandler("cancelar", cancelar))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("resumen", resumen))
    app.add_handler(CommandHandler("saldos", saldos))
    app.add_handler(CommandHandler("ahorro", ahorro))
    app.add_handler(CommandHandler("networth", networth))
    app.add_handler(CommandHandler("deudas", deudas))
    app.add_handler(CommandHandler("deudas_activas", deudas_activas))
    app.add_handler(CommandHandler("pagar", pagar))
    app.add_handler(CommandHandler("neto", neto))

    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    print("Bot finanzas encendido...")
    app.run_polling()

if __name__ == "__main__":
    main()
