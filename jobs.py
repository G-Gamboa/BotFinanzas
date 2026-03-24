from datetime import datetime, timedelta

from .config import TZ, USER_SHEETS
from .finance import build_resumen_mes, build_resumen_semana

def is_last_day_of_month(d):
    return (d + timedelta(days=1)).day == 1

async def job_resumen_semanal(context):
    gc = context.application.bot_data["gc"]
    bot = context.bot
    for uid_str in USER_SHEETS.keys():
        uid = int(uid_str)
        try:
            txt = build_resumen_semana(gc, uid)
            await bot.send_message(chat_id=uid, text=txt)
        except Exception:
            pass

async def job_resumen_fin_de_mes(context):
    hoy = datetime.now(TZ).date()
    if not is_last_day_of_month(hoy):
        return
    gc = context.application.bot_data["gc"]
    bot = context.bot
    for uid_str in USER_SHEETS.keys():
        uid = int(uid_str)
        try:
            txt = build_resumen_mes(gc, uid)
            await bot.send_message(chat_id=uid, text=f"Fin de mes:\n\n{txt}")
        except Exception:
            pass
