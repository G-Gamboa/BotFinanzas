from .catalogs import load_catalogos
from .config import CUENTAS
from .sheets_service import get_sheet_for_user

async def ensure_catalogs(update, context):
    gc = context.application.bot_data["gc"]
    sh = get_sheet_for_user(gc, update.effective_user.id)
    cats = load_catalogos(sh)
    context.user_data["catalogos"] = cats
    context.user_data["cuentas"] = cats.get("CUENTAS") or CUENTAS
