from .config import USER_SHEETS

def allowed(update) -> bool:
    if update.message and update.message.text == "/whoami":
        return True
    uid = str(update.effective_user.id) if update.effective_user else ""
    return uid in USER_SHEETS
