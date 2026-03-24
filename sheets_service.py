import gspread
from google.oauth2.service_account import Credentials

from .config import SERVICE_ACCOUNT_INFO, USER_SHEETS

def get_sheet_for_user(gc, uid: int):
    uid_str = str(uid)
    sheet_id = USER_SHEETS.get(uid_str)
    if not sheet_id:
        raise RuntimeError("Tu usuario no tiene Sheet configurado.")
    return gc.open_by_key(sheet_id)

def gs_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(SERVICE_ACCOUNT_INFO, scopes=scopes)
    return gspread.authorize(creds)
