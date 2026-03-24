def st_get(context):
    if "flow" not in context.user_data:
        context.user_data["flow"] = {"step": None, "data": {}}
    return context.user_data["flow"]

def st_reset(context):
    context.user_data["flow"] = {"step": None, "data": {}}
