
import os
import streamlit as st
import dropbox
from dropbox.exceptions import AuthError, ApiError

st.set_page_config(page_title="Dropbox API connection test", page_icon="🧪", layout="centered")
st.title("🔐 Dropbox API connection test")

# SAFE prefill: try secrets, then env var, else empty — all without crashing
def get_prefill_token() -> str:
    # Prefer env var first (doesn't throw), then attempt secrets
    token = os.getenv("DROPBOX_TOKEN", "")
    if token:
        return token
    try:
        # This may raise StreamlitSecretNotFoundError if no secrets.toml exists
        return st.secrets["DROPBOX_TOKEN"]
    except Exception:
        return ""

token = st.text_input(
    "Dropbox access token",
    value=get_prefill_token(),
    type="password",
    help="Paste a short- or long-lived token. For production, use secrets or OAuth with refresh tokens."
)

def check_dropbox_connection(access_token: str) -> dict:
    result = {"ok": False, "account": None, "message": ""}
    try:
        dbx = dropbox.Dropbox(access_token)
        account = dbx.users_get_current_account()
        result["account"] = {
            "name": account.name.display_name,
            "email": getattr(account, "email", None),
            "account_id": account.account_id,
        }
        dbx.files_list_folder(path="")  # scope/content check
        result["ok"] = True
        result["message"] = "Dropbox connection successful. Token is valid and has read access."
    except AuthError:
        result["message"] = (
            "Authentication failed. The access token is invalid or expired. "
            "Generate a new token or implement OAuth refresh."
        )
    except ApiError as e:
        result["message"] = f"Dropbox API error: {e}"
    except Exception as e:
        result["message"] = f"Unexpected error: {e}"
    return result

if st.button("Check connection", type="primary"):
    if not token:
        st.error("Please paste an access token.")
    else:
        with st.spinner("Contacting Dropbox..."):
            res = check_dropbox_connection(token)
        if res["ok"]:
            st.success(res["message"])
            st.subheader("Account")
            st.json(res["account"])
        else:
            st.error(res["message"])
