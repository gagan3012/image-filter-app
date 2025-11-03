import io, json, streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

st.set_page_config(page_title="Image Triplet Filter", layout="wide")
st.title("Image Triplet Filter")

# ---------- Google Drive helpers ----------
@st.cache_resource
def get_drive():
    # Be tolerant to secrets pasted with """ ... """ (which turns \n into real newlines)
    sa_raw = st.secrets["gcp"]["service_account"]
    if isinstance(sa_raw, str):
        # if there are real newlines inside the private key, re-escape them
        if "private_key" in sa_raw and "\n" in sa_raw and "\\n" not in sa_raw:
            sa_raw = sa_raw.replace("\r\n", "\\n").replace("\n", "\\n")
        sa_info = json.loads(sa_raw)
    else:
        sa_info = dict(sa_raw)

    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)

def drive_download_bytes(drive, file_id: str) -> bytes:
    req = drive.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    return buf.read()

def drive_upload_bytes(drive, parent_id: str, name: str, data: bytes, mime="text/plain", file_id: str | None=None):
    media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime, resumable=False)
    if file_id:
        return drive.files().update(fileId=file_id, media_body=media).execute()
    body = {"name": name, "parents": [parent_id], "mimeType": mime}
    return drive.files().create(body=body, media_body=media, fields="id,name").execute()

def find_file_id_in_folder(drive, folder_id: str, filename: str) -> str | None:
    q = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    resp = drive.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=1).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None

def ensure_empty_txt_in_folder(drive, parent_id: str, name: str) -> str:
    fid = find_file_id_in_folder(drive, parent_id, name)
    if fid:
        return fid
    return drive_upload_bytes(drive, parent_id, name, b"", "text/plain")["id"]

def copy_file_to_folder(drive, src_file_id: str, new_name: str, dest_folder_id: str) -> str:
    body = {"name": new_name, "parents": [dest_folder_id]}
    return drive.files().copy(fileId=src_file_id, body=body, fields="id,name").execute()["id"]

def read_jsonl_from_drive(drive, file_id: str, max_lines: int | None = None):
    raw = drive_download_bytes(drive, file_id).decode("utf-8", errors="ignore")
    out = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
        if max_lines and len(out) >= max_lines:
            break
    return out

def read_text_from_drive(drive, file_id: str) -> str:
    return drive_download_bytes(drive, file_id).decode("utf-8", errors="ignore")

def append_lines_to_drive_text(drive, file_id: str, new_lines: list[str]):
    prev = read_text_from_drive(drive, file_id)
    updated = prev + "".join(new_lines)
    drive_upload_bytes(drive, parent_id="", name="", data=updated.encode("utf-8"), file_id=file_id)

# ---------- Config per category ----------
CATEGORY_CFG = {
    "demography": {
        "jsonl_id": st.secrets["gcp"]["demography_jsonl_id"],
        "src_hypo": st.secrets["gcp"]["demography_hypo_folder"],
        "src_adv":  st.secrets["gcp"]["demography_adv_folder"],
        "dst_hypo": st.secrets["gcp"]["demography_hypo_filtered"],
        "dst_adv":  st.secrets["gcp"]["demography_adv_filtered"],
        "hypo_prefix": "dem_h",
        "adv_prefix":  "dem_ah",
        "log_name": "demography_filtered.jsonl",
        "log_id_secret": "demography_filtered_log_id",
    },
    "animal": {
        "jsonl_id": st.secrets["gcp"]["animal_jsonl_id"],
        "src_hypo": st.secrets["gcp"]["animal_hypo_folder"],
        "src_adv":  st.secrets["gcp"]["animal_adv_folder"],
        "dst_hypo": st.secrets["gcp"]["animal_hypo_filtered"],
        "dst_adv":  st.secrets["gcp"]["animal_adv_filtered"],
        "hypo_prefix": "ani_h",
        "adv_prefix":  "ani_ah",
        "log_name": "animal_filtered.jsonl",
        "log_id_secret": "animal_filtered_log_id",
    },
    "objects": {
        "jsonl_id": st.secrets["gcp"]["objects_jsonl_id"],
        "src_hypo": st.secrets["gcp"]["objects_hypo_folder"],
        "src_adv":  st.secrets["gcp"]["objects_adv_folder"],
        "dst_hypo": st.secrets["gcp"]["objects_hypo_filtered"],
        "dst_adv":  st.secrets["gcp"]["objects_adv_filtered"],
        "hypo_prefix": "obj_h",
        "adv_prefix":  "obj_ah",
        "log_name": "objects_filtered.jsonl",
        "log_id_secret": "objects_filtered_log_id",
    },
}

drive = get_drive()

# ---------- Sidebar ----------
cat = st.sidebar.selectbox("Category", list(CATEGORY_CFG.keys()))
limit = st.sidebar.number_input("Load first N records", 50, 10000, 500, 50)

cfg = CATEGORY_CFG[cat]

# Prefer per-category log file id if provided; else fall back to parent folder creation.
log_file_id = None
try:
    per_cat_log_id = st.secrets["gcp"][cfg["log_id_secret"]]
    if per_cat_log_id:
        log_file_id = per_cat_log_id
except Exception:
    pass

if not log_file_id:
    # will raise KeyError if not set, which is fine (explicit)
    logs_parent = st.secrets["gcp"]["filtered_logs_parent"]
    log_file_id = ensure_empty_txt_in_folder(drive, logs_parent, cfg["log_name"])

# ---------- Navigation ----------
c1, c2, c3 = st.columns([1,1,4])
with c1:
    if st.button("⏮ Prev undecided", use_container_width=True):
        # find previous undecided
        j = st.session_state.idx - 1
        while j >= 0 and meta[j]["id"] in decided_map:
            j -= 1
        if j >= 0: st.session_state.idx = j
with c2:
    if st.button("Next undecided ⏭", use_container_width=True):
        st.session_state.idx = jump_to_next_undecided(st.session_state.idx + 1)
with c3:
    st.write(f"Record {st.session_state.idx+1} / {len(meta)} (undecided left: {sum(1 for m in meta if m['id'] not in decided_map)})")

if not meta:
    st.warning("No records loaded.")
    st.stop()

entry = meta[st.session_state.idx]
st.subheader(entry.get("id","(no id)"))

# ---------- Texts ----------
with st.expander("📝 Text fields", expanded=True):
    st.markdown(f"**TEXT**: {entry.get('text','')}")
    st.markdown(f"**HYPOTHESIS (non-prototype)**: {entry.get('hypothesis','')}")
    st.markdown(f"**ADVERSARIAL (prototype)**: {entry.get('adversarial','')}")

# ---------- Resolve source images in Drive ----------
hypo_name = entry.get("hypo_id")
adv_name  = entry.get("adversarial_id")

src_h_id = find_file_id_in_folder(drive, cfg["src_hypo"], hypo_name) if hypo_name else None
src_a_id = find_file_id_in_folder(drive, cfg["src_adv"],  adv_name)  if adv_name  else None

col1, col2 = st.columns(2)
with col1:
    st.markdown("**Hypothesis (non-proto)**")
    if src_h_id:
        st.image(drive_download_bytes(drive, src_h_id), caption=hypo_name, use_column_width=True)
    else:
        st.error(f"Missing: {hypo_name}")
with col2:
    st.markdown("**Adversarial (proto)**")
    if src_a_id:
        st.image(drive_download_bytes(drive, src_a_id), caption=adv_name, use_column_width=True)
    else:
        st.error(f"Missing: {adv_name}")

# ---------- Select / Reject ----------
sel_col, rej_col, skip_col = st.columns([1,1,2])

def write_decision(status: str, copied_h=None, copied_a=None):
    # make a single JSON line with full metadata + status + (optional) copied ids
    rec = dict(entry)
    rec.update({
        "status": status,
        "hypo_copied_id": copied_h,
        "adv_copied_id":  copied_a
    })
    append_lines_to_drive_text(drive, log_file_id, [json.dumps(rec, ensure_ascii=False) + "\n"])
    # clear caches for decisions and advance
    load_decisions_map.clear()
    st.session_state.idx = jump_to_next_undecided(st.session_state.idx + 1)
    st.experimental_rerun()

with sel_col:
    if st.button("✅ SELECT (copy & log)", type="primary", use_container_width=True, disabled=not (src_h_id and src_a_id)):
        # copy both images to filtered folders
        new_h_id = copy_file_to_folder(drive, src_h_id, hypo_name, cfg["dst_hypo"]) if src_h_id else None
        new_a_id = copy_file_to_folder(drive, src_a_id,  adv_name, cfg["dst_adv"])  if src_a_id else None
        write_decision("selected", new_h_id, new_a_id)

with rej_col:
    if st.button("❌ REJECT (log only)", use_container_width=True):
        write_decision("rejected")

with skip_col:
    if st.button("↪️ Skip", use_container_width=True):
        st.session_state.idx = jump_to_next_undecided(st.session_state.idx + 1)
        st.experimental_rerun()

# =============================================================================================================================
# import io, json, time
# from typing import Dict, List, Optional
# import streamlit as st
# from PIL import Image

# # ---------- AUTH & DRIVE HELPERS ----------
# from google.oauth2.service_account import Credentials
# from pydrive2.auth import GoogleAuth
# from pydrive2.drive import GoogleDrive

# SCOPES = [
#     "https://www.googleapis.com/auth/drive.file",
#     "https://www.googleapis.com/auth/drive.metadata.readonly",
# ]

# @st.cache_resource(show_spinner=False)
# def get_drive() -> GoogleDrive:
#     sa_info = dict(st.secrets["gcp_service_account"])  # service account JSON from secrets
#     creds = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
#     gauth = GoogleAuth(settings={
#         "client_config_backend": "service",
#         "service_config": {
#             "client_user_email": sa_info["client_email"],
#             "client_json": sa_info,
#         },
#         "save_credentials": False,
#         "get_refresh_token": False,
#         "oauth_scope": " ".join(SCOPES),
#     })
#     gauth.credentials = creds
#     return GoogleDrive(gauth)

# def drive_list_children(drive: GoogleDrive, parent_id: str, name_eq: Optional[str] = None, mime_type: Optional[str] = None) -> List[dict]:
#     q = f"'{parent_id}' in parents and trashed=false"
#     if name_eq:
#         q += f" and name='{name_eq}'"
#     if mime_type:
#         q += f" and mimeType='{mime_type}'"
#     files = drive.ListFile({'q': q, 'fields': 'files(id, name, mimeType, parents)'}).GetList()
#     return files

# def drive_ensure_folder(drive: GoogleDrive, parent_id: str, folder_name: str) -> str:
#     exist = drive_list_children(drive, parent_id, name_eq=folder_name, mime_type="application/vnd.google-apps.folder")
#     if exist:
#         return exist[0]['id']
#     f = drive.CreateFile({'title': folder_name, 'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [{'id': parent_id}]})
#     f.Upload()
#     return f['id']

# def drive_read_text(drive: GoogleDrive, file_id: str) -> str:
#     f = drive.CreateFile({'id': file_id})
#     return f.GetContentString()

# def drive_write_text(drive: GoogleDrive, parent_id: str, name: str, text: str, file_id: Optional[str] = None) -> str:
#     if file_id:
#         f = drive.CreateFile({'id': file_id})
#     else:
#         f = drive.CreateFile({'title': name, 'name': name, 'parents': [{'id': parent_id}]})
#     f.SetContentString(text, encoding='utf-8')
#     f.Upload()
#     return f['id']

# def drive_find_file(drive: GoogleDrive, parent_id: str, name: str) -> Optional[str]:
#     hit = drive_list_children(drive, parent_id, name_eq=name)
#     return hit[0]['id'] if hit else None

# def drive_download_image(drive: GoogleDrive, parent_id: str, filename: str) -> Optional[Image.Image]:
#     file_id = drive_find_file(drive, parent_id, filename)
#     if not file_id:
#         return None
#     f = drive.CreateFile({'id': file_id})
#     buf = io.BytesIO()
#     f.GetContentFile('/tmp/_tmp_img', mimetype='image/png')  # fallback path
#     # re-open to PIL
#     try:
#         img = Image.open('/tmp/_tmp_img').convert("RGB")
#         return img
#     except Exception:
#         # alternate: read bytes directly
#         b = io.BytesIO(f.GetContentBinary())
#         try:
#             return Image.open(b).convert("RGB")
#         except Exception:
#             return None

# def drive_copy_to_folder(drive: GoogleDrive, src_parent_id: str, filename: str, dst_parent_id: str) -> Optional[str]:
#     src_id = drive_find_file(drive, src_parent_id, filename)
#     if not src_id:
#         return None
#     src = drive.CreateFile({'id': src_id})
#     copied = drive.CreateFile({'title': filename, 'parents': [{'id': dst_parent_id}]})
#     copied.SetContentFile('/tmp/_tmp_copy')  # we must download+reupload for service accounts reliably
#     # download then upload
#     src.GetContentFile('/tmp/_tmp_copy')
#     copied.Upload()
#     return copied['id']

# # ---------- SIMPLE LOGIN ----------
# # def require_login():
# #     if "auth_user" not in st.session_state:
# #         st.session_state.auth_user = None

# #     if st.session_state.auth_user:
# #         return True

# #     st.title("Image Filtering Login")
# #     user = st.text_input("Name", placeholder="e.g., Gagan")
# #     pwd  = st.text_input("Password", type="password", placeholder="Provided password")
# #     if st.button("Sign in"):
# #         users = st.secrets.get("users", {})
# #         if user in users and str(users[user]) == str(pwd):
# #             st.session_state.auth_user = user
# #             st.experimental_rerun()
# #         else:
# #             st.error("Invalid credentials.")
# #     st.stop()
# def require_login():
#     if "auth_user" not in st.session_state:
#         st.session_state.auth_user = None

#     if st.session_state.auth_user:
#         return True

#     st.title("Image Filtering Login")
#     user_in = st.text_input("Name", placeholder="e.g., Gagan")
#     pwd_in  = st.text_input("Password", type="password", placeholder="Provided password")

#     if st.button("Sign in"):
#         users_map = st.secrets.get("users", {})
#         # normalize: case-insensitive username, strip spaces
#         norm = {k.strip().casefold(): str(v) for k, v in users_map.items()}
#         if user_in.strip().casefold() in norm and str(pwd_in) == norm[user_in.strip().casefold()]:
#             st.session_state.auth_user = user_in.strip()
#             st.experimental_rerun()
#         else:
#             st.error("Invalid credentials.")
#     st.stop()
# # ---------- APP ----------
# def main():
#     require_login()
#     user = st.session_state.auth_user
#     st.sidebar.success(f"Logged in as: {user}")

#     drive = get_drive()

#     st.sidebar.header("Drive config (one-time)")
#     base_folder_id = st.sidebar.text_input(
#         "Dataset images BASE folder ID",
#         help="The Google Drive folder ID of: dataset/images",
#         placeholder="e.g., 1AbCDeFg... (folder id)"
#     )

#     if not base_folder_id:
#         st.info("Paste your `dataset/images` folder ID in the sidebar to continue.")
#         st.stop()

#     # discover category subfolders & JSONLs by name
#     # Required structure:
#     # dataset/images/
#     #   demography/hypo, demography/adv_hypo
#     #   animal/hypo, animal/adv_hypo
#     #   objects/hypo, objects/adv_hypo
#     # and JSONL files in dataset/images parent: demography_images.jsonl, animal_images.jsonl, objects_images.jsonl

#     # Find category folders
#     def find_category(root_id: str, cat: str):
#         cat_id = drive_ensure_folder(drive, root_id, cat)
#         hypo_id = drive_ensure_folder(drive, cat_id, "hypo")
#         adv_id  = drive_ensure_folder(drive, cat_id, "adv_hypo")
#         # filtered subfolders under category
#         filtered_id = drive_ensure_folder(drive, cat_id, "filtered")
#         f_hypo = drive_ensure_folder(drive, filtered_id, "hypo")
#         f_adv  = drive_ensure_folder(drive, filtered_id, "adv_hypo")
#         return dict(cat=cat_id, hypo=hypo_id, adv=adv_id, filt=filtered_id, filt_hypo=f_hypo, filt_adv=f_adv)

#     cats = {
#         "demography": find_category(base_folder_id, "demography"),
#         "animal":     find_category(base_folder_id, "animal"),
#         "objects":    find_category(base_folder_id, "objects"),
#     }

#     # Find JSONL files in BASE (parent of category folders)
#     # These are one level up (the same base folder)
#     def find_jsonl(root_id: str, name: str) -> Optional[str]:
#         return drive_find_file(drive, root_id, name)

#     jsonl_ids = {
#         "demography": find_jsonl(base_folder_id, "demography_images.jsonl"),
#         "animal":     find_jsonl(base_folder_id, "animal_images.jsonl"),
#         "objects":    find_jsonl(base_folder_id, "objects_images.jsonl"),
#     }

#     missing = [k for k,v in jsonl_ids.items() if v is None]
#     if missing:
#         st.warning(f"Missing JSONL in base folder for: {', '.join(missing)}. "
#                    f"Expected files: demography_images.jsonl, animal_images.jsonl, objects_images.jsonl")

#     cat_choice = st.sidebar.radio("Category", ["demography", "animal", "objects"], horizontal=True)
#     st.title(f"Filtering · {cat_choice}")

#     # Load JSONL (cached per file id)
#     @st.cache_data(show_spinner=False)
#     def load_jsonl(fid: str) -> List[dict]:
#         raw = drive_read_text(drive, fid)
#         rows = []
#         for line in raw.splitlines():
#             line = line.strip()
#             if not line:
#                 continue
#             try:
#                 rows.append(json.loads(line))
#             except Exception:
#                 pass
#         return rows

#     file_id = jsonl_ids.get(cat_choice)
#     if not file_id:
#         st.stop()

#     data = load_jsonl(file_id)
#     total = len(data)
#     if total == 0:
#         st.info("No records found in JSONL.")
#         st.stop()

#     # Progress file per user+category
#     progress_name = f"progress_{user}_{cat_choice}.json"
#     prog_id = drive_find_file(drive, base_folder_id, progress_name)
#     if prog_id:
#         try:
#             prog = json.loads(drive_read_text(drive, prog_id))
#         except Exception:
#             prog = {"idx": 0}
#     else:
#         prog = {"idx": 0}

#     idx = st.session_state.get("idx", prog.get("idx", 0))
#     idx = max(0, min(idx, total-1))

#     # Current record
#     row = data[idx]

#     # Resolve folders for images
#     folders = cats[cat_choice]
#     hypo_folder = folders["hypo"]
#     adv_folder  = folders["adv"]
#     filt_hypo   = folders["filt_hypo"]
#     filt_adv    = folders["filt_adv"]

#     # Display metadata
#     with st.expander("Metadata", expanded=True):
#         left, right = st.columns(2)
#         with left:
#             st.markdown(f"**ID**: `{row.get('id','')}`")
#             st.markdown(f"**Knob**: `{row.get('knob','')}` · **Attr**: `{row.get('attr_token','')}`")
#             st.markdown(f"**Category fields**: `{row.get('group_category','')}` / `{row.get('socio_attr','')}` / `{row.get('pole','')}`")
#         with right:
#             st.code(row.get("text",""), language=None)
#             st.code(row.get("hypothesis",""), language=None)
#             st.code(row.get("adversarial",""), language=None)

#     # Load images
#     hyp_name = row.get("hypo_id")
#     adv_name = row.get("adversarial_id")
#     hyp_img = drive_download_image(drive, hypo_folder, hyp_name) if hyp_name else None
#     adv_img = drive_download_image(drive, adv_folder,  adv_name) if adv_name else None

#     c1, c2 = st.columns(2)
#     with c1:
#         st.subheader("Hypothesis (should be TRUE)")
#         if hyp_img: st.image(hyp_img, use_column_width=True)
#         else: st.error(f"Image not found: {hyp_name}")
#     with c2:
#         st.subheader("Adversarial (should be SLIGHTLY WRONG)")
#         if adv_img: st.image(adv_img, use_column_width=True)
#         else: st.error(f"Image not found: {adv_name}")

#     st.caption(f"{idx+1} / {total}")

#     colA, colB, colC, colD = st.columns([1,1,1,2])
#     if colA.button("⬅️ Back", disabled=(idx==0)):
#         idx = max(0, idx-1)
#         st.session_state.idx = idx
#         st.rerun()

#     approved = colB.button("✅ Approve")
#     rejected = colC.button("🚫 Reject")

#     if approved or rejected:
#         # on approve: copy images to filtered/{category}/hypo|adv_hypo and append to filtered jsonl
#         if approved and hyp_name and adv_name:
#             drive_copy_to_folder(drive, hypo_folder, hyp_name, filt_hypo)
#             drive_copy_to_folder(drive,  adv_folder,  adv_name,  filt_adv)

#             # append to filtered jsonl in base
#             filt_name = f"{cat_choice}_filtered.jsonl"
#             filt_id = drive_find_file(drive, base_folder_id, filt_name)
#             line = json.dumps(row, ensure_ascii=False)
#             if filt_id:
#                 # read-append-write (simple & safe for small/med files)
#                 existing = drive_read_text(drive, filt_id) + ("\n" if not existing_endswith_newline(existing:=drive_read_text(drive, filt_id)) else "")
#                 drive_write_text(drive, base_folder_id, filt_name, existing + line + "\n", file_id=filt_id)
#             else:
#                 drive_write_text(drive, base_folder_id, filt_name, line + "\n", file_id=None)

#         # advance index
#         idx = min(total-1, idx+1)
#         st.session_state.idx = idx
#         # persist progress
#         prog["idx"] = idx
#         if prog_id:
#             drive_write_text(drive, base_folder_id, progress_name, json.dumps(prog), file_id=prog_id)
#         else:
#             prog_id = drive_write_text(drive, base_folder_id, progress_name, json.dumps(prog), file_id=None)
#         st.rerun()

# def existing_endswith_newline(s: str) -> bool:
#     return len(s) > 0 and s[-1] == "\n"

# import streamlit as st
# from google.oauth2 import service_account
# from googleapiclient.discovery import build

# st.set_page_config(page_title="Image Filtering", layout="wide")
# st.set_option('client.showErrorDetails', True)

# @st.cache_resource(show_spinner=False)
# def get_drive_client(creds_dict):
#     creds = service_account.Credentials.from_service_account_info(
#         creds_dict,
#         scopes=["https://www.googleapis.com/auth/drive"]
#     )
#     return build("drive", "v3", credentials=creds, cache_discovery=False)

# @st.cache_data(show_spinner=False)
# def list_folder_files(drive, folder_id):
#     # lightweight listing; defer image fetch until needed
#     q = f"'{folder_id}' in parents and trashed = false"
#     files = drive.files().list(q=q, fields="files(id,name,mimeType,size)").execute().get("files", [])
#     return {f["name"]: f["id"] for f in files}

# def main():
#     st.title("Image Filtering")

#     # --- LOGIN ---
#     users = st.secrets["users"]
#     with st.sidebar:
#         st.header("Login")
#         username = st.text_input("Name")
#         password = st.text_input("Password", type="password")
#     if not username or users.get(username) != password:
#         st.info("Enter valid credentials to continue.")
#         st.stop()

#     # --- Drive init is lazy; only if we have secrets ---
#     gcp = st.secrets["gcp_service_account"]
#     drive = get_drive_client(gcp)

#     # --- Small health check (does not scan everything) ---
#     with st.sidebar:
#         st.header("Data source")
#         folder_id = st.text_input("Google Drive folder ID (root that contains dataset/images/*)")
#         if st.button("Test connection"):
#             try:
#                 _ = list_folder_files(drive, folder_id)
#                 st.success("Drive reachable ✅")
#             except Exception as e:
#                 st.error(f"Drive error: {e}")
#                 st.stop()

#     # Defer the rest of your UI… (load category → then list one JSONL → then load 1 pair)
#     # ...

# if __name__ == "__main__":
#     main()
