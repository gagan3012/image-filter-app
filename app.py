import io, json, time
import streamlit as st

# Google Drive API
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

st.set_page_config(page_title="Image Triplet Filter", layout="wide")

# =========================== Drive helpers ===========================

@st.cache_resource
def get_drive():
    sa_raw = st.secrets["gcp"]["service_account"]
    if isinstance(sa_raw, str):
        if '"private_key"' in sa_raw and "\n" in sa_raw and "\\n" not in sa_raw:
            sa_raw = sa_raw.replace("\r\n", "\\n").replace("\n", "\\n")
        sa = json.loads(sa_raw)
    else:
        sa = dict(sa_raw)

    creds = service_account.Credentials.from_service_account_info(
        sa, scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)

def drive_download_bytes(drive, file_id: str) -> bytes:
    req = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    return buf.read()

def find_file_id_in_folder(drive, folder_id: str, filename: str) -> str | None:
    q = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    resp = drive.files().list(
        q=q, spaces="drive",
        fields="files(id,name)", pageSize=1,
        supportsAllDrives=True, includeItemsFromAllDrives=True, corpora="allDrives"
    ).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else None

def create_shortcut_to_file(drive, src_file_id: str, new_name: str, dest_folder_id: str) -> str:
    """Create a zero-quota shortcut pointing to src_file_id."""
    meta = {
        "name": new_name,
        "mimeType": "application/vnd.google-apps.shortcut",
        "parents": [dest_folder_id],
        "shortcutDetails": {"targetId": src_file_id},
    }
    res = drive.files().create(
        body=meta, fields="id,name",
        supportsAllDrives=True
    ).execute()
    return res["id"]

def read_text_from_drive(drive, file_id: str) -> str:
    return drive_download_bytes(drive, file_id).decode("utf-8", errors="ignore")

def append_lines_to_drive_text(drive, file_id: str, new_lines: list[str]):
    prev = read_text_from_drive(drive, file_id)
    updated = prev + "".join(new_lines)
    media = MediaIoBaseUpload(io.BytesIO(updated.encode("utf-8")), mimetype="text/plain", resumable=False)
    drive.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()

def read_jsonl_from_drive(drive, file_id: str, max_lines: int | None = None):
    # Validate ID & type
    try:
        meta = drive.files().get(
            fileId=file_id, fields="id,name,mimeType,trashed",
            supportsAllDrives=True
        ).execute()
    except HttpError as e:
        st.error(f"Could not access JSONL file. Check ID & sharing.\n\n{e}")
        st.stop()

    if meta.get("trashed"):
        st.error(f"JSONL file `{meta.get('name')}` is in Trash.")
        st.stop()

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

# =========================== Category config ===========================

CAT = {
    "demography": {
        "jsonl_id": st.secrets["gcp"]["demography_jsonl_id"],
        "src_hypo": st.secrets["gcp"]["demography_hypo_folder"],
        "src_adv":  st.secrets["gcp"]["demography_adv_folder"],
        "dst_hypo": st.secrets["gcp"]["demography_hypo_filtered"],   # accepted only (shortcuts)
        "dst_adv":  st.secrets["gcp"]["demography_adv_filtered"],    # accepted only (shortcuts)
        "log_hypo": st.secrets["gcp"]["demography_hypo_filtered_log_id"],
        "log_adv":  st.secrets["gcp"]["demography_adv_filtered_log_id"],
        "hypo_prefix": "dem_h",
        "adv_prefix":  "dem_ah",
    },
    "animal": {
        "jsonl_id": st.secrets["gcp"]["animal_jsonl_id"],
        "src_hypo": st.secrets["gcp"]["animal_hypo_folder"],
        "src_adv":  st.secrets["gcp"]["animal_adv_folder"],
        "dst_hypo": st.secrets["gcp"]["animal_hypo_filtered"],
        "dst_adv":  st.secrets["gcp"]["animal_adv_filtered"],
        "log_hypo": st.secrets["gcp"]["animal_hypo_filtered_log_id"],
        "log_adv":  st.secrets["gcp"]["animal_adv_filtered_log_id"],
        "hypo_prefix": "ani_h",
        "adv_prefix":  "ani_ah",
    },
    "objects": {
        "jsonl_id": st.secrets["gcp"]["objects_jsonl_id"],
        "src_hypo": st.secrets["gcp"]["objects_hypo_folder"],
        "src_adv":  st.secrets["gcp"]["objects_adv_folder"],
        "dst_hypo": st.secrets["gcp"]["objects_hypo_filtered"],
        "dst_adv":  st.secrets["gcp"]["objects_adv_filtered"],
        "log_hypo": st.secrets["gcp"]["objects_hypo_filtered_log_id"],
        "log_adv":  st.secrets["gcp"]["objects_adv_filtered_log_id"],
        "hypo_prefix": "obj_h",
        "adv_prefix":  "obj_ah",
    },
}

drive = get_drive()

@st.cache_data(show_spinner=False)
def load_meta(jsonl_id: str):
    return read_jsonl_from_drive(drive, jsonl_id, max_lines=None)

@st.cache_data(show_spinner=False)
def load_map(file_id: str):
    txt = read_text_from_drive(drive, file_id)
    m = {}
    for ln in txt.splitlines():
        ln = ln.strip()
        if not ln: continue
        try:
            r = json.loads(ln)
            m[r["id"]] = r
        except Exception:
            pass
    return m

# =========================== Router state ===========================

if "page" not in st.session_state: st.session_state.page = "home"
if "cat" not in st.session_state:  st.session_state.cat = None
if "idx" not in st.session_state:  st.session_state.idx = 0
if "dec" not in st.session_state:  st.session_state.dec = {}  # per-id temp decisions: {"hypo": "accepted|rejected|None", "adv": ...}

def go(p): st.session_state.page = p

# =========================== HOME ===========================

if st.session_state.page == "home":
    st.title("Image Triplet Filter")
    cat_pick = st.selectbox("Select category", list(CAT.keys()))
    if st.button("Continue ➜", type="primary", key="home_go"):
        st.session_state.cat = cat_pick
        st.session_state.idx = 0
        st.session_state.dec = {}
        go("dashboard")

# =========================== DASHBOARD ===========================

elif st.session_state.page == "dashboard":
    st.button("⬅️ Back", on_click=lambda: go("home"), key="dash_back")
    cat = st.session_state.cat
    if cat is None:
        st.warning("Pick a category first.")
        go("home")
        st.stop()

    cfg = CAT[cat]
    meta = load_meta(cfg["jsonl_id"])
    log_h = load_map(cfg["log_hypo"])
    log_a = load_map(cfg["log_adv"])

    total = len(meta)
    # “completed” = both sides have at least one decision line saved
    completed = sum(1 for m in meta if m["id"] in log_h and m["id"] in log_a)
    pending = total - completed

    st.subheader(f"Category: **{cat}**")
    c1, c2, c3 = st.columns(3)
    c1.metric("Total", total)
    c2.metric("Completed (pair)", completed)
    c3.metric("Pending", pending)

    if st.button("▶️ Start / Resume", type="primary", key="dash_start"):
        # jump to first example where any side is undecided
        def undecided(i):
            _id = meta[i]["id"]
            return not (_id in log_h and _id in log_a)
        nxt = next((i for i in range(len(meta)) if undecided(i)), 0) if meta else 0
        st.session_state.idx = nxt
        st.session_state.dec = {}
        go("review")

# =========================== REVIEW ===========================

elif st.session_state.page == "review":
    top_l, _ = st.columns([1,6])
    top_l.button("⬅️ Back", on_click=lambda: go("dashboard"), key="rev_back")

    cat = st.session_state.cat
    cfg = CAT[cat]
    meta = load_meta(cfg["jsonl_id"])
    if not meta:
        st.warning("No records.")
        st.stop()

    i = max(0, min(st.session_state.idx, len(meta)-1))
    entry = meta[i]
    _id = entry["id"]

    # load prior saved status (so you see what was done already)
    saved_h = load_map(cfg["log_hypo"]).get(_id, {}).get("status")
    saved_a = load_map(cfg["log_adv"]).get(_id, {}).get("status")

    # init temp decisions from saved if not set this session
    if _id not in st.session_state.dec:
        st.session_state.dec[_id] = {
            "hypo": saved_h,
            "adv":  saved_a
        }

    st.subheader(f"{_id}")

    with st.expander("📝 Text / Descriptions", expanded=True):
        st.markdown(f"**TEXT**: {entry.get('text','')}")
        st.markdown(f"**HYPOTHESIS (non-prototype)**: {entry.get('hypothesis','')}")
        st.markdown(f"**ADVERSARIAL (prototype)**: {entry.get('adversarial','')}")

    # Resolve names → file IDs
    hypo_name = entry.get("hypo_id")
    adv_name  = entry.get("adversarial_id")

    src_h_id = find_file_id_in_folder(drive, cfg["src_hypo"], hypo_name) if hypo_name else None
    src_a_id = find_file_id_in_folder(drive, cfg["src_adv"],  adv_name)  if adv_name  else None

    # Side-by-side with independent Accept/Reject
    c1, c2 = st.columns(2)

    # ---------- Hypothesis pane ----------
    with c1:
        st.markdown("**Hypothesis (non-proto)**")
        if src_h_id:
            st.image(drive_download_bytes(drive, src_h_id), caption=hypo_name, use_column_width=True)
        else:
            st.error(f"Missing image: {hypo_name}")

        b1, b2 = st.columns(2)
        with b1:
            if st.button("✅ Accept (hypo)", key=f"acc_h_{_id}", use_container_width=True):
                st.session_state.dec[_id]["hypo"] = "accepted"
        with b2:
            if st.button("❌ Reject (hypo)", key=f"rej_h_{_id}", use_container_width=True):
                st.session_state.dec[_id]["hypo"] = "rejected"

        st.caption(f"Current: {st.session_state.dec[_id]['hypo'] or '—'}  |  Saved: {saved_h or '—'}")

    # ---------- Adversarial pane ----------
    with c2:
        st.markdown("**Adversarial (proto)**")
        if src_a_id:
            st.image(drive_download_bytes(drive, src_a_id), caption=adv_name, use_column_width=True)
        else:
            st.error(f"Missing image: {adv_name}")

        b3, b4 = st.columns(2)
        with b3:
            if st.button("✅ Accept (adv)", key=f"acc_a_{_id}", use_container_width=True):
                st.session_state.dec[_id]["adv"] = "accepted"
        with b4:
            if st.button("❌ Reject (adv)", key=f"rej_a_{_id}", use_container_width=True):
                st.session_state.dec[_id]["adv"] = "rejected"

        st.caption(f"Current: {st.session_state.dec[_id]['adv'] or '—'}  |  Saved: {saved_a or '—'}")

    st.divider()

    # ---------- Save both decisions ----------
    def save_now():
        dec = st.session_state.dec[_id]
        ts  = int(time.time())

        # build base record (keep full metadata for downstream)
        base = dict(entry)

        # hypo record
        rec_h = dict(base)
        rec_h.update({
            "side": "hypothesis",
            "status": dec["hypo"] or "rejected",   # default to rejected if None
            "decided_at": ts,
        })
        # adv record
        rec_a = dict(base)
        rec_a.update({
            "side": "adversarial",
            "status": dec["adv"] or "rejected",
            "decided_at": ts,
        })

        # If accepted → create shortcut in filtered folder
        try:
            if dec["hypo"] == "accepted" and src_h_id:
                new_h_id = create_shortcut_to_file(drive, src_h_id, hypo_name, cfg["dst_hypo"])
                rec_h["copied_id"] = new_h_id
            if dec["adv"] == "accepted" and src_a_id:
                new_a_id = create_shortcut_to_file(drive, src_a_id, adv_name, cfg["dst_adv"])
                rec_a["copied_id"] = new_a_id
        except HttpError as e:
            st.error(
                f"Drive copy/shortcut failed.\n\n"
                f"Source hypo: {src_h_id}\nSource adv: {src_a_id}\n"
                f"Dest hypo folder: {cfg['dst_hypo']}\nDest adv folder: {cfg['dst_adv']}\n\n"
                f"{e}"
            )
            return

        # Write 2 JSONL lines (one per side)
        try:
            append_lines_to_drive_text(drive, cfg["log_hypo"], [json.dumps(rec_h, ensure_ascii=False) + "\n"])
            append_lines_to_drive_text(drive, cfg["log_adv"],  [json.dumps(rec_a, ensure_ascii=False) + "\n"])
            load_map.clear()  # refresh saved status on next run
            st.success("Saved.")
        except HttpError as e:
            st.error(f"Failed to append logs: {e}")
            return

    nav_l, save_c, nav_r = st.columns([1,2,1])
    with nav_l:
        if st.button("⏮ Prev", key=f"prev_{_id}", use_container_width=True):
            st.session_state.idx = max(0, i-1)
            st.rerun()
    with save_c:
        if st.button("💾 Save", type="primary", key=f"save_{_id}", use_container_width=True):
            save_now()
    with nav_r:
        if st.button("Next ⏭", key=f"next_{_id}", use_container_width=True):
            st.session_state.idx = min(len(meta)-1, i+1)
            st.rerun()
