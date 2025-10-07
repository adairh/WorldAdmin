# WorldAdmin_v2.py
# One-file Flask Admin for Đại Việt (Trần, TK XIII)
# Enhancements vs v1:
# - File-locking & atomic saves for basic multi-user safety
# - Server-Sent Events (SSE) broadcast for live notifications & soft-collab
# - Change feed (who/what) + client toasts
# - Person profile normalization: single source of truth + placeholders ({{name}}, {{age}}, ...)
# - One-click "Normalize All" tool to migrate old data into the new structure
# - Persona + Info unified via "profile" with overlay persona (no double-edit)
# - Placeholder-aware Dialogue/Notes rendering helpers
# - Per-NPC chat history, improved in-character chat prompt (no default assistant voice)
# - Global Chat panel (no need to open entry detail first)
# - Small UX boosts: better placeholders, safer JSON parsing, modest pagination hooks
#
# Run:
#   pip install flask>=3.0.0 openai>=2.1.0
#   set OPENAI_API_KEY=...
#   python WorldAdmin_v2.py --file world_with_personas.json --port 8000

from __future__ import annotations
import argparse
import datetime as dt
import json
import os
import threading
from pathlib import Path
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, jsonify, request, Response, stream_with_context
from openai import OpenAI

app = Flask(__name__)

DATA_PATH = "world_with_personas.json"
DATA: List[Dict[str, Any]] = []
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# --- Concurrency & Change Feed ---
_file_lock = threading.Lock()

class ChangeBus:
    def __init__(self):
        self._subscribers: List[queue.Queue] = []
        self._lock = threading.Lock()
    def publish(self, event: Dict[str, Any]):
        with self._lock:
            for q in list(self._subscribers):
                try:
                    q.put_nowait(event)
                except Exception:
                    pass
    def subscribe(self):
        q: queue.Queue = queue.Queue()
        with self._lock:
            self._subscribers.append(q)
        return q
    def unsubscribe(self, q):
        with self._lock:
            if q in self._subscribers:
                self._subscribers.remove(q)

import queue
change_bus = ChangeBus()

# --------- Helpers ---------

def load_data(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def ensure_backup(path: str, payload: Any, keep_last: int = 15):
    os.makedirs(".backups", exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    bak_path = os.path.join(".backups", f"{os.path.basename(path)}.{ts}.bak.json")
    with open(bak_path, "w", encoding="utf-8") as fw:
        json.dump(payload, fw, ensure_ascii=False, indent=2)
    # Prune old backups
    try:
        files = sorted(Path(".backups").glob(f"{os.path.basename(path)}.*.bak.json"), key=lambda p: p.stat().st_mtime)
        for old in files[:-keep_last]:
            try: old.unlink()
            except Exception: pass
    except Exception:
        pass

def atomic_save(path: str, payload: Any):
    with _file_lock:
        ensure_backup(path, payload)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fw:
            json.dump(payload, fw, ensure_ascii=False, indent=2)
        os.replace(tmp, path)

save_data = atomic_save  # alias


def find_index_by_id(entries: List[Dict[str, Any]], id_val: int) -> int:
    for i, e in enumerate(entries):
        try:
            if int(e.get("id")) == int(id_val):
                return i
        except Exception:
            continue
    return -1


def is_khu(entry: Dict[str, Any]) -> bool:
    return entry.get("class") == "khu"


def is_cong_trinh(entry: Dict[str, Any]) -> bool:
    return entry.get("class") == "cong_trinh"


def initials_from(name: str, fallback="ID") -> str:
    s = "".join(ch for ch in (name or "") if ch.isalpha()).upper()
    return (s[:3] if s else fallback)


def get_person(entry: Dict[str, Any], resident_id: str) -> Tuple[str, Dict[str, Any], int]:
    if is_khu(entry):
        arr = entry.setdefault("residents", [])
        for i, r in enumerate(arr):
            if r.get("resident_id") == resident_id:
                return "residents", r, i
    elif is_cong_trinh(entry):
        arr = entry.setdefault("staff", [])
        for i, r in enumerate(arr):
            if r.get("resident_id") == resident_id:
                return "staff", r, i
    return "", {}, -1

# ---------- In-memory indices for O(k) relation cleanup ----------
PERSON_INDEX: dict[str, tuple[int, str, dict]] = {}
REVERSE_REL: dict[str, set[str]] = {}


def _collect_outgoing_links(person: dict) -> set[str]:
    out = set()
    fam = person.get("family", {}) or {}
    spouse = fam.get("spouse_id")
    if isinstance(spouse, str) and spouse:
        out.add(spouse)
    for k in ("parents_ids", "children_ids", "siblings_ids"):
        for rid in fam.get(k, []) or []:
            if isinstance(rid, str) and rid:
                out.add(rid)
    for rel in person.get("relations", []) or []:
        tid = rel.get("target_id")
        if isinstance(tid, str) and tid:
            out.add(tid)
    return out


def _register_person(entry_idx: int, kind: str, person: dict):
    rid = person.get("resident_id")
    if not isinstance(rid, str) or not rid:
        return
    PERSON_INDEX[rid] = (entry_idx, kind, person)
    for tgt in _collect_outgoing_links(person):
        REVERSE_REL.setdefault(tgt, set()).add(rid)


def _unregister_person(person: dict):
    rid = person.get("resident_id")
    if not isinstance(rid, str) or not rid:
        return
    REVERSE_REL.pop(rid, None)
    for tgt in list(REVERSE_REL.keys()):
        bucket = REVERSE_REL[tgt]
        if rid in bucket:
            bucket.discard(rid)
            if not bucket:
                REVERSE_REL.pop(tgt, None)
    PERSON_INDEX.pop(rid, None)


def _reindex_person(entry_idx: int, kind: str, old_person: dict, new_person: dict):
    rid = new_person.get("resident_id")
    if not isinstance(rid, str) or not rid:
        return
    for tgt in _collect_outgoing_links(old_person or {}):
        bucket = REVERSE_REL.get(tgt)
        if bucket:
            bucket.discard(rid)
            if not bucket:
                REVERSE_REL.pop(tgt, None)
    for tgt in _collect_outgoing_links(new_person):
        REVERSE_REL.setdefault(tgt, set()).add(rid)
    PERSON_INDEX[rid] = (entry_idx, kind, new_person)


def build_indexes():
    PERSON_INDEX.clear(); REVERSE_REL.clear()
    for ei, e in enumerate(DATA):
        if is_khu(e):
            for p in e.get("residents", []) or []:
                _register_person(ei, "residents", p)
        elif is_cong_trinh(e):
            for p in e.get("staff", []) or []:
                _register_person(ei, "staff", p)

# ---------- Placeholder system & normalization ----------
PLACEHOLDER_DEFAULTS = {
    "name": "Nguyễn Văn A",
    "age": 25,
    "gender": "nam",
    "job": "nông dân",
    "social": "dân đinh",
}

# Legacy fields mapping → unified profile
PROFILE_KEYS = {
    "ho_ten": "name",
    "tuoi": "age",
    "gioi_tinh": "gender",
    "nghe_nghiep": "job",
    "than_phan_xa_hoi": "social",
}

def person_profile(person: dict) -> dict:
    """Return ensured canonical profile; create if missing; migrate legacy top-level fields once."""
    prof = person.get("profile") or {}
    changed = False
    for src, dst in PROFILE_KEYS.items():
        if src in person and dst not in prof:
            prof[dst] = person.get(src)
            changed = True
    # Fill defaults for missing
    for k, v in PLACEHOLDER_DEFAULTS.items():
        prof.setdefault(k, v)
    if changed or "profile" not in person:
        person["profile"] = prof
    return prof

import re
PLACEHOLDER_PATTERN = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")

def apply_placeholders(text: str, prof: dict) -> str:
    if not isinstance(text, str):
        return text
    def _rep(m):
        key = m.group(1)
        return str(prof.get(key, m.group(0)))
    return PLACEHOLDER_PATTERN.sub(_rep, text)


def ensure_placeholder_usage(person: dict) -> bool:
    """Replace literal occurrences of name in dialogue/notes with {{name}}, etc. Return True if modified."""
    prof = person_profile(person)
    modified = False
    literal_name = str(prof.get("name") or "").strip()
    if not literal_name:
        return False
    # Dialogue
    dlg = person.get("dialogue") or {}
    for key in ("ambient", "interact"):
        lines = dlg.get(key) or []
        new_lines = []
        for s in lines:
            ns = s.replace(literal_name, "{{name}}")
            if ns != s: modified = True
            new_lines.append(ns)
        dlg[key] = new_lines
    person["dialogue"] = dlg
    # Notes
    notes = dlg.get("system_notes") or ""
    new_notes = notes.replace(literal_name, "{{name}}")
    if new_notes != notes:
        dlg["system_notes"] = new_notes
        modified = True
    return modified


def normalize_person_record(person: dict) -> bool:
    """Normalize one person: ensure profile, backfill legacy, promote persona defaults, fix placeholders."""
    changed = False
    prof = person_profile(person)
    # Back-propagate: keep legacy mirrors so old UI/tools still show something
    for src, dst in PROFILE_KEYS.items():
        old = person.get(src)
        new = prof.get(dst)
        if old != new:
            person[src] = new
            changed = True
    # Persona defaults
    per = person.get("persona") or {}
    ng = per.get("ngon_ngu_giao_tiep") or {}
    per.setdefault("vai_tro_loi_thoai", "thường dân")
    per.setdefault("dong_co", "sinh kế")
    per.setdefault("thai_do_phap_luat", "thuận phép")
    per.setdefault("dao_duc", "trung tính")
    ng.setdefault("dai_tu_xung_ho", "ta")
    ng.setdefault("phong_cach_cau", "mộc mạc")
    per["ngon_ngu_giao_tiep"] = ng
    person["persona"] = per
    changed |= ensure_placeholder_usage(person)
    # Ensure chat log exists
    person.setdefault("chat_log", [])
    return changed


def normalize_all(data: List[Dict[str, Any]]) -> int:
    """Normalize every person in every entry. Return number of modified records."""
    modified = 0
    for e in data:
        arrs = []
        if is_khu(e):
            arrs.append(e.setdefault("residents", []))
        elif is_cong_trinh(e):
            arrs.append(e.setdefault("staff", []))
        for arr in arrs:
            for p in arr:
                if normalize_person_record(p):
                    modified += 1
    return modified

# ---------- Family planner (same heuristics) ----------

def scan_and_plan_families(data: List[Dict[str, Any]]) -> int:
    def all_people():
        for e in data:
            if is_khu(e):
                for p in e.get("residents", []) or []:
                    yield e, p
            elif is_cong_trinh(e):
                for p in e.get("staff", []) or []:
                    yield e, p

    buckets: Dict[str, List[Tuple[Dict[str, Any], Dict[str, Any]]]] = {}
    for e, p in all_people():
        ht = p.get("ho_tich", {}) or {}
        key = f"{e.get('id')}|{ht.get('ho_so','?')}|{ht.get('gia_toc','?')}"
        buckets.setdefault(key, []).append((e, p))

    updated = 0
    for key, members in buckets.items():
        hid = f"HH-{key}"
        for _, p in members:
            fam = p.setdefault("family", {})
            if fam.get("household_id") != hid:
                fam["household_id"] = hid
                updated += 1

        people = [p for _, p in members if isinstance(p.get("tuoi"), int)]
        males = [p for p in people if p.get("gioi_tinh") == "nam"]
        females = [p for p in people if p.get("gioi_tinh") == "nữ"]

        used_f_ids: set[str] = set()
        for m in sorted(males, key=lambda x: x.get("tuoi", 0), reverse=True):
            m_age = int(m.get("tuoi", 0) or 0)
            best_f = None
            best_gap = 10**9
            for f in females:
                fid = f.get("resident_id")
                if not isinstance(fid, str) or not fid or fid in used_f_ids:
                    continue
                gap = abs(m_age - int(f.get("tuoi", 0) or 0))
                if gap <= 20 and gap < best_gap:
                    best_gap = gap
                    best_f = f
            if best_f:
                fid = best_f.get("resident_id")
                fam_m = m.setdefault("family", {})
                fam_f = best_f.setdefault("family", {})
                fam_m.setdefault("spouse_id", fid)
                fam_f.setdefault("spouse_id", m.get("resident_id"))
                used_f_ids.add(fid)
                updated += 2

        for a in people:
            a_age = int(a.get("tuoi", 0) or 0)
            a_id = a.get("resident_id")
            for b in people:
                if a is b:
                    continue
                gap = a_age - int(b.get("tuoi", 0) or 0)
                if 16 <= gap <= 40:
                    fa = a.setdefault("family", {})
                    fb = b.setdefault("family", {})
                    fb.setdefault("parents_ids", [])
                    if a_id and a_id not in fb["parents_ids"]:
                        fb["parents_ids"].append(a_id)
                        updated += 1
                    bid = b.get("resident_id")
                    fa.setdefault("children_ids", [])
                    if bid and bid not in fa["children_ids"]:
                        fa["children_ids"].append(bid)
                        updated += 1

        for i, a in enumerate(people):
            for j in range(i + 1, len(people)):
                b = people[j]
                if a.get("resident_id") == b.get("resident_id"):
                    continue
                if b.get("resident_id") in a.get("family", {}).get("children_ids", []):
                    continue
                if a.get("resident_id") in b.get("family", {}).get("children_ids", []):
                    continue
                if a.get("family", {}).get("spouse_id") == b.get("resident_id"):
                    continue
                if b.get("family", {}).get("spouse_id") == a.get("resident_id"):
                    continue
                if abs(int(a.get("tuoi", 0) or 0) - int(b.get("tuoi", 0) or 0)) <= 20:
                    fa = a.setdefault("family", {})
                    fb = b.setdefault("family", {})
                    fa.setdefault("siblings_ids", [])
                    fb.setdefault("siblings_ids", [])
                    if b.get("resident_id") not in fa["siblings_ids"]:
                        fa["siblings_ids"].append(b.get("resident_id"))
                        updated += 1
                    if a.get("resident_id") not in fb["siblings_ids"]:
                        fb["siblings_ids"].append(a.get("resident_id"))
                        updated += 1
    return updated

# ---------- AI helpers ----------

def openai_client() -> Optional[OpenAI]:
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        return None
    return OpenAI(api_key=key)


def person_context(entry: Dict[str, Any], person: Dict[str, Any]) -> str:
    prof = person_profile(person)
    ht = person.get("ho_tich", {}) or {}
    tgtk = ", ".join(person.get("ton_giao_tu_tuong", []) or [])
    tp = person.get("trang_phuc", {}) or {}
    lines = [
        f"Khu/Công trình: {entry.get('region','')} / {entry.get('name','')} ({entry.get('class','')}-{entry.get('subtype','')})",
        f"Mô tả: {entry.get('description','')}",
        f"Hồ sơ: tên={prof.get('name','?')} (giới={prof.get('gender','?')}, tuổi={prof.get('age','?')}), nghề={prof.get('job','')}, thân phận={prof.get('social','')}",
        f"Hộ tịch: {ht.get('loai','')} / {ht.get('ho_so','')} / {ht.get('gia_toc','')}",
        f"Tư tưởng/Tôn giáo: {tgtk}",
        f"Trang phục: áo {tp.get('ao','')}, nón {tp.get('non','')}, giày {tp.get('giay','')}, khăn {tp.get('khau','')}",
    ]
    per = person.get("persona", {}) or {}
    if per:
        lines += [
            f"Persona: vai={per.get('vai_tro_loi_thoai','')}, động cơ={per.get('dong_co','')}, luật={per.get('thai_do_phap_luat','')}, đạo đức={per.get('dao_duc','')}",
            f"Xưng hô: {per.get('ngon_ngu_giao_tiep',{}).get('dai_tu_xung_ho','')}, phong cách: {per.get('ngon_ngu_giao_tiep',{}).get('phong_cach_cau','')}"
        ]
    return "\n".join(lines)

# ---------- JSON CRUD API ----------
@app.get("/api/entries")
def api_list_entries():
    cl = request.args.get("class")
    st = request.args.get("subtype")
    q = (request.args.get("q") or "").strip().lower()
    limit = int(request.args.get("limit", 200))
    offset = int(request.args.get("offset", 0))
    out = []
    for e in DATA:
        if cl and e.get("class") != cl:
            continue
        if st and e.get("subtype") != st:
            continue
        if q:
            hay = f"{e.get('name','')} {e.get('region','')} {e.get('description','')}".lower()
            if q not in hay:
                continue
        out.append(e)
    return jsonify({"items": out[offset: offset+limit], "total": len(out)})

@app.get("/api/entries/<int:item_id>")
def api_get_entry(item_id: int):
    idx = find_index_by_id(DATA, item_id)
    if idx < 0:
        return jsonify({"error": "not found"}), 404
    return jsonify(DATA[idx])

@app.post("/api/entries")
def api_create_entry():
    payload = request.get_json(silent=True) or {}
    with _file_lock:
        if "id" not in payload:
            payload["id"] = max([e.get("id", 0) for e in DATA] + [0]) + 1
        DATA.append(payload)
        save_data(DATA_PATH, DATA)
    change_bus.publish({"type":"entry_created","id": payload["id"], "by": request.remote_addr, "ts": dt.datetime.utcnow().isoformat()})
    return jsonify(payload), 201

@app.put("/api/entries/<int:item_id>")
def api_update_entry(item_id: int):
    idx = find_index_by_id(DATA, item_id)
    if idx < 0:
        return jsonify({"error": "not found"}), 404
    payload = request.get_json(silent=True) or {}
    with _file_lock:
        updated = deepcopy(DATA[idx])
        updated.update(payload)
        DATA[idx] = updated
        save_data(DATA_PATH, DATA)
    change_bus.publish({"type":"entry_updated","id": item_id, "by": request.remote_addr, "ts": dt.datetime.utcnow().isoformat()})
    return jsonify(updated)

@app.delete("/api/entries/<int:item_id>")
def api_delete_entry(item_id: int):
    idx = find_index_by_id(DATA, item_id)
    if idx < 0:
        return jsonify({"error": "not found"}), 404
    with _file_lock:
        removed = DATA.pop(idx)
        save_data(DATA_PATH, DATA)
    change_bus.publish({"type":"entry_deleted","id": item_id, "by": request.remote_addr, "ts": dt.datetime.utcnow().isoformat()})
    return jsonify({"deleted": removed.get("id")})

# Residents (khu)
@app.post("/api/entries/<int:item_id>/residents")
def api_add_resident(item_id: int):
    idx = find_index_by_id(DATA, item_id)
    if idx < 0:
        return jsonify({"error": "not found"}), 404
    entry = DATA[idx]
    if not is_khu(entry):
        return jsonify({"error": "not a 'khu'"}), 400
    resident = request.get_json(silent=True) or {}
    arr = entry.setdefault("residents", [])
    prefix = initials_from(entry.get("name"), "KHU")
    nextnum = 1 + max([
        int(r.get("resident_id", "-0").split("-")[-1])
        for r in arr
        if isinstance(r.get("resident_id"), str) and r["resident_id"].startswith(prefix + "-")
    ] + [0])
    resident["resident_id"] = f"{prefix}-{nextnum:04d}"
    normalize_person_record(resident)
    with _file_lock:
        arr.append(resident)
        if isinstance(entry.get("population_count"), int):
            entry["population_count"] = max(entry["population_count"], len(arr))
        _register_person(idx, "residents", resident)
        save_data(DATA_PATH, DATA)
    change_bus.publish({"type":"person_added","entry": item_id, "rid": resident["resident_id"], "by": request.remote_addr, "ts": dt.datetime.utcnow().isoformat()})
    return jsonify(resident), 201

@app.put("/api/entries/<int:item_id>/residents/<resident_id>")
def api_update_resident(item_id: int, resident_id: str):
    idx = find_index_by_id(DATA, item_id)
    if idx < 0:
        return jsonify({"error": "not found"}), 404
    entry = DATA[idx]
    if not is_khu(entry):
        return jsonify({"error": "not a 'khu'"}), 400
    arr = entry.setdefault("residents", [])
    for i, r in enumerate(arr):
        if r.get("resident_id") == resident_id:
            payload = request.get_json(silent=True) or {}
            before = deepcopy(r)
            updated = deepcopy(r)
            updated.update(payload)
            normalize_person_record(updated)
            updated["resident_id"] = resident_id
            with _file_lock:
                arr[i] = updated
                _reindex_person(idx, "residents", before, updated)
                save_data(DATA_PATH, DATA)
            change_bus.publish({"type":"person_updated","entry": item_id, "rid": resident_id, "by": request.remote_addr, "ts": dt.datetime.utcnow().isoformat()})
            return jsonify(updated)
    return jsonify({"error": "resident not found"}), 404

@app.delete("/api/entries/<int:item_id>/residents/<resident_id>")
def api_delete_resident(item_id: int, resident_id: str):
    idx = find_index_by_id(DATA, item_id)
    if idx < 0:
        return jsonify({"error": "not found"}), 404
    entry = DATA[idx]
    if not is_khu(entry):
        return jsonify({"error": "not a 'khu'"}), 400
    arr = entry.setdefault("residents", [])
    for i, r in enumerate(arr):
        if r.get("resident_id") == resident_id:
            with _file_lock:
                _unregister_person(r)
                arr.pop(i)
                if isinstance(entry.get("population_count"), int):
                    entry["population_count"] = max(len(arr), entry["population_count"] - 1)
                save_data(DATA_PATH, DATA)
            change_bus.publish({"type":"person_deleted","entry": item_id, "rid": resident_id, "by": request.remote_addr, "ts": dt.datetime.utcnow().isoformat()})
            return jsonify({"deleted": resident_id})
    return jsonify({"error": "resident not found"}), 404

# Staff (cong_trinh)
@app.post("/api/entries/<int:item_id>/staff")
def api_add_staff(item_id: int):
    idx = find_index_by_id(DATA, item_id)
    if idx < 0:
        return jsonify({"error": "not found"}), 404
    entry = DATA[idx]
    if not is_cong_trinh(entry):
        return jsonify({"error": "not a 'cong_trinh'"}), 400
    person = request.get_json(silent=True) or {}
    arr = entry.setdefault("staff", [])
    prefix = initials_from(entry.get("name"), "CT")
    nextnum = 1 + max([
        int(r.get("resident_id", "-0").split("-")[-1])
        for r in arr
        if isinstance(r.get("resident_id"), str) and r["resident_id"].startswith(prefix + "-")
    ] + [0])
    person["resident_id"] = f"{prefix}-{nextnum:04d}"
    normalize_person_record(person)
    with _file_lock:
        arr.append(person)
        if isinstance(entry.get("staff_count"), int):
            entry["staff_count"] = max(entry["staff_count"], len(arr))
        _register_person(idx, "staff", person)
        save_data(DATA_PATH, DATA)
    change_bus.publish({"type":"person_added","entry": item_id, "rid": person["resident_id"], "by": request.remote_addr, "ts": dt.datetime.utcnow().isoformat()})
    return jsonify(person), 201

@app.put("/api/entries/<int:item_id>/staff/<resident_id>")
def api_update_staff(item_id: int, resident_id: str):
    idx = find_index_by_id(DATA, item_id)
    if idx < 0:
        return jsonify({"error": "not found"}), 404
    entry = DATA[idx]
    if not is_cong_trinh(entry):
        return jsonify({"error": "not a 'cong_trinh'"}), 400
    arr = entry.setdefault("staff", [])
    for i, r in enumerate(arr):
        if r.get("resident_id") == resident_id:
            payload = request.get_json(silent=True) or {}
            before = deepcopy(r)
            updated = deepcopy(r)
            updated.update(payload)
            normalize_person_record(updated)
            updated["resident_id"] = resident_id
            with _file_lock:
                arr[i] = updated
                _reindex_person(idx, "staff", before, updated)
                save_data(DATA_PATH, DATA)
            change_bus.publish({"type":"person_updated","entry": item_id, "rid": resident_id, "by": request.remote_addr, "ts": dt.datetime.utcnow().isoformat()})
            return jsonify(updated)
    return jsonify({"error": "staff not found"}), 404

@app.delete("/api/entries/<int:item_id>/staff/<resident_id>")
def api_delete_staff(item_id: int, resident_id: str):
    idx = find_index_by_id(DATA, item_id)
    if idx < 0:
        return jsonify({"error": "not found"}), 404
    entry = DATA[idx]
    if not is_cong_trinh(entry):
        return jsonify({"error": "not a 'cong_trinh'"}), 400
    arr = entry.setdefault("staff", [])
    for i, r in enumerate(arr):
        if r.get("resident_id") == resident_id:
            with _file_lock:
                _unregister_person(r)
                arr.pop(i)
                if isinstance(entry.get("staff_count"), int):
                    entry["staff_count"] = max(len(arr), entry["staff_count"] - 1)
                save_data(DATA_PATH, DATA)
            change_bus.publish({"type":"person_deleted","entry": item_id, "rid": resident_id, "by": request.remote_addr, "ts": dt.datetime.utcnow().isoformat()})
            return jsonify({"deleted": resident_id})
    return jsonify({"error": "staff not found"}), 404

# ---------- Impact preview & Cascade delete ----------

def _direct_links_of(person: dict) -> Dict[str, List[str]]:
    fam = person.get("family", {}) or {}
    return {
        "spouse": [fam.get("spouse_id")] if fam.get("spouse_id") else [],
        "parents": list(fam.get("parents_ids") or []),
        "children": list(fam.get("children_ids") or []),
        "siblings": list(fam.get("siblings_ids") or []),
        "relations": [r.get("target_id") for r in (person.get("relations") or []) if isinstance(r.get("target_id"), str)],
    }


def _remove_link_fields(target_person: dict, deleting_id: str):
    fam = target_person.setdefault("family", {})
    if fam.get("spouse_id") == deleting_id:
        fam.pop("spouse_id", None)
    for k in ("parents_ids", "children_ids", "siblings_ids"):
        arr = fam.get(k) or []
        if deleting_id in arr:
            fam[k] = [x for x in arr if x != deleting_id]
    rels = target_person.get("relations") or []
    newrels = [r for r in rels if r.get("target_id") != deleting_id]
    if newrels != rels:
        target_person["relations"] = newrels

@app.get("/api/entries/<int:item_id>/<kind>/<resident_id>/impact")
def api_delete_impact(item_id: int, kind: str, resident_id: str):
    ei = find_index_by_id(DATA, item_id)
    if ei < 0:
        return jsonify({"error": "entry not found"}), 404
    entry = DATA[ei]
    k, person, _ = get_person(entry, resident_id)
    if k != kind:
        return jsonify({"error": "person not found in kind"}), 404
    incoming = sorted(list(REVERSE_REL.get(resident_id, set())))
    direct = _direct_links_of(person)
    return jsonify({
        "resident_id": resident_id,
        "incoming_refs": incoming,
        "direct_refs": direct,
        "counts": {
            "incoming": len(incoming),
            "spouse": len(direct["spouse"]),
            "parents": len(direct["parents"]),
            "children": len(direct["children"]),
            "siblings": len(direct["siblings"]),
            "relations": len(direct["relations"]),
        },
    })

@app.delete("/api/entries/<int:item_id>/<kind>/<resident_id>/cascade")
def api_delete_cascade(item_id: int, kind: str, resident_id: str):
    ei = find_index_by_id(DATA, item_id)
    if ei < 0:
        return jsonify({"error": "entry not found"}), 404
    entry = DATA[ei]
    k, person, idx_in = get_person(entry, resident_id)
    if k != kind or idx_in < 0:
        return jsonify({"error": "person not found"}), 404

    impacted_ids = set(REVERSE_REL.get(resident_id, set()))
    for lst in _direct_links_of(person).values():
        impacted_ids.update([x for x in lst if isinstance(x, str) and x])

    modified = []
    with _file_lock:
        for sid in impacted_ids:
            if sid == resident_id:
                continue
            rec = PERSON_INDEX.get(sid)
            if not rec:
                continue
            sj_ei, sj_kind, sj_person = rec
            before = deepcopy(sj_person)
            _remove_link_fields(sj_person, resident_id)
            _reindex_person(sj_ei, sj_kind, before, sj_person)
            modified.append(sid)

        _unregister_person(person)
        if kind == "residents":
            entry["residents"].pop(idx_in)
            if isinstance(entry.get("population_count"), int):
                entry["population_count"] = max(len(entry["residents"]), entry["population_count"] - 1)
        else:
            entry["staff"].pop(idx_in)
            if isinstance(entry.get("staff_count"), int):
                entry["staff_count"] = max(len(entry["staff"]), entry["staff_count"] - 1)
        save_data(DATA_PATH, DATA)
    change_bus.publish({"type":"person_deleted","entry": item_id, "rid": resident_id, "by": request.remote_addr, "ts": dt.datetime.utcnow().isoformat(), "impacted": len(modified)})
    return jsonify({"deleted": resident_id, "impacted": sorted(list(modified))})

# ---------- Tools ----------
@app.post("/api/tools/scan_families")
def api_scan_families():
    changed = scan_and_plan_families(DATA)
    build_indexes()
    save_data(DATA_PATH, DATA)
    change_bus.publish({"type":"families_scanned","by": request.remote_addr, "ts": dt.datetime.utcnow().isoformat(), "updated": changed})
    return jsonify({"updated_fields": changed})

@app.post("/api/tools/normalize_all")
def api_normalize_all():
    changed = normalize_all(DATA)
    build_indexes()
    save_data(DATA_PATH, DATA)
    change_bus.publish({"type":"normalized_all","by": request.remote_addr, "ts": dt.datetime.utcnow().isoformat(), "updated": changed})
    return jsonify({"updated_records": changed})

# ---------- Helpers: Render placeholders ----------
@app.post("/api/helpers/render")
def api_render_placeholders():
    body = request.get_json(silent=True) or {}
    entry_id = int(body.get("entry_id", -1))
    rid = body.get("resident_id", "")
    text = body.get("text", "")
    idx = find_index_by_id(DATA, entry_id)
    if idx < 0:
        return jsonify({"error": "entry not found"}), 404
    entry = DATA[idx]
    kind, person, _ = get_person(entry, rid)
    if not kind:
        return jsonify({"error": "person not found"}), 404
    prof = person_profile(person)
    return jsonify({"rendered": apply_placeholders(text, prof)})

# ---------- SSE: live notifications ----------
@app.get("/events")
def sse_events():
    @stream_with_context
    def event_stream():
        q = change_bus.subscribe()
        try:
            while True:
                ev = q.get()
                yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
        except GeneratorExit:
            change_bus.unsubscribe(q)
        except Exception:
            change_bus.unsubscribe(q)
    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no"
    }
    return Response(event_stream(), headers=headers)

# ---------- AI endpoints (placeholder-aware, chat history) ----------
@app.post("/api/ai/generate_dialogue")
def api_generate_dialogue():
    body = request.get_json(silent=True) or {}
    entry_id = int(body.get("entry_id", -1))
    rid = body.get("resident_id", "")
    mode = body.get("mode", "ambient")
    count = max(1, min(12, int(body.get("count", 6))))
    idx = find_index_by_id(DATA, entry_id)
    if idx < 0:
        return jsonify({"error": "entry not found"}), 404
    entry = DATA[idx]
    kind, person, _ = get_person(entry, rid)
    if kind == "":
        return jsonify({"error": "person not found"}), 404

    cli = openai_client()
    if not cli:
        return jsonify({"error": "OPENAI_API_KEY missing"}), 400

    prof = person_profile(person)
    sys_prompt = (
        "Bạn là biên kịch thoại cho game bối cảnh Đại Việt thời Trần (TK XIII). "
        "Luôn viết giọng phù hợp persona, không thêm lời dẫn hoặc đánh số. "
        "Không dùng văn phong trợ lý hiện đại; không hỏi người chơi cần gì. "
        "Ưu tiên câu ngắn, tự nhiên, có thể cộc lốc nếu tính cách phù hợp."
    )
    user_prompt = f"""Sinh {count} câu thoại dạng {"đi ngang" if mode=="ambient" else "tương tác (click)"} cho nhân vật sau.
- Mỗi câu trên một dòng, tối đa ~120 ký tự/câu.
- Dùng placeholder {{name}}, {{age}}, {{job}} khi nhắc tới tên/tuổi/nghề để tiện thay thế.
- Nếu persona 'chảnh' hay 'cộc cằn' thì giữ thái độ nhất quán, KHÔNG chuyển sang trợ giúp.
BỐI CẢNH
{person_context(entry, person)}
"""
    resp = cli.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role": "system", "content": sys_prompt},
                  {"role": "user", "content": user_prompt}],
        temperature=0.7,
    )
    text = (resp.choices[0].message.content or "").strip()
    lines = [s.strip() for s in text.splitlines() if s.strip()]

    # Chuẩn hóa: cố gắng ép dùng placeholder thay vì tên thật
    out = []
    for s in lines[:count]:
        out.append(apply_placeholders(s.replace(prof.get("name","") or "", "{{name}}"), prof))

    return jsonify({"mode": mode, "lines": out})

@app.post("/api/ai/chat")
def api_chat():
    body = request.get_json(silent=True) or {}
    entry_id = int(body.get("entry_id", -1))
    rid = body.get("resident_id", "")
    msg = (body.get("message") or "").strip()
    reset = bool(body.get("reset", False))
    idx = find_index_by_id(DATA, entry_id)
    if idx < 0:
        return jsonify({"error": "entry not found"}), 404
    entry = DATA[idx]
    kind, person, _ = get_person(entry, rid)
    if kind == "":
        return jsonify({"error": "person not found"}), 404

    cli = openai_client()
    if not cli:
        return jsonify({"error": "OPENAI_API_KEY missing"}), 400

    # Lịch sử chat theo NPC
    log = person.setdefault("chat_log", [])
    if reset:
        log.clear()

    prof = person_profile(person)
    per = person.get("persona", {}) or {}
    seed = (per.get("prompt_seed") or [])[:4]
    speaking_style = per.get("ngon_ngu_giao_tiep", {}).get("phong_cach_cau", "mộc mạc")
    pron = per.get("ngon_ngu_giao_tiep", {}).get("dai_tu_xung_ho", "ta")

    # System guardrails: cấm giọng "trợ lý hữu ích"
    sys_prompt = (
        "Bạn đóng vai NPC thời Trần (TK XIII). Trả lời NGẮN, đúng chất nhân vật, "
        "không giải thích dài, không hỏi lại khách sáo. Không dùng văn phong trợ lý AI. "
        "Nếu tính cách cộc/kiêu, giữ tông nhất quán và không 'giúp đỡ' quá mức."
    )

    # Build messages: context + last 6 turns
    messages = [{"role": "system", "content": sys_prompt}]
    messages.append({"role": "user", "content": (
        f"Bạn là {prof.get('name','?')} ({pron}), phong cách: {speaking_style}.\n"
        f"BỐI CẢNH:\n{person_context(entry, person)}\n"
        f"SEED: {' | '.join(seed)}\n"
        f"Ghi nhớ: dùng placeholder {{name}} nếu tự xưng hoặc nhắc tên mình.\n"
    )})
    # thêm lịch sử gần nhất
    for m in log[-12:]:
        messages.append({"role": m.get("role","user"), "content": m.get("content","")})
    # câu hiện tại
    messages.append({"role": "user", "content": msg})

    resp = cli.chat.completions.create(
        model=OPENAI_MODEL,
        messages=messages,
        temperature=0.8,
    )
    text = (resp.choices[0].message.content or "").strip()
    # ép placeholder và không quá dài
    text = apply_placeholders(text, prof)
    if len(text) > 300:
        text = text[:297] + "..."

    # Lưu chat log
    now = dt.datetime.utcnow().isoformat()
    log.append({"role":"user","content": msg, "ts": now})
    log.append({"role":"assistant","content": text, "ts": now})
    # cắt ngắn
    if len(log) > 200:
        del log[:-200]

    with _file_lock:
        save_data(DATA_PATH, DATA)

    change_bus.publish({"type":"chat","entry": entry_id, "rid": rid, "by": request.remote_addr, "ts": now})
    return jsonify({"reply": text, "tokens": {"history": len(log)}})

# ---------- Global chat utilities ----------
@app.get("/api/chat/recent")
def api_chat_recent():
    limit = int(request.args.get("limit", 50))
    items = []
    for ei, e in enumerate(DATA):
        arrs = []
        if is_khu(e):
            arrs.append(e.get("residents", []) or [])
        elif is_cong_trinh(e):
            arrs.append(e.get("staff", []) or [])
        for arr in arrs:
            for p in arr:
                for m in p.get("chat_log", []):
                    items.append({
                        "entry_id": e.get("id"),
                        "resident_id": p.get("resident_id"),
                        "name": person_profile(p).get("name"),
                        "role": m.get("role"),
                        "content": m.get("content"),
                        "ts": m.get("ts")
                    })
    items.sort(key=lambda x: x.get("ts") or "", reverse=True)
    return jsonify({"items": items[:limit]})

@app.get("/api/chat/npc/<int:item_id>/<resident_id>")
def api_chat_log(item_id: int, resident_id: str):
    idx = find_index_by_id(DATA, item_id)
    if idx < 0:
        return jsonify({"error": "entry not found"}), 404
    entry = DATA[idx]
    k, person, _ = get_person(entry, resident_id)
    if not k:
        return jsonify({"error": "person not found"}), 404
    return jsonify({"log": person.get("chat_log", [])})

@app.delete("/api/chat/npc/<int:item_id>/<resident_id>")
def api_chat_clear(item_id: int, resident_id: str):
    idx = find_index_by_id(DATA, item_id)
    if idx < 0:
        return jsonify({"error": "entry not found"}), 404
    entry = DATA[idx]
    k, person, _ = get_person(entry, resident_id)
    if not k:
        return jsonify({"error": "person not found"}), 404
    person["chat_log"] = []
    with _file_lock:
        save_data(DATA_PATH, DATA)
    change_bus.publish({"type":"chat_cleared","entry": item_id, "rid": resident_id, "by": request.remote_addr, "ts": dt.datetime.utcnow().isoformat()})
    return jsonify({"ok": True})

# ---------- Minimal UI (compact, có Global Chat + SSE toast + Profile unification) ----------
INDEX_HTML = r"""
<!DOCTYPE html><html lang="vi"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>World Admin v2 — Đại Việt (Trần, TK XIII)</title>
<style>
:root{--bg:#0b0d10;--card:#101418;--text:#e7ebf0;--muted:#a9b4c0;--accent:#4f8cff;--danger:#e74c3c;}
*{box-sizing:border-box}body{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Arial;background:var(--bg);color:var(--text)}
header{padding:12px 16px;border-bottom:1px solid #1e242c;display:flex;gap:12px;align-items:center;flex-wrap:wrap}
h1{font-size:16px;margin:0}
.container{display:grid;grid-template-columns:340px 1fr;min-height:calc(100vh - 54px)}
aside{border-right:1px solid #1e242c;padding:10px;gap:10px;display:flex;flex-direction:column}
.card{background:var(--card);border:1px solid #1e242c;border-radius:12px;padding:10px}
.row{display:flex;gap:8px;align-items:center;margin-bottom:8px}
label{width:110px;font-size:12px;color:var(--muted)}
input,select,textarea{width:100%;background:#0c1014;color:var(--text);border:1px solid #1e242c;border-radius:8px;padding:8px}
button{background:var(--accent);color:#fff;border:none;border-radius:8px;padding:8px 10px;cursor:pointer}
button.ghost{background:#0c1014;border:1px solid #1e242c}
button.danger{background:var(--danger)}
.small{font-size:12px;color:var(--muted)}
.list{max-height:45vh;overflow:auto;display:flex;flex-direction:column;gap:6px}
.item{background:#0c1014;border:1px solid #1e242c;border-radius:8px;padding:8px;cursor:pointer}
.item:hover{border-color:#273446}
.pill{font-size:11px;padding:2px 6px;border-radius:999px;border:1px solid #243143;color:#a9c1ff}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.toast{position:fixed;right:16px;bottom:16px;background:#111b27;color:#e7ebf0;border:1px solid #213044;border-radius:10px;padding:10px 12px;box-shadow:0 8px 30px rgba(0,0,0,.5)}
table{width:100%;border-collapse:collapse}
th,td{padding:6px 8px;border-bottom:1px solid #1e242c;font-size:14px;vertical-align:top}
#globalChat{max-height:40vh;overflow:auto}
</style>
</head><body>
<header>
  <h1>World Admin v2 — Đại Việt (Trần)</h1>
  <button id="btnReload">Reload</button>
  <button class="ghost" id="btnScan">Scan Families</button>
  <button class="ghost" id="btnNormalize">Normalize All</button>
</header>
<div class="container">
  <aside>
    <div class="card">
      <div class="row"><select id="filterClass"><option value="">(All)</option><option value="khu">khu</option><option value="cong_trinh">cong_trinh</option></select>
      <input id="searchBox" placeholder="Search..." /></div>
      <div class="row"><label>Subtype</label><select id="filterSubtype"><option value="">(Any)</option></select></div>
      <div class="row"><label>Page</label><input id="page" type="number" value="1"/></div>
      <div class="row"><label>Page size</label><input id="pageSize" type="number" value="100"/></div>
    </div>
    <div class="card">
      <div class="row"><button class="ghost" id="btnNewEntry">+ Entry</button></div>
      <div id="entries" class="list"></div>
    </div>
    <div class="card">
      <div class="row"><b>Global Chat</b><span class="small">(mới nhất)</span></div>
      <div id="globalChat"></div>
    </div>
  </aside>
  <main>
    <div id="detail" class="card"><div class="small">Select an entry…</div></div>
    <div id="people" class="card"><div class="small">Select an entry to see people.</div></div>
  </main>
</div>
<div id="toast" class="toast" style="display:none;"></div>
<script>
let current=null, currentPeopleType='residents', total=0;

function toast(msg){
  const t=document.getElementById('toast'); t.textContent=msg; t.style.display='block';
  setTimeout(()=>t.style.display='none', 3000);
}
function escapeHtml(s){return (s||'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;')}
function escapeAttr(s){return escapeHtml(s).replaceAll('"','&quot;')}
async function api(path, method='GET', body=null){
  const opt={method,headers:{'Accept':'application/json'}}; if(body){opt.headers['Content-Type']='application/json'; opt.body=JSON.stringify(body)}
  const r=await fetch(path,opt); let j=null; try{j=await r.json()}catch(e){}
  if(!r.ok) throw new Error((j&&j.error)||r.statusText); return j;
}
async function loadEntries(){
  const cl=document.getElementById('filterClass').value;
  const st=document.getElementById('filterSubtype').value;
  const q=document.getElementById('searchBox').value.trim();
  const page=parseInt(document.getElementById('page').value||1);
  const sz=parseInt(document.getElementById('pageSize').value||100);
  const params=new URLSearchParams(); if(cl)params.set('class',cl); if(st)params.set('subtype',st); if(q)params.set('q',q);
  params.set('limit', sz); params.set('offset', (page-1)*sz);
  const j=await api('/api/entries?'+params.toString()); total=j.total||0;
  buildSubtypeOptions(j.items||[]); renderEntries(j.items||[]);
}
function buildSubtypeOptions(items){
  const s=document.getElementById('filterSubtype'); const cur=s.value; const set=new Set();
  items.forEach(e=>e.subtype&&set.add(e.subtype));
  s.innerHTML='<option value="">(Any)</option>'+[...set].sort().map(x=>`<option value="${x}">${x}</option>`).join('');
  if(cur) s.value=cur;
}
function renderEntries(items){
  const el=document.getElementById('entries');
  el.innerHTML = (items||[]).map(e=>`
    <div class="item" onclick="openEntry(${e.id})">
      <div><b>${escapeHtml(e.name||'')}</b> <span class="pill">${escapeHtml(e.class||'-')}</span> <span class="pill">${escapeHtml(e.subtype||'-')}</span></div>
      <div class="small">${escapeHtml(e.region||'')}</div>
      <div class="small">${escapeHtml(e.description||'')}</div>
    </div>`).join('');
}
async function openEntry(id){
  current=await api('/api/entries/'+id);
  currentPeopleType=(current.class==='khu')?'residents':'staff';
  // detail
  document.getElementById('detail').innerHTML = `
    <div class="row grid">
      <div><label>Region</label><input id="f_region" value="${escapeAttr(current.region||'')}"></div>
      <div><label>Name</label><input id="f_name" value="${escapeAttr(current.name||'')}"></div>
      <div><label>Class</label>
        <select id="f_class"><option value="khu" ${current.class==='khu'?'selected':''}>khu</option>
        <option value="cong_trinh" ${current.class==='cong_trinh'?'selected':''}>cong_trinh</option></select>
      </div>
      <div><label>Subtype</label><input id="f_subtype" value="${escapeAttr(current.subtype||'')}"></div>
    </div>
    <div class="row"><label>Description</label><textarea id="f_desc" rows="2">${escapeHtml(current.description||'')}</textarea></div>
    <div class="row">
      ${ current.class==='khu'
          ? `<label>population_count</label><input id="f_pop" type="number" value="${Number(current.population_count||0)}">`
          : `<label>staff_count</label><input id="f_staff" type="number" value="${Number(current.staff_count||0)}">`
      }
    </div>
    <div class="row"><button onclick="saveEntry()">💾 Save</button> <button class="ghost" onclick="createEntry()">+ New</button></div>
  `;
  renderPeople();
}
function renderPeople(){
  const p=document.getElementById('people'); if(!current){p.innerHTML='<div class="small">No entry.</div>'; return;}
  const rows=(current[currentPeopleType]||[]).map(px=>{
    const prof=px.profile||{}; const rid=px.resident_id||'';
    return `<tr>
      <td>${escapeHtml(rid)}</td>
      <td>${escapeHtml(prof.name||px.ho_ten||'')}</td>
      <td>${escapeHtml(String(prof.age||px.tuoi||''))}</td>
      <td>${escapeHtml(prof.gender||px.gioi_tinh||'')}</td>
      <td>${escapeHtml(prof.job||px.nghe_nghiep||'')}</td>
      <td>${escapeHtml(prof.social||px.than_phan_xa_hoi||'')}</td>
      <td>
        <button class="ghost" onclick="openProfile('${rid}')">Profile</button>
        <button class="ghost" onclick="openDialogue('${rid}')">Dialogue</button>
        <button class="ghost" onclick="openBehaviors('${rid}')">Behaviors</button>
        <button onclick="openChat('${rid}')">Chat</button>
        <button class="danger" onclick="confirmDelete('${rid}')">Del</button>
      </td></tr>`;
  }).join('');
  p.innerHTML = `
    <div class="row"><b>People (${currentPeopleType})</b><button class="ghost" onclick="addPerson()">+ Add</button></div>
    <table><thead><tr><th>ID</th><th>Tên</th><th>Tuổi</th><th>Giới</th><th>Nghề</th><th>Thân phận</th><th></th></tr></thead>
    <tbody>${rows}</tbody></table>
  `;
}
async function saveEntry(){
  const payload={region:document.getElementById('f_region').value,
    name:document.getElementById('f_name').value,class:document.getElementById('f_class').value,
    subtype:document.getElementById('f_subtype').value,description:document.getElementById('f_desc').value};
  if(payload.class==='khu') payload.population_count=Number(document.getElementById('f_pop').value||0);
  else payload.staff_count=Number(document.getElementById('f_staff').value||0);
  await api('/api/entries/'+current.id,'PUT',payload); toast('Saved'); loadEntries();
}
async function createEntry(){
  const payload={class:'khu',subtype:'khu_dan_cu',name:'New Entry',region:'',description:''};
  await api('/api/entries','POST',payload); loadEntries();
}
async function addPerson(){
  const prof=prompt('Tên|Tuổi|Giới|Nghề|Thân phận', 'Nguyễn Văn A|25|nam|nông dân|dân đinh')||'';
  const [name,age,gender,job,social]=(prof.split('|').map(s=>s.trim()));
  const payload={ profile:{name,age:Number(age||0),gender,job,social} };
  const url=current.class==='khu'?`/api/entries/${current.id}/residents`:`/api/entries/${current.id}/staff`;
  await api(url,'POST',payload); openEntry(current.id);
}
async function openProfile(rid){
  const arr=current[currentPeopleType]||[]; const p=arr.find(x=>x.resident_id===rid); if(!p) return;
  const prof=Object.assign({name:'',age:0,gender:'',job:'',social:''}, p.profile||{});
  const newer=prompt('Profile: Tên|Tuổi|Giới|Nghề|Thân phận', `${prof.name}|${prof.age}|${prof.gender}|${prof.job}|${prof.social}`);
  if(newer==null) return;
  const [name,age,gender,job,social]=(newer.split('|').map(s=>s.trim()));
  const payload={ profile:{name,age:Number(age||0),gender,job,social} };
  const url=current.class==='khu'?`/api/entries/${current.id}/residents/${rid}`:`/api/entries/${current.id}/staff/${rid}`;
  await api(url,'PUT',payload); openEntry(current.id);
}
async function openDialogue(rid){
  const arr=current[currentPeopleType]||[]; const p=arr.find(x=>x.resident_id===rid)||{};
  const dlg=p.dialogue||{ambient:[],interact:[],system_notes:''};
  const amb=prompt('Ambient (mỗi dòng 1 câu, placeholder {{name}}...)', (dlg.ambient||[]).join('\n'));
  if(amb==null) return;
  const it=prompt('Interact (mỗi dòng 1 câu, placeholder {{name}}...)', (dlg.interact||[]).join('\n')); if(it==null) return;
  const note=prompt('Notes', dlg.system_notes||''); if(note==null) return;
  const payload={ dialogue:{ ambient:(amb||'').split('\n').map(x=>x.trim()).filter(Boolean),
                            interact:(it||'').split('\n').map(x=>x.trim()).filter(Boolean),
                            system_notes:note }};
  const url=current.class==='khu'?`/api/entries/${current.id}/residents/${rid}`:`/api/entries/${current.id}/staff/${rid}`;
  await api(url,'PUT',payload); toast('Dialogue saved'); openEntry(current.id);
}
async function openBehaviors(rid){
  const json=prompt('Paste behaviors JSON (array of objects)', '[]'); if(json==null) return;
  let list=[]; try{ list=JSON.parse(json) }catch(e){ return alert('Invalid JSON')}
  const payload={behaviors:list}; const url=current.class==='khu'?`/api/entries/${current.id}/residents/${rid}`:`/api/entries/${current.id}/staff/${rid}`;
  await api(url,'PUT',payload); toast('Behaviors saved'); openEntry(current.id);
}
async function openChat(rid){
  const say=prompt('Bạn nói:'); if(say==null||!say.trim()) return;
  const j=await api('/api/ai/chat','POST',{entry_id:current.id,resident_id:rid,message:say});
  alert('NPC: '+j.reply);
}
async function confirmDelete(rid){
  if(!confirm('Xóa người này?')) return;
  const kind=currentPeopleType;
  const imp=await api(`/api/entries/${current.id}/${kind}/${rid}/impact`);
  if(!confirm(`Incoming=${imp.counts.incoming}, spouse=${imp.counts.spouse}, parents=${imp.counts.parents}, children=${imp.counts.children}, siblings=${imp.counts.siblings}. Tiếp tục?`)) return;
  await api(`/api/entries/${current.id}/${kind}/${rid}/cascade`,'DELETE'); openEntry(current.id);
}
async function scanFamilies(){ const j=await api('/api/tools/scan_families','POST',{}); toast('Scan done: '+(j.updated_fields||0)); if(current) openEntry(current.id); else loadEntries(); }
async function normalizeAll(){ const j=await api('/api/tools/normalize_all','POST',{}); toast('Normalized: '+(j.updated_records||0)); if(current) openEntry(current.id); else loadEntries(); }
async function loadGlobalChat(){
  const j=await api('/api/chat/recent?limit=60'); const el=document.getElementById('globalChat');
  el.innerHTML=(j.items||[]).map(x=>`<div class="small"><b>${escapeHtml(x.name||x.resident_id||'')}</b> <i>${escapeHtml(x.role)}</i>: ${escapeHtml(x.content||'')}</div>`).join('');
}
document.getElementById('btnReload').addEventListener('click', loadEntries);
document.getElementById('btnScan').addEventListener('click', scanFamilies);
document.getElementById('btnNormalize').addEventListener('click', normalizeAll);
document.getElementById('btnNewEntry').addEventListener('click', createEntry);
document.getElementById('filterClass').addEventListener('change', loadEntries);
document.getElementById('filterSubtype').addEventListener('change', loadEntries);
document.getElementById('searchBox').addEventListener('input', e=>{ clearTimeout(window.__q); window.__q=setTimeout(loadEntries,250); });
document.getElementById('page').addEventListener('change', loadEntries);
document.getElementById('pageSize').addEventListener('change', loadEntries);
loadEntries(); loadGlobalChat();
setInterval(loadGlobalChat, 5000);
// SSE
try{
  const es=new EventSource('/events');
  es.onmessage=(ev)=>{ try{const j=JSON.parse(ev.data); if(j.type){ toast(j.type); if(j.type.startsWith('entry_')||j.type.startsWith('person_')) loadEntries(); if(j.type==='chat') loadGlobalChat(); }}catch(e){} };
}catch(e){}
</script>
</body></html>
"""

@app.get("/")
def index_page() -> Response:
    return Response(INDEX_HTML, mimetype="text/html")

# ---------- Entrypoint ----------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default="world_with_personas.json", help="Path to JSON data file")
    parser.add_argument("--port", type=int, default=8000, help="Port to run")
    args = parser.parse_args()

    DATA_PATH = args.file
    if not os.path.exists(DATA_PATH):
        raise SystemExit(f"Data file not found: {DATA_PATH}")

    DATA = load_data(DATA_PATH)
    # Normalize dữ liệu cũ ngay khi khởi động để hợp nhất profile/persona & placeholders
    normalize_all(DATA)
    build_indexes()
    print(f"📄 Loaded {len(DATA)} entries from {DATA_PATH}")
    print(f"🌐 Opening UI at: http://127.0.0.1:{args.port}")
    # threaded=True cho SSE + nhiều request đồng thời
    app.run(host="127.0.0.1", port=args.port, debug=True, threaded=True)
