#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
stream_server.py — خادم MTProto للبث المباشر من Telegram
★ نسخة محسّنة: إصلاح asyncio + pre-fetch + CORS مضمون + إعادة محاولة ذكية ★
"""

# ═══════════════════════════════════════════════════════════════
# 0) إصلاح توافق asyncio مع Python 3.12+
#    يجب أن يكون قبل أي استيراد لـ pyrogram
# ═══════════════════════════════════════════════════════════════
import asyncio

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

# ═══════════════════════════════════════════════════════════════
# 1) الاستيرادات
# ═══════════════════════════════════════════════════════════════
import json
import os
import re
import time
import traceback
from collections import OrderedDict
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (FileResponse, JSONResponse, Response,
                               StreamingResponse)
from pyrogram import Client
from pyrogram.errors import FileReferenceExpired, FloodWait
from pyrogram.file_id import FileId
from pyrogram.raw.functions.upload import GetFile
from pyrogram.raw.types import (
    InputDocumentFileLocation,
    InputPhotoFileLocation,
)

# ═══════════════════════════════════════════════════════════════
# 2) الإعدادات من المتغيرات البيئية
# ═══════════════════════════════════════════════════════════════
API_ID = int(os.environ.get("API_ID", "0") or "0")
API_HASH = os.environ.get("API_HASH", "").strip()
STRING_SESSION = os.environ.get(
    "STRING_SESSION",
    os.environ.get("SESSION_STRING", ""),
).strip()
# دعم عدة قنوات: CHANNELS="shoofcima,shoofFilm" (مفصولة بفواصل).
# للتوافق الخلفي: CHANNEL أو CHANNEL_ID لقناة واحدة.
_channels_raw = (
    os.environ.get("CHANNELS")
    or os.environ.get("CHANNEL")
    or os.environ.get("CHANNEL_ID")
    or "shoofcima,shoofFilm"   # القناتان افتراضيًا
)
_env_channels = [c.strip() for c in _channels_raw.split(",") if c.strip()]
# ★ القناتان دائمًا معًا (حتى لو متغيّر البيئة يذكر واحدة فقط)
CHANNELS = []
for _c in ["shoofcima", "shoofFilm"] + _env_channels:
    if _c and _c not in CHANNELS:
        CHANNELS.append(_c)
CHANNEL_ID = CHANNELS[0] if CHANNELS else ""  # توافق خلفي

# تحقق صارم قبل أي شيء
_missing = []
if not API_ID:
    _missing.append("API_ID")
if not API_HASH:
    _missing.append("API_HASH")
if not STRING_SESSION:
    _missing.append("STRING_SESSION")

NO_SESSION_MODE = not STRING_SESSION   # يمكن التشغيل لإنشاء جلسة جديدة
if _missing and not NO_SESSION_MODE:
    print(f"❌ متغيرات بيئية ناقصة: {', '.join(_missing)}")
    raise SystemExit(1)
if NO_SESSION_MODE:
    print("⚠️ تشغيل بدون جلسة — استخدم /auth/send_code ثم /auth/verify")

# قواعد Telegram
BLOCK_SIZE = 4096                      # الحجم الأدنى الذي يقرأه Telegram
CHUNK_SIZE = 1024 * 1024               # 1MB للبث

# ═══════════════════════════════════════════════════════════════
# ★ تسخين الكتلة الأولى (warm start)
#   أول طلب لكل حلقة كان يدفع ثمن قراءة الرسالة + GetFile (~9 ث).
#   نخزّن الكتلة الأولى في الذاكرة ونُسخّنها مسبقًا في الخلفية،
#   فيصير بدء التشغيل فوريًا والحلقة تُشغَّل من الكاش بلا Telegram.
# ═══════════════════════════════════════════════════════════════
WARM_ENABLED = os.environ.get("WARM_ENABLED", "1").lower() in ("1", "true", "yes")
WARM_MAX = int(os.environ.get("WARM_MAX", "64"))       # عدد الكتل المحفوظة
WARM_COUNT = int(os.environ.get("WARM_COUNT", "60"))    # عدد الحلقات المسخّنة
WARM_GAP = float(os.environ.get("WARM_GAP", "1.2"))     # فاصل بين الطلبات
WARM_SOURCE_URL = os.environ.get(
    "WARM_SOURCE_URL", "https://web-production-fd654.up.railway.app/videos.json")
_WARM = OrderedDict()          # (ch, mid, offset, limit) -> bytes
_WARM_STATS = {"warmed": 0, "hits": 0, "miss": 0, "last_error": ""}


def _warm_key(ch, mid, offset, limit):
    return (ch or "", int(mid or 0), int(offset), int(limit))


def warm_get(ch, mid, offset, limit, count=True):
    """يقرأ الكتلة من الذاكرة (None إن لم تكن مسخّنة)."""
    if not WARM_ENABLED:
        return None
    key = _warm_key(ch, mid, offset, limit)
    data = _WARM.get(key)
    if data is None:
        if count:
            _WARM_STATS["miss"] += 1
        return None
    _WARM.move_to_end(key)
    if count:
        _WARM_STATS["hits"] += 1
    return data


def warm_put(ch, mid, offset, limit, data):
    """يخزّن الكتلة في الذاكرة (LRU محدود)."""
    if not WARM_ENABLED or not data or int(mid or 0) <= 0:
        return
    key = _warm_key(ch, mid, offset, limit)
    _WARM[key] = data
    _WARM.move_to_end(key)
    while len(_WARM) > WARM_MAX:
        _WARM.popitem(last=False)
MAX_REFRESH_PER_STREAM = 3             # حد إعادة محاولة file_reference
STREAM_TIMEOUT = 600                   # 10 دقائق كحد أقصى للبث الواحد

# ═══════════════════════════════════════════════════════════════
# 3) CORS Headers — ثابتة لكل الردود
# ═══════════════════════════════════════════════════════════════
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
    "Access-Control-Allow-Headers": "Range, Content-Type, Accept, Origin",
    "Access-Control-Expose-Headers": (
        "Content-Length, Content-Range, Accept-Ranges, Content-Type"
    ),
    "Access-Control-Max-Age": "86400",
}

# ═══════════════════════════════════════════════════════════════
# 4) عميل MTProto
# ═══════════════════════════════════════════════════════════════
client = (
    Client(
        "stream_session",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=STRING_SESSION,
        in_memory=True,          # لا نكتب على القرص في Railway
        workers=8,               # زيادة من 4 → 8 لتحمّل متزامن أعلى
        no_updates=True,         # لا نحتاج تحديثات
    )
    if not NO_SESSION_MODE
    else None
)

AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "").strip()

# حالة عامة
START_TIME = time.time()
_auth_client = None


# ═══════════════════════════════════════════════════════════════
# 5) Lifespan — تشغيل/إيقاف العميل
# ═══════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    if NO_SESSION_MODE:
        print("⚠️ لا جلسة — الخادم يعمل لواجهات /auth فقط")
        yield
        return
    print("🚀 بدء تشغيل عميل MTProto...")
    try:
        await client.start()
        me = await client.get_me()
        print(f"✅ العميل يعمل كـ: @{me.username or me.id}")
        if CHANNELS:
            print(f"📺 قنوات التحديث: {', '.join(CHANNELS)}")
        else:
            print("⚠️  CHANNELS غير مضبوط — لن يمكن تحديث file_reference")
    except Exception as e:
        print(f"❌ فشل تشغيل العميل: {e}")
        traceback.print_exc()
        raise

    warm_task = asyncio.create_task(warm_loop())

    yield

    warm_task.cancel()
    try:
        await warm_task
    except (asyncio.CancelledError, Exception):
        pass

    print("👋 إيقاف عميل MTProto...")
    try:
        await client.stop()
    except Exception as e:
        print(f"⚠️ خطأ أثناء الإيقاف: {e}")
    print("✅ تم الإيقاف")


# ═══════════════════════════════════════════════════════════════
# 6) تطبيق FastAPI
# ═══════════════════════════════════════════════════════════════
app = FastAPI(
    title="Telegram Stream Server",
    version="2.1-stream-timeout",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["Range", "Content-Type", "Accept", "Origin"],
    expose_headers=[
        "Content-Length",
        "Content-Range",
        "Accept-Ranges",
        "Content-Type",
    ],
    max_age=86400,
)


@app.middleware("http")
async def ensure_cors(request: Request, call_next):
    """يضمن CORS في كل الردود حتى عند الأخطاء."""
    try:
        response = await call_next(request)
    except Exception as e:
        print(f"❌ Middleware error: {e}")
        traceback.print_exc()
        response = JSONResponse(
            {"error": "internal_error", "message": str(e)},
            status_code=500,
            headers=CORS_HEADERS,
        )
    for k, v in CORS_HEADERS.items():
        response.headers[k] = v
    return response


# ═══════════════════════════════════════════════════════════════
# 7) Endpoints أساسية
# ═══════════════════════════════════════════════════════════════
@app.get("/")
async def root():
    return {
        "service": "Telegram Stream Server",
        "version": "2.1-stream-timeout",
        "status": "ok",
        "uptime_seconds": int(time.time() - START_TIME),
    }


@app.get("/health")
async def health():
    """فحص صحي — Railway يستخدمه لمراقبة الخدمة."""
    try:
        connected = client.is_connected
    except Exception:
        connected = False
    return {
        "status": "ok" if connected else "degraded",
        "mtproto_connected": connected,
        "uptime_seconds": int(time.time() - START_TIME),
        "warm": {
            "enabled": WARM_ENABLED,
            "cached": len(_WARM),
            "max": WARM_MAX,
            "hits": _WARM_STATS["hits"],
            "warmed_total": _WARM_STATS["warmed"],
            "last_error": _WARM_STATS["last_error"],
        },
    }


def _auth_ok(token: str) -> bool:
    return bool(AUTH_TOKEN) and token == AUTH_TOKEN


@app.get("/auth/send_code")
async def auth_send_code(phone: str, token: str):
    """يرسل كود تسجيل الدخول لتليجرام لإنشاء جلسة جديدة."""
    global _auth_client
    if not _auth_ok(token):
        raise HTTPException(403, "forbidden")
    from pyrogram import Client as _C
    _auth_client = _C("auth_tmp", api_id=API_ID, api_hash=API_HASH, in_memory=True)
    await _auth_client.connect()
    sent = await _auth_client.send_code(phone)
    return {"phone_code_hash": sent.phone_code_hash}


@app.get("/auth/verify")
async def auth_verify(code: str, phone: str, phone_code_hash: str, token: str):
    """يتحقق من الكود ويعيد session string."""
    global _auth_client
    if not _auth_ok(token):
        raise HTTPException(403, "forbidden")
    if _auth_client is None:
        raise HTTPException(400, "call /auth/send_code first")
    try:
        await _auth_client.sign_in(phone, phone_code_hash=phone_code_hash, phone_code=code)
    except Exception as e:
        msg = str(e)
        if "SESSION_PASSWORD_NEEDED" in msg.upper():
            raise HTTPException(400, "two_step_password_required")
        raise HTTPException(400, msg[:200])
    sess = await _auth_client.export_session_string()
    me = await _auth_client.get_me()
    # ★ إغلاق عميل التسجيل فورًا: بقاؤه متصلًا يعني أن المفتاح الجديد
    #   «مستخدم في مكانين» فيُرفض بـ406 عند استخدامه في الكرنل.
    try:
        await _auth_client.stop()
    except Exception:
        try:
            await _auth_client.disconnect()
        except Exception:
            pass
    _auth_client = None
    return {"session_string": sess, "user_id": me.id, "username": me.username}


@app.get("/thumb")
async def thumb(mid: int, fid: str = "", ch: str = ""):
    """صورة العمل (مصغّرة الحلقة) مباشرة من تليجرام — تُستخدم كبوستر."""
    if client is None or not getattr(client, "is_connected", False):
        raise HTTPException(503, "not_connected")
    # ★ لا اختلاط بين القناتين: لو وصل file_id نتحقق أن الرسالة هي نفس الملف
    expected_mid = None
    if fid:
        try:
            expected_mid = FileId.decode(fid).media_id
        except Exception:
            expected_mid = None

    order = ([ch] if ch and ch in CHANNELS else []) + [c for c in CHANNELS if c != ch]
    msg = None
    for c in order:
        try:
            m = await asyncio.wait_for(
                client.get_messages(c, int(mid)), timeout=12)
        except Exception:
            m = None
        if not m:
            continue
        if expected_mid is not None:
            got = None
            if m.video:
                try:
                    got = FileId.decode(m.video.file_id).media_id
                except Exception:
                    got = None
            elif m.document:
                try:
                    got = FileId.decode(m.document.file_id).media_id
                except Exception:
                    got = None
            if got != expected_mid:
                continue   # رسالة رقمها متطابق لكن الملف مختلف → قناة أخرى
        msg = m
        break
    if not msg:
        raise HTTPException(404, "not_found")
    path = None
    try:
        if msg.video and msg.video.thumbs:
            path = await asyncio.wait_for(
                client.download_media(msg.video.thumbs[-1], file_name="/tmp/t.jpg"), timeout=20)
        elif msg.photo:
            path = await asyncio.wait_for(
                client.download_media(msg.photo.file_id, file_name="/tmp/t.jpg"), timeout=20)
    except Exception:
        raise HTTPException(504, "download_timeout")
    if not path or not os.path.exists(path):
        raise HTTPException(404, "no_thumb")
    return FileResponse(path, media_type="image/jpeg",
                        headers={"Cache-Control": "public, max-age=604800"})


@app.get("/admin/reconnect")
async def admin_reconnect():
    """إعادة توصيل MTProto يدويًا عند تعليق البث."""
    ok = await reconnect_client()
    return {"reconnected": ok, "connected": bool(getattr(client, "is_connected", False))}


# ═══════════════════════════════════════════════════════════════
# 8) دوال مساعدة
# ═══════════════════════════════════════════════════════════════
def _extract_file_id(msg) -> str:
    """يستخرج file_id من أي نوع وسائط."""
    for attr in ("video", "document", "audio", "animation", "voice"):
        media = getattr(msg, attr, None)
        if media and hasattr(media, "file_id") and media.file_id:
            return media.file_id
    return ""


async def _find_file_id_in_channel(channel, message_id, expected_media_id=None):
    """
    يبحث عن file_id للرسالة داخل قناة واحدة.
    يجرب get_messages أولاً ثم get_chat_history.
    إذا حُدّد expected_media_id فلا يقبل إلا الوسيط المطابق.
    """
    # الطريقة 1: get_messages مباشرة
    try:
        msg = await client.get_messages(channel, message_ids=message_id)
        if msg and msg.media:
            new_fid = _extract_file_id(msg)
            if new_fid:
                if expected_media_id is None or FileId.decode(new_fid).media_id == expected_media_id:
                    return new_fid
    except FloodWait as e:
        print(f"⚠️ FloodWait: انتظار {e.value}s")
        await asyncio.sleep(e.value)
    except Exception as e:
        print(f"⚠️ get_messages({channel}) فشل: {e}")

    # الطريقة 2: البحث في سجل المحادثة
    try:
        async for msg in client.get_chat_history(channel, limit=500):
            if msg.id == message_id and msg.media:
                new_fid = _extract_file_id(msg)
                if new_fid:
                    if expected_media_id is None or FileId.decode(new_fid).media_id == expected_media_id:
                        return new_fid
    except Exception as e:
        print(f"⚠️ get_chat_history({channel}) فشل: {e}")

    return ""


async def refresh_file_id(message_id: int, expected_fid: str = "") -> str:
    """
    يحدّث file_id عندما ينتهي file_reference.
    يجرب كل القنوات المضبوطة (CHANNELS) حتى يجد الرسالة،
    ويتحقق من تطابق media_id لتفادي أخذ رسالة خاطئة عند تعارض الأرقام بين القنوات.
    """
    if not CHANNELS or not message_id:
        print("⚠️ لا يمكن التحديث: CHANNELS أو message_id مفقود")
        return ""

    expected_media_id = None
    if expected_fid:
        try:
            expected_media_id = FileId.decode(expected_fid).media_id
        except Exception:
            expected_media_id = None

    print(f"🔄 تحديث file_id للرسالة {message_id} (قنوات: {', '.join(CHANNELS)})...")

    for channel in CHANNELS:
        new_fid = await _find_file_id_in_channel(channel, message_id, expected_media_id)
        if new_fid:
            print(f"✅ تم التحديث من {channel}: {new_fid[:40]}...")
            return new_fid

    print("❌ كل طرق التحديث فشلت")
    return ""


INVOKE_TIMEOUT = 25          # ثانية — لا نترك الطلب معلّقًا أبدًا


async def _get_file(location, offset: int, limit: int, timeout: int = INVOKE_TIMEOUT):
    """GetFile بمهلة صارمة (كان يعلّق بلا نهاية عند موت اتصال DC)."""
    return await asyncio.wait_for(
        client.invoke(GetFile(location=location, offset=offset, limit=limit)),
        timeout=timeout,
    )


async def reconnect_client():
    """إعادة توصيل MTProto بعد تعليق الاتصال."""
    print("🔌 إعادة توصيل عميل MTProto...")
    try:
        await asyncio.wait_for(client.stop(), 15)
    except Exception:
        pass
    try:
        await client.start()
        me = await client.get_me()
        print(f"✅ أُعيد الاتصال كـ @{me.username or me.id}")
        return True
    except Exception as e:
        print(f"❌ فشل إعادة الاتصال: {e}")
        return False


def build_location(file_id: FileId):
    """يبني InputFileLocation الصحيح حسب نوع الملف."""
    ft_str = str(file_id.file_type).lower()

    DOC_TYPES = {
        "video", "document", "audio", "animation", "gif",
        "voice", "sticker", "secure",
        "3", "4", "5", "6", "7", "8", "9",
    }
    PHOTO_TYPES = {"photo", "profile_photo", "thumbnail", "0", "1", "2"}

    if ft_str in PHOTO_TYPES:
        return InputPhotoFileLocation(
            id=file_id.media_id,
            access_hash=file_id.access_hash,
            file_reference=file_id.file_reference,
            thumb_size="",
        )

    # افتراضي: مستند
    return InputDocumentFileLocation(
        id=file_id.media_id,
        access_hash=file_id.access_hash,
        file_reference=file_id.file_reference,
        thumb_size="",
    )


def align_down(x: int, alignment: int) -> int:
    """يقرّب لأسفل لأقرب مضاعف."""
    return (x // alignment) * alignment


# ═══════════════════════════════════════════════════════════════
# 9) جلب كتلة واحدة مع معالجة الأخطاء
# ═══════════════════════════════════════════════════════════════
async def fetch_chunk(
    location,
    offset: int,
    limit: int,
    mid: int,
    current_fid: str,
    refresh_state: dict,
):
    """
    يجلب كتلة واحدة من Telegram.
    يعيد: (data, new_location, new_fid)
    """
    try:
        result = await _get_file(location, offset, limit)
        return result.bytes, location, current_fid

    except asyncio.TimeoutError:
        print(f"⏱️ مهلة GetFile عند offset={offset} — إعادة توصيل")
        await reconnect_client()
        try:
            result = await _get_file(location, offset, limit, timeout=40)
            return result.bytes, location, current_fid
        except Exception as e:
            print(f"❌ فشل بعد إعادة التوصيل: {e}")
            return b"", location, current_fid

    except FileReferenceExpired:
        print(f"🔄 FileReferenceExpired عند offset={offset}")

        if refresh_state["attempts"] >= MAX_REFRESH_PER_STREAM or not mid:
            print("❌ لا يمكن التحديث — إيقاف")
            return b"", location, current_fid

        refresh_state["attempts"] += 1
        new_fid = await refresh_file_id(mid, current_fid)
        if not new_fid:
            return b"", location, current_fid

        try:
            decoded = FileId.decode(new_fid)
            new_location = build_location(decoded)
            print(f"✅ تحديث ناجح (محاولة {refresh_state['attempts']})")

            result = await _get_file(new_location, offset, limit)
            return result.bytes, new_location, new_fid
        except Exception as e:
            print(f"❌ فشل decode بعد التحديث: {e}")
            return b"", location, current_fid

    except FloodWait as e:
        print(f"⚠️ FloodWait عند offset={offset}: انتظار {e.value}s")
        await asyncio.sleep(e.value)
        # إعادة محاولة واحدة
        try:
            result = await _get_file(location, offset, limit)
            return result.bytes, location, current_fid
        except Exception as e2:
            print(f"❌ فشل بعد FloodWait: {e2}")
            return b"", location, current_fid

    except Exception as e:
        print(f"⚠️ خطأ كتلة عند offset={offset}: {e}")
        traceback.print_exc()
        return b"", location, current_fid


# ═══════════════════════════════════════════════════════════════
# 9.5) تسخين مسبق للكتلة الأولى
# ═══════════════════════════════════════════════════════════════
def _load_warm_items():
    """أول حلقة من كل عمل، من كتالوج الموقع (لا يحتاج Telegram)."""
    import urllib.request
    try:
        with urllib.request.urlopen(WARM_SOURCE_URL, timeout=25) as r:
            vids = json.loads(r.read().decode("utf-8"))
    except Exception as e:
        _WARM_STATS["last_error"] = f"{type(e).__name__}: {e}"
        print(f"⚠️ تعذّر جلب قائمة التسخين: {e}")
        return []
    first = {}
    for v in vids if isinstance(vids, list) else []:
        mid = int(v.get("mid") or 0)
        if mid <= 0:
            continue
        ch = (v.get("ch") or "").strip()
        key = (v.get("series") or "", ch)
        try:
            ep = int(v.get("episode") or 0)
        except Exception:
            ep = 0
        cur = first.get(key)
        if cur is None or (ep and (not cur[0] or ep < cur[0])):
            first[key] = (ep, mid, ch)
    return [(ch, mid) for (_ep, mid, ch) in first.values()]


async def _warm_episode(ch, mid, limit=CHUNK_SIZE):
    """يجلب الكتلة الأولى لحلقة ويحفظها في الذاكرة."""
    if mid <= 0 or client is None or not getattr(client, "is_connected", False):
        return False
    if _warm_key(ch, mid, 0, limit) in _WARM:
        return True
    order = [ch] if ch in CHANNELS else list(CHANNELS)
    media = None
    for name in order:
        try:
            m = await client.get_messages(name, mid)
        except Exception:
            m = None
        media = (getattr(m, "video", None) or getattr(m, "document", None)) if m else None
        if media:
            break
    if not media:
        return False
    try:
        location = build_location(FileId.decode(media.file_id))
        data, _loc, _fid = await fetch_chunk(
            location, 0, limit, mid, media.file_id, {"attempts": 0})
    except Exception as e:
        _WARM_STATS["last_error"] = f"{type(e).__name__}: {e}"
        return False
    if not data:
        return False
    warm_put(ch, mid, 0, limit, data)
    _WARM_STATS["warmed"] += 1
    return True


async def warm_loop():
    """تسخين عند البدء ثم كل 6 ساعات."""
    if not WARM_ENABLED:
        print("ℹ️  التسخين المسبق معطّل (WARM_ENABLED=0)")
        return
    await asyncio.sleep(15)
    while True:
        try:
            items = await asyncio.to_thread(_load_warm_items)
            todo = items[:WARM_COUNT]
            print(f"🔥 تسخين مسبق: {len(todo)} حلقة...")
            done = 0
            for ch, mid in todo:
                if await _warm_episode(ch, mid, CHUNK_SIZE):
                    done += 1
                await asyncio.sleep(WARM_GAP)
            print(f"✅ تسخين مسبق: {done} كتلة أولى جاهزة (المخزون {len(_WARM)})")
        except Exception as e:
            _WARM_STATS["last_error"] = f"{type(e).__name__}: {e}"
            print(f"⚠️ حلقة التسخين: {e}")
        await asyncio.sleep(6 * 3600)


async def _resolve_media(ch, mid):
    """يعيد (file_id, size, اسم القناة) من رسالة Telegram."""
    order = [ch] if ch in CHANNELS else list(CHANNELS)
    for name in order:
        try:
            m = await client.get_messages(name, mid)
        except Exception:
            m = None
        media = (getattr(m, "video", None) or getattr(m, "document", None)) if m else None
        if media:
            return (media.file_id, int(getattr(media, "file_size", 0) or 0), name)
    return "", 0, ch


# ═══════════════════════════════════════════════════════════════
# 10) Endpoint البث
# ═══════════════════════════════════════════════════════════════
@app.options("/stream")
async def stream_options():
    """preflight request."""
    return Response(status_code=200, headers=CORS_HEADERS)


@app.head("/stream")
async def stream_head(fid: str, size: int = 0, mid: int = 0):
    """HEAD — يُرجع الترويسات فقط بدون بث."""
    if not fid or not size:
        return Response(status_code=400, headers=CORS_HEADERS)
    return Response(
        status_code=200,
        headers={
            **CORS_HEADERS,
            "Content-Type": "video/mp4",
            "Content-Length": str(size),
            "Accept-Ranges": "bytes",
        },
    )


@app.get("/stream")
async def stream(
    request: Request,
    fid: str = "",
    size: int = 0,
    mid: int = 0,
    ch: str = "",
):
    """
    بث الفيديو. pre-fetch الكتلة الأولى لضمان CORS.
    يدعم Range requests (تقديم/إرجاع).

    ★ mid + ch هما المصدر الموثوق: يُقرأ الملف من الرسالة نفسها،
      حتى لا يُبَث ملف قديم/عمل آخر بنفس المعرّف.
    """
    # ─── تحديد الملف: الرسالة أولًا ثم المعرّف ───
    # ★ الكتلة الأولى من الذاكرة إن كانت مسخّنة (توفير قراءة الرسالة + GetFile)
    cached_first = warm_get(ch, mid, 0, CHUNK_SIZE, count=False) if mid > 0 and size > 0 else None

    if (mid > 0 and (cached_first is None or not fid)
            and client is not None and getattr(client, "is_connected", False)):
        _fid, _size, _ch = await _resolve_media(ch, mid)
        if _fid:
            fid = _fid
            size = _size or size
            ch = _ch
            if cached_first is None:
                cached_first = warm_get(ch, mid, 0, CHUNK_SIZE, count=False)

    if not fid and cached_first is None:
        raise HTTPException(400, "missing fid parameter")

    file_id = None
    if fid:
        try:
            file_id = FileId.decode(fid)
        except Exception as e:
            if cached_first is None:
                print(f"❌ FileId.decode فشل: {e}")
                raise HTTPException(400, f"invalid file_id: {e}")
            print(f"⚠️ file_id غير صالح لكن أول كتلة مسخّنة: {e}")

    file_size = int(size)
    if file_size <= 0:
        raise HTTPException(400, "missing or invalid 'size' parameter")

    # ─── تحليل Range ───
    range_header = request.headers.get("range")
    start, end = 0, file_size - 1

    if range_header:
        m = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if m:
            start = int(m.group(1))
            end = int(m.group(2)) if m.group(2) else file_size - 1
            if start > end or end >= file_size or start < 0:
                raise HTTPException(
                    416,
                    "range not satisfiable",
                    headers={"Content-Range": f"bytes */{file_size}"},
                )

    length = end - start + 1

    ftype = file_id.file_type if file_id is not None else "cached"
    print(
        f"📥 بث: type={ftype}, size={file_size / 1024 / 1024:.1f}MB, "
        f"range={start}-{end} ({length / 1024 / 1024:.2f}MB), mid={mid}, "
        f"warm={'yes' if cached_first else 'no'}"
    )

    location = build_location(file_id) if file_id is not None else None
    aligned_start = align_down(start, CHUNK_SIZE)
    skip = start - aligned_start

    # ★ إن لم يكن لدينا file_id (رابط بالمعرّف فقط) نجلبه الآن قبل أي كتلة
    if location is None:
        _fid, _size, _ch = await _resolve_media(ch, mid)
        if not _fid:
            return JSONResponse(
                status_code=503,
                content={"error": "stream_unavailable",
                         "message": "تعذّر جلب الفيديو من Telegram. حاول مجدداً."},
                headers=CORS_HEADERS,
            )
        ch = _ch or ch
        location = build_location(FileId.decode(_fid))

    # ═══════════════════════════════════════════════════════════
    # pre-fetch الكتلة الأولى قبل الترويسات
    # ═══════════════════════════════════════════════════════════
    refresh_state = {"attempts": 0}

    first_data = b""
    if aligned_start == 0 and cached_first:
        first_data = cached_first
        new_fid = fid
        print(f"⚡ كتلة أولى من الذاكرة: {len(first_data)} bytes")
    else:
        try:
            first_data, location, new_fid = await fetch_chunk(
                location,
                aligned_start,
                CHUNK_SIZE,
                mid,
                fid,
                refresh_state,
            )
            if aligned_start == 0:
                warm_put(ch, mid, 0, CHUNK_SIZE, first_data)
        except Exception as e:
            print(f"❌ pre-fetch فشل: {e}")
            traceback.print_exc()
            first_data = b""

    if not first_data:
        print("❌ pre-fetch أعاد بيانات فارغة")
        return JSONResponse(
            status_code=503,
            content={
                "error": "stream_unavailable",
                "message": "تعذّر جلب الفيديو من Telegram. حاول مجدداً.",
            },
            headers=CORS_HEADERS,
        )

    # قص الكتلة الأولى حسب skip/length
    if skip > 0:
        first_data = first_data[skip:]
    if len(first_data) > length:
        first_data = first_data[:length]

    first_sent = len(first_data)
    print(f"✅ pre-fetch: {first_sent} bytes")

    # ═══════════════════════════════════════════════════════════
    # مولّد البث
    # ═══════════════════════════════════════════════════════════
    async def generate():
        nonlocal location
        try:
            yield first_data

            if first_sent >= length:
                print(f"✅ اكتمل (كتلة واحدة): {first_sent} bytes")
                return

            read_offset = aligned_start + CHUNK_SIZE
            remaining = length - first_sent
            total_sent = first_sent
            started = time.time()

            while remaining > 0:
                # حماية من البث الطويل جداً
                if time.time() - started > STREAM_TIMEOUT:
                    print(f"⏱️ تجاوز وقت البث ({STREAM_TIMEOUT}s)")
                    break

                try:
                    data, location, _ = await fetch_chunk(
                        location,
                        read_offset,
                        CHUNK_SIZE,
                        mid,
                        fid,
                        refresh_state,
                    )
                except Exception as e:
                    print(f"⚠️ فشل كتلة عند offset={read_offset}: {e}")
                    break

                if not data:
                    print(f"⚠️ كتلة فارغة عند offset={read_offset}")
                    break

                if len(data) > remaining:
                    data = data[:remaining]

                yield data
                total_sent += len(data)
                read_offset += CHUNK_SIZE
                remaining -= len(data)

                if total_sent % (10 * CHUNK_SIZE) == 0:
                    print(f"   ... أُرسل {total_sent / 1024 / 1024:.1f}MB")

            if remaining > 0:
                print(f"⚠️ انتهى مبكراً: {remaining / 1024 / 1024:.2f}MB متبقية")
            else:
                print(f"✅ اكتمل: {total_sent / 1024 / 1024:.2f}MB")

        except asyncio.CancelledError:
            print("🛑 العميل أغلق الاتصال (CancelledError)")
            raise
        except Exception as e:
            print(f"❌ خطأ غير متوقع في generate: {e}")
            traceback.print_exc()
            raise

    # ─── الترويسات ───
    headers = {
        **CORS_HEADERS,
        "Content-Type": "video/mp4",
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=86400",
        "Content-Length": str(length),
    }

    if range_header:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        return StreamingResponse(generate(), status_code=206, headers=headers)

    return StreamingResponse(generate(), status_code=200, headers=headers)


# ═══════════════════════════════════════════════════════════════
# 11) معالج الأخطاء العام
# ═══════════════════════════════════════════════════════════════
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    print(f"❌ استثناء غير معالج: {exc}")
    traceback.print_exc()
    return JSONResponse(
        {"error": "internal_error", "message": str(exc)},
        status_code=500,
        headers=CORS_HEADERS,
    )


# ═══════════════════════════════════════════════════════════════
# 12) نقطة التشغيل
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    host = os.environ.get("HOST", "0.0.0.0")

    print(f"🚀 بدء Telegram Stream Server على {host}:{port}")

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level="info",
        access_log=True,
        timeout_keep_alive=75,
        h11_max_incomplete_event_size=16 * 1024,
    )

# ===================== إدارة القناتين (بمفتاح AUTH_TOKEN) =====================

def _check_token(token: str):
    if not AUTH_TOKEN or token != AUTH_TOKEN:
        raise HTTPException(403, "forbidden")


def _msg_info(m, chat_name: str) -> dict:
    cap = (m.caption or m.text or "").strip().replace("\n", " ")[:160]
    media = m.video or m.document
    fid = ""
    size = 0
    dur = 0
    try:
        if m.video:
            fid = m.video.file_id
            size = m.video.file_size or 0
            dur = m.video.duration or 0
        elif m.document:
            fid = m.document.file_id
            size = m.document.file_size or 0
    except Exception:
        pass
    return {
        "chat": chat_name,
        "mid": m.id,
        "fid": fid,
        "size": size,
        "duration": dur,
        "caption": cap,
        "date": m.date.isoformat() if m.date else "",
    }


async def _iter_channel(chat_name: str, limit: int, query: str = ""):
    out = []
    if query:
        async for m in client.search_messages(chat_name, query=query, limit=limit):
            out.append(_msg_info(m, chat_name))
    else:
        async for m in client.get_chat_history(chat_name, limit=limit):
            out.append(_msg_info(m, chat_name))
    return out


@app.get("/admin/me")
async def admin_me(token: str):
    """رقم الهاتف المسجّل في الجلسة الحالية (لبدء جلسة جديدة منفصلة)."""
    _check_token(token)
    if client is None or not getattr(client, "is_connected", False):
        raise HTTPException(503, "not_connected")
    try:
        me = await client.get_me()
    except Exception as e:
        raise HTTPException(500, str(e)[:150])
    return {
        "id": me.id,
        "username": me.username,
        "phone": getattr(me, "phone_number", "") or "",
        "first_name": me.first_name,
    }


@app.get("/admin/scan")
async def admin_scan(token: str, title: str = "", limit: int = 5000):
    """يبحث عن عمل في القناتين ويعيد حلقاته من كل قناة."""
    _check_token(token)
    if client is None or not getattr(client, "is_connected", False):
        raise HTTPException(503, "not_connected")
    res = {}
    for ch in CHANNELS:
        try:
            res[ch] = await _iter_channel(ch, limit, title) if title else await _iter_channel(ch, limit)
        except Exception as e:
            res[ch] = [{"error": str(e)[:120]}]
    return res


@app.get("/admin/message")
async def admin_message(token: str, mid: int, ch: str = ""):
    """بيانات رسالة واحدة في القنوات: الاسم الحقيقي للملف + الوصف + الحجم + المدة."""
    _check_token(token)
    if client is None or not getattr(client, "is_connected", False):
        raise HTTPException(503, "not_connected")
    names = [ch] if ch in CHANNELS else list(CHANNELS)
    out = {}
    for name in names:
        try:
            m = await client.get_messages(name, mid)
        except Exception as e:
            out[name] = {"error": str(e)[:150]}
            continue
        if not m or getattr(m, "empty", False):
            out[name] = None
            continue
        media = getattr(m, "video", None) or getattr(m, "document", None)
        out[name] = {
            "caption": (getattr(m, "caption", "") or "")[:300],
            "file_name": getattr(media, "file_name", "") if media else "",
            "size": int(getattr(media, "file_size", 0) or 0) if media else 0,
            "duration": int(getattr(media, "duration", 0) or 0) if media else 0,
            "date": m.date.isoformat() if getattr(m, "date", None) else "",
            "kind": type(media).__name__ if media else "",
        }
    return out


@app.get("/admin/duplicates")
async def admin_duplicates(token: str, limit: int = 5000):
    """يكشف التكرار بين القناتين: نفس الملف، أو نفس الاسم والحلقة."""
    _check_token(token)
    if client is None or not getattr(client, "is_connected", False):
        raise HTTPException(503, "not_connected")
    all_msgs = []
    for ch in CHANNELS:
        try:
            all_msgs.extend(await _iter_channel(ch, limit))
        except Exception:
            pass
    by_fid = {}
    for m in all_msgs:
        if m["fid"]:
            by_fid.setdefault(m["fid"], []).append(m)
    same_file = [v for v in by_fid.values() if len({x["chat"] for x in v}) > 1]

    import re as _re
    def key(cap):
        t = _re.sub(r"-\s*S\d+E\d+.*$", "", cap).strip()
        t = _re.sub(r"[^\u0621-\u064Aa-zA-Z0-9]", "", t).lower()
        ep = _re.search(r"S(\d+)E(\d+)", cap)
        return (t, ep.group(1), ep.group(2)) if ep else (t, "", "")

    by_key = {}
    for m in all_msgs:
        if m["fid"]:
            by_key.setdefault(key(m["caption"]), []).append(m)
    same_ep = [v for k, v in by_key.items() if k[0] and len({x["chat"] for x in v}) > 1]

    return {
        "total": len(all_msgs),
        "per_channel": {ch: sum(1 for m in all_msgs if m["chat"] == ch) for ch in CHANNELS},
        "same_file_groups": len(same_file),
        "same_episode_groups": len(same_ep),
        "same_file": [[{k: x[k] for k in ("chat", "mid", "caption")} for x in g[:4]] for g in same_file[:400]],
        "same_episode": [[{k: x[k] for k in ("chat", "mid", "caption")} for x in g[:4]] for g in same_ep[:400]],
    }


@app.post("/admin/delete")
async def admin_delete(token: str, chat: str, mids: str):
    """يحذف رسائل محددة من قناة واحدة (لمنع التكرار)."""
    _check_token(token)
    if chat not in CHANNELS:
        raise HTTPException(400, "bad_channel")
    if client is None or not getattr(client, "is_connected", False):
        raise HTTPException(503, "not_connected")
    ids = [int(x) for x in mids.split(",") if x.strip().isdigit()]
    if not ids:
        raise HTTPException(400, "no_mids")
    try:
        await client.delete_messages(chat, ids)
    except Exception as e:
        raise HTTPException(500, str(e)[:200])
    return {"deleted": len(ids), "chat": chat, "mids": ids}

