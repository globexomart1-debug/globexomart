# =========================
# GLOBEXOMART BOT - COMPLETE VERSION
# With Working Subfolders, Fast Response, Fixed Give Points
# =========================

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup
import os, time, random, string, threading, hashlib, hmac, json, csv, io, zipfile, traceback, logging, re, unicodedata
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from pymongo import MongoClient, ReturnDocument, WriteConcern
from pymongo.errors import PyMongoError, AutoReconnect, ConnectionFailure, ConfigurationError
from datetime import datetime, timedelta
from functools import wraps

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_ID_RAW = os.environ.get("ADMIN_ID", "").strip()
MONGO_URI = os.environ.get("MONGO_URI", "").strip()

def _strip_invisible(value):
    return "".join(ch for ch in str(value) if unicodedata.category(ch) not in ("Cf", "Cc")).strip()

def sanitize_mongo_uri(uri):
    """Remove invisible characters and unsafe URI options."""
    try:
        uri = _strip_invisible(uri)
        parts = urlsplit(uri)
        allowed = {
            "retrywrites", "journal", "readpreference", "replicaset", "authsource",
            "tls", "ssl", "tlsallowinvalidcertificates", "connecttimeoutms",
            "sockettimeoutms", "serverselectiontimeoutms", "maxpoolsize",
            "minpoolsize", "appname", "directconnection", "compressors",
            "zlibcompressionlevel", "uuidrepresentation"
        }
        cleaned = []
        for k, v in parse_qsl(parts.query, keep_blank_values=True):
            k = _strip_invisible(k)
            v = _strip_invisible(v)
            if k.lower() in allowed:
                cleaned.append((k, v))
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(cleaned), parts.fragment))
    except Exception:
        return _strip_invisible(uri)

MONGO_URI = sanitize_mongo_uri(MONGO_URI)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is missing")
if not ADMIN_ID_RAW or not ADMIN_ID_RAW.isdigit():
    raise RuntimeError("ADMIN_ID must contain digits only")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable is missing")

ADMIN_ID = int(ADMIN_ID_RAW)

# =========================
# 🌐 MONGODB SETUP (RELIABLE)
# =========================
def connect_mongodb():
    last_error = None
    for attempt in range(1, 11):
        try:
            mongo_client = MongoClient(
                MONGO_URI,
                serverSelectionTimeoutMS=15000,
                connectTimeoutMS=15000,
                socketTimeoutMS=30000,
                maxPoolSize=100,
                minPoolSize=2,
                appname="globexomart-bot",
                retryWrites=True,
                retryReads=True,
            )
            mongo_client.admin.command("ping")
            print("✅ MongoDB connected")
            return mongo_client
        except Exception as exc:
            last_error = exc
            print(f"⚠️ MongoDB connection attempt {attempt}/10 failed: {exc}", flush=True)
            time.sleep(min(attempt * 3, 20))
    raise RuntimeError(f"MongoDB connection failed after retries: {last_error}")

client = connect_mongodb()
db = client["globexomart_fresh_v1"]

# Fresh Globexomart database. No collections/data are read from the old bot database.
users_col = db["users"]
folders_col = db["folders"]
codes_col = db["codes"]
config_col = db["config"]
custom_buttons_col = db["custom_buttons"]
admins_col = db["admins"]
payments_col = db["payments"]
subscriptions_col = db["subscriptions"]
item_purchases_col = db["item_purchases"]
wallet_tx_col = db["wallet_transactions"]
# Dedicated shopping collections. Products are intentionally separate from method folders.
shop_products_col = db["shop_products"]
shop_orders_col = db["shop_orders"]
# Dedicated fresh collections for the new bot.
logs_col = db["logs"]
broadcasts_col = db["broadcasts"]
auto_posts_col = db["auto_posts"]
source_chats_col = db["source_chats"]
point_history_col = db["point_history"]
purchases_col = db["purchases"]
referrals_col = db["referrals"]
backups_col = db["backups"]
promoted_channels_col = db["promoted_channels"]
pending_methods_col = db["pending_methods"]
group_warnings_col = db["group_warnings"]
group_message_log_col = db["group_message_log"]
scam_reports_col = db["scam_reports"]
vip_events_col = db["vip_events"]
force_join_stats_col = db["force_join_stats"]
auto_broadcasts_col = db["auto_broadcasts"]
referral_withdrawals_col = db["referral_withdrawals"]
referral_sales_col = db["referral_sales"]
# Added configurable modules: support chat, reseller APIs and per-channel offers.
support_chats_col = db["support_chats"]
reseller_orders_col = db["reseller_orders"]
channel_offer_usage_col = db["channel_offer_usage"]
earn_applications_col = db["earn_applications"]

# Index creation must never prevent the bot from starting.
def ensure_indexes():
    index_jobs = [
        (users_col, "points", {}),
        (users_col, "vip", {}),
        (users_col, "referrals_count", {}),
        (folders_col, [("cat", 1), ("parent", 1)], {}),
        (folders_col, "number", {"unique": True, "sparse": True}),
        (logs_col, [("created_at", -1)], {}),
        (broadcasts_col, [("run_at", 1), ("status", 1)], {}),
        (auto_posts_col, [("next_run", 1), ("active", 1)], {}),
        (point_history_col, [("user_id", 1), ("created_at", -1)], {}),
        (payments_col, [("user_id", 1), ("created_at", -1)], {}),
        (subscriptions_col, [("user_id", 1), ("status", 1), ("expires_at", 1)], {}),
        (item_purchases_col, [("user_id", 1), ("folder_id", 1), ("status", 1)], {}),
        (wallet_tx_col, [("user_id", 1), ("type", 1), ("status", 1), ("created_at", -1)], {}),
        (shop_products_col, [("kind", 1), ("active", 1), ("position", 1), ("created_at", -1)], {}),
        (shop_orders_col, [("user_id", 1), ("created_at", -1)], {}),
        (shop_orders_col, [("product_id", 1), ("created_at", -1)], {}),
        (pending_methods_col, [("status", 1), ("created_at", -1)], {}),
        (group_warnings_col, [("group_id", 1), ("user_id", 1)], {"unique": True}),
        (group_message_log_col, [("group_id", 1), ("message_id", 1)], {"unique": True}),
        (scam_reports_col, [("status", 1), ("created_at", -1)], {}),
        (scam_reports_col, [("target_username", 1), ("status", 1)], {}),
        (vip_events_col, [("created_at", -1), ("user_id", 1)], {}),
        (force_join_stats_col, [("chat_id", 1), ("user_id", 1)], {"unique": True}),
        (auto_broadcasts_col, [("active", 1), ("next_run", 1)], {}),
        (referral_withdrawals_col, [("user_id", 1), ("status", 1), ("created_at", -1)], {}),
        (referral_sales_col, [("referrer_id", 1), ("buyer_id", 1), ("created_at", -1)], {}),
        (support_chats_col, [("user_id", 1), ("updated_at", -1)], {"unique": True}),
        (reseller_orders_col, [("user_id", 1), ("created_at", -1)], {}),
        (channel_offer_usage_col, [("user_id", 1), ("offer_id", 1)], {"unique": True}),
        (earn_applications_col, [("status", 1), ("created_at", -1)], {}),
        (earn_applications_col, [("user_id", 1), ("created_at", -1)], {}),
    ]
    for collection, keys, options in index_jobs:
        try:
            collection.create_index(keys, **options)
        except Exception as exc:
            print(f"⚠️ Index skipped for {collection.name}: {exc}", flush=True)

ensure_indexes()

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown", threaded=True, num_threads=int(os.environ.get("BOT_WORKERS", "12")))
# Secondary client with no default parse mode. Use it for user-generated text and legacy posts.
raw_bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None, threaded=False)

# =========================
# ❌ GLOBAL CANCELLABLE FLOW SYSTEM
# =========================
# Every next-step flow automatically receives an inline Cancel button.
# Users/admins may also type /cancel, cancel, or ❌ Cancel at any step.
_ORIGINAL_REGISTER_NEXT_STEP_HANDLER = bot.register_next_step_handler
_CANCEL_TEXTS = {"/cancel", "cancel", "❌ cancel", "✖ cancel", "stop"}

def _is_cancel_input(message):
    try:
        return str(getattr(message, "text", "") or "").strip().lower() in _CANCEL_TEXTS
    except Exception:
        return False

def _cancel_inline_markup(existing=None):
    kb = InlineKeyboardMarkup(row_width=1)
    try:
        rows = getattr(existing, "keyboard", None) or []
        for row in rows:
            # Preserve any existing inline buttons on the prompt.
            kb.keyboard.append(row)
    except Exception:
        pass
    # Avoid adding the same cancel button twice.
    try:
        for row in kb.keyboard:
            for btn in row:
                if getattr(btn, "callback_data", None) == "globalcancel":
                    return kb
    except Exception:
        pass
    kb.add(InlineKeyboardButton("❌ Cancel", callback_data="globalcancel"))
    return kb

def _clear_known_flow_state(uid, chat_id=None):
    """Clear per-user temporary dictionaries used by interactive flows."""
    keys = {uid, str(uid)}
    if chat_id is not None:
        keys.update({chat_id, str(chat_id)})
    # Keep this intentionally limited to transient flow/session dictionaries.
    transient_names = (
        "_points_payment_state", "_product_admin_state", "_product_qty_state",
        "_product_edit_state", "_move_state", "_folder_admin_state",
        "_join_notify_pending", "_pending_auto", "_import_state",
        "_group_manage_pending", "_pending_approval_state", "_scam_report_sessions",
        "_channel_submit_sessions", "_vip_payment_state", "_item_payment_state",
        "_wallet_state", "_ref_withdraw_state", "_auto_broadcast_state", "_earn_application_state",
    )
    for name in transient_names:
        store = globals().get(name)
        if isinstance(store, dict):
            for key in list(keys):
                store.pop(key, None)

def _cancel_active_flow(uid, chat_id=None, send_message=True):
    chat_id = int(chat_id if chat_id is not None else uid)
    try:
        bot.clear_step_handler_by_chat_id(chat_id)
    except Exception:
        pass
    _clear_known_flow_state(uid, chat_id)
    if not send_message:
        return
    try:
        markup = admin_menu() if is_admin(uid) else main_menu(uid)
    except Exception:
        markup = None
    try:
        raw_bot.send_message(chat_id, "❌ Action cancelled. No changes were saved for the unfinished step.", reply_markup=markup)
    except Exception:
        pass

def _register_cancellable_next_step(message, callback, *args, **kwargs):
    # Add a visible cancel button to the prompt that starts the next step.
    try:
        existing = getattr(message, "reply_markup", None)
        bot.edit_message_reply_markup(
            message.chat.id,
            message.message_id,
            reply_markup=_cancel_inline_markup(existing),
        )
    except Exception:
        # Some Telegram message types cannot have their markup edited.
        pass

    def guarded_callback(next_message, *cb_args, **cb_kwargs):
        if _is_cancel_input(next_message):
            _cancel_active_flow(next_message.from_user.id, next_message.chat.id)
            return None
        return callback(next_message, *cb_args, **cb_kwargs)

    return _ORIGINAL_REGISTER_NEXT_STEP_HANDLER(message, guarded_callback, *args, **kwargs)

# Patch once so all existing and future next-step flows are cancellable automatically.
bot.register_next_step_handler = _register_cancellable_next_step

# Cache for frequently accessed data
_config_cache = None
_config_cache_time = 0
CACHE_TTL = 30

def get_cached_config():
    global _config_cache, _config_cache_time
    now = time.time()
    if _config_cache and (now - _config_cache_time) < CACHE_TTL:
        return _config_cache
    _config_cache = get_config()
    _config_cache_time = now
    return _config_cache

# =========================
# 🔐 SECURITY
# =========================
def validate_request(message):
    if not message or not message.from_user:
        return False
    if len(message.text or "") > 4096:
        return False
    return True

def hash_user_data(uid):
    secret = os.environ.get("BOT_TOKEN", "secret_key")
    return hmac.new(secret.encode(), str(uid).encode(), hashlib.sha256).hexdigest()[:16]

# =========================
# ⚙️ CONFIG SYSTEM
# =========================
def get_config():
    cfg = config_col.find_one({"_id": "config"})
    if not cfg:
        cfg = {
            "_id": "config",
            "force_channels": [],
            "custom_buttons": [],
            "vip_msg": "💎 Buy VIP to unlock this!",
            "welcome": "🔥 Welcome to GLOBEXOMART BOT",
            "ref_reward": 5,
            "notify": True,
            "purchase_msg": "💰 Purchase VIP to access premium features!",
            "next_folder_number": 1,
            "points_per_dollar": 100,
            "contact_username": None,
            "contact_link": None,
            "vip_contact": None,
            "vip_price": 25,
            "vip_points_price": 0,
            "payment_methods": ["💵 USDT (TRC20)"],
            "usdt_network": "TRC20",
            "usdt_address": "",
            "discount_enabled": False,
            "discount_percent": 0,
            "vip_expiry_notice_days": 5,
            "deposit_enabled": True,
            "withdraw_enabled": True,
            "access_chats": [],
            "daily_owner_report": True,
            "subscription_plans": {"1M": {"days": 30, "price": 25}, "2M": {"days": 60, "price": 40}, "4M": {"days": 120, "price": 60}, "1Y": {"days": 365, "price": 100}},
            "referral_vip_count": 50,
            "referral_purchase_count": 10,
            "vip_duration_days": 30,
            "binance_coin": "USDT",
            "binance_network": "TRC20",
            "binance_address": "",
            "binance_memo": "",
            "require_screenshot": True,
            "auto_import_free_source": None,
            "auto_import_vip_source": None,
            "recent_admin_chat_id": None,
            "recent_admin_chat_title": None,
            "hidden_main_buttons": [],
            "force_groups": [],
            "join_notify_group": None,
            "join_notify_enabled": True,
            "method_notify_group": None,
            "method_notify_enabled": True,
            "group_import_notify_enabled": True,
            "group_import_notify_auto_delete_enabled": True,
            "group_import_notify_auto_delete_seconds": 60,
            "user_method_notifications_enabled": True,
            "user_product_notifications_enabled": True,
            "referral_commission_percent": 15.0,
            "referral_min_withdraw_usdt": 10.0,
            "referral_vip_bonus_target": 10,
            "referral_vip_bonus_usdt": 10.0,
            "vip_buy_message": "💎 GLOBEXOMART VIP includes Recorded Working Methods, Files, Documents, Links, Videos, Tools, Premium Accounts, Giveaways, Selling Market access, Private Guidance, Live Classes and 24/7 Support.",
            "vip_rules_message": "📜 VIP Instructions & Rules\n\n• Keep VIP content private.\n• Do not resell or leak protected material without permission.\n• Follow admin instructions and community rules.\n• Contact support if you need help.",
            "force_join_stats_enabled": True,
            "manual_methods_list": "",
            "auto_import_free_source": None,
            "auto_import_vip_source": None,
            "proof_channel": None,
            "about_links": [],
            "support_chat_enabled": True,
            "support_chat_notifications": True,
            "channel_offers": [],
            "reseller_apis": []
        }
        config_col.insert_one(cfg)
    return cfg

def set_config(key, value):
    global _config_cache
    _config_cache = None
    config_col.update_one({"_id": "config"}, {"$set": {key: value}}, upsert=True)


def normalize_chat_reference(value):
    value = _strip_invisible(value or "").strip()
    if not value:
        raise ValueError("Chat/link cannot be empty")
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    m = re.search(r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z0-9_]{5,})", value, re.I)
    if m:
        return "@" + m.group(1)
    if value.startswith("@") and re.fullmatch(r"@[A-Za-z0-9_]{5,}", value):
        return value
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", value):
        return "@" + value
    raise ValueError("Send @username, username, t.me link, or numeric chat ID")

def normalize_url_or_username(value):
    value = _strip_invisible(value or "").strip()
    if value.startswith(("http://", "https://", "tg://")):
        return value
    ref = normalize_chat_reference(value)
    if isinstance(ref, int):
        raise ValueError("A numeric ID cannot be opened as a button link")
    return f"https://t.me/{ref.lstrip('@')}"

def admin_success(uid, text="Process Complete", reply_markup=None):
    # Raw send prevents user supplied names/errors from breaking Markdown parsing.
    raw_bot.send_message(uid, f"✅ {text}", reply_markup=reply_markup or admin_menu())

def admin_error(uid, exc, reply_markup=None):
    raw_bot.send_message(uid, f"❌ Process Failed\n{str(exc)[:1000]}", reply_markup=reply_markup or admin_menu())

def send_method_notification(action, folder):
    cfg = get_cached_config()
    if not cfg.get("method_notify_enabled", True):
        return
    targets = cfg.get("method_notify_groups") or []
    legacy = cfg.get("method_notify_group") or cfg.get("join_notify_group")
    if legacy and legacy not in targets:
        targets.append(legacy)
    if not targets:
        return
    cat = str(folder.get("cat", "")).upper()
    name = str(folder.get("name", "Unknown"))
    price = folder.get("price", 0)
    text = f"🔔 METHOD {str(action).upper()}\n\n📂 Category: {cat}\n📄 Name: {name}\n💰 Price: {price} points"
    for target in targets:
        try:
            raw_bot.send_message(target, text)
        except Exception as exc:
            log_event("method_notification_error", target=target, details={"error": str(exc)}, level="error")

def append_to_manual_methods_list(folder):
    """Append a newly published method to the admin-managed methods list once."""
    try:
        cfg = get_config()
        current = (cfg.get("manual_methods_list") or "").strip()
        name = str(folder.get("name") or "Unnamed Method").strip()
        cat = str(folder.get("cat") or "methods").upper()
        if not name:
            return
        existing_lines = {line.strip().lstrip("•-📌💎🆓📱🛠 ").strip().lower() for line in current.splitlines()}
        if name.lower() in existing_lines:
            return
        addition = f"• {name}"
        current = current + "\n" + addition if current else f"📋 GLOBEXOMART METHODS LIST\n\n{cat}\n{addition}"
        set_config("manual_methods_list", current)
    except Exception as exc:
        log_event("manual_methods_list_append_error", details={"error": str(exc)}, level="error")

# =========================
# 👑 MULTIPLE ADMINS SYSTEM
# =========================
def init_admins():
    if not admins_col.find_one({"_id": ADMIN_ID}):
        admins_col.insert_one({
            "_id": ADMIN_ID,
            "username": None,
            "added_by": "system",
            "added_at": time.time(),
            "is_owner": True
        })

init_admins()

def is_admin(uid):
    uid = int(uid) if isinstance(uid, str) else uid
    if uid == ADMIN_ID:
        return True
    return admins_col.find_one({"_id": uid}) is not None

def add_admin(uid, username=None, added_by=None):
    uid = int(uid) if isinstance(uid, str) else uid
    if admins_col.find_one({"_id": uid}):
        return False
    admins_col.insert_one({
        "_id": uid,
        "username": username,
        "added_by": added_by,
        "added_at": time.time(),
        "is_owner": False
    })
    return True

def remove_admin(uid):
    uid = int(uid) if isinstance(uid, str) else uid
    if uid == ADMIN_ID:
        return False
    result = admins_col.delete_one({"_id": uid})
    return result.deleted_count > 0

def get_all_admins():
    return list(admins_col.find({}))

# =========================
# 👤 USER SYSTEM
# =========================
class User:
    _cache = {}
    _cache_time = {}
    
    def __init__(self, uid):
        self.uid = str(uid)
        
        if uid in self._cache and (time.time() - self._cache_time.get(uid, 0)) < 30:
            self.data = self._cache[uid]
            return
        
        data = users_col.find_one({"_id": self.uid})
        
        if not data:
            data = {
                "_id": self.uid,
                "points": 0,
                "vip": False,
                "vip_expiry": None,
                "ref": None,
                "refs": 0,
                "refs_who_bought_vip": 0,
                "purchased_methods": [],
                "used_codes": [],
                "username": None,
                "created_at": time.time(),
                "last_active": time.time(),
                "hash_id": hash_user_data(uid),
                "total_points_earned": 0,
                "total_points_spent": 0,
                "usdt_balance": 0.0,
                "referral_balance_usdt": 0.0,
                "referral_total_earned_usdt": 0.0,
                "referral_bonus_earned_usdt": 0.0,
                "referral_bonus_awarded": False,
                "banned": False,
                "muted": False
            }
            users_col.insert_one(data)
        
        self.data = data
        self._cache[uid] = data
        self._cache_time[uid] = time.time()
    
    def save(self):
        users_col.update_one({"_id": self.uid}, {"$set": self.data})
        self._cache[self.uid] = self.data
        self._cache_time[self.uid] = time.time()
    
    def is_vip(self):
        if self.data.get("vip", False):
            expiry = self.data.get("vip_expiry")
            if expiry and expiry < time.time():
                self.data["vip"] = False
                self.data["vip_expiry"] = None
                self.save()
                return False
            return True
        return False
    
    def points(self): 
        return self.data.get("points", 0)
    
    def purchased_methods(self): 
        return self.data.get("purchased_methods", [])
    
    def used_codes(self): 
        return self.data.get("used_codes", [])
    
    def username(self): 
        return self.data.get("username", None)
    
    def update_username(self, username):
        if username != self.data.get("username"):
            self.data["username"] = username
            self.save()
    
    def add_points(self, p):
        self.data["points"] += p
        self.data["total_points_earned"] = self.data.get("total_points_earned", 0) + p
        self.save()
    
    def spend_points(self, p):
        self.data["points"] -= p
        self.data["total_points_spent"] = self.data.get("total_points_spent", 0) + p
        self.save()
    
    def make_vip(self, duration_days=None):
        self.data["vip"] = True
        if duration_days and duration_days > 0:
            self.data["vip_expiry"] = time.time() + (duration_days * 86400)
        else:
            self.data["vip_expiry"] = None
        self.save()
    
    def remove_vip(self):
        self.data["vip"] = False
        self.data["vip_expiry"] = None
        self.save()
    
    def purchase_method(self, method_name, price):
        if self.points() >= price:
            self.spend_points(price)
            if method_name not in self.purchased_methods():
                self.data["purchased_methods"].append(method_name)
                self.save()
            return True
        return False
    
    def can_access_method(self, method_name):
        return self.is_vip() or method_name in self.purchased_methods()
    
    def add_used_code(self, code):
        if code not in self.used_codes():
            self.data["used_codes"].append(code)
            self.save()
            return True
        return False
    
    def has_used_code(self, code):
        return code in self.used_codes()
    
    def add_ref(self):
        self.data["refs"] = self.data.get("refs", 0) + 1
        self.save()
        
        config = get_cached_config()
        required_refs = config.get("referral_vip_count", 50)
        
        if self.data["refs"] >= required_refs and not self.is_vip():
            self.make_vip(config.get("vip_duration_days", 30))
            return True
        return False
    
    def add_ref_bought_vip(self):
        self.data["refs_who_bought_vip"] = self.data.get("refs_who_bought_vip", 0) + 1
        self.save()
        
        config = get_cached_config()
        required_purchases = config.get("referral_purchase_count", 10)
        
        if self.data["refs_who_bought_vip"] >= required_purchases and not self.is_vip():
            self.make_vip(config.get("vip_duration_days", 30))
            return True
        return False
    
    def get_refs_count(self):
        return self.data.get("refs", 0)
    
    def get_refs_bought_vip_count(self):
        return self.data.get("refs_who_bought_vip", 0)

# =========================
# 📁 FOLDER SYSTEM (WITH WORKING SUBFOLDERS)
# =========================
class FS:
    def add(self, cat, name, files, price, parent=None, number=None, text_content=None, at_start=False):
        if number is None:
            config = get_config()
            number = config.get("next_folder_number", 1)
            set_config("next_folder_number", number + 1)
        
        folder_data = {
            "cat": cat,
            "name": name,
            "files": files,
            "price": price,
            "parent": parent,
            "number": number,
            "created_at": time.time(),
            "sort_priority": -time.time() if at_start else 0
        }
        
        if text_content:
            folder_data["text_content"] = text_content
        
        folders_col.insert_one(folder_data)
        return number
    
    def get(self, cat, parent=None):
        query = {"cat": cat}
        if parent:
            query["parent"] = parent
        else:
            query["parent"] = None
        return list(folders_col.find(query).sort([("pinned", -1), ("pinned_at", -1), ("sort_priority", 1), ("created_at", -1)]))
    
    def get_one(self, cat, name, parent=None):
        query = {"cat": cat, "name": name}
        if parent:
            query["parent"] = parent
        return folders_col.find_one(query)
    
    def get_by_number(self, number):
        return folders_col.find_one({"number": number})
    
    def update_numbers_after_delete(self, deleted_number):
        folders_col.update_many(
            {"number": {"$gt": deleted_number}},
            {"$inc": {"number": -1}}
        )
        config = get_config()
        current_next = config.get("next_folder_number", 1)
        if current_next > deleted_number:
            set_config("next_folder_number", current_next - 1)
    
    def delete_all_subfolders(self, cat, parent_name):
        subfolders = list(folders_col.find({"cat": cat, "parent": parent_name}))
        for sub in subfolders:
            self.delete_all_subfolders(cat, sub["name"])
            folders_col.delete_one({"_id": sub["_id"]})
    
    def delete(self, cat, name, parent=None):
        query = {"cat": cat, "name": name}
        if parent:
            query["parent"] = parent
        else:
            query["parent"] = None
        
        folder = folders_col.find_one(query)
        if not folder:
            return False
        
        number = folder.get("number")
        self.delete_all_subfolders(cat, name)
        folders_col.delete_one(query)
        
        if number:
            self.update_numbers_after_delete(number)
        
        return True
    
    def edit_price(self, cat, name, price, parent=None):
        query = {"cat": cat, "name": name}
        if parent:
            query["parent"] = parent
        folders_col.update_one(query, {"$set": {"price": price}})
    
    def edit_name(self, cat, old, new, parent=None):
        query = {"cat": cat, "name": old}
        if parent:
            query["parent"] = parent
        folders_col.update_one(query, {"$set": {"name": new}})
        folders_col.update_many({"cat": cat, "parent": old}, {"$set": {"parent": new}})
    
    def move_folder(self, number, new_parent):
        folders_col.update_one({"number": number}, {"$set": {"parent": new_parent}})
    
    def edit_content(self, cat, name, content_type, content, parent=None):
        query = {"cat": cat, "name": name}
        if parent:
            query["parent"] = parent
        
        if content_type == "text":
            folders_col.update_one(query, {"$set": {"text_content": content}})
        elif content_type == "files":
            folders_col.update_one(query, {"$set": {"files": content}})
        return True

fs = FS()

# =========================
# 🏆 CODES SYSTEM
# =========================
class Codes:
    def generate(self, pts, count, multi_use=False, expiry_days=None):
        res = []
        expiry = time.time() + (expiry_days * 86400) if expiry_days else None
        
        for _ in range(count):
            code = "GLOBEXOMART" + ''.join(random.choices(string.ascii_uppercase+string.digits, k=8))
            while codes_col.find_one({"_id": code}):
                code = "GLOBEXOMART" + ''.join(random.choices(string.ascii_uppercase+string.digits, k=8))
            
            codes_col.insert_one({
                "_id": code,
                "points": pts,
                "used": False,
                "multi_use": multi_use,
                "used_count": 0,
                "max_uses": 0 if not multi_use else 10,
                "expiry": expiry,
                "created_at": time.time(),
                "used_by_users": []
            })
            res.append(code)
        return res
    
    def redeem(self, code, user):
        code_data = codes_col.find_one({"_id": code})
        
        if not code_data:
            return False, 0, "invalid"
        
        if code_data.get("expiry") and time.time() > code_data["expiry"]:
            return False, 0, "expired"
        
        if not code_data.get("multi_use", False) and code_data.get("used", False):
            return False, 0, "already_used"
        
        if user.uid in code_data.get("used_by_users", []):
            return False, 0, "already_used_by_user"
        
        if code_data.get("multi_use", False):
            used_count = code_data.get("used_count", 0)
            max_uses = code_data.get("max_uses", 10)
            if used_count >= max_uses:
                return False, 0, "max_uses_reached"
        
        pts = code_data["points"]
        user.add_points(pts)
        
        update_data = {
            "$push": {"used_by_users": user.uid},
            "$inc": {"used_count": 1}
        }
        
        if not code_data.get("multi_use", False):
            update_data["$set"] = {"used": True}
        
        codes_col.update_one({"_id": code}, update_data)
        user.add_used_code(code)
        
        return True, pts, "success"
    
    def get_all_codes(self):
        return list(codes_col.find({}).sort("created_at", -1))
    
    def get_stats(self):
        total = codes_col.count_documents({})
        used = codes_col.count_documents({"used": True})
        unused = total - used
        multi_use = codes_col.count_documents({"multi_use": True})
        return total, used, unused, multi_use

codesys = Codes()

# =========================
# 📦 POINTS PACKAGES SYSTEM
# =========================
def get_points_packages():
    packages = config_col.find_one({"_id": "points_packages"})
    if not packages:
        default_packages = {
            "_id": "points_packages",
            "packages": [
                {"points": 100, "price": 5, "currency": "USD", "bonus": 0, "active": True},
                {"points": 250, "price": 10, "currency": "USD", "bonus": 25, "active": True},
                {"points": 550, "price": 20, "currency": "USD", "bonus": 100, "active": True},
                {"points": 1500, "price": 50, "currency": "USD", "bonus": 500, "active": True},
                {"points": 3500, "price": 100, "currency": "USD", "bonus": 1500, "active": True},
                {"points": 10000, "price": 250, "currency": "USD", "bonus": 5000, "active": True}
            ]
        }
        config_col.insert_one(default_packages)
        return default_packages["packages"]
    return packages["packages"]

def save_points_packages(packages):
    config_col.update_one(
        {"_id": "points_packages"},
        {"$set": {"packages": packages}},
        upsert=True
    )

# =========================
# 🚫 FORCE JOIN (FAST)
# =========================
_force_cache = {}
FORCE_CACHE_TTL = 10

def force_block(uid):
    global _force_cache
    now = time.time()
    
    if is_admin(uid):
        return False
    
    cfg = get_cached_config()
    force_channels = cfg.get("force_channels", [])
    force_groups = cfg.get("force_groups", [])
    force_targets = list(dict.fromkeys(force_channels + force_groups))
    
    if not force_targets:
        return False
    
    for ch in force_targets:
        try:
            member = bot.get_chat_member(ch, uid)
            if member.status in ["left", "kicked"]:
                kb = InlineKeyboardMarkup()
                for channel in force_targets:
                    kb.add(InlineKeyboardButton(f"📢 Join {channel}", url=f"https://t.me/{channel.replace('@','')}"))
                kb.add(InlineKeyboardButton("✅ I Joined", callback_data="recheck"))
                bot.send_message(uid, "🚫 **Access Restricted!**\n\nPlease join the following channels:", reply_markup=kb, parse_mode="Markdown")
                return True
        except:
            kb = InlineKeyboardMarkup()
            for channel in force_targets:
                kb.add(InlineKeyboardButton(f"📢 Join {channel}", url=f"https://t.me/{channel.replace('@','')}"))
            kb.add(InlineKeyboardButton("✅ I Joined", callback_data="recheck"))
            bot.send_message(uid, f"🚫 **Please join required channels!**", reply_markup=kb, parse_mode="Markdown")
            return True
    
    try:
        if get_cached_config().get("force_join_stats_enabled", True):
            record_force_join_verification(uid, force_targets)
    except Exception:
        pass
    return False

def force_join_handler(func):
    @wraps(func)
    def wrapper(message):
        if force_block(message.from_user.id):
            return
        return func(message)
    return wrapper

# =========================
# 📱 MAIN MENU
# =========================
def get_custom_buttons():
    cfg = get_cached_config()
    return cfg.get("custom_buttons", [])

def add_custom_button(button_text, button_type, button_data):
    cfg = get_config()
    buttons = cfg.get("custom_buttons", [])
    buttons.append({
        "text": button_text,
        "type": button_type,
        "data": button_data
    })
    set_config("custom_buttons", buttons)

def remove_custom_button(button_text):
    cfg = get_config()
    buttons = cfg.get("custom_buttons", [])
    buttons = [b for b in buttons if b["text"] != button_text]
    set_config("custom_buttons", buttons)

def get_hidden_main_buttons():
    cfg = get_cached_config()
    return set(cfg.get("hidden_main_buttons", []))

MAIN_MENU_ROWS = [
    ("📚 Methods", "🛍 Products"),
    ("⭐ Buy VIP", "💰 Points"),
    ("🎁 Referral", "💼 Earn"),
    ("👤 Account", "ℹ️ About Us"),
    ("💬 Chat Admin",),
    ("🆔 Chat ID", "🏆 Redeem"),
    ("💳 Deposit", "💸 Withdraw"),
]

MAIN_MENU_BUTTONS = [button for row in MAIN_MENU_ROWS for button in row]

def main_menu(uid):
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    hidden = get_hidden_main_buttons()

    for row in MAIN_MENU_ROWS:
        visible = [button for button in row if button not in hidden]
        if visible:
            kb.row(*visible)

    custom_btns = get_custom_buttons()
    if custom_btns:
        row = []
        for btn in custom_btns:
            if btn["text"] in hidden:
                continue
            row.append(btn["text"])
            if len(row) == 2:
                kb.row(*row)
                row = []
        if row:
            kb.row(*row)

    if is_admin(uid):
        kb.row("⚙️ ADMIN PANEL")

    return kb

def finalize_pending_referral(uid, telegram_user=None):
    """Credit a referral only after the referred user passes all force-join checks."""
    uid_str = str(uid)
    row = users_col.find_one({"_id": uid_str}, {"pending_ref": 1, "ref": 1, "username": 1}) or {}
    ref_id = row.get("pending_ref")
    if not ref_id or row.get("ref"):
        return False
    if str(ref_id) == uid_str or not str(ref_id).isdigit():
        users_col.update_one({"_id": uid_str}, {"$unset": {"pending_ref": ""}})
        return False
    if not users_col.find_one({"_id": str(ref_id)}, {"_id": 1}):
        users_col.update_one({"_id": uid_str}, {"$unset": {"pending_ref": ""}})
        return False

    # Claim once. Only the process that changes ref from None/missing gets to reward.
    claimed = users_col.update_one(
        {"_id": uid_str, "$or": [{"ref": None}, {"ref": {"$exists": False}}], "pending_ref": str(ref_id)},
        {"$set": {"ref": str(ref_id)}, "$unset": {"pending_ref": ""}},
    )
    if claimed.modified_count != 1:
        return False

    ref_user = User(str(ref_id))
    reward = int(get_cached_config().get("ref_reward", 5))
    old_balance = ref_user.points()
    ref_user.add_points(reward)
    got_vip = ref_user.add_ref()
    display_name = None
    if telegram_user is not None:
        display_name = getattr(telegram_user, "username", None)
        if not display_name:
            display_name = " ".join(x for x in [getattr(telegram_user, "first_name", None), getattr(telegram_user, "last_name", None)] if x)
    display_name = display_name or row.get("username") or uid_str
    vip_msg = ""
    if got_vip:
        vip_msg = f"\n\n🎊 You reached **{ref_user.get_refs_count()} referrals** and received **FREE VIP ACCESS!**"
    try:
        if not get_cached_config().get("user_referral_notifications_enabled", True):
            return True
        bot.send_message(
            int(ref_id),
            f"🎉 **REFERRAL COMPLETED!** 🎉\n\n"
            f"👤 **{display_name}** joined all required channels and groups.\n"
            f"💫 You received **{reward:,} points!**\n"
            f"💰 Previous balance: **{old_balance:,}**\n"
            f"🏆 New balance: **{ref_user.points():,}**\n"
            f"👥 Total referrals: **{ref_user.get_refs_count()}**{vip_msg}\n\n"
            f"🥳 Enjoy your reward! 🚀",
            parse_mode="Markdown",
        )
    except Exception as exc:
        log_event("referral_reward_notify_error", ref_id, uid, {"error": str(exc)}, level="error")
    return True

# =========================
# 🚀 START
# =========================
@bot.message_handler(commands=["start"])
def start_cmd(m):
    if not validate_request(m):
        return
    
    uid = m.from_user.id
    ban_row = users_col.find_one({"_id": str(uid)}, {"banned": 1}) or {}
    if ban_row.get("banned") and not is_admin(uid):
        return raw_bot.send_message(uid, "⛔ Your access to this bot has been restricted.")
    args = m.text.split()
    is_new_user = users_col.find_one({"_id": str(uid)}, {"_id": 1}) is None
    
    user = User(uid)
    
    if m.from_user.username:
        user.update_username(m.from_user.username)
    users_col.update_one(
        {"_id": str(uid)},
        {"$set": {
            "first_name": m.from_user.first_name or "",
            "last_name": m.from_user.last_name or "",
            "username": m.from_user.username or user.data.get("username"),
            "last_active": time.time(),
        }},
    )
    
    if len(args) > 1:
        ref_id = args[1].strip()
        if ref_id != str(uid) and ref_id.isdigit() and not user.data.get("ref"):
            # Do not credit now. It becomes valid only after all force-join requirements pass.
            if users_col.find_one({"_id": ref_id}, {"_id": 1}):
                users_col.update_one(
                    {"_id": str(uid), "$or": [{"ref": None}, {"ref": {"$exists": False}}]},
                    {"$set": {"pending_ref": ref_id}},
                )
                user.data["pending_ref"] = ref_id

    if force_block(uid):
        return

    finalize_pending_referral(uid, m.from_user)
    user = User(uid)
    cfg = get_cached_config()
    welcome_msg = cfg.get("welcome", "Welcome to GLOBEXOMART BOT!")
    
    first_name = getattr(m.from_user, "first_name", None) or "Member"
    welcome_text = (
        f"✨ WELCOME, {first_name.upper()}! ✨\n\n"
        "Welcome to GLOBEXOMART — your place to access useful methods, accounts, premium tools and earning opportunities.\n\n"
        "📚 METHODS & GUIDES\n"
        "Explore free and VIP methods, files, documents, tools and step-by-step guides.\n\n"
        "🛍 ACCOUNTS & PRODUCTS\n"
        "Browse available accounts and other products directly inside the bot.\n\n"
        "💼 EARN WITH US\n"
        "Invite users through your referral link and earn USDT when your referrals purchase VIP plans. You can also apply to work with us if you have a public profile, audience or channel.\n\n"
        "╭━━━━━━━━━━━━━━━━━━╮\n"
        f"💰 Points Balance: {user.points()}\n"
        f"💎 VIP Status: {'ACTIVE' if user.is_vip() else 'NOT ACTIVE'}\n"
        "╰━━━━━━━━━━━━━━━━━━╯\n\n"
        "Choose an option from the menu below 👇"
    )

    raw_bot.send_message(uid, welcome_text, reply_markup=main_menu(uid))

    if is_new_user:
        guide_text = (
            "📘 QUICK START GUIDE\n\n"
            "📚 METHODS — Free methods may be free/points-based; VIP methods are included with VIP or can be purchased individually in USDT.\n"
            "🛍 PRODUCTS — Free products may be free/points-based; paid products are purchased using your approved USDT account balance.\n"
            "💰 POINTS — Check your balance, earnings, spending and rewards.\n"
            "⭐ BUY VIP — See VIP price, benefits and payment details.\n"
            "🎁 REFERRAL — Share your personal link and earn points after your friend joins every required group/channel.\n"
            "📚 MY METHODS — Open methods you already purchased.\n"
            "💳 DEPOSIT / 💸 WITHDRAW — Manage your USDT bot balance through admin-reviewed requests.\n"
            "🏆 REDEEM — Redeem a valid points code.\n"
            
            "🛡 SAFETY COMMANDS\n"
            "• /scammer @username — Submit a scam report.\n"
            "• Reply with /scammer — Report the person you replied to.\n"
            "• /check @username — Check scam-report status.\n"
            "• /scammerlist — View reported accounts.\n\n"
            "Use the menu buttons below whenever you need a feature. 🚀"
        )
        raw_bot.send_message(uid, guide_text, reply_markup=main_menu(uid), disable_web_page_preview=True)

        notify_group = cfg.get("join_notify_group")
        if notify_group and cfg.get("join_notify_enabled", True):
            try:
                full_name = " ".join(x for x in [m.from_user.first_name, m.from_user.last_name] if x) or "Unknown"
                username = f"@{m.from_user.username}" if m.from_user.username else "No username"
                referrer = user.data.get("ref") or "Direct join"
                bot.send_message(
                    notify_group,
                    f"🆕 **New User Joined Bot**\n\n"
                    f"👤 Name: {full_name}\n"
                    f"🔗 Username: {username}\n"
                    f"🆔 User ID: `{uid}`\n"
                    f"🌐 Language: {m.from_user.language_code or 'Unknown'}\n"
                    f"🎁 Referrer: `{referrer}`\n"
                    f"🕒 Joined: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    parse_mode="Markdown",
                )
            except Exception as exc:
                log_event("join_notification_error", uid, notify_group, {"error": str(exc)}, level="error")

# =========================
# 💰 POINTS COMMAND
# =========================
@bot.message_handler(func=lambda m: m.text in ("💰 POINTS", "💰 Points"))
@force_join_handler
def points_cmd(m):
    uid = m.from_user.id
    user = User(uid)
    cfg = get_cached_config()
    points = int(user.points() or 0)
    purchased_count = len(user.purchased_methods())
    ref_count = user.get_refs_count()
    ref_bought_count = user.get_refs_bought_vip_count()
    vip_text = "ACTIVE 👑" if user.is_vip() else "Not active"

    text = (
        "💎  GLOBEXOMART POINTS WALLET  💎\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💰 Current balance: {points:,} points\n"
        f"👑 VIP status: {vip_text}\n"
        f"📚 Purchased methods: {purchased_count}\n\n"
        "📊  YOUR ACTIVITY\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🎁 Verified referrals: {ref_count}\n"
        f"⭐ Referral purchases: {ref_bought_count}\n"
        f"✨ Total points earned: {int(user.data.get('total_points_earned', 0)):,}\n"
        f"🛍 Total points spent: {int(user.data.get('total_points_spent', 0)):,}\n\n"
        "🚀  EARN MORE POINTS\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "• Invite friends using your referral link\n"
        "• Redeem reward codes\n"
        "• Purchase a points package\n\n"
        f"🎯 {cfg.get('referral_vip_count', 50)} verified referrals = FREE VIP\n"
        f"🎯 {cfg.get('referral_purchase_count', 10)} referral purchases = FREE VIP"
    )
    kb = InlineKeyboardMarkup(row_width=2)
    kb.row(
        InlineKeyboardButton("💎 Buy Points", callback_data="open_points_shop"),
        InlineKeyboardButton("🎁 Referral", callback_data="open_referral_card"),
    )
    kb.row(InlineKeyboardButton("🔄 Refresh Balance", callback_data="check_balance"))
    raw_bot.send_message(uid, text, reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "open_points_shop")
def open_points_shop_callback(c):
    bot.answer_callback_query(c.id)
    send_points_shop(c.from_user.id)

# =========================
# 💎 GET POINTS
# =========================
def send_points_shop(uid):
    user = User(uid)
    packages = get_points_packages()
    active_packages = [(i, p) for i, p in enumerate(packages) if p.get("active", True)]

    message = "💎 **BUY POINTS** 💎\n\n"
    message += f"💰 Your balance: **{user.points():,} points**\n\n"
    if active_packages:
        message += "📦 **SELECT A PACKAGE**\n\n"
        for _, pkg in active_packages:
            base = int(pkg.get("points", 0))
            bonus = int(pkg.get("bonus", 0))
            total = base + bonus
            price = float(pkg.get("price", 0) or 0)
            message += f"💠 **{total:,} points** — **${price:g} USDT**"
            if bonus:
                message += f"  _(includes +{bonus:,} bonus)_"
            message += "\n"
        message += "\nTap a package below to view payment details."
    else:
        message += "❌ No point packages are currently available."

    kb = InlineKeyboardMarkup(row_width=1)
    for idx, pkg in active_packages:
        total = int(pkg.get("points", 0)) + int(pkg.get("bonus", 0))
        price = float(pkg.get("price", 0) or 0)
        kb.add(InlineKeyboardButton(f"💠 {total:,} Points — ${price:g}", callback_data=f"pointpkg|{idx}"))
    kb.add(InlineKeyboardButton("🔄 Refresh Balance", callback_data="check_balance"))
    bot.send_message(uid, message, reply_markup=kb, parse_mode="Markdown")


_points_payment_state = {}


@bot.callback_query_handler(func=lambda c: c.data.startswith("pointpkg|"))
def points_package_select_cb(c):
    try:
        idx = int(c.data.split("|", 1)[1])
        packages = get_points_packages()
        if idx < 0 or idx >= len(packages):
            return bot.answer_callback_query(c.id, "Package not found", True)
        pkg = packages[idx]
        if not pkg.get("active", True):
            return bot.answer_callback_query(c.id, "This package is unavailable", True)
        base = int(pkg.get("points", 0))
        bonus = int(pkg.get("bonus", 0))
        total = base + bonus
        price = float(pkg.get("price", 0) or 0)
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("✅ I Paid — Submit Proof", callback_data=f"pointpaid|{idx}"))
        kb.add(InlineKeyboardButton("⬅️ Back to Packages", callback_data="open_points_shop"))
        raw_bot.edit_message_text(
            f"💎 POINTS PACKAGE\n\nPoints: {total:,}\nPrice: ${price:g} USDT\n\n{_usdt_instructions(price)}\n\nAfter paying, tap I Paid — Submit Proof. You must send the transaction ID and payment screenshot. Points are added only after admin approval.",
            c.from_user.id,
            c.message.message_id,
            reply_markup=kb,
        )
        bot.answer_callback_query(c.id)
    except Exception as exc:
        bot.answer_callback_query(c.id, "Could not open package", True)
        log_event("points_package_open_error", c.from_user.id, details={"error": str(exc)}, level="error")


@bot.callback_query_handler(func=lambda c: c.data.startswith("pointpaid|"))
def points_payment_start_cb(c):
    try:
        idx = int(c.data.split("|", 1)[1])
        packages = get_points_packages()
        if idx < 0 or idx >= len(packages):
            return bot.answer_callback_query(c.id, "Package not found", True)
        pkg = packages[idx]
        if not pkg.get("active", True):
            return bot.answer_callback_query(c.id, "This package is unavailable", True)
        base = int(pkg.get("points", 0))
        bonus = int(pkg.get("bonus", 0))
        total = base + bonus
        price = float(pkg.get("price", 0) or 0)
        pending = payments_col.find_one({"user_id": int(c.from_user.id), "type": "points_package", "status": "pending"})
        if pending:
            return bot.answer_callback_query(c.id, "You already have a points payment pending review", True)
        _points_payment_state[c.from_user.id] = {
            "package_index": idx,
            "base_points": base,
            "bonus_points": bonus,
            "total_points": total,
            "amount": price,
            "username": c.from_user.username,
            "first_name": c.from_user.first_name,
            "chat_id": c.message.chat.id,
        }
        msg = raw_bot.send_message(
            c.from_user.id,
            f"🧾 POINTS PAYMENT PROOF\n\nPackage: {total:,} points\nAmount: ${price:g} USDT\n\nStep 1/2: Send the transaction ID / TxID exactly as shown by your wallet or exchange."
        )
        bot.register_next_step_handler(msg, points_payment_txid_step)
        bot.answer_callback_query(c.id, "Send transaction ID")
    except Exception as exc:
        bot.answer_callback_query(c.id, "Could not start payment", True)
        log_event("points_payment_start_error", c.from_user.id, details={"error": str(exc)}, level="error")


def points_payment_txid_step(m):
    state = _points_payment_state.get(m.from_user.id)
    if not state:
        return raw_bot.send_message(m.chat.id, "Payment session expired. Open Buy Points again.")
    txid = (m.text or "").strip()
    if len(txid) < 6:
        msg = raw_bot.send_message(m.chat.id, "❌ Transaction ID looks too short. Send the complete TxID.")
        bot.register_next_step_handler(msg, points_payment_txid_step)
        return
    duplicate = payments_col.find_one({
        "transaction_id": txid,
        "status": {"$in": ["pending", "approved", "paid"]},
    }) or wallet_tx_col.find_one({
        "transaction_id": txid,
        "status": {"$in": ["pending", "approved"]},
    })
    if duplicate:
        msg = raw_bot.send_message(m.chat.id, "❌ This Transaction ID has already been submitted. Send a different valid TxID.")
        bot.register_next_step_handler(msg, points_payment_txid_step)
        return
    state["transaction_id"] = txid[:300]
    msg = raw_bot.send_message(m.chat.id, "📸 Step 2/2: Now send the payment screenshot as a photo.")
    bot.register_next_step_handler(msg, points_payment_screenshot_step)


def points_payment_screenshot_step(m):
    state = _points_payment_state.get(m.from_user.id)
    if not state:
        return raw_bot.send_message(m.chat.id, "Payment session expired. Open Buy Points again.")
    if m.content_type != "photo":
        msg = raw_bot.send_message(m.chat.id, "❌ A payment screenshot is required. Please send it as a photo.")
        bot.register_next_step_handler(msg, points_payment_screenshot_step)
        return
    state = _points_payment_state.pop(m.from_user.id)
    username = m.from_user.username or state.get("username")
    display = f"@{username}" if username else (m.from_user.first_name or state.get("first_name") or str(m.from_user.id))
    doc = {
        "user_id": int(m.from_user.id),
        "chat_id": int(m.chat.id),
        "username": username,
        "first_name": m.from_user.first_name or state.get("first_name"),
        "type": "points_package",
        "package_index": int(state["package_index"]),
        "base_points": int(state["base_points"]),
        "bonus_points": int(state["bonus_points"]),
        "points": int(state["total_points"]),
        "amount": float(state["amount"]),
        "currency": "USDT",
        "mode": "manual",
        "transaction_id": state["transaction_id"],
        "screenshot_chat_id": int(m.chat.id),
        "screenshot_message_id": int(m.message_id),
        "status": "pending",
        "created_at": time.time(),
    }
    pid = payments_col.insert_one(doc).inserted_id
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Approve + Add Points", callback_data=f"pointsapprove|{pid}"),
        InlineKeyboardButton("❌ Reject", callback_data=f"pointsreject|{pid}"),
    )
    admin_text = (
        "💎 POINTS PAYMENT REVIEW\n\n"
        f"👤 User: {display}\n"
        f"🆔 User ID: {m.from_user.id}\n"
        f"💬 Chat ID: {m.chat.id}\n"
        f"📦 Package: {state['total_points']:,} points\n"
        f"💰 Amount: ${state['amount']:g} USDT\n"
        f"🔗 Transaction ID: {state['transaction_id']}\n\n"
        "Verify the transaction ID and screenshot before approving."
    )
    for admin in get_all_admins():
        try:
            aid = int(admin["_id"])
            raw_bot.send_message(aid, admin_text, reply_markup=kb)
            raw_bot.copy_message(aid, m.chat.id, m.message_id)
        except Exception as exc:
            log_event("points_payment_admin_delivery_error", m.from_user.id, details={"admin": str(admin.get('_id')), "error": str(exc)}, level="error")
    raw_bot.send_message(m.chat.id, "✅ Points payment submitted. Your points will be added only after admin approval.")


@bot.callback_query_handler(func=lambda c: c.data.startswith("pointsapprove|") or c.data.startswith("pointsreject|"))
def review_points_payment_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    try:
        from bson import ObjectId
        action, pid = c.data.split("|", 1)
        pay = payments_col.find_one({"_id": ObjectId(pid), "type": "points_package"})
        if not pay:
            return bot.answer_callback_query(c.id, "Payment not found", True)
        if pay.get("status") != "pending":
            return bot.answer_callback_query(c.id, f"Already {pay.get('status')}", True)
        uid = int(pay["user_id"])
        if action == "pointsreject":
            payments_col.update_one({"_id": pay["_id"]}, {"$set": {"status": "rejected", "reviewed_at": time.time(), "reviewed_by": c.from_user.id}})
            bot.answer_callback_query(c.id, "Rejected")
            try:
                raw_bot.edit_message_text(c.message.text + "\n\n❌ REJECTED", c.message.chat.id, c.message.message_id)
            except Exception:
                pass
            try:
                raw_bot.send_message(uid, "❌ Your points-package payment was rejected. No points were added.")
            except Exception:
                pass
            return

        # Atomic status claim prevents double-credit if two admins press Approve.
        claimed = payments_col.update_one(
            {"_id": pay["_id"], "status": "pending"},
            {"$set": {"status": "approved", "reviewed_at": time.time(), "reviewed_by": c.from_user.id}},
        )
        if claimed.modified_count != 1:
            return bot.answer_callback_query(c.id, "Already reviewed", True)
        points = int(pay.get("points", 0) or 0)
        users_col.update_one(
            {"_id": str(uid)},
            {"$inc": {"points": points, "total_points_earned": points}, "$set": {"last_active": time.time()}},
            upsert=True,
        )
        User._cache.pop(str(uid), None)
        User._cache_time.pop(str(uid), None)
        payments_col.update_one({"_id": pay["_id"]}, {"$set": {"credited_points": points, "credited_at": time.time()}})
        new_balance = int((users_col.find_one({"_id": str(uid)}, {"points": 1}) or {}).get("points", 0))
        bot.answer_callback_query(c.id, "Approved — points added", True)
        try:
            raw_bot.edit_message_text(c.message.text + f"\n\n✅ APPROVED\nAdded: {points:,} points", c.message.chat.id, c.message.message_id)
        except Exception:
            pass
        try:
            raw_bot.send_message(uid, f"✅ PAYMENT APPROVED\n\n{points:,} points were added to your account.\nNew balance: {new_balance:,} points")
        except Exception:
            pass
    except Exception as exc:
        bot.answer_callback_query(c.id, "Review failed", True)
        admin_error(c.from_user.id, exc)

@bot.message_handler(func=lambda m: m.text == "💎 GET POINTS")
@force_join_handler
def get_points_button(m):
    send_points_shop(m.from_user.id)

# =========================
# 📂 SHOW FOLDERS (FAST)
# =========================
def _service_status(folder):
    status = str(folder.get("stock_status") or "in_stock").strip().lower().replace(" ", "_")
    return "out_of_stock" if status in ("out", "out_of_stock", "sold_out", "unavailable") else "in_stock"

def _service_card(folder):
    name = str(folder.get("name") or "Unnamed Product")
    price = float(folder.get("price", 0) or 0)
    cat = folder.get("cat")
    duration = str(folder.get("duration") or "Not specified")
    warranty = str(folder.get("warranty") or "Not specified")
    status = _service_status(folder)
    status_text = "✅ IN STOCK" if status == "in_stock" else "❌ OUT OF STOCK"
    price_text = f"${price:g} USDT" if cat == "paid_service" else ("FREE" if price <= 0 else f"{int(price)} points")
    return (
        f"🛍️ {name}\n\n"
        f"💰 Price: {price_text}\n"
        f"⏳ Duration: {duration}\n"
        f"🛡 Warranty: {warranty}\n"
        f"📦 Status: {status_text}"
    )

def get_folders_kb(cat, parent=None, page=0, items_per_page=15):
    data = fs.get(cat, parent)
    
    start = page * items_per_page
    end = start + items_per_page
    page_items = data[start:end]
    
    kb = InlineKeyboardMarkup(row_width=2)
    
    for item in page_items:
        name = item["name"]
        price = item.get("price", 0)
        # Check if has subfolders
        subfolders = fs.get(cat, name)
        icon = "📁" if subfolders else "📄"
        pin = "📌 " if item.get("pinned") else ""
        patched = "🛑 " if item.get("patched") else ""
        stock = ""
        if cat in ("free_service", "paid_service"):
            stock = "✅ " if _service_status(item) == "in_stock" else "❌ "
        text = f"{pin}{patched}{stock}{icon} {name}"
        if price > 0:
            if cat in ("vip", "paid_service"):
                text += f" (${float(price):g} USDT)"
            else:
                text += f" ({int(price)} pts)"
        
        kb.add(InlineKeyboardButton(text, callback_data=f"openid|{item['_id']}"))
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️", callback_data=f"page|{cat}|{page-1}|{parent or ''}"))
    if end < len(data):
        nav_buttons.append(InlineKeyboardButton("➡️", callback_data=f"page|{cat}|{page+1}|{parent or ''}"))
    
    if nav_buttons:
        kb.row(*nav_buttons)
    
    if parent:
        kb.add(InlineKeyboardButton("🔙 Back", callback_data=f"back|{cat}|{parent}"))
    
    return kb

@bot.message_handler(func=lambda m: m.text in ["📚 Methods", "🛍 Products"])
@force_join_handler
def open_catalog_menu(m):
    kb = InlineKeyboardMarkup(row_width=1)
    if m.text == "📚 Methods":
        # One button per row: easier to read on mobile and preserves custom/premium Unicode emoji text.
        kb.add(InlineKeyboardButton("🆓 Free Methods", callback_data="catalog|free"))
        kb.add(InlineKeyboardButton("💎 VIP Methods", callback_data="catalog|vip"))
        raw_bot.send_message(m.from_user.id, "📚 METHODS\n\nChoose a category:", reply_markup=kb)
    else:
        kb.add(InlineKeyboardButton("🆓 Free Products", callback_data="shopcat|free"))
        kb.add(InlineKeyboardButton("💵 Paid Products", callback_data="shopcat|paid"))
        if get_cached_config().get("reseller_apis") or []:
            kb.add(InlineKeyboardButton("🌐 Other Products", callback_data="reseller|providers"))
        kb.add(InlineKeyboardButton("🧾 My Product Orders", callback_data="shoporders|mine"))
        raw_bot.send_message(m.from_user.id, "🛍 PRODUCTS\n\nDigital products are delivered instantly from stock after purchase.\nPaid products use your approved USDT account balance.\nOther Products can be supplied live through admin-configured reseller APIs.", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("catalog|"))
def catalog_category_callback(c):
    if force_block(c.from_user.id):
        return
    cat = c.data.split("|", 1)[1]
    labels = {
        "free": "🆓 Free Methods",
        "vip": "💎 VIP Methods",
        "free_service": "🆓 Free Products",
        "paid_service": "💵 Paid Products",
    }
    data = fs.get(cat)
    bot.answer_callback_query(c.id)
    if not data:
        return raw_bot.send_message(c.from_user.id, f"{labels.get(cat, cat)}\n\nNo items available yet.")
    raw_bot.send_message(c.from_user.id, f"{labels.get(cat, cat)}\n\nChoose an item:", reply_markup=get_folders_kb(cat))

# =========================
# 📂 OPEN FOLDER (WITH WORKING SUBFOLDERS)
# =========================
@bot.callback_query_handler(func=lambda c: c.data.startswith("open|"))
def open_folder(c):
    uid = c.from_user.id
    user = User(uid)
    parts = c.data.split("|")
    cat = parts[1]
    name = parts[2]
    parent = parts[3] if len(parts) > 3 and parts[3] else None
    folder = fs.get_one(cat, name, parent if parent else None)
    if not folder:
        return bot.answer_callback_query(c.id, "❌ Item not found")

    subfolders = fs.get(cat, name)
    if subfolders:
        kb = InlineKeyboardMarkup(row_width=1)
        for sub in subfolders:
            deeper = fs.get(cat, sub["name"])
            icon = "📁" if deeper else "📄"
            patched = "🛑 " if sub.get("patched") else ""
            price = float(sub.get("price", 0) or 0)
            suffix = ""
            if price > 0:
                suffix = f" (${price:g} USDT)" if cat in ("vip", "paid_service") else f" ({int(price)} pts)"
            kb.add(InlineKeyboardButton(f"{patched}{icon} {sub['name']}{suffix}"[:64], callback_data=f"openid|{sub['_id']}"))
        kb.add(InlineKeyboardButton("🔙 Back", callback_data=f"back|{cat}|{name}"))
        try:
            bot.edit_message_text(f"📁 <b>{name}</b>", uid, c.message.message_id, reply_markup=kb, parse_mode="HTML")
        except Exception:
            raw_bot.send_message(uid, f"📁 {name}", reply_markup=kb)
        return bot.answer_callback_query(c.id)

    price = float(folder.get("price", 0) or 0)
    folder_id = str(folder["_id"])
    owns_item = item_purchases_col.find_one({"user_id": int(uid), "folder_id": folder_id, "status": "paid"}) is not None

    if cat in ("free_service", "paid_service") and not owns_item:
        card = _service_card(folder)
        if _service_status(folder) != "in_stock":
            bot.answer_callback_query(c.id, "Out of stock", True)
            return raw_bot.send_message(uid, card + "\n\nThis product is currently unavailable.")

    if cat in ("free", "free_service") and price > 0:
        purchase_key = f"{cat}:{folder_id}"
        if purchase_key not in user.purchased_methods():
            if user.points() < int(price):
                bot.answer_callback_query(c.id, f"Need {int(price)} points", True)
                return raw_bot.send_message(uid, f"🔒 {name}\n\nPrice: {int(price)} points\nYour balance: {user.points()} points")
            user.spend_points(int(price))
            if purchase_key not in user.data.setdefault("purchased_methods", []):
                user.data["purchased_methods"].append(purchase_key)
                user.save()

    if cat == "vip" and not user.is_vip() and not owns_item:
        kb = InlineKeyboardMarkup(row_width=1)
        if price > 0:
            kb.add(InlineKeyboardButton(f"💵 Pay ${price:g} USDT", callback_data=f"buyitem|{folder_id}"))
        kb.add(InlineKeyboardButton("⭐ Buy VIP", callback_data="get_vip"))
        bot.answer_callback_query(c.id, "VIP method", True)
        msg = f"💎 {name}\n\nVIP members: included"
        if price > 0:
            msg += f"\nFree users: ${price:g} USDT"
        return raw_bot.send_message(uid, msg, reply_markup=kb)

    if cat == "paid_service" and not owns_item:
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton(f"🛒 Buy for ${price:g} USDT", callback_data=f"buyitem|{folder_id}"))
        bot.answer_callback_query(c.id, "Digital product", True)
        return raw_bot.send_message(uid, _service_card(folder) + "\n\nPaid products are purchased from your approved USDT account balance. VIP does not make paid products free.", reply_markup=kb)

    bot.answer_callback_query(c.id, "📤 Sending...")
    if cat in ("free_service", "paid_service"):
        raw_bot.send_message(uid, _service_card(folder))
    patched_note = "\n\n🛑 Status: PATCHED / may no longer work." if folder.get("patched") else ""
    text_content = folder.get("text_content")
    if text_content:
        raw_bot.send_message(uid, f"📄 {name}{patched_note}\n\n{text_content}")
    files = folder.get("files", []) or []
    for f in files:
        try:
            raw_bot.copy_message(uid, f["chat"], f["msg"])
            time.sleep(0.08)
        except Exception as exc:
            log_event("item_copy_error", uid, details={"folder_id": folder_id, "error": str(exc)}, level="error")
    service_msg = folder.get("service_msg")
    if service_msg:
        raw_bot.send_message(uid, service_msg)
    if not text_content and not files and not service_msg:
        raw_bot.send_message(uid, f"📁 {name}\n\nNo content uploaded yet.{patched_note}")

# =========================
# 🔙 BACK BUTTON
# =========================
@bot.callback_query_handler(func=lambda c: c.data.startswith("back|"))
def back_handler(c):
    _, cat, current_parent = c.data.split("|")
    
    parent_folder = fs.get_one(cat, current_parent)
    if parent_folder:
        grand_parent = parent_folder.get("parent")
        bot.edit_message_reply_markup(
            c.from_user.id,
            c.message.message_id,
            reply_markup=get_folders_kb(cat, grand_parent)
        )
    else:
        bot.edit_message_reply_markup(
            c.from_user.id,
            c.message.message_id,
            reply_markup=get_folders_kb(cat)
        )
    bot.answer_callback_query(c.id)

# =========================
# 📄 PAGINATION
# =========================
@bot.callback_query_handler(func=lambda c: c.data.startswith("page|"))
def page_handler(c):
    _, cat, page, parent = c.data.split("|")
    parent = parent if parent != "None" else None
    
    try:
        bot.edit_message_reply_markup(
            c.from_user.id,
            c.message.message_id,
            reply_markup=get_folders_kb(cat, parent, int(page))
        )
    except:
        pass
    bot.answer_callback_query(c.id)

# =========================
# 💰 BUY METHOD
# =========================
@bot.callback_query_handler(func=lambda c: c.data.startswith("buy|"))
def buy_method(c):
    uid = c.from_user.id
    user = User(uid)
    
    try:
        _, cat, method_name, price = c.data.split("|")
        price = int(price)
    except:
        bot.answer_callback_query(c.id, "Invalid")
        return

    folder = folders_col.find_one({"cat": cat, "name": method_name})
    if folder and folder.get("expired"):
        bot.answer_callback_query(c.id, "⛔ This method has expired", True)
        raw_bot.send_message(uid, f"⛔ METHOD EXPIRED\n\n{method_name} is unavailable and cannot be purchased or opened.")
        return
    
    if user.is_vip():
        bot.answer_callback_query(c.id, "✅ You are VIP!", True)
        open_folder(c)
        return
    
    if user.can_access_method(method_name):
        bot.answer_callback_query(c.id, "✅ You own this!", True)
        open_folder(c)
        return
    
    if user.points() < price:
        bot.answer_callback_query(c.id, f"❌ Need {price} pts! You have {user.points()}", True)
        return
    
    if user.purchase_method(method_name, price):
        bot.answer_callback_query(c.id, f"✅ Purchased! -{price} pts", True)
        bot.edit_message_text(
            f"✅ **Purchased!**\n\nYou now own: {method_name}\nRemaining: {user.points()} pts",
            uid,
            c.message.message_id,
            parse_mode="Markdown"
        )
    else:
        bot.answer_callback_query(c.id, "❌ Failed!", True)

# =========================
# CALLBACK HANDLERS
# =========================
@bot.callback_query_handler(func=lambda c: c.data == "get_vip_legacy_unused")
def get_vip_callback(c):
    uid = c.from_user.id
    user = User(uid)
    cfg = get_cached_config()
    
    if user.is_vip():
        bot.answer_callback_query(c.id, "✅ Already VIP!", True)
        return
    
    vip_msg = cfg.get("vip_msg", "💎 Buy VIP!")
    vip_price_usd = cfg.get("vip_price", 50)
    vip_price_points = cfg.get("vip_points_price", 5000)
    vip_contact = cfg.get("vip_contact")
    
    binance_address = cfg.get("binance_address", "")
    binance_coin = cfg.get("binance_coin", "USDT")
    binance_network = cfg.get("binance_network", "TRC20")
    binance_memo = cfg.get("binance_memo", "")
    
    message = f"💎 **VIP**\n\n{vip_msg}\n\n💰 Price:\n• ${vip_price_usd} USD\n• {vip_price_points} points\n\n"
    
    if binance_address:
        message += f"💳 **Binance:**\nCoin: {binance_coin}\nNetwork: {binance_network}\nAddress: `{binance_address}`\n"
        if binance_memo:
            message += f"Memo: `{binance_memo}`\n"
        message += f"Amount: ${vip_price_usd}\n\n"
    
    message += f"✨ Benefits:\n• All VIP methods\n• Priority support\n• No points needed\n\n"
    
    if vip_contact:
        message += f"📞 Contact: {vip_contact}\n"
    
    message += f"\n🆔 ID: `{uid}`\n💰 Points: {user.points()}"
    
    kb = InlineKeyboardMarkup()
    if vip_price_points and vip_price_points > 0 and user.points() >= vip_price_points:
        kb.add(InlineKeyboardButton(f"⭐ Buy with {vip_price_points} pts", callback_data="buy_vip_points"))
    if vip_contact:
        if vip_contact.startswith("http"):
            kb.add(InlineKeyboardButton("📞 Contact", url=vip_contact))
        elif vip_contact.startswith("@"):
            kb.add(InlineKeyboardButton("📞 Contact", url=f"https://t.me/{vip_contact.replace('@', '')}"))
    
    bot.edit_message_text(message, uid, c.message.message_id, reply_markup=kb if kb.keyboard else None, parse_mode="Markdown")
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "buy_vip_points")
def buy_vip_points_callback(c):
    uid = c.from_user.id
    user = User(uid)
    cfg = get_cached_config()
    vip_price_points = cfg.get("vip_points_price", 5000)
    
    if user.is_vip():
        bot.answer_callback_query(c.id, "✅ Already VIP!", True)
        return
    
    if user.points() >= vip_price_points:
        user.spend_points(vip_price_points)
        user.make_vip(cfg.get("vip_duration_days", 30))
        bot.answer_callback_query(c.id, f"✅ VIP Purchased! -{vip_price_points} pts", True)
        bot.edit_message_text(
            f"🎉 **CONGRATULATIONS!** 🎉\n\nYou are now VIP!\n\n💰 Points: {user.points()}",
            uid,
            c.message.message_id,
            parse_mode="Markdown"
        )
    else:
        bot.answer_callback_query(c.id, f"❌ Need {vip_price_points} pts! You have {user.points()}", True)

@bot.callback_query_handler(func=lambda c: c.data == "get_points")
def get_points_callback(c):
    if force_block(c.from_user.id):
        bot.answer_callback_query(c.id, "Join required chats first", True)
        return
    send_points_shop(c.from_user.id)
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "cancel_buy")
def cancel_buy(c):
    bot.edit_message_text("❌ Cancelled", c.from_user.id, c.message.message_id)
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "check_balance")
def check_balance_callback(c):
    uid = c.from_user.id
    user = User(uid)
    
    bot.answer_callback_query(c.id, f"💰 Balance: {user.points()} pts", True)
    bot.edit_message_text(
        f"💰 **Balance**\n\nPoints: {user.points()}\nVIP: {'✅' if user.is_vip() else '❌'}\nReferrals: {user.get_refs_count()}",
        uid,
        c.message.message_id,
        parse_mode="Markdown"
    )

@bot.callback_query_handler(func=lambda c: c.data == "get_referral")
def get_referral_callback(c):
    uid = c.from_user.id
    cfg = get_cached_config()
    link = f"https://t.me/{bot.get_me().username}?start={uid}"
    
    bot.edit_message_text(
        f"🎁 **Referral Link**\n\n`{link}`\n\n✨ Rewards:\n• +{cfg.get('ref_reward', 5)} pts per referral\n• {cfg.get('referral_vip_count', 50)} referrals → FREE VIP\n• {cfg.get('referral_purchase_count', 10)} referral purchases → FREE VIP",
        uid,
        c.message.message_id,
        parse_mode="Markdown"
    )

@bot.callback_query_handler(func=lambda c: c.data == "get_vip_info")
def get_vip_info_callback(c):
    uid = c.from_user.id
    cfg = get_cached_config()
    vip_contact = cfg.get("vip_contact")
    vip_price_usd = cfg.get("vip_price", 50)
    vip_price_points = cfg.get("vip_points_price", 5000)
    
    message = f"⭐ **VIP Benefits** ⭐\n\n✨ Why become VIP?\n• ALL VIP methods\n• No points needed\n• Priority support\n• Exclusive content\n\n💰 Price: ${vip_price_usd} or {vip_price_points} pts\n\n🎁 FREE VIP:\n• Invite {cfg.get('referral_vip_count', 50)} users\n• Get {cfg.get('referral_purchase_count', 10)} referrals to buy VIP\n\n"
    
    if vip_contact:
        message += f"📞 Contact: {vip_contact}"
    
    kb = InlineKeyboardMarkup()
    if vip_contact:
        if vip_contact.startswith("http"):
            kb.add(InlineKeyboardButton("📞 Contact", url=vip_contact))
        elif vip_contact.startswith("@"):
            kb.add(InlineKeyboardButton("📞 Contact", url=f"https://t.me/{vip_contact.replace('@', '')}"))
    
    bot.edit_message_text(message, uid, c.message.message_id, reply_markup=kb if kb.keyboard else None, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data == "recheck")
def recheck(c):
    uid = c.from_user.id
    user = User(uid)
    
    if not force_block(uid):
        try:
            bot.edit_message_text("✅ **Access Granted!**", uid, c.message.message_id, parse_mode="Markdown")
        except:
            pass
        bot.send_message(uid, f"🎉 Welcome!\n\n💰 Points: {user.points()}", reply_markup=main_menu(uid))
    else:
        bot.answer_callback_query(c.id, "❌ Join channels first!", True)

# =========================
# 📚 MY METHODS
# =========================
@bot.message_handler(func=lambda m: m.text == "📚 MY METHODS")
@force_join_handler
def show_purchased_methods(m):
    uid = m.from_user.id
    user = User(uid)
    
    purchased = user.purchased_methods()
    
    if user.is_vip():
        bot.send_message(uid, "💎 **VIP Member**\n\nAccess to ALL VIP methods!", parse_mode="Markdown")
        return
    
    if not purchased:
        bot.send_message(uid, f"📚 **Your Methods**\n\nNo purchased methods yet.\n\n💰 Points: {user.points()}", parse_mode="Markdown")
        return
    
    all_vip_methods = {item["name"]: item for item in fs.get("vip")}
    
    kb = InlineKeyboardMarkup(row_width=2)
    for method in purchased:
        row = all_vip_methods.get(method)
        if row:
            kb.add(InlineKeyboardButton(f"📄 {method}", callback_data=f"openid|{row['_id']}"))
    
    bot.send_message(uid, f"📚 **Your Methods** ({len(purchased)})\n\n💰 Points: {user.points()}", reply_markup=kb, parse_mode="Markdown")


# =========================
# 📋 BEAUTIFUL / ADMIN-EDITABLE METHODS LIST
# =========================
@bot.message_handler(func=lambda m: m.text == "📋 METHODS LIST")
@force_join_handler
def methods_list_cmd(m):
    cfg = get_cached_config()
    manual = (cfg.get("manual_methods_list") or "").strip()
    if manual:
        remaining = manual
        while remaining:
            if len(remaining) <= 4096:
                raw_bot.send_message(m.from_user.id, remaining)
                break
            cut = remaining.rfind("\n", 0, 4096)
            if cut < 100:
                cut = 4096
            raw_bot.send_message(m.from_user.id, remaining[:cut])
            remaining = remaining[cut:].lstrip("\n")
        return

    categories = [
        ("free", "🆓 FREE METHODS", "▫️"),
        ("vip", "👑 VIP METHODS", "▫️"),
        ("apps", "📱 PREMIUM APPS", "▫️"),
        ("services", "🛠 SERVICES", "▫️"),
    ]
    sections, total = [], 0
    for cat, title, icon in categories:
        rows = fs.get(cat)
        if not rows:
            continue
        total += len(rows)
        lines = [f"{title}  •  {len(rows)}"]
        for row in rows:
            pin = "📌 " if row.get("pinned") else ""
            patched = "🛑 " if row.get("patched") else ""
            lines.append(f"{pin}{patched}{icon} {row.get('name', 'Unnamed Method')}")
        sections.append("\n".join(lines))
    text = (
        "╔════════════════════╗\n"
        "      📋 GLOBEXOMART METHODS\n"
        "╚════════════════════╝\n\n"
        f"✨ Available methods: {total}\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        + ("\n\n━━━━━━━━━━━━━━━━━━━━\n\n".join(sections) if sections else "No methods are available yet.")
        + "\n\n━━━━━━━━━━━━━━━━━━━━\n🚀 Select a category from the main menu to open a method."
    )
    while text:
        if len(text) <= 4096:
            raw_bot.send_message(m.from_user.id, text)
            break
        cut = text.rfind("\n", 0, 4096)
        if cut < 100:
            cut = 4096
        raw_bot.send_message(m.from_user.id, text[:cut])
        text = text[cut:].lstrip("\n")

@bot.message_handler(func=lambda m: m.text == "📝 Edit Methods List" and is_admin(m.from_user.id))
def edit_methods_list_menu(m):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✍️ Send New Manual List", callback_data="methodslist|edit"))
    kb.add(InlineKeyboardButton("♻️ Rebuild Automatically", callback_data="methodslist|rebuild"))
    kb.add(InlineKeyboardButton("🗑 Clear Manual List", callback_data="methodslist|clear"))
    current = (get_config().get("manual_methods_list") or "").strip()
    status = "Manual list is active." if current else "Automatic list is active."
    raw_bot.send_message(m.from_user.id, f"📋 METHODS LIST MANAGER\n\n{status}", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("methodslist|"))
def methods_list_admin_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    action = c.data.split("|", 1)[1]
    try:
        if action == "edit":
            msg = raw_bot.send_message(c.from_user.id, "Send the complete methods list exactly as users should see it.\n\nYou can use plain text, emojis and multiple lines.")
            bot.register_next_step_handler(msg, save_manual_methods_list)
            return bot.answer_callback_query(c.id, "Send the list now")
        if action == "clear":
            set_config("manual_methods_list", "")
            admin_success(c.from_user.id, "Manual methods list cleared. Automatic list is active.")
            return bot.answer_callback_query(c.id, "Cleared")
        if action == "rebuild":
            lines = ["📋 GLOBEXOMART METHODS LIST", ""]
            for cat, title in (("free","🆓 FREE METHODS"),("vip","👑 VIP METHODS"),("apps","📱 PREMIUM APPS"),("services","🛠 SERVICES")):
                rows = fs.get(cat)
                if not rows:
                    continue
                lines.append(title)
                for row in rows:
                    prefix = "📌 " if row.get("pinned") else "• "
                    suffix = " ⛔ EXPIRED" if row.get("expired") else ""
                    lines.append(f"{prefix}{row.get('name','Unnamed Method')}{suffix}")
                lines.append("")
            set_config("manual_methods_list", "\n".join(lines).strip())
            admin_success(c.from_user.id, "Methods list rebuilt from current bot methods.")
            return bot.answer_callback_query(c.id, "Rebuilt")
    except Exception as exc:
        admin_error(c.from_user.id, exc)

def save_manual_methods_list(m):
    try:
        text = (m.text or m.caption or "").strip()
        if not text:
            raise ValueError("The methods list cannot be empty")
        if len(text) > 50000:
            raise ValueError("The methods list is too long")
        set_config("manual_methods_list", text)
        admin_success(m.from_user.id, "Manual methods list saved successfully.")
    except Exception as exc:
        admin_error(m.from_user.id, exc)

# =========================
# 👤 ACCOUNT
# =========================
@bot.message_handler(func=lambda m: m.text in ("👤 ACCOUNT", "👤 Account"))
@force_join_handler
def account_cmd(m):
    uid = m.from_user.id
    user = User(uid)
    
    status = "💎 VIP" if user.is_vip() else "🆓 Free"
    purchased_count = len(user.purchased_methods())
    ref_count = user.get_refs_count()
    ref_bought_count = user.get_refs_bought_vip_count()
    
    account_text = f"**👤 Account**\n\n"
    account_text += f"┌ Status: {status}\n"
    account_text += f"├ Points: {user.points()}\n"
    account_text += f"├ USDT Balance: ${float(user.data.get('usdt_balance', 0) or 0):.2f}\n"
    account_text += f"├ Referrals: {ref_count}\n"
    account_text += f"├ Referral Purchases: {ref_bought_count}\n"
    account_text += f"├ Purchased: {purchased_count} methods\n"
    account_text += f"├ Earned: {user.data.get('total_points_earned', 0)}\n"
    account_text += f"└ Spent: {user.data.get('total_points_spent', 0)}\n\n"
    
    if not user.is_vip():
        cfg = get_cached_config()
        account_text += f"💡 **FREE VIP:**\n• Invite {cfg.get('referral_vip_count', 50)} users\n• Get {cfg.get('referral_purchase_count', 10)} referrals to buy VIP\n"
    
    account_text += f"\n🆔 ID: `{uid}`"
    
    bot.send_message(uid, account_text, parse_mode="Markdown")

# =========================
# 🎁 REFERRAL
# =========================
def send_referral_card(uid):
    user = User(uid)
    cfg = get_cached_config()
    bot_username = bot.get_me().username
    link = f"https://t.me/{bot_username}?start={uid}"
    refs = int(user.get_refs_count() or 0)
    reward = int(cfg.get("ref_reward", 5) or 0)
    target = int(cfg.get("referral_vip_count", 50) or 50)
    bought = int(user.get_refs_bought_vip_count() or 0)
    bought_target = int(cfg.get("referral_purchase_count", 10) or 10)
    progress = min(100, int((refs / target) * 100)) if target else 100
    filled = min(10, progress // 10)
    bar = "🟩" * filled + "⬜" * (10 - filled)

    text = (
        "🎁  INVITE & EARN  🎁\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Share your personal link and earn rewards after your friend joins all required channels and groups.\n\n"
        f"🔗 Your referral link:\n{link}\n\n"
        "📊  YOUR PROGRESS\n"
        f"{bar}  {progress}%\n"
        f"👥 Verified referrals: {refs}/{target}\n"
        f"💫 Referral points earned: {refs * reward:,}\n"
        f"👑 Referral purchases: {bought}/{bought_target}\n\n"
        "🏆  REWARDS\n"
        f"• +{reward} points for every verified referral\n"
        f"• {target} verified referrals unlock FREE VIP\n"
        f"• {bought_target} referral purchases unlock FREE VIP\n\n"
        f"💰 Current balance: {int(user.points()):,} points"
    )
    share_text = "Join GLOBEXOMART and earn premium methods"
    share_url = f"https://t.me/share/url?url={link}&text={share_text.replace(' ', '%20')}"
    kb = InlineKeyboardMarkup(row_width=2)
    kb.row(
        InlineKeyboardButton("📤 Share Link", url=share_url),
        InlineKeyboardButton("💰 My Points", callback_data="open_points_card"),
    )
    raw_bot.send_message(uid, text, reply_markup=kb, disable_web_page_preview=True)

@bot.message_handler(func=lambda m: m.text in ("🎁 REFERRAL", "🎁 Referral"))
@force_join_handler
def referral_cmd(m):
    send_referral_card(m.from_user.id)

@bot.callback_query_handler(func=lambda c: c.data == "open_referral_card")
def open_referral_card_callback(c):
    bot.answer_callback_query(c.id)
    send_referral_card(c.from_user.id)

@bot.callback_query_handler(func=lambda c: c.data == "open_points_card")
def open_points_card_callback(c):
    bot.answer_callback_query(c.id)
    fake = type("M", (), {"from_user": c.from_user})()
    points_cmd.__wrapped__(fake) if hasattr(points_cmd, "__wrapped__") else points_cmd(fake)

# =========================
# 💼 EARN WITH GLOBEXOMART
# =========================
_earn_application_state = {}


def _earn_plan_lines():
    cfg = get_cached_config()
    pct = max(0.0, min(15.0, float(cfg.get("referral_commission_percent", 15) or 0)))
    plans = get_subscription_plans() if "get_subscription_plans" in globals() else (cfg.get("subscription_plans") or {})
    rows = []
    for code, row in plans.items():
        if not row.get("active", True):
            continue
        name = str(row.get("name") or code)
        price = max(0.0, float(row.get("price", 0) or 0))
        earning = round(price * pct / 100.0, 2)
        rows.append((int(row.get("duration_minutes", float(row.get("days", 1) or 1) * 1440) or 1),
                     f"• {name}: ${price:g} plan → you earn ${earning:.2f}"))
    rows.sort(key=lambda x: x[0])
    return pct, [line for _, line in rows]


def send_earn_menu(uid):
    cfg = get_cached_config()
    pct, plan_lines = _earn_plan_lines()
    target = int(cfg.get("referral_vip_bonus_target", 10) or 10)
    prize = float(cfg.get("referral_vip_bonus_usdt", 10) or 0)
    minimum = float(cfg.get("referral_min_withdraw_usdt", 10) or 0)
    text = (
        "💼 EARN WITH GLOBEXOMART\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Earn {pct:g}% commission whenever someone joins through your referral link and buys a paid VIP plan.\n\n"
        "💵 EARNINGS PER VIP PLAN\n"
        + ("\n".join(plan_lines) if plan_lines else "No active VIP plans right now.")
        + "\n\n"
        "📘 HOW TO EARN\n"
        "1. Open Referral and copy your personal referral link.\n"
        "2. Share it with friends, customers, your audience or community.\n"
        "3. The user must start the bot through your link and complete the required joins.\n"
        "4. When that referred user buys a paid VIP plan, your USDT commission is added automatically.\n"
        f"5. Withdraw your referral balance once it reaches ${minimum:g}.\n\n"
        f"🏆 BONUS: {target} unique paid VIP referrals = +${prize:g} USDT bonus.\n\n"
        "The amount shown per plan is based on the normal plan price. If a buyer receives a discount, commission is calculated from the amount actually paid.\n\n"
        "Want to work with us directly? Submit your username and your public channel/profile below."
    )
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🎁 Open My Referral", callback_data="open_referral_card"))
    kb.add(InlineKeyboardButton("🤝 Apply to Work With Us", callback_data="earnapply|start"))
    raw_bot.send_message(uid, text, reply_markup=kb, disable_web_page_preview=True)


@bot.message_handler(func=lambda m: m.text in ("💼 EARN", "💼 Earn"))
@force_join_handler
def earn_cmd(m):
    send_earn_menu(m.from_user.id)


@bot.callback_query_handler(func=lambda c: c.data == "earnapply|start")
def earn_apply_start(c):
    uid = c.from_user.id
    existing = earn_applications_col.find_one({"user_id": int(uid), "status": {"$in": ["pending", "reviewing"]}})
    if existing:
        bot.answer_callback_query(c.id, "You already have an application under review.", True)
        return
    _earn_application_state[uid] = {}
    msg = raw_bot.send_message(uid, "🤝 WORK WITH US APPLICATION\n\nStep 1/2: Send the username/name you want us to identify you by.\nExample: @username")
    bot.register_next_step_handler(msg, earn_apply_username_step)
    bot.answer_callback_query(c.id)


def earn_apply_username_step(m):
    uid = m.from_user.id
    state = _earn_application_state.get(uid)
    if state is None:
        return raw_bot.send_message(m.chat.id, "Application session expired. Open Earn and try again.")
    username = (m.text or "").strip()
    if not username or len(username) > 150:
        msg = raw_bot.send_message(m.chat.id, "❌ Send a valid username/name (maximum 150 characters).")
        bot.register_next_step_handler(msg, earn_apply_username_step)
        return
    state["work_username"] = username
    msg = raw_bot.send_message(m.chat.id, "Step 2/2: Send your public Telegram channel link/username, social profile, or write NONE if you do not have one.\nExample: @mychannel or https://t.me/mychannel")
    bot.register_next_step_handler(msg, earn_apply_channel_step)


def earn_apply_channel_step(m):
    uid = m.from_user.id
    state = _earn_application_state.pop(uid, None)
    if not state:
        return raw_bot.send_message(m.chat.id, "Application session expired. Open Earn and try again.")
    channel = (m.text or "").strip()
    if not channel or len(channel) > 500:
        _earn_application_state[uid] = state
        msg = raw_bot.send_message(m.chat.id, "❌ Send a valid public channel/profile, or type NONE.")
        bot.register_next_step_handler(msg, earn_apply_channel_step)
        return
    doc = {
        "user_id": int(uid),
        "telegram_username": m.from_user.username,
        "first_name": m.from_user.first_name,
        "work_username": state.get("work_username"),
        "public_channel": channel,
        "status": "pending",
        "created_at": time.time(),
        "updated_at": time.time(),
        "replies": [],
    }
    oid = earn_applications_col.insert_one(doc).inserted_id
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("👀 View", callback_data=f"earnapp|view|{oid}"),
        InlineKeyboardButton("💬 Reply", callback_data=f"earnapp|reply|{oid}"),
    )
    tg = f"@{m.from_user.username}" if m.from_user.username else "No Telegram username"
    notice = (
        "🤝 NEW EARN APPLICATION\n\n"
        f"User ID: {uid}\nTelegram: {tg}\n"
        f"Submitted username: {doc['work_username']}\n"
        f"Public channel/profile: {channel}"
    )
    for adm in get_all_admins():
        try:
            raw_bot.send_message(int(adm["_id"]), notice, reply_markup=kb, disable_web_page_preview=True)
        except Exception:
            pass
    raw_bot.send_message(m.chat.id, "✅ Application submitted. Our team can now review it and reply to you through the bot.", reply_markup=main_menu(uid))


def _earn_application_text(row):
    created = datetime.fromtimestamp(float(row.get("created_at", time.time()))).strftime("%Y-%m-%d %H:%M")
    tg = f"@{row.get('telegram_username')}" if row.get("telegram_username") else "No Telegram username"
    replies = row.get("replies") or []
    last_reply = replies[-1].get("text") if replies else None
    text = (
        "🤝 EARN APPLICATION\n\n"
        f"Status: {str(row.get('status', 'pending')).upper()}\n"
        f"User ID: {row.get('user_id')}\n"
        f"Telegram: {tg}\n"
        f"Submitted username: {row.get('work_username', '-')}\n"
        f"Public channel/profile: {row.get('public_channel', '-')}\n"
        f"Submitted: {created}"
    )
    if last_reply:
        text += f"\n\nLast admin reply:\n{last_reply}"
    return text


@bot.message_handler(func=lambda m: m.text == "🤝 Earn Applications" and is_admin(m.from_user.id))
def earn_applications_admin(m):
    rows = list(earn_applications_col.find({}).sort("created_at", -1).limit(20))
    if not rows:
        return raw_bot.send_message(m.chat.id, "🤝 No earn applications yet.", reply_markup=admin_menu())
    kb = InlineKeyboardMarkup(row_width=1)
    for row in rows:
        status = str(row.get("status", "pending"))
        name = str(row.get("work_username") or row.get("telegram_username") or row.get("user_id"))[:25]
        kb.add(InlineKeyboardButton(f"{'🟡' if status == 'pending' else '🟢' if status == 'answered' else '⚪'} {name} • {status}", callback_data=f"earnapp|view|{row['_id']}"))
    raw_bot.send_message(m.chat.id, f"🤝 EARN APPLICATIONS\n\nShowing latest {len(rows)} applications:", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("earnapp|"))
def earn_application_callback(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    try:
        from bson import ObjectId
        _, action, oid = c.data.split("|", 2)
        row = earn_applications_col.find_one({"_id": ObjectId(oid)})
        if not row:
            return bot.answer_callback_query(c.id, "Application not found", True)
        if action == "view":
            kb = InlineKeyboardMarkup(row_width=2)
            kb.add(InlineKeyboardButton("💬 Reply", callback_data=f"earnapp|reply|{oid}"))
            if row.get("status") != "closed":
                kb.add(InlineKeyboardButton("✅ Close", callback_data=f"earnapp|close|{oid}"))
            raw_bot.send_message(c.from_user.id, _earn_application_text(row), reply_markup=kb, disable_web_page_preview=True)
        elif action == "reply":
            msg = raw_bot.send_message(c.from_user.id, f"Send your reply to user {row.get('user_id')}:")
            bot.register_next_step_handler(msg, lambda m: earn_application_reply_step(m, oid))
        elif action == "close":
            earn_applications_col.update_one({"_id": row["_id"]}, {"$set": {"status": "closed", "updated_at": time.time(), "closed_by": c.from_user.id}})
            bot.answer_callback_query(c.id, "Application closed")
            return
        bot.answer_callback_query(c.id)
    except Exception as exc:
        admin_error(c.from_user.id, exc)


def earn_application_reply_step(m, oid):
    if not is_admin(m.from_user.id):
        return
    try:
        from bson import ObjectId
        text = (m.text or "").strip()
        if not text:
            raise ValueError("Reply cannot be empty")
        row = earn_applications_col.find_one({"_id": ObjectId(oid)})
        if not row:
            raise ValueError("Application not found")
        reply = {"admin_id": int(m.from_user.id), "text": text[:4000], "created_at": time.time()}
        earn_applications_col.update_one({"_id": row["_id"]}, {"$push": {"replies": reply}, "$set": {"status": "answered", "updated_at": time.time()}})
        raw_bot.send_message(int(row["user_id"]), f"💬 GLOBEXOMART TEAM REPLY\n\n{text}")
        admin_success(m.from_user.id, "Reply sent to applicant.")
    except Exception as exc:
        admin_error(m.from_user.id, exc)


# =========================
# 🏆 REDEEM CODE
# =========================
@bot.message_handler(func=lambda m: m.text in ("🏆 REDEEM", "🏆 Redeem"))
@force_join_handler
def redeem_cmd(m):
    msg = bot.send_message(m.from_user.id, "🎫 **Enter code:**", parse_mode="Markdown")
    bot.register_next_step_handler(msg, redeem_code)

def redeem_code(m):
    uid = m.from_user.id
    user = User(uid)
    code = m.text.strip().upper()
    
    success, pts, reason = codesys.redeem(code, user)
    
    if success:
        bot.send_message(uid, f"✅ **Redeemed!**\n\n+{pts} points\n💰 Balance: {user.points()}", parse_mode="Markdown")
    else:
        messages = {
            "invalid": "❌ Invalid code!",
            "already_used": "❌ Code already used!",
            "already_used_by_user": "❌ You already used this code!",
            "expired": "❌ Code expired!",
            "max_uses_reached": "❌ Max uses reached!"
        }
        bot.send_message(uid, messages.get(reason, "❌ Invalid code!"), parse_mode="Markdown")

# =========================
# 🆔 CHAT ID
# =========================
@bot.message_handler(func=lambda m: m.text in ("🆔 CHAT ID", "🆔 Chat ID"))
@force_join_handler
def chatid_cmd(m):
    uid = m.from_user.id
    user = User(uid)
    
    bot.send_message(uid, f"🆔 **Your ID:** `{uid}`\n\n💰 Points: {user.points()}\n⭐ VIP: {'✅' if user.is_vip() else '❌'}\n👥 Referrals: {user.get_refs_count()}", parse_mode="Markdown")

# =========================
# ⭐ BUY VIP
# =========================
def _vip_plan_selection(uid, edit_message_id=None):
    current = _active_subscription(uid) if "_active_subscription" in globals() else None
    plans = get_subscription_plans() if "get_subscription_plans" in globals() else {}
    current_rank = 0
    current_text = ""
    if current:
        current_row = plans.get(str(current.get("plan", "")).upper()) or {}
        current_rank = _plan_rank(current_row)
        exp_value = current.get("expires_at")
        exp = "Lifetime" if exp_value is None else datetime.fromtimestamp(exp_value).strftime("%Y-%m-%d %H:%M")
        current_text = f"\n\n✅ Current plan: {current_row.get('name', current.get('plan'))}\nExpires: {exp}\nOnly higher plans are shown for upgrade."

    available = []
    for code, row in _sorted_active_plans(highest_first=True):
        if current and _plan_rank(row) <= current_rank:
            continue
        available.append((code, row))

    kb = InlineKeyboardMarkup(row_width=1)
    lines = []
    for code, row in available:
        base = float(row.get("price", 0) or 0)
        price = _effective_plan_price(uid, code) if "_effective_plan_price" in globals() else base
        duration = _format_duration_minutes(row.get("duration_minutes", 1440))
        label = str(row.get("name") or code)
        price_label = f"${price:g}"
        if price < base:
            price_label += f" (was ${base:g})"
        button_prefix = "⬆️ Upgrade" if current else "💎"
        kb.add(InlineKeyboardButton(f"{button_prefix} {label} • {duration} • ${price:g}", callback_data=f"subplan|{code}"))
        lines.append(f"• {label} — {duration} — {price_label} USDT")

    if not current and "_eligible_trial_offer" in globals() and _eligible_trial_offer(uid):
        kb.add(InlineKeyboardButton("🎁 Claim Channel Trial", callback_data="offertrial|claim"))

    cfg = get_cached_config()
    intro = cfg.get("vip_buy_message") or "💎 GLOBEXOMART VIP"
    if available:
        heading = "⬆️ AVAILABLE UPGRADES" if current else "💳 VIP PLANS"
        text = f"{intro}{current_text}\n\n{heading}\n" + "\n".join(lines)
    elif current:
        text = f"{intro}{current_text}\n\n🏆 You are already on the highest available VIP plan."
    else:
        text = f"{intro}\n\nNo active VIP plans are currently available."
    text += "\n\n⏳ VIP access expires automatically at the end of the selected interval."
    if edit_message_id:
        raw_bot.edit_message_text(text, int(uid), int(edit_message_id), reply_markup=kb if kb.keyboard else None)
    else:
        raw_bot.send_message(int(uid), text, reply_markup=kb if kb.keyboard else None)


@bot.message_handler(func=lambda m: m.text in ("⭐ BUY VIP", "⭐ Buy VIP"))
@force_join_handler
def buy_vip_button(m):
    _vip_plan_selection(m.from_user.id)


@bot.callback_query_handler(func=lambda c: c.data == "get_vip")
def get_vip_callback_v2(c):
    try:
        _vip_plan_selection(c.from_user.id, c.message.message_id)
        bot.answer_callback_query(c.id)
    except Exception as exc:
        bot.answer_callback_query(c.id, "Could not open VIP plans", True)
        log_event("vip_plan_list_error", c.from_user.id, details={"error": str(exc)}, level="error")

# =========================
# ❌ GLOBAL CANCEL HANDLERS
# =========================
@bot.callback_query_handler(func=lambda c: c.data == "globalcancel")
def global_cancel_callback(c):
    _cancel_active_flow(c.from_user.id, c.message.chat.id)
    try:
        bot.answer_callback_query(c.id, "Cancelled")
    except Exception:
        pass

@bot.message_handler(commands=["cancel"])
def global_cancel_command(m):
    _cancel_active_flow(m.from_user.id, m.chat.id)

@bot.message_handler(func=lambda m: str(getattr(m, "text", "") or "").strip().lower() in {"cancel", "❌ cancel", "✖ cancel", "stop"})
def global_cancel_text(m):
    _cancel_active_flow(m.from_user.id, m.chat.id)

# =========================
# ⚙️ ADMIN PANEL (SHORTENED FOR SPEED)
# =========================
def admin_menu():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    try:
        unread_chats = int(support_chats_col.count_documents({"unread_admin": {"$gt": 0}})) if "support_chats_col" in globals() else 0
    except Exception:
        unread_chats = 0
    chats_label = f"💬 Chats ({unread_chats})" if unread_chats else "💬 Chats"

    kb.row("📦 Upload Free Method", "💎 Upload VIP Method")
    kb.row("🆓 Upload Free Product", "💵 Upload Paid Product")
    kb.row("🛍 Product Manager", "🗑 Delete Folder")
    kb.row("✏️ Edit Price", "✏️ Edit Name")
    kb.row("📝 Edit Content")
    kb.row("🔀 Move Folder", "🛑 Patch Method")
    kb.row("✅ Unpatch Method", "👑 Add VIP")
    kb.row("👑 Remove VIP", "💰 Give Points")
    kb.row("🎫 Generate Codes", "📊 View Codes")
    kb.row("📦 Points Packages", "👥 Admin Management")
    kb.row("📞 Set Contacts", "⚙️ VIP Settings")
    kb.row(chats_label, "ℹ️ About Us Setup")
    kb.row("🧾 Proof Settings", "🏷 Channel Offers")
    kb.row("🌐 Reseller APIs")
    kb.row("💳 Payment Methods", "🏦 Binance Settings")
    kb.row("🧾 VIP Manager", "🎯 VIP Channel")
    kb.row("📈 VIP Analytics", "📊 Force Join Stats")
    kb.row("🎁 Referral Settings", "💸 Ref Withdrawals")
    kb.row("🤝 Earn Applications")
    kb.row("📣 Auto Broadcast", "📝 VIP Messages")
    kb.row("💳 Deposits", "💸 Withdrawals")
    kb.row("🏷 Discounts", "🧾 Payments")
    kb.row("📸 Screenshot", "🔘 Button Manager")
    kb.row("🙈 Hide Button", "👁 Show Button")
    kb.row("📢 Force Join", "👥 Join Notifications")
    kb.row("⚙️ Settings")
    kb.row("📊 Stats", "📢 Broadcast")
    kb.row("🔔 Notify", "🛡 Group Management")
    kb.row("📊 Leaderboard")
    kb.row("🔎 Search", "📣 Auto Posts")
    kb.row("📥 Auto Import", "⏳ Pending Methods")
    kb.row("📌 Pin Methods", "📝 Edit Methods List")
    kb.row("📣 Channel Approvals", "📨 Group Messenger")
    kb.row("🧾 Logs")
    kb.row("💾 Backup/Export")
    kb.row("❌ Cancel", "❌ Exit")

    return kb

@bot.message_handler(func=lambda m: m.text == "⚙️ ADMIN PANEL" and is_admin(m.from_user.id))
def open_admin(m):
    bot.send_message(m.from_user.id, "⚙️ **Admin Panel**", reply_markup=admin_menu(), parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "❌ Exit" and is_admin(m.from_user.id))
def exit_admin(m):
    bot.send_message(m.from_user.id, "Exited", reply_markup=main_menu(m.from_user.id))

# =========================
# 📊 LEADERBOARD (NEW FEATURE)
# =========================
@bot.message_handler(func=lambda m: m.text == "📊 Leaderboard" and is_admin(m.from_user.id))
def leaderboard_menu(m):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("🏆 Top Referrals", callback_data="top_referrals"),
        InlineKeyboardButton("💰 Top Points", callback_data="top_points"),
        InlineKeyboardButton("⭐ Top Earners", callback_data="top_earned")
    )
    bot.send_message(m.from_user.id, "📊 **Leaderboard**\n\nSelect leaderboard type:", reply_markup=kb, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data == "top_referrals")
def top_referrals_cb(c):
    users = list(users_col.find({}).sort("refs", -1).limit(30))
    text = "🏆 **TOP 30 USERS BY REFERRALS** 🏆\n\n"
    
    for i, user in enumerate(users, 1):
        username = user.get("username") or f"User_{user['_id'][:6]}"
        refs = user.get("refs", 0)
        is_vip = "👑" if user.get("vip", False) else "📌"
        text += f"{i}. {is_vip} <code>{username}</code> → {refs} referrals\n"
    
    if not users:
        text += "No users found!"
    
    bot.edit_message_text(text, c.from_user.id, c.message.message_id, parse_mode="HTML")
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "top_points")
def top_points_cb(c):
    users = list(users_col.find({}).sort("points", -1).limit(30))
    text = "💰 **TOP 30 USERS BY POINTS** 💰\n\n"
    
    for i, user in enumerate(users, 1):
        username = user.get("username") or f"User_{user['_id'][:6]}"
        points = user.get("points", 0)
        is_vip = "👑" if user.get("vip", False) else "📌"
        text += f"{i}. {is_vip} <code>{username}</code> → {points:,} pts\n"
    
    if not users:
        text += "No users found!"
    
    bot.edit_message_text(text, c.from_user.id, c.message.message_id, parse_mode="HTML")
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "top_earned")
def top_earned_cb(c):
    users = list(users_col.find({}).sort("total_points_earned", -1).limit(30))
    text = "⭐ **TOP 30 USERS BY POINTS EARNED** ⭐\n\n"
    
    for i, user in enumerate(users, 1):
        username = user.get("username") or f"User_{user['_id'][:6]}"
        earned = user.get("total_points_earned", 0)
        is_vip = "👑" if user.get("vip", False) else "📌"
        text += f"{i}. {is_vip} <code>{username}</code> → {earned:,} pts earned\n"
    
    if not users:
        text += "No users found!"
    
    bot.edit_message_text(text, c.from_user.id, c.message.message_id, parse_mode="HTML")
    bot.answer_callback_query(c.id)

# =========================
# 📤 UPLOAD SYSTEM (FAST)
# =========================
upload_sessions = {}

def start_upload(uid, cat, is_service=False):
    upload_sessions[uid] = {"cat": cat, "service": is_service, "files": [], "step": "name"}
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row("📄 Text", "📁 Files")
    kb.row("/cancel")
    msg = bot.send_message(uid, f"📤 **Upload to {cat.upper()}**\n\nChoose content type:", reply_markup=kb, parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda m: upload_type_choice(m, cat, is_service))

def upload_type_choice(m, cat, is_service):
    if m.text == "/cancel":
        upload_sessions.pop(m.from_user.id, None)
        bot.send_message(m.from_user.id, "❌ Cancelled", reply_markup=admin_menu())
        return
    
    if m.text == "📄 Text":
        msg = bot.send_message(m.from_user.id, "📝 **Product/Method name:**", parse_mode="Markdown")
        bot.register_next_step_handler(msg, lambda x: upload_text_name(x, cat, is_service))
    elif m.text == "📁 Files":
        kb = ReplyKeyboardMarkup(resize_keyboard=True)
        kb.row("/done", "/cancel")
        msg = bot.send_message(m.from_user.id, f"📤 **Upload files**\n\nSend text, chat messages, photos, videos, files, documents or other Telegram content. Use /done when finished:", reply_markup=kb, parse_mode="Markdown")
        bot.register_next_step_handler(msg, lambda x: upload_file_step(x, cat, m.from_user.id, [], is_service))
    else:
        bot.send_message(m.from_user.id, "❌ Invalid", reply_markup=admin_menu())

def upload_text_name(m, cat, is_service):
    name = m.text
    msg = bot.send_message(m.from_user.id, "💰 **Price**\nFree/Free Product: points (0 = free)\nVIP/Paid Product: USDT:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: upload_text_price(x, cat, name, is_service))

def _normalize_stock_status(value):
    value = str(value or "").strip().lower().replace(" ", "_")
    if value in ("out", "out_of_stock", "sold_out", "unavailable", "0", "no"):
        return "out_of_stock"
    if value in ("in", "in_stock", "available", "1", "yes"):
        return "in_stock"
    raise ValueError("Send IN STOCK or OUT OF STOCK")


def upload_text_price(m, cat, name, is_service):
    try:
        price = float(m.text) if cat in ("vip", "paid_service") else int(m.text)
        if price < 0:
            raise ValueError()
        if is_service:
            msg = raw_bot.send_message(m.from_user.id, "⏳ PRODUCT DURATION\n\nExample: 30 Days, 1 Year, Lifetime, or No duration")
            bot.register_next_step_handler(msg, lambda x: service_text_duration_step(x, cat, name, price))
        else:
            msg = bot.send_message(m.from_user.id, "📝 **Content:**", parse_mode="Markdown")
            bot.register_next_step_handler(msg, lambda x: upload_text_save(x, cat, name, price, False))
    except Exception:
        bot.send_message(m.from_user.id, "❌ Invalid price!")


def service_text_duration_step(m, cat, name, price):
    duration = (m.text or "Not specified").strip()
    msg = raw_bot.send_message(m.from_user.id, "🛡 WARRANTY\n\nExample: 7 Days Replacement, 30 Days, No Warranty")
    bot.register_next_step_handler(msg, lambda x: service_text_warranty_step(x, cat, name, price, duration))


def service_text_warranty_step(m, cat, name, price, duration):
    warranty = (m.text or "Not specified").strip()
    msg = raw_bot.send_message(m.from_user.id, "📦 STOCK STATUS\n\nSend: IN STOCK or OUT OF STOCK")
    bot.register_next_step_handler(msg, lambda x: service_text_stock_step(x, cat, name, price, duration, warranty))


def service_text_stock_step(m, cat, name, price, duration, warranty):
    try:
        stock_status = _normalize_stock_status(m.text)
    except Exception as exc:
        msg = raw_bot.send_message(m.from_user.id, f"❌ {exc}\n\nSend: IN STOCK or OUT OF STOCK")
        bot.register_next_step_handler(msg, lambda x: service_text_stock_step(x, cat, name, price, duration, warranty))
        return
    msg = raw_bot.send_message(m.from_user.id, "📦 PRODUCT DELIVERY / DETAILS\n\nSend the digital account, link, code, instructions, or other product content users should receive after access/payment approval.")
    bot.register_next_step_handler(msg, lambda x: upload_text_save(x, cat, name, price, True, duration, warranty, stock_status))


def upload_text_save(m, cat, name, price, is_service, duration=None, warranty=None, stock_status=None):
    text_content = m.text
    # For normal FREE/VIP method posts, retain the original Telegram message.
    # Delivering it later through copy_message preserves custom/premium emojis
    # and every Telegram entity automatically. Service/product records keep their
    # plain-text field because other product logic reads that field directly.
    preserve_original = (cat in ("free", "vip") and not is_service and getattr(m, "message_id", None))
    original_files = [{"chat": m.chat.id, "msg": m.message_id, "type": m.content_type}] if preserve_original else []
    stored_text = None if preserve_original else text_content
    number = fs.add(cat, name, original_files, price, text_content=stored_text)
    folder = fs.get_by_number(number)
    if is_service and folder:
        folders_col.update_one(
            {"_id": folder["_id"]},
            {"$set": {
                "service_msg": text_content,
                "duration": duration or "Not specified",
                "warranty": warranty or "Not specified",
                "stock_status": stock_status or "in_stock",
                "product_type": "digital_product",
            }},
        )
        folder = folders_col.find_one({"_id": folder["_id"]}) or folder
    send_method_notification("uploaded", folder or {"cat":cat,"name":name,"number":number,"price":price})
    if cat in ("free", "vip"):
        notify_all_users_about_method(folder or {"cat":cat,"name":name,"number":number,"price":price}, "uploaded")
    unit = "USDT" if cat in ("vip", "paid_service") else "points"
    extra = ""
    if is_service:
        extra = f"\n⏳ {duration}\n🛡 {warranty}\n📦 {'IN STOCK' if stock_status == 'in_stock' else 'OUT OF STOCK'}"
    bot.send_message(m.from_user.id, f"✅ Added!\n📌 #{number}\n📂 {name}\n💰 {price} {unit}{extra}", reply_markup=admin_menu())
    upload_sessions.pop(m.from_user.id, None)


def upload_file_step(m, cat, uid, files, is_service):
    if m.text == "/cancel":
        upload_sessions.pop(uid, None)
        bot.send_message(uid, "❌ Cancelled", reply_markup=admin_menu())
        return
    if m.text == "/done":
        if not files:
            bot.send_message(uid, "❌ No files!")
            return
        msg = bot.send_message(uid, "📝 **Product/Folder name:**", parse_mode="Markdown")
        bot.register_next_step_handler(msg, lambda x: upload_file_name(x, cat, files, is_service))
        return
    # Store the original Telegram message and deliver it later with copy_message.
    # This supports text, photo, video, document, audio, voice, animation, sticker,
    # video-note, contact, location, venue, poll and most other copyable message types.
    if getattr(m, "message_id", None):
        files.append({"chat": m.chat.id, "msg": m.message_id, "type": m.content_type})
        prompt = raw_bot.send_message(uid, f"✅ Saved ({len(files)} item(s)). Send anything else, or /done when finished.")
    else:
        prompt = raw_bot.send_message(uid, "Send a Telegram message/file/media item, or /done when finished.")
    bot.register_next_step_handler(prompt, lambda x: upload_file_step(x, cat, uid, files, is_service))


def upload_file_name(m, cat, files, is_service):
    name = m.text
    msg = bot.send_message(m.from_user.id, "💰 **Price**\nFree/Free Product: points (0 = free)\nVIP/Paid Product: USDT:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: upload_file_save(x, cat, name, files, is_service))


def upload_file_save(m, cat, name, files, is_service):
    try:
        price = float(m.text) if cat in ("vip", "paid_service") else int(m.text)
        if price < 0:
            raise ValueError()
        if is_service:
            state = {"cat": cat, "name": name, "files": files, "price": price}
            msg = raw_bot.send_message(m.from_user.id, "⏳ PRODUCT DURATION\n\nExample: 30 Days, 1 Year, Lifetime")
            bot.register_next_step_handler(msg, lambda x: service_file_duration_step(x, state))
            return
        number = fs.add(cat, name, files, price)
        _folder = fs.get_by_number(number) or {"cat":cat,"name":name,"number":number,"price":price}
        send_method_notification("uploaded", _folder)
        if cat in ("free", "vip"):
            notify_all_users_about_method(_folder, "uploaded")
        bot.send_message(m.from_user.id, f"✅ Uploaded!\n📌 #{number}\n📂 {name}\n💰 {price} points\n📁 {len(files)} files", reply_markup=admin_menu())
        upload_sessions.pop(m.from_user.id, None)
    except Exception:
        bot.send_message(m.from_user.id, "❌ Invalid price!")


def service_file_duration_step(m, state):
    state["duration"] = (m.text or "Not specified").strip()
    msg = raw_bot.send_message(m.from_user.id, "🛡 WARRANTY\n\nExample: 7 Days Replacement, 30 Days, No Warranty")
    bot.register_next_step_handler(msg, lambda x: service_file_warranty_step(x, state))


def service_file_warranty_step(m, state):
    state["warranty"] = (m.text or "Not specified").strip()
    msg = raw_bot.send_message(m.from_user.id, "📦 STOCK STATUS\n\nSend: IN STOCK or OUT OF STOCK")
    bot.register_next_step_handler(msg, lambda x: service_file_stock_step(x, state))


def service_file_stock_step(m, state):
    try:
        state["stock_status"] = _normalize_stock_status(m.text)
    except Exception as exc:
        msg = raw_bot.send_message(m.from_user.id, f"❌ {exc}\n\nSend: IN STOCK or OUT OF STOCK")
        bot.register_next_step_handler(msg, lambda x: service_file_stock_step(x, state))
        return
    msg = raw_bot.send_message(m.from_user.id, "📝 PRODUCT NOTE\n\nSend delivery instructions/description shown with the product. Send `skip` if the uploaded files are enough.")
    bot.register_next_step_handler(msg, lambda x: service_file_finish(x, state))


def service_file_finish(m, state):
    note = "" if (m.text or "").strip().lower() == "skip" else (m.text or "")
    number = fs.add(state["cat"], state["name"], state["files"], state["price"], text_content=None)
    folder = fs.get_by_number(number)
    if folder:
        folders_col.update_one(
            {"_id": folder["_id"]},
            {"$set": {
                "service_msg": note,
                "duration": state["duration"],
                "warranty": state["warranty"],
                "stock_status": state["stock_status"],
                "product_type": "digital_product",
            }},
        )
        folder = folders_col.find_one({"_id": folder["_id"]}) or folder
    send_method_notification("uploaded", folder or {"cat":state["cat"],"name":state["name"],"number":number,"price":state["price"]})
    unit = "USDT" if state["cat"] == "paid_service" else "points"
    raw_bot.send_message(
        m.from_user.id,
        f"✅ DIGITAL PRODUCT ADDED\n\n📌 #{number}\n🛍️ {state['name']}\n💰 {state['price']} {unit}\n⏳ {state['duration']}\n🛡 {state['warranty']}\n📦 {'IN STOCK' if state['stock_status']=='in_stock' else 'OUT OF STOCK'}\n📁 {len(state['files'])} file(s)",
        reply_markup=admin_menu(),
    )
    upload_sessions.pop(m.from_user.id, None)


# ==========================================================
# 🛍 GLOBEXOMART DIGITAL PRODUCT SHOP
# Dedicated inventory + balance checkout subsystem.
# This intentionally does NOT use the methods/folders uploader.
# ==========================================================
from bson import ObjectId

_product_admin_state = {}
_product_qty_state = {}
_product_edit_state = {}
_product_purchase_lock = threading.RLock()


def _shop_oid(value):
    try:
        return ObjectId(str(value))
    except Exception:
        return None


def _shop_money(value):
    try:
        return f"${float(value):.2f} USDT"
    except Exception:
        return "$0.00 USDT"


def _parse_stock_items(raw):
    """One item per line, or multi-line items separated with a line containing --- ."""
    text = str(raw or "").strip()
    if not text:
        return []
    if re.search(r"(?m)^\s*---\s*$", text):
        return [x.strip() for x in re.split(r"(?m)^\s*---\s*$", text) if x.strip()]
    return [x.strip() for x in text.splitlines() if x.strip()]


def _shop_stock_count(product):
    return len(product.get("stock", []) or [])


def _shop_is_available(product):
    return bool(product.get("active", True)) and not bool(product.get("force_out_of_stock", False)) and _shop_stock_count(product) > 0


def _shop_status(product):
    if not product.get("active", True):
        return "HIDDEN"
    return "IN STOCK" if _shop_is_available(product) else "OUT OF STOCK"


def _shop_rules():
    cfg = get_cached_config()
    rules = cfg.get("product_bulk_discounts") or [
        {"quantity": 2, "percent": 5},
        {"quantity": 5, "percent": 10},
        {"quantity": 10, "percent": 15},
    ]
    clean=[]
    for row in rules:
        try:
            q=int(row.get("quantity",0)); pc=int(row.get("percent",0))
            if q > 1 and 0 <= pc <= 100:
                clean.append({"quantity":q,"percent":pc})
        except Exception:
            pass
    return sorted(clean, key=lambda x:x["quantity"])


def _shop_discount(quantity):
    best=0
    for rule in _shop_rules():
        if int(quantity) >= int(rule["quantity"]):
            best=max(best,int(rule["percent"]))
    return best


def _shop_discount_text():
    rules=_shop_rules()
    if not rules:
        return "No bulk discounts"
    return "\n".join(f"• {r['quantity']} items = {r['percent']}% OFF" for r in rules)


def _shop_card(product, admin=False):
    stock=_shop_stock_count(product)
    kind=product.get("kind","paid")
    if kind == "free":
        pts=int(product.get("points_price",0) or 0)
        price_line="FREE" if pts <= 0 else f"{pts:,} points"
    else:
        price_line=_shop_money(product.get("price_usdt",0))
    text=(
        f"🛍️ {product.get('name','Product')}\n\n"
        f"💵 Price: {price_line}\n"
        f"📦 Stock: {stock}\n"
        f"📌 Status: {_shop_status(product)}\n"
        f"⏳ Duration: {product.get('duration') or 'Not specified'}\n"
        f"🛡 Warranty: {product.get('warranty') or 'Not specified'}\n"
    )
    if admin:
        text += f"🔥 Sales: {int(product.get('sales',0) or 0)}\n💰 Revenue: {_shop_money(product.get('revenue',0))}\n📍 Position: {int(product.get('position',999999) or 999999)}\n"
    desc=str(product.get("description") or "").strip()
    if desc:
        text += "\n" + desc
    if kind == "paid":
        text += "\n\n📦 Bulk discounts:\n" + _shop_discount_text()
    return text


def _shop_next_position(kind):
    row=shop_products_col.find_one({"kind":kind}, sort=[("position",-1)])
    try:
        return int(row.get("position",0))+1 if row else 1
    except Exception:
        return 1


def _shop_product(pid):
    oid=_shop_oid(pid)
    return shop_products_col.find_one({"_id":oid}) if oid else None


def _shop_list_keyboard(kind, page=0, page_size=12):
    rows=list(shop_products_col.find({"kind":kind,"active":{"$ne":False}}))
    rows.sort(key=lambda x:(0 if _shop_is_available(x) else 1, int(x.get("position",999999) or 999999), str(x.get("name","")).lower()))
    start=max(0,int(page))*page_size
    subset=rows[start:start+page_size]
    kb=InlineKeyboardMarkup(row_width=1)
    for product in subset:
        stock=_shop_stock_count(product)
        status="✅" if _shop_is_available(product) else "❌"
        if kind=="paid":
            price=f"${float(product.get('price_usdt',0) or 0):g}"
        else:
            pts=int(product.get("points_price",0) or 0)
            price="FREE" if pts<=0 else f"{pts} pts"
        kb.add(InlineKeyboardButton(f"{status} {product.get('name','Product')[:34]} • {price} • {stock} left", callback_data=f"shopview|{product['_id']}"))
    nav=[]
    if page>0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"shoppage|{kind}|{page-1}"))
    if start+page_size < len(rows):
        nav.append(InlineKeyboardButton("➡️", callback_data=f"shoppage|{kind}|{page+1}"))
    if nav: kb.row(*nav)
    kb.add(InlineKeyboardButton("🧾 My Orders", callback_data="shoporders|mine"))
    return kb, len(rows)


@bot.callback_query_handler(func=lambda c: c.data.startswith("shopcat|"))
def shop_catalog_cb(c):
    if force_block(c.from_user.id):
        return
    kind=c.data.split("|",1)[1]
    if kind not in ("free","paid"):
        return bot.answer_callback_query(c.id,"Invalid category",True)
    kb,count=_shop_list_keyboard(kind)
    title="🆓 FREE PRODUCTS" if kind=="free" else "💵 PAID PRODUCTS"
    note="Free products can be free or use points." if kind=="free" else "Paid products use your approved USDT bot balance."
    raw_bot.send_message(c.from_user.id,f"{title}\n\n{note}\nProducts: {count}",reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("shoppage|"))
def shop_page_cb(c):
    try:
        _,kind,page=c.data.split("|",2)
        kb,count=_shop_list_keyboard(kind,int(page))
        bot.edit_message_reply_markup(c.message.chat.id,c.message.message_id,reply_markup=kb)
        bot.answer_callback_query(c.id,f"{count} products")
    except Exception as exc:
        bot.answer_callback_query(c.id,str(exc)[:120],True)


@bot.callback_query_handler(func=lambda c: c.data.startswith("shopview|"))
def shop_view_cb(c):
    if force_block(c.from_user.id):
        return
    product=_shop_product(c.data.split("|",1)[1])
    if not product or product.get("active") is False:
        return bot.answer_callback_query(c.id,"Product not found",True)
    kb=InlineKeyboardMarkup(row_width=1)
    if _shop_is_available(product):
        label="🛒 Buy Now" if product.get("kind")=="paid" else ("🎁 Get Free" if int(product.get("points_price",0) or 0)<=0 else f"💎 Buy for {int(product.get('points_price',0))} points")
        kb.add(InlineKeyboardButton(label, callback_data=f"shopbuyask|{product['_id']}"))
    else:
        kb.add(InlineKeyboardButton("❌ OUT OF STOCK", callback_data="shopnoop"))
    kb.add(InlineKeyboardButton("🧾 My Orders", callback_data="shoporders|mine"))
    raw_bot.send_message(c.from_user.id,_shop_card(product),reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data == "shopnoop")
def shop_noop(c):
    bot.answer_callback_query(c.id,"Out of stock",True)


@bot.callback_query_handler(func=lambda c: c.data.startswith("shopbuyask|"))
def shop_buy_ask(c):
    if force_block(c.from_user.id):
        return
    product=_shop_product(c.data.split("|",1)[1])
    if not product or not _shop_is_available(product):
        return bot.answer_callback_query(c.id,"Product is out of stock",True)
    # Quantity input mirrors the dedicated shopping bot. Free products default to one but may also be bought in quantity.
    _product_qty_state[c.from_user.id]={"product_id":str(product["_id"])}
    msg=raw_bot.send_message(c.from_user.id,f"🛒 BUY {product.get('name')}\n\nAvailable stock: {_shop_stock_count(product)}\nSend quantity to buy (example: 1).\n\n/cancel to stop.")
    bot.register_next_step_handler(msg, shop_quantity_received)
    bot.answer_callback_query(c.id,"Send quantity")


def shop_quantity_received(m):
    uid=m.from_user.id
    state=_product_qty_state.get(uid)
    if not state:
        return
    if (m.text or "").strip().lower()=="/cancel":
        _product_qty_state.pop(uid,None)
        return raw_bot.send_message(uid,"❌ Purchase cancelled.",reply_markup=main_menu(uid))
    try:
        qty=int((m.text or "").strip())
        if qty<=0 or qty>100:
            raise ValueError()
    except Exception:
        msg=raw_bot.send_message(uid,"❌ Send a quantity from 1 to 100, or /cancel.")
        return bot.register_next_step_handler(msg,shop_quantity_received)
    product=_shop_product(state["product_id"])
    if not product or not _shop_is_available(product) or _shop_stock_count(product)<qty:
        _product_qty_state.pop(uid,None)
        return raw_bot.send_message(uid,"❌ Not enough stock for that quantity.")
    if product.get("kind")=="paid":
        each=float(product.get("price_usdt",0) or 0)
        subtotal=round(each*qty,2)
        disc=_shop_discount(qty)
        total=round(subtotal*(100-disc)/100.0,2)
        balance=_user_usdt_balance(uid)
        price_text=f"Price each: {_shop_money(each)}\nSubtotal: {_shop_money(subtotal)}\nDiscount: {disc}%\nTotal: {_shop_money(total)}\nYour balance: {_shop_money(balance)}"
    else:
        each=int(product.get("points_price",0) or 0)
        total=each*qty
        user=User(uid)
        price_text=(f"Price each: {each} points\nTotal: {total} points\nYour points: {user.points()}" if each>0 else "Total: FREE")
    _product_qty_state.pop(uid,None)
    kb=InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✅ Confirm Buy",callback_data=f"shopbuyconfirm|{product['_id']}|{qty}"))
    kb.add(InlineKeyboardButton("❌ Cancel",callback_data="shopcancel"))
    raw_bot.send_message(uid,f"✅ CONFIRM PURCHASE\n\nProduct: {product.get('name')}\nQuantity: {qty}\n{price_text}",reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data == "shopcancel")
def shop_cancel_cb(c):
    bot.answer_callback_query(c.id,"Cancelled")
    try: bot.edit_message_text("❌ Purchase cancelled.",c.message.chat.id,c.message.message_id)
    except Exception: pass


def _shop_deliver(uid, product, delivered, order_id):
    raw_bot.send_message(uid,f"✅ PURCHASE COMPLETE\n\nOrder: {str(order_id)[-8:]}\nProduct: {product.get('name')}\nQuantity: {len(delivered)}\n\nYour digital product data:")
    for i,item in enumerate(delivered,1):
        text=str(item)
        if len(delivered)>1:
            text=f"📦 Item {i}/{len(delivered)}\n\n"+text
        # Telegram text limit safety.
        while text:
            raw_bot.send_message(uid,text[:4000])
            text=text[4000:]
    common=str(product.get("delivery_note") or "").strip()
    if common:
        raw_bot.send_message(uid,"📝 Product instructions\n\n"+common)


@bot.callback_query_handler(func=lambda c: c.data.startswith("shopbuyconfirm|"))
def shop_buy_confirm(c):
    if force_block(c.from_user.id):
        return
    try:
        _,pid,qty_raw=c.data.split("|",2)
        qty=int(qty_raw)
        if qty<=0 or qty>100:
            raise ValueError("Invalid quantity")
    except Exception:
        return bot.answer_callback_query(c.id,"Invalid purchase",True)
    uid=int(c.from_user.id)
    with _product_purchase_lock:
        product=_shop_product(pid)
        if not product or not _shop_is_available(product):
            return bot.answer_callback_query(c.id,"Product is out of stock",True)
        stock=list(product.get("stock",[]) or [])
        if len(stock)<qty:
            return bot.answer_callback_query(c.id,"Not enough stock",True)
        delivered=stock[:qty]
        remaining=stock[qty:]
        kind=product.get("kind")
        if kind=="paid":
            each=float(product.get("price_usdt",0) or 0)
            subtotal=round(each*qty,2)
            disc=_shop_discount(qty)
            total=round(subtotal*(100-disc)/100.0,2)
            if total<=0:
                return bot.answer_callback_query(c.id,"Product price is not configured",True)
            debit=users_col.update_one({"_id":str(uid),"usdt_balance":{"$gte":total}},{"$inc":{"usdt_balance":-total}})
            if debit.modified_count!=1:
                raw_bot.send_message(uid,f"❌ Insufficient USDT balance.\nNeed: {_shop_money(total)}\nYour balance: {_shop_money(_user_usdt_balance(uid))}\n\nUse 💳 Deposit first.")
                return bot.answer_callback_query(c.id,"Insufficient balance",True)
            paid_with="USDT Balance"
        else:
            each=int(product.get("points_price",0) or 0)
            subtotal=each*qty
            disc=0
            total=subtotal
            if total>0:
                # Atomic points debit.
                debit=users_col.update_one({"_id":str(uid),"points":{"$gte":total}},{"$inc":{"points":-total,"total_points_spent":total}})
                if debit.modified_count!=1:
                    return bot.answer_callback_query(c.id,"Not enough points",True)
            paid_with="Points" if total>0 else "Free"
        try:
            upd=shop_products_col.update_one({"_id":product["_id"],"stock":stock},{"$set":{"stock":remaining,"updated_at":time.time()},"$inc":{"sales":qty,"revenue":float(total) if kind=="paid" else 0}})
            if upd.modified_count!=1:
                # Compensate if another purchase changed stock first.
                if kind=="paid": users_col.update_one({"_id":str(uid)},{"$inc":{"usdt_balance":total}})
                elif total>0: users_col.update_one({"_id":str(uid)},{"$inc":{"points":total,"total_points_spent":-total}})
                return bot.answer_callback_query(c.id,"Stock changed. Please try again.",True)
            order={
                "user_id":uid,"chat_id":int(c.message.chat.id),"username":c.from_user.username,
                "product_id":str(product["_id"]),"product_name":product.get("name"),"kind":kind,
                "quantity":qty,"price_each":each,"subtotal":subtotal,"discount_percent":disc,
                "total":total,"paid_with":paid_with,"delivered":delivered,"status":"completed",
                "created_at":time.time(),
            }
            result=shop_orders_col.insert_one(order)
            _publish_proof("PRODUCT", order)
            if kind=="paid":
                payments_col.insert_one({"user_id":uid,"type":"product","product_id":str(product["_id"]),"amount":float(total),"currency":"USDT","mode":"balance","status":"paid","created_at":time.time()})
            _shop_deliver(uid,product,delivered,result.inserted_id)
            admin_text=(f"🛒 NEW PRODUCT ORDER\n\nOrder: {str(result.inserted_id)[-8:]}\nUser: @{c.from_user.username or 'None'}\nUser ID: {uid}\nChat ID: {c.message.chat.id}\nProduct: {product.get('name')}\nQuantity: {qty}\nTotal: {_shop_money(total) if kind=='paid' else (str(total)+' points' if total else 'FREE')}\nPaid with: {paid_with}")
            for adm in get_all_admins():
                try: raw_bot.send_message(int(adm["_id"]),admin_text)
                except Exception: pass
            bot.answer_callback_query(c.id,"Purchase completed",True)
        except Exception as exc:
            # Best-effort compensation.
            if kind=="paid": users_col.update_one({"_id":str(uid)},{"$inc":{"usdt_balance":total}})
            elif total>0: users_col.update_one({"_id":str(uid)},{"$inc":{"points":total,"total_points_spent":-total}})
            log_event("shop_purchase_error",uid,details={"error":str(exc),"product_id":pid},level="error")
            raw_bot.send_message(uid,"❌ Purchase failed due to a database error. Your balance was restored. Contact admin if needed.")


@bot.callback_query_handler(func=lambda c: c.data == "shoporders|mine")
def shop_my_orders(c):
    rows=list(shop_orders_col.find({"user_id":int(c.from_user.id)}).sort("created_at",-1).limit(10))
    if not rows:
        text="🧾 MY PRODUCT ORDERS\n\nNo product orders yet."
    else:
        lines=["🧾 MY PRODUCT ORDERS\n"]
        for o in rows:
            total=_shop_money(o.get("total",0)) if o.get("kind")=="paid" else (f"{o.get('total',0)} points" if o.get("total",0) else "FREE")
            lines.append(f"• {o.get('product_name')} ×{o.get('quantity',1)} — {total} — #{str(o.get('_id'))[-8:]}")
        text="\n".join(lines)
    raw_bot.send_message(c.from_user.id,text)
    bot.answer_callback_query(c.id)


# ---------- Admin product upload wizard ----------
def _shop_start_admin_upload(uid, kind):
    _product_admin_state[uid]={"mode":"create","kind":kind,"step":"name","data":{}}
    label="FREE PRODUCT" if kind=="free" else "PAID PRODUCT"
    raw_bot.send_message(uid,f"➕ ADD {label}\n\nStep 1/6 — Send product name.\n\n/cancel to stop.")


@bot.message_handler(func=lambda m: m.text == "🆓 Upload Free Product" and is_admin(m.from_user.id))
def shop_upload_free_button(m):
    _shop_start_admin_upload(m.from_user.id,"free")


@bot.message_handler(func=lambda m: m.text == "💵 Upload Paid Product" and is_admin(m.from_user.id))
def shop_upload_paid_button(m):
    _shop_start_admin_upload(m.from_user.id,"paid")


def _shop_save_new_product(uid,state):
    d=state["data"]; kind=state["kind"]
    stock=_parse_stock_items(d.get("stock_raw",""))
    doc={
        "name":d["name"],"kind":kind,
        "price_usdt":float(d.get("price_usdt",0) or 0),
        "points_price":int(d.get("points_price",0) or 0),
        "description":d.get("description","")[:1500],
        "duration":d.get("duration") or "Not specified",
        "warranty":d.get("warranty") or "Not specified",
        "delivery_note":d.get("delivery_note") or "",
        "stock":stock,"active":True,"force_out_of_stock":False,
        "position":_shop_next_position(kind),"sales":0,"revenue":0.0,
        "created_by":uid,"created_at":time.time(),"updated_at":time.time(),
    }
    result=shop_products_col.insert_one(doc)
    doc["_id"]=result.inserted_id
    _product_admin_state.pop(uid,None)
    notify_all_users_about_product(doc)
    raw_bot.send_message(uid,"✅ PRODUCT CREATED\n\n"+_shop_card(doc,admin=True)+f"\n\nProduct ID: {doc['_id']}",reply_markup=admin_menu())


@bot.message_handler(func=lambda m: m.from_user is not None and m.from_user.id in _product_admin_state, content_types=["text"])
def shop_admin_wizard_message(m):
    uid=m.from_user.id
    state=_product_admin_state.get(uid)
    if not state: return
    text=(m.text or "").strip()
    if text.lower()=="/cancel":
        _product_admin_state.pop(uid,None)
        return raw_bot.send_message(uid,"❌ Product upload cancelled.",reply_markup=admin_menu())
    d=state["data"]; step=state["step"]
    try:
        if step=="name":
            if len(text)<2: raise ValueError("Name is too short")
            d["name"]=text[:100]
            state["step"]="price"
            prompt="Step 2/6 — Send points price (0 = completely free)." if state["kind"]=="free" else "Step 2/6 — Send product price in USDT, for example 5 or 5.50."
            return raw_bot.send_message(uid,prompt)
        if step=="price":
            if state["kind"]=="free":
                val=int(text); 
                if val<0: raise ValueError("Price cannot be negative")
                d["points_price"]=val
            else:
                val=float(text.replace(",","."));
                if val<=0: raise ValueError("Paid product price must be greater than 0")
                d["price_usdt"]=round(val,2)
            state["step"]="description"
            return raw_bot.send_message(uid,"Step 3/6 — Send public product description.")
        if step=="description":
            d["description"]=text[:1500]
            state["step"]="duration"
            return raw_bot.send_message(uid,"Step 4/6 — Send duration. Example: 30 Days, 1 Year, Lifetime, No duration.")
        if step=="duration":
            d["duration"]=text[:120]
            state["step"]="warranty"
            return raw_bot.send_message(uid,"Step 5/6 — Send warranty. Example: 7 Days Replacement, 30 Days, No Warranty.")
        if step=="warranty":
            d["warranty"]=text[:160]
            state["step"]="stock"
            return raw_bot.send_message(uid,"Step 6/6 — Send INITIAL STOCK.\n\n• One account/link/code per line.\n• For a multi-line item, separate items with a line containing ---\n• Send /skip to create it OUT OF STOCK.\n\nExample:\nemail1@example.com|pass1\nemail2@example.com|pass2")
        if step=="stock":
            d["stock_raw"]="" if text.lower()=="/skip" else text
            state["step"]="delivery_note"
            return raw_bot.send_message(uid,"Optional product instructions sent after purchase. Send text now, or /skip.")
        if step=="delivery_note":
            d["delivery_note"]="" if text.lower()=="/skip" else text[:2000]
            return _shop_save_new_product(uid,state)
    except Exception as exc:
        raw_bot.send_message(uid,f"❌ {exc}\nPlease try this step again, or /cancel.")


# ---------- Admin product manager ----------
def _shop_admin_product_keyboard(product):
    pid=str(product["_id"])
    kb=InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("✏️ Name",callback_data=f"shopadm_edit|name|{pid}"),InlineKeyboardButton("💵 Price",callback_data=f"shopadm_edit|price|{pid}"))
    kb.add(InlineKeyboardButton("📝 Description",callback_data=f"shopadm_edit|description|{pid}"),InlineKeyboardButton("⏳ Duration",callback_data=f"shopadm_edit|duration|{pid}"))
    kb.add(InlineKeyboardButton("🛡 Warranty",callback_data=f"shopadm_edit|warranty|{pid}"),InlineKeyboardButton("📝 Delivery Note",callback_data=f"shopadm_edit|delivery_note|{pid}"))
    kb.add(InlineKeyboardButton("➕ Add Stock",callback_data=f"shopadm_stockadd|{pid}"),InlineKeyboardButton("📦 View Stock",callback_data=f"shopadm_stockview|{pid}"))
    kb.add(InlineKeyboardButton("📌 Position",callback_data=f"shopadm_edit|position|{pid}"),InlineKeyboardButton("📛 Stockout Label",callback_data=f"shopadm_edit|stockout_label|{pid}"))
    kb.add(InlineKeyboardButton("🟢/🔴 Toggle Active",callback_data=f"shopadm_toggle|{pid}"),InlineKeyboardButton("📦 Force Stockout",callback_data=f"shopadm_stockout|{pid}"))
    kb.add(InlineKeyboardButton("🗑 Delete",callback_data=f"shopadm_deleteask|{pid}"))
    return kb


@bot.message_handler(func=lambda m: m.text == "🛍 Product Manager" and is_admin(m.from_user.id))
def shop_product_manager_button(m):
    _shop_send_manager(m.from_user.id)


def _shop_send_manager(uid):
    rows=list(shop_products_col.find({}).sort([("kind",1),("position",1),("created_at",-1)]).limit(100))
    kb=InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("➕ Add Free Product",callback_data="shopadm_new|free"),InlineKeyboardButton("➕ Add Paid Product",callback_data="shopadm_new|paid"))
    kb.add(InlineKeyboardButton("🧾 Product Orders",callback_data="shopadm_orders"),InlineKeyboardButton("📊 Product Stats",callback_data="shopadm_stats"))
    for p in rows:
        status="✅" if _shop_is_available(p) else "❌"
        kind="FREE" if p.get("kind")=="free" else "PAID"
        kb.add(InlineKeyboardButton(f"{status} {kind} • {p.get('name','Product')[:32]} • {_shop_stock_count(p)} stock",callback_data=f"shopadm_view|{p['_id']}"))
    text="🛍 PRODUCT MANAGER\n\nDedicated digital-product inventory.\nProducts: %d\n\nBulk discounts:\n%s\n\n/setproductdiscounts 2=5,5=10,10=15"%(len(rows),_shop_discount_text())
    raw_bot.send_message(uid,text,reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("shopadm_new|"))
def shop_admin_new_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    kind=c.data.split("|",1)[1]
    _shop_start_admin_upload(c.from_user.id,kind)
    bot.answer_callback_query(c.id,"Upload started")


@bot.callback_query_handler(func=lambda c: c.data.startswith("shopadm_view|"))
def shop_admin_view_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    p=_shop_product(c.data.split("|",1)[1])
    if not p: return bot.answer_callback_query(c.id,"Product not found",True)
    raw_bot.send_message(c.from_user.id,_shop_card(p,admin=True),reply_markup=_shop_admin_product_keyboard(p))
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("shopadm_stockview|"))
def shop_admin_stock_view(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    p=_shop_product(c.data.split("|",1)[1])
    if not p: return bot.answer_callback_query(c.id,"Product not found",True)
    stock=list(p.get("stock",[]) or [])
    preview=stock[:50]
    text=f"📦 STOCK — {p.get('name')}\n\nTotal: {len(stock)}\n\n"
    if preview:
        for i,item in enumerate(preview,1): text += f"{i}. {str(item)[:300]}\n"
        if len(stock)>50: text += "\nShowing first 50 only."
    else: text += "No stock."
    raw_bot.send_message(c.from_user.id,text[:4000])
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("shopadm_stockadd|"))
def shop_admin_stock_add_start(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    p=_shop_product(c.data.split("|",1)[1])
    if not p: return bot.answer_callback_query(c.id,"Product not found",True)
    _product_edit_state[c.from_user.id]={"product_id":str(p["_id"]),"field":"add_stock"}
    raw_bot.send_message(c.from_user.id,"➕ ADD STOCK\n\nSend one item per line. For multi-line items use a line containing --- between items.\n/cancel to stop.")
    bot.answer_callback_query(c.id,"Send stock")


@bot.callback_query_handler(func=lambda c: c.data.startswith("shopadm_edit|"))
def shop_admin_edit_start(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    try: _,field,pid=c.data.split("|",2)
    except Exception: return bot.answer_callback_query(c.id,"Invalid",True)
    p=_shop_product(pid)
    if not p: return bot.answer_callback_query(c.id,"Product not found",True)
    prompts={
        "name":"Send new product name:","price":"Send new price (USDT for paid / points for free):",
        "description":"Send new public description:","duration":"Send new duration:","warranty":"Send new warranty:",
        "delivery_note":"Send new post-purchase instructions (send clear to remove):","position":"Send display position number (1 = first):",
        "stockout_label":"Send custom out-of-stock label (send clear for default OUT OF STOCK):",
    }
    _product_edit_state[c.from_user.id]={"product_id":pid,"field":field}
    raw_bot.send_message(c.from_user.id,prompts.get(field,"Send new value:")+"\n\n/cancel to stop.")
    bot.answer_callback_query(c.id,"Send value")


@bot.message_handler(func=lambda m: m.from_user is not None and m.from_user.id in _product_edit_state, content_types=["text"])
def shop_admin_edit_receive(m):
    uid=m.from_user.id; st=_product_edit_state.get(uid); text=(m.text or "").strip()
    if not st: return
    if text.lower()=="/cancel":
        _product_edit_state.pop(uid,None)
        return raw_bot.send_message(uid,"❌ Edit cancelled.",reply_markup=admin_menu())
    p=_shop_product(st["product_id"])
    if not p:
        _product_edit_state.pop(uid,None); return raw_bot.send_message(uid,"❌ Product not found.")
    field=st["field"]
    try:
        if field=="add_stock":
            items=_parse_stock_items(text)
            if not items: raise ValueError("No stock items found")
            shop_products_col.update_one({"_id":p["_id"]},{"$push":{"stock":{"$each":items}},"$set":{"force_out_of_stock":False,"updated_at":time.time()}})
            msg=f"✅ Added {len(items)} stock item(s). Total stock: {_shop_stock_count(_shop_product(st['product_id']))}"
        elif field=="name":
            if len(text)<2: raise ValueError("Name too short")
            shop_products_col.update_one({"_id":p["_id"]},{"$set":{"name":text[:100],"updated_at":time.time()}}); msg="✅ Name updated."
        elif field=="price":
            if p.get("kind")=="paid":
                val=float(text.replace(",","."));
                if val<=0: raise ValueError("Price must be > 0")
                shop_products_col.update_one({"_id":p["_id"]},{"$set":{"price_usdt":round(val,2),"updated_at":time.time()}})
            else:
                val=int(text); 
                if val<0: raise ValueError("Points cannot be negative")
                shop_products_col.update_one({"_id":p["_id"]},{"$set":{"points_price":val,"updated_at":time.time()}})
            msg="✅ Price updated."
        elif field=="position":
            val=int(text); 
            if val<1: raise ValueError("Position must be 1 or higher")
            shop_products_col.update_one({"_id":p["_id"]},{"$set":{"position":val,"updated_at":time.time()}}); msg="✅ Position updated."
        else:
            key=field
            val="" if text.lower()=="clear" else text
            limit=1500 if field=="description" else 2000 if field=="delivery_note" else 160
            shop_products_col.update_one({"_id":p["_id"]},{"$set":{key:val[:limit],"updated_at":time.time()}}); msg=f"✅ {field.replace('_',' ').title()} updated."
        _product_edit_state.pop(uid,None)
        raw_bot.send_message(uid,msg,reply_markup=admin_menu())
    except Exception as exc:
        raw_bot.send_message(uid,f"❌ {exc}\nTry again, or /cancel.")


@bot.callback_query_handler(func=lambda c: c.data.startswith("shopadm_toggle|"))
def shop_admin_toggle(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    p=_shop_product(c.data.split("|",1)[1]);
    if not p: return bot.answer_callback_query(c.id,"Product not found",True)
    val=not bool(p.get("active",True))
    shop_products_col.update_one({"_id":p["_id"]},{"$set":{"active":val,"updated_at":time.time()}})
    bot.answer_callback_query(c.id,"Active" if val else "Hidden",True)


@bot.callback_query_handler(func=lambda c: c.data.startswith("shopadm_stockout|"))
def shop_admin_stockout(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    p=_shop_product(c.data.split("|",1)[1]);
    if not p: return bot.answer_callback_query(c.id,"Product not found",True)
    val=not bool(p.get("force_out_of_stock",False))
    shop_products_col.update_one({"_id":p["_id"]},{"$set":{"force_out_of_stock":val,"updated_at":time.time()}})
    bot.answer_callback_query(c.id,"Forced OUT OF STOCK" if val else "Stock status automatic",True)


@bot.callback_query_handler(func=lambda c: c.data.startswith("shopadm_deleteask|"))
def shop_admin_delete_ask(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    p=_shop_product(c.data.split("|",1)[1]);
    if not p: return bot.answer_callback_query(c.id,"Product not found",True)
    kb=InlineKeyboardMarkup(); kb.add(InlineKeyboardButton("✅ Yes, Delete",callback_data=f"shopadm_delete|{p['_id']}"),InlineKeyboardButton("❌ Cancel",callback_data="shopcancel"))
    raw_bot.send_message(c.from_user.id,f"Delete product permanently?\n\n{p.get('name')}",reply_markup=kb); bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("shopadm_delete|"))
def shop_admin_delete(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    p=_shop_product(c.data.split("|",1)[1]);
    if not p: return bot.answer_callback_query(c.id,"Product not found",True)
    shop_products_col.delete_one({"_id":p["_id"]})
    raw_bot.send_message(c.from_user.id,f"🗑 Deleted: {p.get('name')}",reply_markup=admin_menu()); bot.answer_callback_query(c.id,"Deleted",True)


@bot.callback_query_handler(func=lambda c: c.data == "shopadm_orders")
def shop_admin_orders(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    rows=list(shop_orders_col.find({}).sort("created_at",-1).limit(30))
    text="🧾 RECENT PRODUCT ORDERS\n\n"
    if not rows: text += "No orders yet."
    for o in rows:
        total=_shop_money(o.get("total",0)) if o.get("kind")=="paid" else (f"{o.get('total',0)} points" if o.get("total",0) else "FREE")
        text += f"• #{str(o.get('_id'))[-8:]} • {o.get('product_name')} ×{o.get('quantity',1)} • {total} • @{o.get('username') or 'None'} ({o.get('user_id')})\n"
    raw_bot.send_message(c.from_user.id,text[:4000]); bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data == "shopadm_stats")
def shop_admin_stats(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    total=shop_products_col.count_documents({}); active=shop_products_col.count_documents({"active":{"$ne":False}}); orders=shop_orders_col.count_documents({})
    pipeline=[{"$match":{"kind":"paid"}},{"$group":{"_id":None,"revenue":{"$sum":"$total"}}}]
    row=next(iter(shop_orders_col.aggregate(pipeline)),{})
    revenue=float(row.get("revenue",0) or 0)
    top=list(shop_products_col.find({}).sort("sales",-1).limit(5))
    text=f"📊 PRODUCT STATS\n\nProducts: {active}/{total} active\nOrders: {orders}\nRevenue: {_shop_money(revenue)}\n\nTop products:\n"
    text += "\n".join(f"• {p.get('name')}: {int(p.get('sales',0) or 0)} sales" for p in top) if top else "None"
    raw_bot.send_message(c.from_user.id,text); bot.answer_callback_query(c.id)


@bot.message_handler(commands=["setproductdiscounts"])
def shop_set_discounts(m):
    if not is_admin(m.from_user.id): return
    raw=(m.text or "").replace("/setproductdiscounts","",1).strip()
    if not raw:
        return raw_bot.send_message(m.from_user.id,"Usage: /setproductdiscounts 2=5,5=10,10=15\n\nCurrent:\n"+_shop_discount_text())
    rules=[]
    try:
        for chunk in raw.replace(";",",").split(","):
            nums=re.findall(r"\d+",chunk)
            if len(nums)<2: continue
            q=int(nums[0]); pc=int(nums[1])
            if q>1 and 0<=pc<=100: rules.append({"quantity":q,"percent":pc})
        if not rules: raise ValueError("No valid rules")
        set_config("product_bulk_discounts",rules)
        raw_bot.send_message(m.from_user.id,"✅ Product bulk discounts updated:\n"+_shop_discount_text(),reply_markup=admin_menu())
    except Exception as exc:
        raw_bot.send_message(m.from_user.id,f"❌ {exc}\nUse: /setproductdiscounts 2=5,5=10,10=15")


@bot.message_handler(func=lambda m: m.text in ["📦 Upload Free Method", "💎 Upload VIP Method"] and is_admin(m.from_user.id))
def upload_handler(m):
    cats = {
        "📦 Upload Free Method": ("free", False),
        "💎 Upload VIP Method": ("vip", False),
    }
    cat, is_service = cats[m.text]
    start_upload(m.from_user.id, cat, is_service)


# =========================
# 🛍 DIGITAL PRODUCT MANAGER
# =========================
@bot.message_handler(func=lambda m: m.text == "🛍 Product Manager" and is_admin(m.from_user.id))
def product_manager_menu(m):
    rows = list(folders_col.find({"cat": {"$in": ["free_service", "paid_service"]}}).sort("created_at", -1).limit(50))
    if not rows:
        return raw_bot.send_message(m.from_user.id, "🛍 PRODUCT MANAGER\n\nNo products uploaded yet.", reply_markup=admin_menu())
    kb = InlineKeyboardMarkup(row_width=1)
    for row in rows:
        status = "✅" if _service_status(row) == "in_stock" else "❌"
        price = float(row.get("price", 0) or 0)
        unit = f"${price:g}" if row.get("cat") == "paid_service" else ("FREE" if price <= 0 else f"{int(price)} pts")
        kb.add(InlineKeyboardButton(f"{status} #{row.get('number')} • {row.get('name','Product')[:28]} • {unit}", callback_data=f"productmgr|{row['_id']}"))
    raw_bot.send_message(m.from_user.id, "🛍 PRODUCT MANAGER\n\nTap a product to view/toggle stock.\n\nCommands:\n/productstatus NUMBER in|out\n/productduration NUMBER text\n/productwarranty NUMBER text", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("productmgr|"))
def product_manager_item_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    from bson import ObjectId
    row = folders_col.find_one({"_id": ObjectId(c.data.split("|",1)[1])})
    if not row:
        return bot.answer_callback_query(c.id, "Product not found", True)
    kb = InlineKeyboardMarkup(row_width=2)
    if _service_status(row) == "in_stock":
        kb.add(InlineKeyboardButton("❌ Set OUT OF STOCK", callback_data=f"productstock|out|{row['_id']}"))
    else:
        kb.add(InlineKeyboardButton("✅ Set IN STOCK", callback_data=f"productstock|in|{row['_id']}"))
    raw_bot.send_message(c.from_user.id, _service_card(row) + f"\n\nProduct #: {row.get('number')}", reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("productstock|"))
def product_stock_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    from bson import ObjectId
    _, mode, oid = c.data.split("|", 2)
    status = "in_stock" if mode == "in" else "out_of_stock"
    folders_col.update_one({"_id": ObjectId(oid)}, {"$set": {"stock_status": status}})
    bot.answer_callback_query(c.id, "IN STOCK" if status == "in_stock" else "OUT OF STOCK", True)


@bot.message_handler(commands=["productstatus", "productduration", "productwarranty"])
def product_edit_commands(m):
    if not is_admin(m.from_user.id):
        return
    try:
        parts = (m.text or "").split(maxsplit=2)
        if len(parts) < 3:
            raise ValueError("Usage: /productstatus NUMBER in|out, /productduration NUMBER text, /productwarranty NUMBER text")
        cmd = parts[0].lower()
        number = int(parts[1])
        value = parts[2].strip()
        row = folders_col.find_one({"number": number, "cat": {"$in": ["free_service", "paid_service"]}})
        if not row:
            raise ValueError("Product not found")
        if cmd == "/productstatus":
            status = _normalize_stock_status(value)
            folders_col.update_one({"_id": row["_id"]}, {"$set": {"stock_status": status}})
        elif cmd == "/productduration":
            folders_col.update_one({"_id": row["_id"]}, {"$set": {"duration": value}})
        else:
            folders_col.update_one({"_id": row["_id"]}, {"$set": {"warranty": value}})
        updated = folders_col.find_one({"_id": row["_id"]})
        raw_bot.send_message(m.from_user.id, "✅ PRODUCT UPDATED\n\n" + _service_card(updated), reply_markup=admin_menu())
    except Exception as exc:
        raw_bot.send_message(m.from_user.id, f"❌ {exc}", reply_markup=admin_menu())


# =========================
# 🔀 MOVE FOLDER
# =========================
_move_state = {}

def _method_select_keyboard(prefix, category=None, include_expired=True):
    query = {} if category is None else {"cat": category}
    if not include_expired:
        query["expired"] = {"$ne": True}
    rows = list(folders_col.find(query).sort([("pinned", -1), ("created_at", -1)]))
    kb = InlineKeyboardMarkup(row_width=1)
    for row in rows[:80]:
        label = f"{str(row.get('cat','')).upper()} • {row.get('name','Unnamed')}"
        if row.get("parent"):
            label += f" / {row.get('parent')}"
        kb.add(InlineKeyboardButton(label[:62], callback_data=f"{prefix}|{row['_id']}"))
    return kb

@bot.message_handler(func=lambda m: m.text == "🔀 Move Folder" and is_admin(m.from_user.id))
def move_folder_start(m):
    raw_bot.send_message(m.from_user.id, "🔀 MOVE METHOD / FOLDER\n\nSelect what you want to move:", reply_markup=_method_select_keyboard("moveselect"))

@bot.callback_query_handler(func=lambda c: c.data.startswith("moveselect|"))
def move_select_cb(c):
    if not is_admin(c.from_user.id): return
    from bson import ObjectId
    row = folders_col.find_one({"_id": ObjectId(c.data.split("|",1)[1])})
    if not row: return bot.answer_callback_query(c.id,"Not found",True)
    _move_state[c.from_user.id] = str(row["_id"])
    candidates = list(folders_col.find({"cat": row.get("cat"), "_id": {"$ne": row["_id"]}}).sort("name",1))
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🏠 Move to Main Level", callback_data="movedest|root"))
    for dest in candidates[:80]:
        kb.add(InlineKeyboardButton(f"📁 {dest.get('name','Unnamed')}"[:62], callback_data=f"movedest|{dest['_id']}"))
    raw_bot.send_message(c.from_user.id, f"Selected: {row.get('name')}\n\nChoose the destination:", reply_markup=kb)
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("movedest|"))
def move_dest_cb(c):
    if not is_admin(c.from_user.id): return
    from bson import ObjectId
    source_id = _move_state.pop(c.from_user.id, None)
    if not source_id: return bot.answer_callback_query(c.id,"Session expired",True)
    source = folders_col.find_one({"_id": ObjectId(source_id)})
    if not source: return bot.answer_callback_query(c.id,"Source not found",True)
    dest_value = c.data.split("|",1)[1]
    parent = None
    if dest_value != "root":
        dest = folders_col.find_one({"_id": ObjectId(dest_value)})
        if not dest: return bot.answer_callback_query(c.id,"Destination not found",True)
        parent = dest.get("name")
    folders_col.update_one({"_id": source["_id"]}, {"$set": {"parent": parent, "updated_at": now_ts()}})
    admin_success(c.from_user.id, f"Moved {source.get('name')} to {parent or 'Main Level'}")
    bot.answer_callback_query(c.id,"Moved")

@bot.message_handler(func=lambda m: m.text in ("🛑 Patch Method", "✅ Unpatch Method") and is_admin(m.from_user.id))
def expire_restore_menu(m):
    mode = "patch" if m.text.startswith("🛑") else "unpatch"
    query = {"patched": {"$ne": True}} if mode == "patch" else {"patched": True}
    rows = list(folders_col.find(query).sort([("cat", 1), ("pinned", -1), ("name", 1)]))
    kb = InlineKeyboardMarkup(row_width=1)
    for row in rows[:100]:
        icon = "⏳" if mode == "patch" else "✅"
        kb.add(InlineKeyboardButton(
            f"{icon} {str(row.get('cat', '')).upper()} • {row.get('name', 'Unnamed')}"[:62],
            callback_data=f"methodstatusconfirm|{mode}|{row['_id']}",
        ))
    raw_bot.send_message(
        m.from_user.id,
        ("⛔ Select the method to expire:" if mode == "patch" else "✅ Select the method to restore:")
        if rows else "No methods are available for this action.",
        reply_markup=kb if rows else None,
    )


@bot.callback_query_handler(func=lambda c: c.data.startswith("methodstatusconfirm|"))
def method_status_confirm_cb(c):
    if not is_admin(c.from_user.id):
        return
    from bson import ObjectId
    try:
        _, mode, oid = c.data.split("|", 2)
        row = folders_col.find_one({"_id": ObjectId(oid)})
        if not row:
            return bot.answer_callback_query(c.id, "Method not found", True)
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton(
                "⛔ Yes, Patch" if mode == "patch" else "✅ Yes, Unpatch",
                callback_data=f"methodstatusapply|{mode}|{oid}",
            ),
            InlineKeyboardButton("❌ Cancel", callback_data="methodstatuscancel"),
        )
        raw_bot.send_message(
            c.from_user.id,
            f"{'⛔ PATCH METHOD' if mode == 'expire' else '✅ UNPATCH METHOD'}\n\n{row.get('name')}\n\n"
            + ("Users will not be able to buy, open, or receive this method." if mode == "patch" else "Users will be able to access this method again."),
            reply_markup=kb,
        )
        bot.answer_callback_query(c.id)
    except Exception as exc:
        admin_error(c.from_user.id, exc)


@bot.callback_query_handler(func=lambda c: c.data.startswith("methodstatusapply|"))
def method_status_apply_cb(c):
    if not is_admin(c.from_user.id):
        return
    from bson import ObjectId
    try:
        _, mode, oid = c.data.split("|", 2)
        patched = mode == "patch"
        result = folders_col.update_one(
            {"_id": ObjectId(oid)},
            {"$set": {
                "patched": patched,
                "active": True,
                "updated_at": now_ts(),
                "patched_at": now_ts() if patched else None,
                "patched_by": c.from_user.id if patched else None,
            }},
        )
        if result.matched_count != 1:
            raise ValueError("Method was not found or could not be updated")
        row = folders_col.find_one({"_id": ObjectId(oid)})
        if not row or bool(row.get("patched")) != patched:
            raise ValueError("Expiry status verification failed")
        send_method_notification("patched" if patched else "unpatched", row)
        admin_success(c.from_user.id, f"{row.get('name')} {'patched' if patched else 'unpatched'} successfully")
        bot.answer_callback_query(c.id, "Updated")
    except Exception as exc:
        admin_error(c.from_user.id, exc)


@bot.callback_query_handler(func=lambda c: c.data == "methodstatuscancel")
def method_status_cancel_cb(c):
    if is_admin(c.from_user.id):
        bot.answer_callback_query(c.id, "Cancelled")
        raw_bot.send_message(c.from_user.id, "❌ Cancelled", reply_markup=admin_menu())

# =========================
# 🗂 FOLDER ACTION PICKER
# =========================
_folder_admin_state = {}

def folder_action_keyboard(action, page=0, per_page=20):
    rows = list(folders_col.find({}, {"number":1,"name":1,"cat":1,"parent":1,"price":1}).sort([("cat",1),("number",1)]))
    kb = InlineKeyboardMarkup(row_width=1)
    start = page * per_page
    for f in rows[start:start+per_page]:
        label = f"[{f.get('number','?')}] {str(f.get('cat','')).upper()} • {f.get('name')}"
        if f.get('parent'): label += f" / {f.get('parent')}"
        kb.add(InlineKeyboardButton(label[:60], callback_data=f"folderact|{action}|{f.get('number')}"))
    nav=[]
    if page>0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"folderpage|{action}|{page-1}"))
    if start+per_page<len(rows): nav.append(InlineKeyboardButton("➡️", callback_data=f"folderpage|{action}|{page+1}"))
    if nav: kb.row(*nav)
    return kb

def show_folder_action(uid, action, title):
    kb=folder_action_keyboard(action)
    if not kb.keyboard:
        return bot.send_message(uid,"❌ No methods/folders found.",reply_markup=admin_menu())
    bot.send_message(uid,title,reply_markup=kb,parse_mode="Markdown")

@bot.message_handler(func=lambda m: m.text == "🗑 Delete Folder" and is_admin(m.from_user.id))
def del_start(m): show_folder_action(m.from_user.id,"delete","🗑 **Select a method/folder to delete:**")

@bot.message_handler(func=lambda m: m.text == "✏️ Edit Price" and is_admin(m.from_user.id))
def edit_price_start(m): show_folder_action(m.from_user.id,"price","✏️ **Select a method/folder to edit price:**")

@bot.message_handler(func=lambda m: m.text == "✏️ Edit Name" and is_admin(m.from_user.id))
def edit_name_start(m): show_folder_action(m.from_user.id,"name","✏️ **Select a method/folder to rename:**")

@bot.message_handler(func=lambda m: m.text == "📝 Edit Content" and is_admin(m.from_user.id))
def edit_content_start(m): show_folder_action(m.from_user.id,"content","📝 **Select a method/folder to edit content:**")

@bot.callback_query_handler(func=lambda c:c.data.startswith("folderpage|"))
def folder_page_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    _,action,page=c.data.split("|")
    bot.edit_message_reply_markup(c.from_user.id,c.message.message_id,reply_markup=folder_action_keyboard(action,int(page)))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c:c.data.startswith("folderact|"))
def folder_action_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    try:
        _,action,num=c.data.split("|"); folder=fs.get_by_number(int(num))
        if not folder: raise ValueError("Folder not found")
        _folder_admin_state[c.from_user.id]={"action":action,"number":int(num)}
        if action=="delete":
            kb=InlineKeyboardMarkup(row_width=2)
            kb.add(InlineKeyboardButton("✅ Confirm Delete",callback_data=f"folderconfirm|delete|{num}"),InlineKeyboardButton("❌ Cancel",callback_data="folderconfirm|cancel|0"))
            bot.send_message(c.from_user.id,f"⚠️ Delete **[{num}] {folder['name']}** from **{folder['cat'].upper()}**?\nThis also deletes its subfolders.",reply_markup=kb,parse_mode="Markdown")
        elif action=="price":
            msg=bot.send_message(c.from_user.id,f"Current price: `{folder.get('price',0)}`\nSend the new price:",parse_mode="Markdown");bot.register_next_step_handler(msg,folder_price_step)
        elif action=="name":
            msg=bot.send_message(c.from_user.id,f"Current name: **{folder['name']}**\nSend the new name:",parse_mode="Markdown");bot.register_next_step_handler(msg,folder_name_step)
        else:
            edit_sessions[c.from_user.id]={"cat":folder['cat'],"name":folder['name'],"parent":folder.get('parent'),"number":int(num)}
            kb=InlineKeyboardMarkup(row_width=2);kb.add(InlineKeyboardButton("📝 Text",callback_data="edit_text"),InlineKeyboardButton("📁 Files",callback_data="edit_files"),InlineKeyboardButton("❌ Cancel",callback_data="edit_cancel"))
            bot.send_message(c.from_user.id,f"📝 **Edit [{num}] {folder['name']}**\nWhat do you want to update?",reply_markup=kb,parse_mode="Markdown")
        bot.answer_callback_query(c.id)
    except Exception as exc: bot.answer_callback_query(c.id,str(exc),True)

@bot.callback_query_handler(func=lambda c:c.data.startswith("folderconfirm|"))
def folder_confirm_cb(c):
    if not is_admin(c.from_user.id): return
    try:
        _,action,num=c.data.split("|")
        if action=="cancel":
            bot.edit_message_text("❌ Cancelled",c.from_user.id,c.message.message_id);return bot.answer_callback_query(c.id)
        folder=fs.get_by_number(int(num))
        if not folder: raise ValueError("Folder no longer exists")
        ok=fs.delete(folder['cat'],folder['name'],folder.get('parent'))
        if not ok: raise ValueError("Delete failed")
        bot.edit_message_text(f"✅ Process Complete\nDeleted: [{num}] {folder['name']}",c.from_user.id,c.message.message_id)
        bot.answer_callback_query(c.id,"Deleted")
    except Exception as exc: admin_error(c.from_user.id,exc);bot.answer_callback_query(c.id,"Failed",True)

def folder_price_step(m):
    try:
        st=_folder_admin_state.pop(m.from_user.id,None); folder=fs.get_by_number(st['number']) if st else None
        if not folder: raise ValueError("Session expired or folder missing")
        price=int((m.text or '').strip())
        if price<0: raise ValueError("Price cannot be negative")
        folders_col.update_one({"_id":folder["_id"]},{"$set":{"price":price}})
        folder['price']=price;send_method_notification("updated",folder);admin_success(m.from_user.id,f"Price updated to {price} points")
    except Exception as exc: admin_error(m.from_user.id,exc)

def folder_name_step(m):
    try:
        st=_folder_admin_state.pop(m.from_user.id,None); folder=fs.get_by_number(st['number']) if st else None
        if not folder: raise ValueError("Session expired or folder missing")
        new=(m.text or '').strip()
        if not new or len(new)>100: raise ValueError("Name must be 1-100 characters")
        old=folder['name']; folders_col.update_one({"_id":folder["_id"]},{"$set":{"name":new}});folders_col.update_many({"cat":folder['cat'],"parent":old},{"$set":{"parent":new}})
        folder['name']=new;send_method_notification("updated",folder);admin_success(m.from_user.id,f"Renamed to {new}")
    except Exception as exc: admin_error(m.from_user.id,exc)

edit_sessions = {}

@bot.callback_query_handler(func=lambda c: c.data == "edit_text")
def edit_text_cb(c):
    uid = c.from_user.id
    if uid not in edit_sessions:
        bot.answer_callback_query(c.id, "Session expired!")
        return
    
    s = edit_sessions[uid]
    folder = fs.get_one(s["cat"], s["name"])
    current = folder.get("text_content", "No content")[:200]
    msg = bot.send_message(uid, f"📝 **Current:**\n{current}\n\nSend NEW text:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, save_edit_text)
    bot.answer_callback_query(c.id)

def save_edit_text(m):
    uid = m.from_user.id
    if uid not in edit_sessions:
        bot.send_message(uid, "Session expired!", reply_markup=admin_menu())
        return
    
    s = edit_sessions[uid]
    fs.edit_content(s["cat"], s["name"], "text", m.text, s.get("parent"))
    folder=fs.get_by_number(s.get("number")) or fs.get_one(s["cat"],s["name"],s.get("parent")); send_method_notification("updated",folder or s)
    bot.send_message(uid, f"✅ Text updated!", reply_markup=admin_menu())
    edit_sessions.pop(uid, None)

@bot.callback_query_handler(func=lambda c: c.data == "edit_files")
def edit_files_cb(c):
    uid = c.from_user.id
    if uid not in edit_sessions:
        bot.answer_callback_query(c.id, "Session expired!")
        return
    
    edit_sessions[uid]["new_files"] = []
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add("/done", "/cancel")
    msg = bot.send_message(uid, "📁 Send NEW files\n/done when finished:", reply_markup=kb, parse_mode="Markdown")
    bot.register_next_step_handler(msg, process_edit_files)
    bot.answer_callback_query(c.id)

def process_edit_files(m):
    uid = m.from_user.id
    if m.text == "/cancel":
        edit_sessions.pop(uid, None)
        bot.send_message(uid, "❌ Cancelled", reply_markup=admin_menu())
        return
    
    if m.text == "/done":
        if uid not in edit_sessions:
            bot.send_message(uid, "Session expired!")
            return
        s = edit_sessions[uid]
        if not s.get("new_files"):
            bot.send_message(uid, "❌ No files!")
            return
        fs.edit_content(s["cat"], s["name"], "files", s["new_files"], s.get("parent"))
        folder=fs.get_by_number(s.get("number")) or fs.get_one(s["cat"],s["name"],s.get("parent")); send_method_notification("updated",folder or s)
        bot.send_message(uid, f"✅ {len(s['new_files'])} file(s) updated!", reply_markup=admin_menu())
        edit_sessions.pop(uid, None)
        return
    
    if m.content_type in ["document", "photo", "video"]:
        edit_sessions[uid]["new_files"].append({"chat": m.chat.id, "msg": m.message_id, "type": m.content_type})
        bot.send_message(uid, f"✅ Saved ({len(edit_sessions[uid]['new_files'])} files)")
    else:
        bot.send_message(uid, "❌ Send documents, photos, or videos!")
    bot.register_next_step_handler(m, process_edit_files)

@bot.callback_query_handler(func=lambda c: c.data == "edit_cancel")
def edit_cancel_cb(c):
    edit_sessions.pop(c.from_user.id, None)
    bot.edit_message_text("❌ Cancelled", c.from_user.id, c.message.message_id)
    bot.send_message(c.from_user.id, "Returning...", reply_markup=admin_menu())
    bot.answer_callback_query(c.id)

# =========================
# 👑 ADD VIP
# =========================
@bot.message_handler(func=lambda m: m.text == "👑 Add VIP" and is_admin(m.from_user.id))
def add_vip_start(m):
    msg = raw_bot.send_message(m.from_user.id, "👑 ADD VIP\n\nSend user ID or @username.")
    bot.register_next_step_handler(msg, add_vip_process)

def _parse_vip_duration_spec(raw):
    s=(raw or "").strip().lower().replace(" ","")
    if s in ("lifetime","life","permanent","forever","0"):
        return None, "Lifetime"
    mt=re.fullmatch(r"(\d+(?:\.\d+)?)(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days|w|week|weeks|mo|month|months|y|yr|yrs|year|years)",s)
    if not mt:
        raise ValueError("Use: 30m, 2h, 7d, 2w, 1mo, 1y, lifetime")
    value=float(mt.group(1)); unit=mt.group(2)
    mul={"m":60,"min":60,"mins":60,"minute":60,"minutes":60,"h":3600,"hr":3600,"hrs":3600,"hour":3600,"hours":3600,"d":86400,"day":86400,"days":86400,"w":604800,"week":604800,"weeks":604800,"mo":2592000,"month":2592000,"months":2592000,"y":31536000,"yr":31536000,"yrs":31536000,"year":31536000,"years":31536000}
    secs=int(value*mul[unit])
    if secs<=0: raise ValueError("Duration must be positive")
    return secs, (raw or "").strip()

def add_vip_process(m):
    try:
        inp=(m.text or "").strip()
        target=bot.get_chat(inp).id if inp.startswith("@") else int(inp)
        msg=raw_bot.send_message(m.from_user.id, f"User: {target}\n\nSend VIP interval. Examples: 30m, 2h, 7d, 2w, 1mo, 6mo, 1y, lifetime")
        bot.register_next_step_handler(msg, lambda x: add_vip_duration_process(x,int(target)))
    except Exception:
        raw_bot.send_message(m.from_user.id,"❌ Invalid user ID/username.",reply_markup=admin_menu())

def add_vip_duration_process(m,target):
    try:
        seconds,label=_parse_vip_duration_spec(m.text)
        sub,links=grant_custom_vip(int(target),seconds,label,added_by=m.from_user.id)
        raw_bot.send_message(m.from_user.id,f"✅ VIP granted to {target}\nInterval: {label}",reply_markup=admin_menu())
        _send_vip_welcome_bundle(int(target),sub,links)
    except Exception as exc:
        raw_bot.send_message(m.from_user.id,f"❌ {exc}",reply_markup=admin_menu())

# =========================
# 👑 REMOVE VIP
# =========================
@bot.message_handler(func=lambda m: m.text == "👑 Remove VIP" and is_admin(m.from_user.id))
def remove_vip_start(m):
    msg = bot.send_message(m.from_user.id, "👑 **Remove VIP**\n\nSend user ID or @username:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, remove_vip_process)

def remove_vip_process(m):
    inp = m.text.strip()
    if inp.startswith("@"):
        try:
            target = bot.get_chat(inp).id
        except:
            bot.send_message(m.from_user.id, "❌ User not found!")
            return
    else:
        try:
            target = int(inp)
        except:
            bot.send_message(m.from_user.id, "❌ Invalid ID!")
            return
    
    u = User(target)
    if not u.is_vip():
        bot.send_message(m.from_user.id, "⚠️ Not VIP!")
        return
    
    u.remove_vip()
    bot.send_message(m.from_user.id, f"✅ VIP removed from {target}!")
    try:
        bot.send_message(target, "⚠️ VIP status removed.", parse_mode="Markdown")
    except:
        pass

# =========================
# 💰 GIVE POINTS (FIXED - FULLY WORKING)
# =========================
@bot.message_handler(func=lambda m: m.text == "💰 Give Points" and is_admin(m.from_user.id))
def give_points_start(m):
    msg = bot.send_message(m.from_user.id, 
        "💰 **Give Points**\n\n"
        "Send: `user_id points`\n\n"
        "Example: `7712834912 200`\n\n"
        "*User must have started the bot first*",
        parse_mode="Markdown")
    bot.register_next_step_handler(msg, give_points_process)

def give_points_process(m):
    admin_id = m.from_user.id
    try:
        if not is_admin(admin_id):
            raise PermissionError("Admins only")
        parts = (m.text or "").strip().split()
        if len(parts) != 2:
            raise ValueError("Send exactly: user_id points")
        user_id_text, points_text = parts
        if not re.fullmatch(r"\\d{5,20}", user_id_text):
            raise ValueError("Invalid Telegram user ID")
        if not re.fullmatch(r"\\d{1,7}", points_text):
            raise ValueError("Points must contain digits only")
        user_id = int(user_id_text)
        amount = int(points_text)
        if not 1 <= amount <= 1_000_000:
            raise ValueError("Points must be between 1 and 1,000,000")

        reliable_users = users_col.with_options(write_concern=WriteConcern(w=1))
        before = reliable_users.find_one({"_id": str(user_id)})
        if not before:
            raise ValueError("User not found. Ask the user to send /start first")
        old_balance = int(before.get("points", 0) or 0)
        result = reliable_users.update_one(
            {"_id": str(user_id)},
            {"$inc": {"points": amount, "total_points_earned": amount}, "$set": {"last_active": time.time()}},
        )
        if result.matched_count != 1 or result.modified_count != 1:
            raise RuntimeError("Database did not update the user balance")
        after = reliable_users.find_one({"_id": str(user_id)}, {"points": 1, "username": 1}) or {}
        new_balance = int(after.get("points", old_balance + amount))

        for key in (user_id, str(user_id)):
            User._cache.pop(key, None)
            User._cache_time.pop(key, None)
        try:
            point_history_col.with_options(write_concern=WriteConcern(w=1)).insert_one({
                "user_id": str(user_id), "amount": amount, "reason": "manual_give_points",
                "admin_id": str(admin_id), "created_at": time.time(),
            })
        except Exception:
            pass

        username = after.get("username")
        user_label = f"@{username}" if username else str(user_id)
        raw_bot.send_message(
            admin_id,
            "✅  POINTS ADDED SUCCESSFULLY\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 User: {user_label}\n"
            f"🆔 ID: {user_id}\n"
            f"➕ Added: {amount:,} points\n"
            f"💰 Previous: {old_balance:,}\n"
            f"💎 New balance: {new_balance:,}",
            reply_markup=admin_menu(),
        )
        try:
            raw_bot.send_message(
                user_id,
                "🎉✨  CONGRATULATIONS!  ✨🎉\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💫 You have received {amount:,} points!\n\n"
                f"💰 Previous balance: {old_balance:,}\n"
                f"💎 New balance: {new_balance:,}\n\n"
                "🥳 Enjoy your reward and unlock more methods! 🚀",
                reply_markup=main_menu(user_id),
            )
        except Exception as notify_exc:
            raw_bot.send_message(admin_id, f"⚠️ Points were added, but notification failed: {notify_exc}")
    except Exception as exc:
        admin_error(admin_id, f"{exc}\n\nExample: 7712834912 200")

# =========================
# 🎫 GENERATE CODES (FIXED)
# =========================
@bot.message_handler(func=lambda m: m.text == "🎫 Generate Codes" and is_admin(m.from_user.id))
def gen_codes_start(m):
    msg = bot.send_message(
        m.from_user.id,
        "🎫 **Generate Codes**\n\n"
        "Send: `points count type expiry_days`\n\n"
        "Type: `single` or `multi`\n"
        "Expiry: `0` for no expiry\n\n"
        "Examples:\n"
        "`100 5 single 0`\n"
        "`250 10 multi 7`",
        parse_mode="Markdown",
    )
    bot.register_next_step_handler(msg, generate_codes_process)

def generate_codes_process(m):
    uid = m.from_user.id
    try:
        if not is_admin(uid):
            raise PermissionError("Admins only")
        parts = (m.text or "").strip().lower().split()
        if len(parts) != 4:
            raise ValueError("Send exactly: points count single|multi expiry_days")
        points, count, code_type, expiry_raw = int(parts[0]), int(parts[1]), parts[2], int(parts[3])
        if not 1 <= points <= 1_000_000:
            raise ValueError("Points must be between 1 and 1,000,000")
        if not 1 <= count <= 100:
            raise ValueError("Code count must be between 1 and 100")
        if code_type not in ("single", "multi"):
            raise ValueError("Type must be single or multi")
        if not 0 <= expiry_raw <= 3650:
            raise ValueError("Expiry must be between 0 and 3650 days")

        expiry = time.time() + expiry_raw * 86400 if expiry_raw else None
        reliable_codes = codes_col.with_options(write_concern=WriteConcern(w=1))
        generated = []
        for _ in range(count):
            for attempt in range(20):
                code = "GLOBEXOMART" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
                if not reliable_codes.find_one({"_id": code}, {"_id": 1}):
                    break
            doc = {
                "_id": code, "points": points, "used": False,
                "multi_use": code_type == "multi", "used_count": 0,
                "max_uses": 10 if code_type == "multi" else 1,
                "expiry": expiry, "created_at": time.time(), "used_by_users": [],
            }
            reliable_codes.insert_one(doc)
            generated.append(code)
        if len(generated) != count:
            raise RuntimeError("Some codes could not be created")

        header = (
            "✅  CODES GENERATED\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 Value: {points:,} points each\n"
            f"🎟 Quantity: {count}\n"
            f"🔁 Type: {code_type.upper()}\n"
            f"⏳ Expiry: {expiry_raw if expiry_raw else 'No expiry'}\n\n"
        )
        # Split safely to stay under Telegram's message limit.
        chunks, current = [], header
        for code in generated:
            line = code + "\n"
            if len(current) + len(line) > 3900:
                chunks.append(current.rstrip())
                current = line
            else:
                current += line
        if current.strip():
            chunks.append(current.rstrip())
        for i, chunk in enumerate(chunks):
            raw_bot.send_message(uid, chunk, reply_markup=admin_menu() if i == len(chunks)-1 else None)
    except Exception as exc:
        admin_error(uid, f"{exc}\n\nExample: 100 5 single 0")

# =========================
# 📊 VIEW CODES
# =========================
@bot.message_handler(func=lambda m: m.text == "📊 View Codes" and is_admin(m.from_user.id))
def view_codes(m):
    codes = codesys.get_all_codes()
    if not codes:
        bot.send_message(m.from_user.id, "📊 No codes!")
        return
    
    total, used, unused, multi = codesys.get_stats()
    text = f"📊 **Codes**\n\nTotal: {total}\nUsed: {used}\nUnused: {unused}\nMulti: {multi}\n\n"
    
    unused_codes = [c for c in codes if not c.get("used", False)][:5]
    if unused_codes:
        text += "**Recent:**\n"
        for c in unused_codes:
            text += f"• `{c['_id']}` - {c['points']} pts\n"
    
    bot.send_message(m.from_user.id, text, parse_mode="Markdown")

# =========================
# 📦 POINTS PACKAGES
# =========================
@bot.message_handler(func=lambda m: m.text == "📦 Points Packages" and is_admin(m.from_user.id))
def packages_cmd(m):
    pkgs = get_points_packages()
    text = "📦 **Points Packages**\n\n"
    for i, p in enumerate(pkgs, 1):
        status = "✅" if p.get("active", True) else "❌"
        text += f"{i}. {status} {p['points']} pts - ${p['price']}"
        if p.get("bonus", 0) > 0:
            text += f" (+{p['bonus']})"
        text += "\n"
    text += "\n/addpackage pts price bonus\n/editpackage num pts price bonus\n/togglepackage num\n/delpackage num"
    bot.send_message(m.from_user.id, text, parse_mode="Markdown")

@bot.message_handler(commands=["addpackage", "editpackage", "togglepackage", "delpackage"])
def pkg_commands(m):
    if not is_admin(m.from_user.id):
        return
    
    cmd = m.text.split()[0][1:]
    pkgs = get_points_packages()
    
    try:
        if cmd == "addpackage":
            _, pts, price, bonus = m.text.split()
            pkgs.append({"points": int(pts), "price": int(price), "bonus": int(bonus), "active": True})
            save_points_packages(pkgs)
            bot.send_message(m.from_user.id, f"✅ Added: {pts} pts for ${price}")
        elif cmd == "editpackage":
            _, num, pts, price, bonus = m.text.split()
            num = int(num) - 1
            if 0 <= num < len(pkgs):
                pkgs[num].update({"points": int(pts), "price": int(price), "bonus": int(bonus)})
                save_points_packages(pkgs)
                bot.send_message(m.from_user.id, f"✅ Package {num+1} updated!")
            else:
                bot.send_message(m.from_user.id, "❌ Invalid number!")
        elif cmd == "togglepackage":
            _, num = m.text.split()
            num = int(num) - 1
            if 0 <= num < len(pkgs):
                pkgs[num]["active"] = not pkgs[num].get("active", True)
                save_points_packages(pkgs)
                status = "activated" if pkgs[num]["active"] else "deactivated"
                bot.send_message(m.from_user.id, f"✅ Package {num+1} {status}!")
            else:
                bot.send_message(m.from_user.id, "❌ Invalid number!")
        elif cmd == "delpackage":
            _, num = m.text.split()
            num = int(num) - 1
            if 0 <= num < len(pkgs):
                removed = pkgs.pop(num)
                save_points_packages(pkgs)
                bot.send_message(m.from_user.id, f"✅ Removed: {removed['points']} pts")
            else:
                bot.send_message(m.from_user.id, "❌ Invalid number!")
    except:
        bot.send_message(m.from_user.id, f"❌ Use: /{cmd} ...")

# =========================
# 👥 ADMIN MANAGEMENT
# =========================
@bot.message_handler(func=lambda m: m.text == "👥 Admin Management" and is_admin(m.from_user.id))
def admin_management_cmd(m):
    if m.from_user.id != ADMIN_ID:
        bot.send_message(m.from_user.id, "❌ Owner only!")
        return
    
    admins = get_all_admins()
    text = "👥 **Admins**\n\n"
    for a in admins:
        owner = " 👑" if a["_id"] == ADMIN_ID else ""
        text += f"• `{a['_id']}`{owner}\n"
    text += "\n/addadmin id\n/removeadmin id\n/listadmins"
    bot.send_message(m.from_user.id, text, parse_mode="Markdown")

@bot.message_handler(commands=["addadmin", "removeadmin", "listadmins"])
def admin_commands(m):
    if m.from_user.id != ADMIN_ID:
        return
    
    cmd = m.text.split()[0][1:]
    
    if cmd == "listadmins":
        admins = get_all_admins()
        text = "👥 Admins:\n"
        for a in admins:
            text += f"• `{a['_id']}`\n"
        bot.send_message(m.from_user.id, text, parse_mode="Markdown")
        return
    
    try:
        _, uid = m.text.split()
        uid = int(uid)
        
        if cmd == "addadmin":
            if admins_col.find_one({"_id": uid}):
                bot.send_message(m.from_user.id, "❌ Already admin!")
                return
            admins_col.insert_one({"_id": uid, "added_at": time.time()})
            bot.send_message(m.from_user.id, f"✅ Admin {uid} added!")
            try:
                bot.send_message(uid, "🎉 You are now an admin!")
            except:
                pass
        else:
            if uid == ADMIN_ID:
                bot.send_message(m.from_user.id, "❌ Cannot remove owner!")
                return
            result = admins_col.delete_one({"_id": uid})
            if result.deleted_count > 0:
                bot.send_message(m.from_user.id, f"✅ Admin {uid} removed!")
            else:
                bot.send_message(m.from_user.id, "❌ Not an admin!")
    except:
        bot.send_message(m.from_user.id, f"❌ Use: /{cmd} user_id")

# =========================
# 📞 SET CONTACTS
# =========================
@bot.message_handler(func=lambda m: m.text == "📞 Set Contacts" and is_admin(m.from_user.id))
def set_contacts_menu(m):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("💰 Points Contact", callback_data="set_points"), InlineKeyboardButton("⭐ VIP Contact", callback_data="set_vip"), InlineKeyboardButton("📋 View", callback_data="view_contacts"))
    bot.send_message(m.from_user.id, "📞 **Contacts**", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "set_points")
def set_points_contact(c):
    msg = bot.send_message(c.from_user.id, "💰 Send @username or link:\nSend 'none' to remove", parse_mode="Markdown")
    bot.register_next_step_handler(msg, save_points_contact)
    bot.answer_callback_query(c.id)

def save_points_contact(m):
    if m.text.lower() == "none":
        set_config("contact_username", None)
        set_config("contact_link", None)
    elif m.text.startswith("http"):
        set_config("contact_link", m.text)
        set_config("contact_username", None)
    elif m.text.startswith("@"):
        set_config("contact_username", m.text)
        set_config("contact_link", None)
    else:
        bot.send_message(m.from_user.id, "❌ Invalid!")
        return
    bot.send_message(m.from_user.id, "✅ Updated!", reply_markup=admin_menu())

@bot.callback_query_handler(func=lambda c: c.data == "set_vip")
def set_vip_contact(c):
    msg = bot.send_message(c.from_user.id, "⭐ Send @username or link:\nSend 'none' to remove", parse_mode="Markdown")
    bot.register_next_step_handler(msg, save_vip_contact)
    bot.answer_callback_query(c.id)

def save_vip_contact(m):
    if m.text.lower() == "none":
        set_config("vip_contact", None)
    elif m.text.startswith("http") or m.text.startswith("@"):
        set_config("vip_contact", m.text)
    else:
        bot.send_message(m.from_user.id, "❌ Invalid!")
        return
    bot.send_message(m.from_user.id, "✅ Updated!", reply_markup=admin_menu())

@bot.callback_query_handler(func=lambda c: c.data == "view_contacts")
def view_contacts_cb(c):
    cfg = get_config()
    points = cfg.get("contact_username") or cfg.get("contact_link") or "Not set"
    vip = cfg.get("vip_contact") or "Not set"
    bot.edit_message_text(f"📞 Points: {points}\n⭐ VIP: {vip}", c.from_user.id, c.message.message_id, parse_mode="Markdown")
    bot.answer_callback_query(c.id)

# =========================
# 🔘 BUTTON MANAGER (BUTTON-BASED)
# =========================
_button_wizard = {}

def button_manager_keyboard():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("➕ Add Link Button", callback_data="btnmgr|add|link"),
        InlineKeyboardButton("📁 Add Folder Button", callback_data="btnmgr|add|folder"),
        InlineKeyboardButton("➖ Remove Button", callback_data="btnmgr|remove"),
        InlineKeyboardButton("📋 View Buttons", callback_data="btnmgr|view"),
    )
    kb.add(InlineKeyboardButton("❌ Close", callback_data="btnmgr|close"))
    return kb

@bot.message_handler(func=lambda m: m.text == "🔘 Button Manager" and is_admin(m.from_user.id))
def button_manager_cmd(m):
    bot.send_message(m.from_user.id, "🔘 **Button Manager**\n\nChoose an action:", reply_markup=button_manager_keyboard(), parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data.startswith("btnmgr|"))
def button_manager_callback(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    parts = c.data.split("|")
    action = parts[1]
    try:
        if action == "close":
            bot.delete_message(c.from_user.id, c.message.message_id)
            return bot.answer_callback_query(c.id)
        if action == "view":
            buttons = get_custom_buttons()
            text = "🔘 **Custom Buttons**\n\n" + ("\n".join(f"{i+1}. {b.get('text')} — {b.get('type')}" for i,b in enumerate(buttons)) if buttons else "No custom buttons.")
            bot.send_message(c.from_user.id, text, parse_mode="Markdown")
            return bot.answer_callback_query(c.id, "List opened")
        if action == "remove":
            buttons = get_custom_buttons()
            if not buttons:
                return bot.answer_callback_query(c.id, "No custom buttons", True)
            kb = InlineKeyboardMarkup(row_width=1)
            for i,b in enumerate(buttons):
                kb.add(InlineKeyboardButton(f"❌ {b.get('text')}", callback_data=f"btnmgr|delete|{i}"))
            bot.send_message(c.from_user.id, "Select a button to remove:", reply_markup=kb)
            return bot.answer_callback_query(c.id)
        if action == "delete":
            idx = int(parts[2]); buttons = get_custom_buttons()
            if idx < 0 or idx >= len(buttons): raise ValueError("Button no longer exists")
            removed = buttons.pop(idx); set_config("custom_buttons", buttons)
            bot.edit_message_text(f"✅ Process Complete\nRemoved: {removed.get('text')}", c.from_user.id, c.message.message_id)
            return bot.answer_callback_query(c.id, "Removed")
        if action == "add":
            typ = parts[2]
            _button_wizard[c.from_user.id] = {"type": typ}
            msg = bot.send_message(c.from_user.id, "Send the button name/text:")
            bot.register_next_step_handler(msg, button_name_step)
            return bot.answer_callback_query(c.id, "Continue in chat")
    except Exception as exc:
        bot.answer_callback_query(c.id, f"Error: {exc}", True)
        bot.send_message(c.from_user.id, f"❌ Process Failed\n{exc}", reply_markup=admin_menu())

def button_name_step(m):
    try:
        state = _button_wizard.get(m.from_user.id)
        if not state: raise ValueError("Session expired")
        text = (m.text or "").strip()
        if not text or len(text) > 50: raise ValueError("Button name must be 1-50 characters")
        state["text"] = text
        prompt = "Send link, @username, username, or t.me link:" if state["type"] == "link" else "Send the folder number:"
        msg = bot.send_message(m.from_user.id, prompt)
        bot.register_next_step_handler(msg, button_data_step)
    except Exception as exc:
        bot.send_message(m.from_user.id, f"❌ Process Failed\n{exc}", reply_markup=admin_menu())

def button_data_step(m):
    try:
        state = _button_wizard.pop(m.from_user.id, None)
        if not state: raise ValueError("Session expired")
        data = (m.text or "").strip()
        if state["type"] == "link":
            data = normalize_url_or_username(data)
        else:
            if not data.isdigit() or not fs.get_by_number(int(data)): raise ValueError("Folder number not found")
        add_custom_button(state["text"], state["type"], data)
        raw_bot.send_message(m.from_user.id, f"✅ Process Complete\nButton added: {state['text']}", reply_markup=admin_menu())
    except Exception as exc:
        bot.send_message(m.from_user.id, f"❌ Process Failed\n{exc}", reply_markup=admin_menu())

# =========================
# 📢 FORCE JOIN: CHANNELS + GROUPS
# =========================
def force_join_menu_keyboard():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("➕ Add Channel", callback_data="force|add|channel"),
        InlineKeyboardButton("➕ Add Group", callback_data="force|add|group"),
        InlineKeyboardButton("➖ Remove Channel", callback_data="force|remove|channel"),
        InlineKeyboardButton("➖ Remove Group", callback_data="force|remove|group"),
        InlineKeyboardButton("📋 View Required Chats", callback_data="force|view"),
    )
    kb.add(InlineKeyboardButton("❌ Close", callback_data="force|close"))
    return kb

@bot.message_handler(func=lambda m: m.text == "📢 Force Join" and is_admin(m.from_user.id))
def force_join_menu(m):
    bot.send_message(m.from_user.id, "📢 **Force Join Manager**\n\nFor private groups, use the numeric chat ID (`-100...`). The bot must be an admin.", reply_markup=force_join_menu_keyboard(), parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data.startswith("force|"))
def force_join_callback(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id, "Admin only", True)
    _, action, *rest = c.data.split("|")
    try:
        if action == "close":
            bot.delete_message(c.from_user.id, c.message.message_id); return bot.answer_callback_query(c.id)
        if action == "view":
            cfg=get_config(); channels=cfg.get("force_channels",[]); groups=cfg.get("force_groups",[])
            text="📢 **Required Channels**\n"+("\n".join(channels) if channels else "None")+"\n\n👥 **Required Groups**\n"+("\n".join(map(str,groups)) if groups else "None")
            bot.send_message(c.from_user.id,text,parse_mode="Markdown"); return bot.answer_callback_query(c.id,"Opened")
        typ=rest[0]
        key="force_channels" if typ=="channel" else "force_groups"
        if action=="add":
            _button_wizard[c.from_user.id]={"force_key":key,"force_type":typ}
            msg=bot.send_message(c.from_user.id, "Send @username or numeric chat ID (`-100...`):", parse_mode="Markdown")
            bot.register_next_step_handler(msg, force_add_step); return bot.answer_callback_query(c.id,"Continue in chat")
        if action=="remove":
            items=get_config().get(key,[])
            if not items:return bot.answer_callback_query(c.id,"Nothing to remove",True)
            kb=InlineKeyboardMarkup(row_width=1)
            for i,item in enumerate(items):kb.add(InlineKeyboardButton(f"❌ {item}",callback_data=f"force|delete|{typ}|{i}"))
            bot.send_message(c.from_user.id,"Select an item to remove:",reply_markup=kb); return bot.answer_callback_query(c.id)
        if action=="delete":
            typ,index=rest[0],int(rest[1]); key="force_channels" if typ=="channel" else "force_groups"; items=get_config().get(key,[])
            if index<0 or index>=len(items):raise ValueError("Item no longer exists")
            removed=items.pop(index);set_config(key,items);bot.edit_message_text(f"✅ Process Complete\nRemoved: {removed}",c.from_user.id,c.message.message_id);return bot.answer_callback_query(c.id,"Removed")
    except Exception as exc:
        bot.answer_callback_query(c.id,f"Error: {exc}",True);bot.send_message(c.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())

def force_add_step(m):
    try:
        state=_button_wizard.pop(m.from_user.id,None)
        if not state or "force_key" not in state:raise ValueError("Session expired")
        value=(m.text or "").strip()
        if not value.startswith("@"):
            try:value=str(int(value))
            except:raise ValueError("Use @username or numeric chat ID")
        # Verify bot can access the chat.
        chat=bot.get_chat(value)
        bot_member=bot.get_chat_member(chat.id,bot.get_me().id)
        if bot_member.status not in ("administrator","creator"):raise ValueError("Make the bot admin in that channel/group first")
        items=get_config().get(state["force_key"],[])
        normalized=str(chat.id) if value.lstrip("-").isdigit() else value
        if normalized in items:raise ValueError("Already added")
        items.append(normalized);set_config(state["force_key"],items)
        bot.send_message(m.from_user.id,f"✅ Process Complete\nAdded: {normalized}",reply_markup=admin_menu())
    except Exception as exc:
        bot.send_message(m.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())

# =========================
# 👥 NEW USER JOIN NOTIFICATIONS
# =========================
def join_notification_keyboard():
    cfg=get_cached_config(); kb=InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("➕ Set Join Group",callback_data="joinnotify|set"),InlineKeyboardButton("🗑 Remove Join Group",callback_data="joinnotify|remove"))
    kb.add(InlineKeyboardButton(f"👤 Join Alerts: {'ON' if cfg.get('join_notify_enabled',True) else 'OFF'}",callback_data="joinnotify|togglejoin"))
    kb.add(InlineKeyboardButton("➕ Set Method Group",callback_data="joinnotify|setmethod"),InlineKeyboardButton("🗑 Remove Method Group",callback_data="joinnotify|removemethod"))
    kb.add(InlineKeyboardButton(f"🔔 Method Alerts: {'ON' if cfg.get('method_notify_enabled',True) else 'OFF'}",callback_data="joinnotify|togglemethod"))
    kb.add(InlineKeyboardButton("📋 View Settings",callback_data="joinnotify|view"))
    return kb

@bot.message_handler(func=lambda m:m.text=="👥 Join Notifications" and is_admin(m.from_user.id))
def join_notification_menu(m):
    bot.send_message(m.from_user.id,"👥 **Notification Settings**\n\nAccepts @username, username, t.me link, or numeric ID. Bot must be admin.",reply_markup=join_notification_keyboard(),parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c:c.data.startswith("joinnotify|"))
def join_notification_cb(c):
    if not is_admin(c.from_user.id):return bot.answer_callback_query(c.id,"Admin only",True)
    action=c.data.split("|")[1]
    try:
        cfg=get_config()
        if action=="view":
            text=f"👤 Join group: `{cfg.get('join_notify_group') or 'Not set'}`\nJoin alerts: **{'ON' if cfg.get('join_notify_enabled',True) else 'OFF'}**\n\n🔔 Method group: `{cfg.get('method_notify_group') or cfg.get('join_notify_group') or 'Not set'}`\nMethod alerts: **{'ON' if cfg.get('method_notify_enabled',True) else 'OFF'}**"
            bot.send_message(c.from_user.id,text,parse_mode="Markdown",reply_markup=join_notification_keyboard());return bot.answer_callback_query(c.id)
        if action=="remove": set_config("join_notify_group",None);admin_success(c.from_user.id,"Join notification group removed");return bot.answer_callback_query(c.id,"Removed")
        if action=="removemethod": set_config("method_notify_group",None);admin_success(c.from_user.id,"Method notification group removed");return bot.answer_callback_query(c.id,"Removed")
        if action=="togglejoin": set_config("join_notify_enabled",not cfg.get("join_notify_enabled",True));bot.edit_message_reply_markup(c.from_user.id,c.message.message_id,reply_markup=join_notification_keyboard());return bot.answer_callback_query(c.id,"Updated")
        if action=="togglemethod": set_config("method_notify_enabled",not cfg.get("method_notify_enabled",True));bot.edit_message_reply_markup(c.from_user.id,c.message.message_id,reply_markup=join_notification_keyboard());return bot.answer_callback_query(c.id,"Updated")
        _join_notify_pending[c.from_user.id]="method" if action=="setmethod" else "join"
        msg=bot.send_message(c.from_user.id,"Send group @username, username, t.me link, or numeric ID:");bot.register_next_step_handler(msg,save_join_notification_group);bot.answer_callback_query(c.id,"Continue in chat")
    except Exception as exc: admin_error(c.from_user.id,exc)

_join_notify_pending={}
def save_join_notification_group(m):
    try:
        value=normalize_chat_reference(m.text); chat=bot.get_chat(value); member=bot.get_chat_member(chat.id,bot.get_me().id)
        if member.status not in ("administrator","creator"):raise ValueError("Bot must be admin in the group")
        kind=_join_notify_pending.pop(m.from_user.id,"join")
        key="method_notify_group" if kind=="method" else "join_notify_group"
        set_config(key,chat.id)
        if kind == "method":
            groups = get_config().get("method_notify_groups", [])
            if chat.id not in groups:
                groups.append(chat.id)
                set_config("method_notify_groups", groups)
        admin_success(m.from_user.id,f"{'Method' if kind=='method' else 'Join'} notification group set: {chat.id}")
        bot.send_message(chat.id,f"✅ This group will receive {'method upload/update' if kind=='method' else 'new-user join'} notifications.")
    except Exception as exc: admin_error(m.from_user.id,exc)

# =========================
# ⚙️ SETTINGS
# =========================
@bot.message_handler(func=lambda m: m.text == "⚙️ Settings" and is_admin(m.from_user.id))
def settings_cmd(m):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("⭐ VIP Msg", callback_data="set_vip_msg"), InlineKeyboardButton("🏠 Welcome", callback_data="set_welcome"), InlineKeyboardButton("💰 Ref Reward", callback_data="set_reward"), InlineKeyboardButton("💵 Points/$", callback_data="set_ppd"))
    bot.send_message(m.from_user.id, "⚙️ **Settings**", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "set_vip_msg")
def set_vip_msg_cb(c):
    msg = bot.send_message(c.from_user.id, "Send new VIP message:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: update_config("vip_msg", x.text) or bot.send_message(x.from_user.id, "✅ Updated!", reply_markup=admin_kb()))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_welcome")
def set_welcome_cb(c):
    msg = bot.send_message(c.from_user.id, "Send new welcome message:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: update_config("welcome", x.text) or bot.send_message(x.from_user.id, "✅ Updated!", reply_markup=admin_kb()))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_reward")
def set_reward_cb(c):
    current = get_config().get("ref_reward", 5)
    msg = bot.send_message(c.from_user.id, f"Current: {current}\nSend new amount:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: update_config("ref_reward", int(x.text)) or bot.send_message(x.from_user.id, f"✅ Set to {x.text} points!", reply_markup=admin_kb()) if x.text.isdigit() else bot.send_message(x.from_user.id, "❌ Invalid!"))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_ppd")
def set_ppd_cb(c):
    current = get_config().get("points_per_dollar", 100)
    msg = bot.send_message(c.from_user.id, f"Current: {current} pts = $1\nSend new value:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: update_config("points_per_dollar", int(x.text)) or bot.send_message(x.from_user.id, f"✅ Set to {x.text} pts = $1!", reply_markup=admin_kb()) if x.text.isdigit() else bot.send_message(x.from_user.id, "❌ Invalid!"))
    bot.answer_callback_query(c.id)

# =========================
# 📊 STATS (FIXED VIP COUNT)
# =========================
@bot.message_handler(func=lambda m: m.text == "📊 Stats" and is_admin(m.from_user.id))
def stats_cmd(m):
    total = users_col.count_documents({})
    vip = users_col.count_documents({"vip": True})
    free = total - vip
    
    all_u = list(users_col.find({}))
    points = sum(u.get("points", 0) for u in all_u)
    earned = sum(u.get("total_points_earned", 0) for u in all_u)
    spent = sum(u.get("total_points_spent", 0) for u in all_u)
    refs = sum(u.get("refs", 0) for u in all_u)
    purchases = sum(len(u.get("purchased_methods", [])) for u in all_u)
    
    free_f = folders_col.count_documents({"cat": "free"})
    vip_f = folders_col.count_documents({"cat": "vip"})
    apps_f = folders_col.count_documents({"cat": "apps"})
    svc_f = folders_col.count_documents({"cat": "services"})
    
    total_c, used_c, _, _ = codesys.get_stats()
    
    text = f"📊 **GLOBEXOMART STATISTICS**\n\n"
    text += f"👥 **USERS:**\n"
    text += f"┌ Total Users: `{total}`\n"
    text += f"├ VIP Users: `{vip}`\n"
    text += f"└ Free Users: `{free}`\n\n"
    
    text += f"💰 **POINTS:**\n"
    text += f"┌ Current Total: `{points:,}`\n"
    text += f"├ Total Earned: `{earned:,}`\n"
    text += f"├ Total Spent: `{spent:,}`\n"
    text += f"└ Avg per User: `{points//total if total > 0 else 0}`\n\n"
    
    text += f"📚 **CONTENT:**\n"
    text += f"┌ FREE METHODS: `{free_f}`\n"
    text += f"├ VIP METHODS: `{vip_f}`\n"
    text += f"├ PREMIUM APPS: `{apps_f}`\n"
    text += f"└ SERVICES: `{svc_f}`\n\n"
    
    text += f"📈 **ACTIVITY:**\n"
    text += f"┌ Total Referrals: `{refs}`\n"
    text += f"├ Total Purchases: `{purchases}`\n"
    text += f"├ Total Codes: `{total_c}`\n"
    text += f"├ Used Codes: `{used_c}`\n"
    text += f"└ Unused Codes: `{total_c - used_c}`"
    
    bot.send_message(m.from_user.id, text, parse_mode="Markdown")

# =========================
# 📢 BROADCAST
# =========================
@bot.message_handler(func=lambda m: m.text == "📢 Broadcast" and is_admin(m.from_user.id))
def broadcast_cmd(m):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("All", callback_data="bc_all"), InlineKeyboardButton("VIP", callback_data="bc_vip"), InlineKeyboardButton("Free", callback_data="bc_free"))
    bot.send_message(m.from_user.id, "📢 Broadcast to:", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("bc_"))
def broadcast_target_cb(c):
    target = c.data[3:]
    msg = bot.send_message(c.from_user.id, f"Send message to {target.upper()} users:", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: send_broadcast(x, target))
    bot.answer_callback_query(c.id)

def send_broadcast(m, target):
    # Copy the exact admin message instead of rebuilding it. Telegram copy_message
    # preserves message/caption entities, including Premium/custom emoji IDs,
    # formatting, links and media captions automatically.
    query = {}
    if target == "vip":
        query = {"vip": True}
    elif target == "free":
        query = {"vip": False}
    
    users = list(users_col.find(query))
    if not users:
        bot.send_message(m.from_user.id, "❌ No users!")
        return
    
    status = bot.send_message(m.from_user.id, f"📤 Broadcasting to {len(users)} users...")
    sent, failed = 0, 0
    
    for u in users:
        try:
            uid = int(u["_id"])
            # This is entity-safe: custom/premium emojis are preserved without
            # manually extracting or storing custom_emoji_id values.
            bot.copy_message(uid, m.chat.id, m.message_id)
            sent += 1
            if sent % 20 == 0:
                time.sleep(0.3)
        except:
            failed += 1
    
    bot.edit_message_text(f"✅ Done!\n📤 Sent: {sent}\n❌ Failed: {failed}", m.from_user.id, status.message_id)

# =========================
# 🔔 NOTIFY
# =========================
@bot.message_handler(commands=["legacy_notify_toggle_disabled"])
def toggle_notify_cmd(m):
    cfg=get_config(); new=not cfg.get("method_notify_enabled",True);set_config("method_notify_enabled",new)
    bot.send_message(m.from_user.id,f"🔔 Method upload/update notifications: {'ON' if new else 'OFF'}",reply_markup=admin_menu())

# =========================
# 🏦 BINANCE SETTINGS
# =========================
@bot.message_handler(func=lambda m: m.text == "🏦 Binance Settings" and is_admin(m.from_user.id))
def binance_settings_menu(m):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("💰 Coin", callback_data="set_binance_coin"), InlineKeyboardButton("🌐 Network", callback_data="set_binance_network"), InlineKeyboardButton("📍 Address", callback_data="set_binance_address"), InlineKeyboardButton("📝 Memo", callback_data="set_binance_memo"), InlineKeyboardButton("📋 View", callback_data="view_binance_settings"))
    bot.send_message(m.from_user.id, "🏦 **Binance**", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "set_binance_coin")
def set_binance_coin_cb(c):
    msg = bot.send_message(c.from_user.id, f"Coin (USDT, BUSD, BTC):\nCurrent: {get_config().get('binance_coin', 'USDT')}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("binance_coin", x.text.upper()) or bot.send_message(x.from_user.id, f"✅ Set to {x.text.upper()}", reply_markup=admin_menu()))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_binance_network")
def set_binance_network_cb(c):
    msg = bot.send_message(c.from_user.id, f"Network (TRC20, BEP20, ERC20):\nCurrent: {get_config().get('binance_network', 'TRC20')}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("binance_network", x.text.upper()) or bot.send_message(x.from_user.id, f"✅ Set to {x.text.upper()}", reply_markup=admin_menu()))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_binance_address")
def set_binance_address_cb(c):
    msg = bot.send_message(c.from_user.id, f"Address:\nCurrent: {get_config().get('binance_address', 'Not set')}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("binance_address", x.text) or bot.send_message(x.from_user.id, f"✅ Address saved!", reply_markup=admin_menu()))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_binance_memo")
def set_binance_memo_cb(c):
    msg = bot.send_message(c.from_user.id, f"Memo/Tag (send 'none' to clear):\nCurrent: {get_config().get('binance_memo', 'None')}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("binance_memo", "" if x.text.lower() == "none" else x.text) or bot.send_message(x.from_user.id, f"✅ Memo saved!", reply_markup=admin_menu()))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "view_binance_settings")
def view_binance_settings_cb(c):
    cfg = get_config()
    text = f"🏦 **Binance**\n\n💰 Coin: {cfg.get('binance_coin', 'USDT')}\n🌐 Network: {cfg.get('binance_network', 'TRC20')}\n📍 Address: `{cfg.get('binance_address', 'Not set')}`\n📝 Memo: `{cfg.get('binance_memo', 'None') or 'None'}`\n📸 Screenshot: {'Yes' if cfg.get('require_screenshot', True) else 'No'}"
    bot.edit_message_text(text, c.from_user.id, c.message.message_id, parse_mode="Markdown")
    bot.answer_callback_query(c.id)

# =========================
# 📸 SCREENSHOT
# =========================
@bot.message_handler(func=lambda m: m.text == "📸 Screenshot" and is_admin(m.from_user.id))
def screenshot_setting_menu(m):
    cfg = get_config()
    current = cfg.get("require_screenshot", True)
    status = "✅ ENABLED" if current else "❌ DISABLED"
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔘 Toggle", callback_data="toggle_screenshot"))
    bot.send_message(m.from_user.id, f"📸 **Screenshot**\n\n{status}\n\nRequire screenshot for payments.", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data == "toggle_screenshot")
def toggle_screenshot_cb(c):
    cfg = get_config()
    current = cfg.get("require_screenshot", True)
    set_config("require_screenshot", not current)
    new_status = "ENABLED" if not current else "DISABLED"
    bot.answer_callback_query(c.id, f"Screenshot {new_status}!")
    bot.edit_message_text(f"✅ Screenshot {new_status}!", c.from_user.id, c.message.message_id)
    bot.send_message(c.from_user.id, "Returning...", reply_markup=admin_menu())

# =========================
# 💳 PAYMENT METHODS
# =========================
@bot.message_handler(func=lambda m: m.text == "💳 Payment Methods" and is_admin(m.from_user.id))
def payment_methods_menu(m):
    methods = get_config().get("payment_methods", ["💳 Binance", "💵 USDT"])
    text = "💳 **Payment Methods**\n\n"
    for i, mtd in enumerate(methods, 1):
        text += f"{i}. {mtd}\n"
    text += "\n/addmethod name\n/removemethod number\n/listmethods"
    bot.send_message(m.from_user.id, text, parse_mode="Markdown")

@bot.message_handler(commands=["addmethod", "removemethod", "listmethods"])
def payment_commands(m):
    if not is_admin(m.from_user.id):
        return
    
    cmd = m.text.split()[0][1:]
    methods = get_config().get("payment_methods", ["💳 Binance", "💵 USDT"])
    
    if cmd == "listmethods":
        text = "💳 **Methods**\n\n"
        for i, mtd in enumerate(methods, 1):
            text += f"{i}. {mtd}\n"
        bot.send_message(m.from_user.id, text, parse_mode="Markdown")
        return
    
    try:
        if cmd == "addmethod":
            method = m.text.replace("/addmethod", "").strip()
            if not method:
                bot.send_message(m.from_user.id, "❌ Usage: /addmethod name")
                return
            methods.append(method)
            set_config("payment_methods", methods)
            bot.send_message(m.from_user.id, f"✅ Added: {method}")
        elif cmd == "removemethod":
            _, num = m.text.split()
            num = int(num) - 1
            if 0 <= num < len(methods):
                removed = methods.pop(num)
                set_config("payment_methods", methods)
                bot.send_message(m.from_user.id, f"✅ Removed: {removed}")
            else:
                bot.send_message(m.from_user.id, "❌ Invalid number!")
    except:
        bot.send_message(m.from_user.id, f"❌ Use: /{cmd} ...")

# =========================
# ⚙️ VIP SETTINGS
# =========================
@bot.message_handler(func=lambda m: m.text == "⚙️ VIP Settings" and is_admin(m.from_user.id))
def vip_settings_menu(m):
    kb = InlineKeyboardMarkup(row_width=2)
    for code, row in get_subscription_plans().items():
        kb.add(
            InlineKeyboardButton(f"✏️ {row.get('name',code)} Name", callback_data=f"vipplanname|{code}"),
            InlineKeyboardButton(f"💰 {row.get('name',code)} Price", callback_data=f"vipplanprice|{code}"),
        )
        kb.add(
            InlineKeyboardButton(f"⏱ {row.get('name',code)} Time", callback_data=f"vipplanduration|{code}"),
            InlineKeyboardButton(("🟢 " if row.get('active',True) else "⚪ ") + "Toggle", callback_data=f"vipplantoggle|{code}"),
        )
        kb.add(InlineKeyboardButton(f"🏷 {row.get('name',code)} Discount", callback_data=f"vipplandiscount|{code}"))
    kb.add(InlineKeyboardButton("➕ Add Plan / Trial", callback_data="vipplanadd"))
    kb.add(
        InlineKeyboardButton("🎁 Invite Points", callback_data="set_invite_points"),
        InlineKeyboardButton("⏳ Expiry Notice", callback_data="set_expiry_notice"),
    )
    kb.add(
        InlineKeyboardButton("👥 Referral VIP", callback_data="set_ref_vip_count"),
        InlineKeyboardButton("📋 View Plans", callback_data="view_vip_plans"),
    )
    kb.add(
        InlineKeyboardButton("📝 Buy VIP Message", callback_data="vipmsg|buy"),
        InlineKeyboardButton("📜 VIP Rules", callback_data="vipmsg|rules"),
    )
    kb.add(
        InlineKeyboardButton("🎁 Referral Settings", callback_data="refsettings|open"),
        InlineKeyboardButton("📈 VIP Analytics", callback_data="vipanalytics|open"),
    )
    bot.send_message(m.from_user.id, "⚙️ **VIP Settings**\n\nEdit each plan price and interval:", reply_markup=kb, parse_mode="Markdown")


@bot.callback_query_handler(func=lambda c: c.data.startswith("vipplanprice|"))
def vip_plan_price_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    code = c.data.split("|", 1)[1]
    plans = get_config().get("subscription_plans") or SUBSCRIPTION_PLANS
    current = (plans.get(code) or {}).get("price", 0)
    msg = raw_bot.send_message(c.from_user.id, f"Send new {code} price in USDT.\nCurrent: ${current}")
    bot.register_next_step_handler(msg, lambda m: vip_plan_price_save(m, code))
    bot.answer_callback_query(c.id)

def vip_plan_price_save(m, code):
    try:
        value = float(m.text)
        if value < 0:
            raise ValueError()
        cfg = get_config()
        plans = cfg.get("subscription_plans") or SUBSCRIPTION_PLANS.copy()
        row = dict(plans.get(code) or {})
        row["price"] = value
        row.setdefault("days", SUBSCRIPTION_PLANS.get(code, {}).get("days", 30))
        plans[code] = row
        set_config("subscription_plans", plans)
        raw_bot.send_message(m.chat.id, f"✅ {code} price set to ${value:g} USDT.", reply_markup=admin_menu())
    except Exception:
        raw_bot.send_message(m.chat.id, "❌ Invalid price.", reply_markup=admin_menu())

@bot.callback_query_handler(func=lambda c: c.data.startswith("vipplandays|"))
def vip_plan_days_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    code = c.data.split("|", 1)[1]
    plans = get_config().get("subscription_plans") or SUBSCRIPTION_PLANS
    current = (plans.get(code) or {}).get("days", 0)
    msg = raw_bot.send_message(c.from_user.id, f"Send new {code} interval in days.\nCurrent: {current}")
    bot.register_next_step_handler(msg, lambda m: vip_plan_days_save(m, code))
    bot.answer_callback_query(c.id)

def vip_plan_days_save(m, code):
    try:
        value = int(m.text)
        if value <= 0:
            raise ValueError()
        cfg = get_config()
        plans = cfg.get("subscription_plans") or SUBSCRIPTION_PLANS.copy()
        row = dict(plans.get(code) or {})
        row["days"] = value
        row.setdefault("price", SUBSCRIPTION_PLANS.get(code, {}).get("price", 0))
        plans[code] = row
        set_config("subscription_plans", plans)
        raw_bot.send_message(m.chat.id, f"✅ {code} interval set to {value} days.", reply_markup=admin_menu())
    except Exception:
        raw_bot.send_message(m.chat.id, "❌ Invalid day interval.", reply_markup=admin_menu())

@bot.callback_query_handler(func=lambda c: c.data.startswith("vipplanduration|"))
def vip_plan_duration_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    code = c.data.split("|", 1)[1]
    row = get_subscription_plans().get(code, {})
    current = _format_duration_minutes(row.get("duration_minutes", 1440))
    msg = raw_bot.send_message(c.from_user.id, f"Send new duration for {row.get('name', code)}.\nCurrent: {current}\n\nExamples: 30m, 2h, 7d, 1440m")
    bot.register_next_step_handler(msg, lambda m: vip_plan_duration_save(m, code))
    bot.answer_callback_query(c.id)


def _parse_duration_minutes(value):
    text = str(value or "").strip().lower().replace(" ", "")
    match = re.fullmatch(r"(\d+)(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)?", text)
    if not match:
        raise ValueError("Use a duration like 30m, 2h, or 7d")
    amount = int(match.group(1))
    unit = match.group(2) or "m"
    if amount < 1:
        raise ValueError("Duration must be at least 1 minute")
    if unit.startswith("h"):
        return amount * 60
    if unit.startswith("d"):
        return amount * 1440
    return amount


def vip_plan_duration_save(m, code):
    try:
        minutes = _parse_duration_minutes(m.text)
        plans = get_config().get("subscription_plans") or {}
        row = dict(plans.get(code) or get_subscription_plans().get(code) or {"price": 0})
        row["duration_minutes"] = minutes
        row["days"] = minutes / 1440.0
        plans[code] = row
        set_config("subscription_plans", plans)
        raw_bot.send_message(m.chat.id, f"✅ {row.get('name', code)} duration set to {_format_duration_minutes(minutes)}.", reply_markup=admin_menu())
    except Exception as exc:
        raw_bot.send_message(m.chat.id, f"❌ {exc}", reply_markup=admin_menu())


@bot.callback_query_handler(func=lambda c: c.data.startswith("vipplandiscount|"))
def vip_plan_discount_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    code = c.data.split("|", 1)[1]
    row = get_subscription_plans().get(code, {})
    msg = raw_bot.send_message(c.from_user.id, f"Send discount % for {row.get('name', code)} (0-100).\nCurrent: {row.get('discount_percent', 0):g}%")
    bot.register_next_step_handler(msg, lambda m: vip_plan_discount_save(m, code))
    bot.answer_callback_query(c.id)


def vip_plan_discount_save(m, code):
    try:
        pct = float(m.text)
        if pct < 0 or pct > 100:
            raise ValueError("Discount must be 0-100")
        plans = get_config().get("subscription_plans") or {}
        row = dict(plans.get(code) or get_subscription_plans().get(code) or {})
        row["discount_percent"] = pct
        plans[code] = row
        set_config("subscription_plans", plans)
        msg = raw_bot.send_message(m.chat.id, f"✅ {row.get('name', code)} discount set to {pct:g}%.\n\nNow send the discount broadcast message, or type SKIP.")
        bot.register_next_step_handler(msg, lambda x: vip_plan_discount_broadcast_step(x, code))
    except Exception as exc:
        raw_bot.send_message(m.chat.id, f"❌ {exc}", reply_markup=admin_menu())


def vip_plan_discount_broadcast_step(m, code):
    text = (m.text or "").strip()
    if text.upper() == "SKIP":
        return raw_bot.send_message(m.chat.id, "✅ Discount saved without broadcast.", reply_markup=admin_menu())
    row = get_subscription_plans().get(code, {})
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(f"🛒 Buy {row.get('name', code)} Now", callback_data=f"subplan|{code}"))
    sent = 0
    for u in users_col.find({}, {"_id": 1}):
        try:
            raw_bot.send_message(int(u["_id"]), text, reply_markup=kb)
            sent += 1
            time.sleep(0.02)
        except Exception:
            pass
    raw_bot.send_message(m.chat.id, f"✅ Discount broadcast sent to {sent} users.", reply_markup=admin_menu())


@bot.callback_query_handler(func=lambda c: c.data == "set_invite_points")
def set_invite_points_cb(c):
    if not is_admin(c.from_user.id):
        return
    msg = raw_bot.send_message(c.from_user.id, f"Points awarded per verified invite.\nCurrent: {get_config().get('ref_reward',5)}")
    bot.register_next_step_handler(msg, save_invite_points)
    bot.answer_callback_query(c.id)

def save_invite_points(m):
    try:
        value = int(m.text)
        if value < 0: raise ValueError()
        set_config("ref_reward", value)
        raw_bot.send_message(m.chat.id, f"✅ Invite reward set to {value} points.", reply_markup=admin_menu())
    except Exception:
        raw_bot.send_message(m.chat.id, "❌ Invalid points.", reply_markup=admin_menu())

@bot.callback_query_handler(func=lambda c: c.data == "set_expiry_notice")
def set_expiry_notice_cb(c):
    if not is_admin(c.from_user.id):
        return
    msg = raw_bot.send_message(c.from_user.id, f"Start VIP expiry reminders this many days before expiry.\nCurrent: {get_config().get('vip_expiry_notice_days',5)}")
    bot.register_next_step_handler(msg, save_expiry_notice)
    bot.answer_callback_query(c.id)

def save_expiry_notice(m):
    try:
        value = int(m.text)
        if value < 1: raise ValueError()
        set_config("vip_expiry_notice_days", value)
        raw_bot.send_message(m.chat.id, f"✅ Expiry reminders start {value} days before expiry and repeat daily.", reply_markup=admin_menu())
    except Exception:
        raw_bot.send_message(m.chat.id, "❌ Invalid number of days.", reply_markup=admin_menu())

@bot.callback_query_handler(func=lambda c: c.data == "view_vip_plans")
def view_vip_plans_cb(c):
    if not is_admin(c.from_user.id):
        return
    plans = get_subscription_plans()
    lines = ["⚙️ VIP PLANS"]
    for code, row in plans.items():
        lines.append(f"{row.get('name',code)} [{code}]: ${row['price']:g} USDT • {_format_duration_minutes(row.get('duration_minutes', 1440))} • Discount {row.get('discount_percent',0):g}% • {'ON' if row.get('active',True) else 'OFF'}")
    lines.append(f"Invite reward: {get_config().get('ref_reward',5)} points")
    lines.append(f"Expiry reminders: {get_config().get('vip_expiry_notice_days',5)} days before expiry")
    raw_bot.send_message(c.from_user.id, "\\n".join(lines))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_vip_price_usd")
def set_vip_price_usd_cb(c):
    msg = bot.send_message(c.from_user.id, f"USD Price:\nCurrent: ${get_config().get('vip_price', 50)}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("vip_price", int(x.text)) or bot.send_message(x.from_user.id, f"✅ Set to ${x.text}", reply_markup=admin_menu()) if x.text.isdigit() else bot.send_message(x.from_user.id, "❌ Invalid!"))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_vip_price_points")
def set_vip_price_points_cb(c):
    msg = bot.send_message(c.from_user.id, f"Points Price:\nCurrent: {get_config().get('vip_points_price', 5000)}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("vip_points_price", int(x.text)) or bot.send_message(x.from_user.id, f"✅ Set to {x.text} points", reply_markup=admin_menu()) if x.text.isdigit() else bot.send_message(x.from_user.id, "❌ Invalid!"))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_ref_vip_count")
def set_ref_vip_count_cb(c):
    msg = bot.send_message(c.from_user.id, f"Referrals for VIP:\nCurrent: {get_config().get('referral_vip_count', 50)}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("referral_vip_count", int(x.text)) or bot.send_message(x.from_user.id, f"✅ Set to {x.text} referrals", reply_markup=admin_menu()) if x.text.isdigit() else bot.send_message(x.from_user.id, "❌ Invalid!"))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_ref_purchase_count")
def set_ref_purchase_count_cb(c):
    msg = bot.send_message(c.from_user.id, f"Referral Purchases for VIP:\nCurrent: {get_config().get('referral_purchase_count', 10)}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("referral_purchase_count", int(x.text)) or bot.send_message(x.from_user.id, f"✅ Set to {x.text} purchases", reply_markup=admin_menu()) if x.text.isdigit() else bot.send_message(x.from_user.id, "❌ Invalid!"))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "set_vip_duration")
def set_vip_duration_cb(c):
    msg = bot.send_message(c.from_user.id, f"VIP Duration (days, 0 = permanent):\nCurrent: {get_config().get('vip_duration_days', 30)}", parse_mode="Markdown")
    bot.register_next_step_handler(msg, lambda x: set_config("vip_duration_days", int(x.text)) or bot.send_message(x.from_user.id, f"✅ Set to {x.text} days" + (" (permanent)" if int(x.text) == 0 else ""), reply_markup=admin_menu()) if x.text.isdigit() else bot.send_message(x.from_user.id, "❌ Invalid!"))
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "view_vip_settings")
def view_vip_settings_cb(c):
    cfg = get_config()
    text = f"📋 **VIP Settings**\n\n💰 USD: ${cfg.get('vip_price', 50)}\n💎 Points: {cfg.get('vip_points_price', 5000)}\n👥 Referrals: {cfg.get('referral_vip_count', 50)}\n🛒 Purchases: {cfg.get('referral_purchase_count', 10)}\n📅 Duration: {cfg.get('vip_duration_days', 30)} days" + (" (permanent)" if cfg.get('vip_duration_days', 30) == 0 else "")
    bot.edit_message_text(text, c.from_user.id, c.message.message_id, parse_mode="Markdown")
    bot.answer_callback_query(c.id)

# =========================
# 🔗 ADD CUSTOM LINK
# =========================
@bot.message_handler(func=lambda m: m.text == "🔗 Add Custom Link" and is_admin(m.from_user.id))
def add_custom_link_cmd(m):
    msg = bot.send_message(m.from_user.id, "🔗 **Add Link**\n\nSend: `text|url`\nExample: `Website|https://example.com`", parse_mode="Markdown")
    bot.register_next_step_handler(msg, add_custom_link_process)

def add_custom_link_process(m):
    try:
        parts = m.text.split("|")
        if len(parts) != 2:
            bot.send_message(m.from_user.id, "❌ Use: text|url")
            return
        text, url = parts[0].strip(), parts[1].strip()
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        add_custom_button(text, "link", url)
        bot.send_message(m.from_user.id, f"✅ Added: {text}", reply_markup=admin_menu())
    except:
        bot.send_message(m.from_user.id, "❌ Invalid format!")

# =========================
# 📋 VIEW LINKS
# =========================
@bot.message_handler(func=lambda m: m.text == "📋 View Links" and is_admin(m.from_user.id))
def view_links_cmd(m):
    btns = get_custom_buttons()
    if not btns:
        bot.send_message(m.from_user.id, "📋 No buttons!")
        return
    text = "📋 **Buttons**\n\n"
    for i, b in enumerate(btns, 1):
        text += f"{i}. {b['text']} ({b['type']})\n"
    bot.send_message(m.from_user.id, text, parse_mode="Markdown")


# =========================
# 🧩 COMPLETE UPDATE EXTENSIONS
# =========================
TZ_OFFSET_SECONDS = 5 * 3600  # Asia/Karachi
_scheduler_stop = threading.Event()

def now_ts():
    return time.time()

def log_event(action, actor=None, target=None, details=None, level="info"):
    try:
        logs_col.insert_one({"action": action, "actor": str(actor) if actor is not None else None,
                             "target": str(target) if target is not None else None,
                             "details": details or {}, "level": level, "created_at": now_ts()})
    except Exception:
        pass

def done(uid, extra=""):
    bot.send_message(uid, "✅ Done Successfully" + (f"\n{extra}" if extra else ""), reply_markup=admin_menu())

def safe_admin(handler):
    @wraps(handler)
    def wrapped(m, *a, **kw):
        if not is_admin(m.from_user.id): return
        try: return handler(m, *a, **kw)
        except Exception as exc:
            log_event("admin_error", m.from_user.id, details={"error": str(exc), "trace": traceback.format_exc()}, level="error")
            bot.send_message(m.from_user.id, f"❌ {type(exc).__name__}: {exc}")
    return wrapped

def add_point_history(uid, amount, reason, admin_id=None, note=None):
    point_history_col.insert_one({"user_id": str(uid), "amount": int(amount), "reason": reason,
                                  "admin_id": str(admin_id) if admin_id else None, "note": note,
                                  "created_at": now_ts()})
    log_event("points_adjusted", admin_id, uid, {"amount": amount, "reason": reason, "note": note})

def atomic_adjust_points(uid, amount, reason="manual", admin_id=None, note=None):
    uid = str(uid)
    if amount < 0:
        doc = users_col.find_one_and_update({"_id": uid, "points": {"$gte": abs(amount)}},
            {"$inc": {"points": amount, "total_points_spent": abs(amount)}, "$set": {"last_active": now_ts()}},
            return_document=ReturnDocument.AFTER)
    else:
        doc = users_col.find_one_and_update({"_id": uid},
            {"$inc": {"points": amount, "total_points_earned": amount}, "$set": {"last_active": now_ts()}},
            return_document=ReturnDocument.AFTER)
    if doc:
        User._cache.pop(uid, None); User._cache_time.pop(uid, None)
        add_point_history(uid, amount, reason, admin_id, note)
    return doc



@bot.message_handler(func=lambda m: m.text == "💾 Backup/Export" and is_admin(m.from_user.id))
def backup_export_menu(m):
    bot.send_message(m.from_user.id,"💾 **Backup / Export**\n\n`/backup`\n`/export users`\n`/export vip`\n`/export referrals`\n`/export purchases`\n`/export payments`",parse_mode="Markdown")

def send_json_document(uid, name, data):
    raw=json.dumps(data,default=str,ensure_ascii=False,indent=2).encode(); f=io.BytesIO(raw); f.name=name; bot.send_document(uid,f)

@bot.message_handler(commands=["backup","export"])
def backup_export_commands(m):
    if not is_admin(m.from_user.id): return
    try:
        if m.text.startswith('/backup'):
            payload={n:list(db[n].find({})) for n in db.list_collection_names()}; raw=json.dumps(payload,default=str,ensure_ascii=False).encode()
            z=io.BytesIO();
            with zipfile.ZipFile(z,'w',zipfile.ZIP_DEFLATED) as zz: zz.writestr('globexomart_backup.json',raw)
            z.seek(0); z.name=f"globexomart_backup_{datetime.now().strftime('%Y%m%d_%H%M')}.zip"; bot.send_document(m.from_user.id,z); log_event('backup',m.from_user.id)
        else:
            kind=(m.text.split(maxsplit=1)[1] if len(m.text.split(maxsplit=1))>1 else 'users').lower()
            mapping={'users':(users_col,{}),'vip':(users_col,{'vip':True}),'referrals':(users_col,{'ref':{'$ne':None}}),'purchases':(purchases_col,{}),'payments':(payments_col,{})}
            col,q=mapping.get(kind,mapping['users']); send_json_document(m.from_user.id,f'{kind}.json',list(col.find(q)))
    except Exception as exc: bot.send_message(m.from_user.id,f"❌ Export failed: {exc}")



@bot.message_handler(func=lambda m: m.text == "📣 Auto Posts" and is_admin(m.from_user.id))
def auto_posts_menu(m):
    bot.send_message(m.from_user.id,"📣 **Auto Posts Manager**\n\nChoose an action:",reply_markup=auto_posts_keyboard(),parse_mode="Markdown")

def auto_posts_keyboard():
    kb=InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("⏱ Every Hours",callback_data="autoui|create|hours"),InlineKeyboardButton("🗓 Daily Time",callback_data="autoui|create|daily"),InlineKeyboardButton("📋 List Posts",callback_data="autoui|list"),InlineKeyboardButton("⏸ Pause",callback_data="autoui|manage|pause"),InlineKeyboardButton("▶️ Resume",callback_data="autoui|manage|resume"),InlineKeyboardButton("🗑 Delete",callback_data="autoui|manage|delete"))
    return kb

_pending_auto={}

def auto_post_item_keyboard(action):
    rows=list(auto_posts_col.find({}).sort("created_at",-1).limit(30));kb=InlineKeyboardMarkup(row_width=1)
    for x in rows:
        label=f"{len(x.get('channels') or [x.get('channel')])} chat(s) | {x.get('schedule')} {x.get('value')} | {'ON' if x.get('active') else 'OFF'}"
        kb.add(InlineKeyboardButton(label,callback_data=f"autoui|do|{action}|{x['_id']}"))
    return kb

@bot.callback_query_handler(func=lambda c:c.data.startswith("autoui|"))
def auto_ui_callback(c):
    if not is_admin(c.from_user.id):return bot.answer_callback_query(c.id,"Admin only",True)
    parts=c.data.split("|");action=parts[1]
    try:
        if action=="list":
            rows=list(auto_posts_col.find({}).sort("created_at",-1).limit(30));text="📋 **Auto Posts**\n\n"+("\n".join(f"`{x['_id']}`\n{x.get('channel')} — {x.get('schedule')} {x.get('value')} — {'ON' if x.get('active') else 'OFF'}" for x in rows) if rows else "No auto posts.")
            bot.send_message(c.from_user.id,text,parse_mode="Markdown");return bot.answer_callback_query(c.id)
        if action=="manage":
            manage=parts[2];kb=auto_post_item_keyboard(manage)
            if not kb.keyboard:return bot.answer_callback_query(c.id,"No auto posts",True)
            bot.send_message(c.from_user.id,f"Select auto post to {manage}:",reply_markup=kb);return bot.answer_callback_query(c.id)
        if action=="do":
            from bson import ObjectId
            manage,oid=parts[2],ObjectId(parts[3])
            if manage in ("pause","resume"):
                result=auto_posts_col.update_one({"_id":oid},{"$set":{"active":manage=="resume"}})
            else:result=auto_posts_col.delete_one({"_id":oid})
            if not result.modified_count and not getattr(result,"deleted_count",0):raise ValueError("Auto post not found or unchanged")
            bot.edit_message_text(f"✅ Process Complete\nAuto post {manage}d.",c.from_user.id,c.message.message_id);return bot.answer_callback_query(c.id,"Done")
        if action=="create":
            mode=parts[2];_pending_auto[c.from_user.id]={"schedule":"every_hours" if mode=="hours" else "daily"}
            msg=bot.send_message(c.from_user.id,"Send one or multiple target channels/groups. Separate them with commas or new lines.\n\nExamples:\n`@channel1, @channel2`\n`-1001234567890`",parse_mode="Markdown");bot.register_next_step_handler(msg,auto_target_step);return bot.answer_callback_query(c.id,"Continue in chat")
    except Exception as exc:bot.answer_callback_query(c.id,f"Error: {exc}",True);bot.send_message(c.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())

def auto_target_step(m):
    try:
        state = _pending_auto.get(m.from_user.id)
        if not state:
            raise ValueError("Session expired. Open Auto Posts again")
        raw = (m.text or "").strip()
        refs = [x.strip() for x in re.split(r"[,\n]+", raw) if x.strip()]
        if not refs:
            raise ValueError("Send at least one channel or group")
        targets = []
        for ref in refs:
            target = normalize_chat_reference(ref)
            chat = bot.get_chat(target)
            targets.append(int(chat.id))
        state["channels"] = list(dict.fromkeys(targets))
        state["channel"] = state["channels"][0]  # backward compatibility
        prompt = "Send interval in hours, for example `2`:" if state["schedule"] == "every_hours" else "Send daily time as `HH:MM`:"
        msg = bot.send_message(m.from_user.id, prompt, parse_mode="Markdown")
        bot.register_next_step_handler(msg, auto_schedule_step)
    except Exception as exc:
        _pending_auto.pop(m.from_user.id, None)
        bot.send_message(m.from_user.id, f"❌ Process Failed\n{exc}", reply_markup=admin_menu())

def auto_schedule_step(m):
    try:
        state=_pending_auto.get(m.from_user.id);value=(m.text or "").strip()
        if state["schedule"]=="every_hours":
            if float(value)<=0:raise ValueError("Hours must be greater than 0")
        else:
            hh,mm=map(int,value.split(":"));
            if not(0<=hh<=23 and 0<=mm<=59):raise ValueError("Time must be HH:MM")
        state["value"]=value
        msg=bot.send_message(m.from_user.id,"Now send or forward the post content:")
        bot.register_next_step_handler(msg,auto_content_step)
    except Exception as exc:_pending_auto.pop(m.from_user.id,None);bot.send_message(m.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())


def normalize_post_button_url(value):
    value = (value or "").strip()
    if not value:
        raise ValueError("Button link is empty")
    if value.startswith("@"):
        return "https://t.me/" + value[1:]
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", value):
        return "https://t.me/" + value
    if value.startswith("t.me/"):
        return "https://" + value
    if value.startswith("telegram.me/"):
        return "https://" + value
    if value.startswith(("http://", "https://", "tg://")):
        return value
    raise ValueError("Use @username, username, t.me link, or full https:// link")

def auto_content_step(m):
    try:
        state = _pending_auto.get(m.from_user.id)
        if not state:
            raise ValueError("Session expired. Open Auto Posts again")
        state["payload"] = _message_payload(m)
        msg = bot.send_message(
            m.from_user.id,
            "Add a button below this post?\n\nSend: `Button Name | link-or-username`\nExample: `Join Channel | @globexomartprime1`\n\nSend `skip` for no button.",
            parse_mode="Markdown",
        )
        bot.register_next_step_handler(msg, save_auto_post)
    except Exception as exc:
        _pending_auto.pop(m.from_user.id, None)
        admin_error(m.from_user.id, exc)

def _message_payload(m):
    """Store the original Telegram message so formatting/entities are preserved.

    Copying the source message avoids Markdown parse errors in long posts and keeps
    link previews, custom entities, captions and media exactly as the admin sent them.
    Legacy fields are also stored as a fallback for older deployments/records.
    """
    payload = {
        "content_type": m.content_type,
        "source_chat": m.chat.id,
        "source_message": m.message_id,
    }
    if m.content_type == "text":
        payload["text"] = m.text or ""
    elif m.content_type == "photo":
        payload.update({"file_id": m.photo[-1].file_id, "caption": m.caption or ""})
    elif m.content_type == "video":
        payload.update({"file_id": m.video.file_id, "caption": m.caption or ""})
    elif m.content_type == "document":
        payload.update({"file_id": m.document.file_id, "caption": m.caption or ""})
    elif m.content_type == "animation":
        payload.update({"file_id": m.animation.file_id, "caption": m.caption or ""})
    return payload


def _payload_reply_markup(payload):
    button = payload.get("button") or {}
    if not button.get("text") or not button.get("url"):
        return None
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(button["text"], url=button["url"]))
    return kb

def _send_payload(target, payload):
    """Send payload and return all Telegram message IDs created.

    New auto-posts are copied from the original admin message. This preserves all
    Telegram entities and prevents global Markdown parsing from breaking long text.
    """
    sent_ids = []
    reply_markup = _payload_reply_markup(payload)

    # Preferred path for all newly created auto-posts.
    if payload.get("source_chat") is not None and payload.get("source_message") is not None:
        copied = bot.copy_message(
            target,
            payload["source_chat"],
            payload["source_message"],
            reply_markup=reply_markup,
        )
        sent_ids.append(copied.message_id)
        return sent_ids

    # Backward-compatible fallback for old database records.
    typ = payload.get("content_type")
    if typ == "text":
        text = payload.get("text", "")
        chunks = [text[i:i + 4096] for i in range(0, len(text), 4096)] or [""]
        for index, chunk in enumerate(chunks):
            markup = reply_markup if index == len(chunks) - 1 else None
            sent_ids.append(raw_bot.send_message(target, chunk, reply_markup=markup).message_id)
    elif typ == "photo":
        sent_ids.append(raw_bot.send_photo(target, payload["file_id"], caption=payload.get("caption") or None, reply_markup=reply_markup).message_id)
    elif typ == "video":
        sent_ids.append(raw_bot.send_video(target, payload["file_id"], caption=payload.get("caption") or None, reply_markup=reply_markup).message_id)
    elif typ == "document":
        sent_ids.append(raw_bot.send_document(target, payload["file_id"], caption=payload.get("caption") or None, reply_markup=reply_markup).message_id)
    elif typ == "animation":
        sent_ids.append(raw_bot.send_animation(target, payload["file_id"], caption=payload.get("caption") or None, reply_markup=reply_markup).message_id)
    else:
        raise ValueError("Stored post content is unavailable. Recreate this auto post.")
    return sent_ids


def _delete_previous_auto_messages(target, message_ids):
    for message_id in message_ids or []:
        try:
            bot.delete_message(target, int(message_id))
        except Exception as exc:
            log_event("auto_post_old_delete_error", target=target, details={"message_id": message_id, "error": str(exc)}, level="warning")

def save_auto_post(m):
    try:
        x = _pending_auto.pop(m.from_user.id, None)
        if not x:
            raise ValueError("Session expired. Open Auto Posts and start again")
        now = now_ts()
        if x["schedule"] == "every_hours":
            next_run = now + float(x["value"]) * 3600
        else:
            hh, mm = map(int, x["value"].split(":"))
            local = datetime.utcfromtimestamp(now + TZ_OFFSET_SECONDS)
            nxt = local.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if nxt <= local:
                nxt += timedelta(days=1)
            next_run = nxt.timestamp() - TZ_OFFSET_SECONDS
        button_input = (m.text or "").strip()
        payload = x.get("payload")
        if not payload:
            raise ValueError("Post content is missing. Start again")
        if button_input.lower() not in ("skip", "none", "no", "0"):
            if "|" not in button_input:
                raise ValueError("Use: Button Name | link-or-username, or send skip")
            button_text, button_value = [part.strip() for part in button_input.split("|", 1)]
            if not button_text:
                raise ValueError("Button name cannot be empty")
            payload["button"] = {"text": button_text[:64], "url": normalize_post_button_url(button_value)}
        channels = x.get("channels") or [x.get("channel")]
        last_messages = {}
        failures = []
        for target in channels:
            try:
                last_messages[str(target)] = _send_payload(target, payload)
            except Exception as exc:
                failures.append(f"{target}: {exc}")
        if len(failures) == len(channels):
            raise ValueError("Could not post to any channel: " + "; ".join(failures))
        doc = {**x, "channels": channels, "payload": payload, "next_run": next_run, "active": True, "created_at": now, "last_message_ids": last_messages}
        auto_posts_col.insert_one(doc)
        detail = f"Auto post created for {len(channels)} channel(s). Test post sent. Previous post will be deleted before every new post."
        if failures:
            detail += "\n⚠️ Failed targets: " + "; ".join(failures)
        admin_success(m.from_user.id, detail)
    except Exception as exc:
        admin_error(m.from_user.id, exc)

@bot.message_handler(func=lambda m: m.text == "📥 Auto Import" and is_admin(m.from_user.id))
def auto_import_menu(m):
    bot.send_message(m.from_user.id,"📥 **Auto Import / Upload**\n\nChoose an action:",reply_markup=auto_import_keyboard(),parse_mode="Markdown")

def auto_import_keyboard():
    kb=InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("➕ Add Source", callback_data="importui|sourceadd"),
        InlineKeyboardButton("➖ Remove Source", callback_data="importui|sourceremove"),
        InlineKeyboardButton("📋 View Sources", callback_data="importui|sourcelist"),
        InlineKeyboardButton("📤 Import/Upload Method", callback_data="importui|method"),
        InlineKeyboardButton("📚 Import Old Method", callback_data="importui|oldmethod"),
        InlineKeyboardButton("🆓 Set FREE by Link/ID", callback_data="importui|setfree"),
        InlineKeyboardButton("💎 Set VIP by Link/ID", callback_data="importui|setvip"),
        InlineKeyboardButton("🆓 Use Recent Chat as FREE", callback_data="importui|recentfree"),
        InlineKeyboardButton("💎 Use Recent Chat as VIP", callback_data="importui|recentvip"),
        InlineKeyboardButton("📋 View Auto Channels", callback_data="importui|viewauto"),
    )
    return kb

_import_state={}
@bot.callback_query_handler(func=lambda c:c.data.startswith("importui|"))
def import_ui_callback(c):
    if not is_admin(c.from_user.id):return bot.answer_callback_query(c.id,"Admin only",True)
    action=c.data.split("|")[1]
    try:
        if action=="sourcelist":
            rows=list(source_chats_col.find({}));bot.send_message(c.from_user.id,"📋 **Sources**\n\n"+("\n".join(str(x['_id']) for x in rows) if rows else "No sources."),parse_mode="Markdown");return bot.answer_callback_query(c.id)
        if action=="sourceremove":
            rows=list(source_chats_col.find({}));
            if not rows:return bot.answer_callback_query(c.id,"No sources",True)
            kb=InlineKeyboardMarkup(row_width=1)
            for i,x in enumerate(rows):kb.add(InlineKeyboardButton(f"❌ {x['_id']}",callback_data=f"importui|deletesource|{i}"))
            _import_state[c.from_user.id]={"sources":[x['_id'] for x in rows]};bot.send_message(c.from_user.id,"Select source to remove:",reply_markup=kb);return bot.answer_callback_query(c.id)
        if action=="deletesource":
            state=_import_state.get(c.from_user.id,{});idx=int(c.data.split("|")[2]);src=state.get("sources",[])[idx];source_chats_col.delete_one({"_id":src});bot.edit_message_text(f"✅ Process Complete\nRemoved source: {src}",c.from_user.id,c.message.message_id);return bot.answer_callback_query(c.id,"Removed")
        if action=="viewauto":
            cfg=get_config();bot.send_message(c.from_user.id,f"🆓 FREE source: `{cfg.get('auto_import_free_source') or 'Not set'}`\n💎 VIP source: `{cfg.get('auto_import_vip_source') or 'Not set'}`",parse_mode="Markdown");return bot.answer_callback_query(c.id)
        if action in ("recentfree", "recentvip"):
            cfg = get_config()
            chat_id = cfg.get("recent_admin_chat_id")
            title = cfg.get("recent_admin_chat_title") or str(chat_id or "")
            if not chat_id:
                raise ValueError("No recent private chat detected. Add the bot as administrator in the group/channel first, then reopen this menu.")
            category = "free" if action == "recentfree" else "vip"
            key = "auto_import_free_source" if category == "free" else "auto_import_vip_source"
            set_config(key, int(chat_id))
            source_chats_col.update_one(
                {"_id": int(chat_id)},
                {"$set": {"active": True, "category": category, "title": title, "added_at": now_ts()}},
                upsert=True,
            )
            admin_success(c.from_user.id, f"{category.upper()} private source set: {title} (`{chat_id}`)")
            return bot.answer_callback_query(c.id, "Source saved")
        if action in ("setfree","setvip"):
            _import_state[c.from_user.id]={"set_source_category":"free" if action=="setfree" else "vip"}
            msg=bot.send_message(c.from_user.id,"Send channel @username, username, t.me link, or numeric ID. Make bot admin:");bot.register_next_step_handler(msg,import_set_category_source);return bot.answer_callback_query(c.id,"Continue in chat")
        if action=="sourceadd":
            msg=bot.send_message(c.from_user.id,"Send source chat @username, username, t.me link, or numeric ID:");bot.register_next_step_handler(msg,import_add_source_step);return bot.answer_callback_query(c.id,"Continue in chat")
        if action in ("method", "oldmethod"):
            _import_state[c.from_user.id]={"step":"category", "old_import": action == "oldmethod"};kb=InlineKeyboardMarkup(row_width=2)
            for cat,label in [("free","FREE"),("vip","VIP"),("apps","APPS"),("services","SERVICES")]:kb.add(InlineKeyboardButton(label,callback_data=f"importcat|{cat}"))
            bot.send_message(c.from_user.id,"Choose destination category:",reply_markup=kb);return bot.answer_callback_query(c.id)
    except Exception as exc:bot.send_message(c.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())

def import_add_source_step(m):
    try:
        src=normalize_chat_reference(m.text);bot.get_chat(src);source_chats_col.update_one({'_id':src},{'$set':{'active':True,'added_at':now_ts()}},upsert=True);bot.send_message(m.from_user.id,f"✅ Process Complete\nSource added: {src}",reply_markup=admin_menu())
    except Exception as exc:bot.send_message(m.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())

def import_set_category_source(m):
    try:
        st=_import_state.pop(m.from_user.id,None)
        if not st: raise ValueError("Session expired")
        ref=normalize_chat_reference(m.text);chat=bot.get_chat(ref);member=bot.get_chat_member(chat.id,bot.get_me().id)
        if member.status not in ("administrator","creator"): raise ValueError("Make the bot admin in that channel")
        key="auto_import_free_source" if st["set_source_category"]=="free" else "auto_import_vip_source"
        set_config(key,chat.id);source_chats_col.update_one({"_id":chat.id},{"$set":{"active":True,"category":st["set_source_category"],"added_at":now_ts()}},upsert=True)
        admin_success(m.from_user.id,f"{st['set_source_category'].upper()} auto-import channel set: {chat.id}")
    except Exception as exc: admin_error(m.from_user.id,exc)

@bot.callback_query_handler(func=lambda c:c.data.startswith("importcat|"))
def import_category_cb(c):
    if not is_admin(c.from_user.id):return
    _import_state[c.from_user.id]={"category":c.data.split("|",1)[1]};msg=bot.send_message(c.from_user.id,"Send price in points (0 for free):");bot.register_next_step_handler(msg,import_price_step);bot.answer_callback_query(c.id)

def import_price_step(m):
    try:
        state=_import_state.get(m.from_user.id);price=int((m.text or "").strip());
        if price<0:raise ValueError("Price cannot be negative")
        state["price"]=price
        prompt = "Forward the old method post from your group/channel to me now:" if state.get("old_import") else "Now send or forward the method file/message:"
        msg=bot.send_message(m.from_user.id,prompt);bot.register_next_step_handler(msg,import_method_step)
    except Exception as exc:_import_state.pop(m.from_user.id,None);bot.send_message(m.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())

def import_method_step(m):
    try:
        state=_import_state.pop(m.from_user.id,None)
        if not state:raise ValueError("Session expired")
        name=((m.text or m.caption or 'Imported Method').strip().splitlines()[0][:100]);files=[{'chat':m.chat.id,'msg':m.message_id,'type':m.content_type}];number=fs.add(state['category'],name,files,state['price']);_folder=fs.get_by_number(number) or {'cat':state['category'],'name':name,'number':number,'price':state['price']};send_method_notification('uploaded',_folder);notify_all_users_about_method(_folder,'uploaded');log_event('method_imported',m.from_user.id,number,{'name':name});raw_bot.send_message(m.from_user.id,f"✅ Process Complete\nImported: {name}",reply_markup=admin_menu())
    except Exception as exc:bot.send_message(m.from_user.id,f"❌ Process Failed\n{exc}",reply_markup=admin_menu())

@bot.my_chat_member_handler()
def remember_admin_chat(update):
    """Detect groups/channels where the bot becomes admin and offer VIP-channel approval."""
    try:
        chat = update.chat
        new_status = update.new_chat_member.status
        if new_status not in ("administrator", "creator"):
            return
        if chat.type not in ("group", "supergroup", "channel"):
            return
        chat_id = int(chat.id)
        title = getattr(chat, "title", None) or str(chat_id)
        set_config("recent_admin_chat_id", chat_id)
        set_config("recent_admin_chat_title", title)
        existing = source_chats_col.find_one({"_id": chat_id}) or {}
        source_chats_col.update_one(
            {"_id": chat_id},
            {"$set": {"active": True, "title": title, "chat_type": chat.type, "detected_at": now_ts(), "detected_by_admin_event": True}},
            upsert=True,
        )

        # Keep the existing auto-import convenience for obvious FREE/VIP source names.
        upper_title = title.upper()
        category = None
        if "VIP" in upper_title or "PREMIUM" in upper_title:
            category = "vip"
        elif "FREE" in upper_title:
            category = "free"
        if category:
            key = "auto_import_vip_source" if category == "vip" else "auto_import_free_source"
            set_config(key, chat_id)
            source_chats_col.update_one({"_id": chat_id}, {"$set": {"category": category}})

        # Ask once whether this detected chat should be used for paid VIP access.
        vip_status = existing.get("vip_access_status")
        already_access = any(str(x) == str(chat_id) for x in (get_config().get("access_chats", []) or []))
        if already_access:
            source_chats_col.update_one({"_id": chat_id}, {"$set": {"vip_access_status": "approved"}})
            vip_status = "approved"
        if vip_status not in ("approved", "rejected", "pending"):
            source_chats_col.update_one({"_id": chat_id}, {"$set": {"vip_access_status": "pending", "vip_detected_at": now_ts()}})
            kb = InlineKeyboardMarkup(row_width=2)
            kb.add(
                InlineKeyboardButton("✅ Set as VIP Channel", callback_data=f"vipdetect|approve|{chat_id}"),
                InlineKeyboardButton("❌ Not VIP", callback_data=f"vipdetect|reject|{chat_id}"),
            )
            suggested = "\n⭐ Name suggests this may be a VIP channel." if category == "vip" else ""
            msg = (
                f"🎯 VIP CHANNEL DETECTED\n\n"
                f"📌 {title}\n"
                f"🆔 {chat_id}\n"
                f"Type: {chat.type}{suggested}\n\n"
                "Should this chat receive paid VIP members?\n"
                "If approved, the bot will automatically generate one-user invite links after VIP payment approval and remove users when VIP expires."
            )
            for adm in get_all_admins():
                try:
                    raw_bot.send_message(int(adm["_id"]), msg, reply_markup=kb)
                except Exception:
                    pass
        else:
            # Still notify owner that the chat was detected for auto-import purposes.
            try:
                detected = f"\n✅ Automatically selected as {category.upper()} import source." if category else ""
                raw_bot.send_message(ADMIN_ID, f"🤖 Chat detected: {title} ({chat_id}){detected}")
            except Exception:
                pass
    except Exception as exc:
        log_event("remember_admin_chat_error", details={"error": str(exc)}, level="error")


@bot.callback_query_handler(func=lambda c: c.data.startswith("vipdetect|"))
def vip_detect_review_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    try:
        _, action, chat_id_raw = c.data.split("|", 2)
        chat_id = int(chat_id_raw)
        row = source_chats_col.find_one({"_id": chat_id}) or {}
        title = row.get("title") or str(chat_id)
        if action == "approve":
            cfg = get_config()
            chats = cfg.get("access_chats", []) or []
            if str(chat_id) not in [str(x) for x in chats]:
                chats.append(chat_id)
                set_config("access_chats", chats)
            source_chats_col.update_one({"_id": chat_id}, {"$set": {"vip_access_status": "approved", "vip_approved_by": c.from_user.id, "vip_approved_at": now_ts()}}, upsert=True)
            text = f"✅ VIP CHANNEL APPROVED\n\n{title}\n{chat_id}\n\nOne-user invite links will be generated automatically for approved VIP payments."
            answer = "VIP channel added"
        else:
            source_chats_col.update_one({"_id": chat_id}, {"$set": {"vip_access_status": "rejected", "vip_rejected_by": c.from_user.id, "vip_rejected_at": now_ts()}}, upsert=True)
            text = f"❌ NOT SET AS VIP CHANNEL\n\n{title}\n{chat_id}"
            answer = "Not VIP"
        try:
            raw_bot.edit_message_text(text, c.message.chat.id, c.message.message_id)
        except Exception:
            raw_bot.send_message(c.from_user.id, text)
        bot.answer_callback_query(c.id, answer, True)
    except Exception as exc:
        bot.answer_callback_query(c.id, "Could not save", True)
        admin_error(c.from_user.id, exc)


def _set_import_source_from_chat(message, category):
    try:
        chat = message.chat
        if chat.type not in ("group", "supergroup", "channel"):
            raise ValueError("Send this command inside the source group/channel")
        member = bot.get_chat_member(chat.id, bot.get_me().id)
        if member.status not in ("administrator", "creator"):
            raise ValueError("Make the bot administrator first")
        key = "auto_import_vip_source" if category == "vip" else "auto_import_free_source"
        set_config(key, int(chat.id))
        set_config("recent_admin_chat_id", int(chat.id))
        set_config("recent_admin_chat_title", getattr(chat, "title", None) or str(chat.id))
        source_chats_col.update_one(
            {"_id": int(chat.id)},
            {"$set": {"active": True, "category": category, "title": getattr(chat, "title", None), "added_at": now_ts()}},
            upsert=True,
        )
        bot.send_message(chat.id, f"✅ This private chat is now the {category.upper()} auto-import source.")
        try:
            admin_success(ADMIN_ID, f"{category.upper()} private source connected: {getattr(chat, 'title', chat.id)} (`{chat.id}`)")
        except Exception:
            pass
    except Exception as exc:
        try:
            bot.send_message(message.chat.id, f"❌ Process Failed\n{exc}")
        except Exception:
            pass


@bot.message_handler(commands=["setvipimport"])
def set_vip_import_here(m):
    _set_import_source_from_chat(m, "vip")


@bot.message_handler(commands=["setfreeimport"])
def set_free_import_here(m):
    _set_import_source_from_chat(m, "free")


def _auto_import_category_for_chat(chat_id):
    """Return FREE/VIP category for a configured auto-import source chat."""
    cfg = get_cached_config()

    def same_chat(saved, current):
        if saved is None:
            return False
        try:
            return int(str(saved).strip()) == int(current)
        except (TypeError, ValueError):
            return str(saved).strip().lower() == str(current).strip().lower()

    if same_chat(cfg.get("auto_import_free_source"), chat_id):
        return "free"
    if same_chat(cfg.get("auto_import_vip_source"), chat_id):
        return "vip"
    return None


def _import_payload_message(command_message, part):
    """For #methodN.part replies, import the replied content, not the tag message."""
    replied = getattr(command_message, "reply_to_message", None)
    if part and replied is not None:
        return replied
    return command_message


def _send_method_attachment_picker(pending_id, cat):
    from bson import ObjectId
    pending = pending_methods_col.find_one({"_id": ObjectId(str(pending_id))})
    if not pending:
        return
    rows = list(folders_col.find({"cat": cat}).sort([("pinned", -1), ("created_at", -1)]).limit(80))
    if not rows:
        return
    kb = InlineKeyboardMarkup(row_width=1)
    for row in rows:
        kb.add(InlineKeyboardButton(f"📄 #{row.get('number','?')} • {row.get('name','Unnamed')}"[:62], callback_data=f"attachmethodpick|{pending['_id']}|{row['_id']}"))
    text = f"➕ ADD METHOD PART\n\nA source post tagged {pending.get('tag','method part')} is waiting.\nSelect the existing method that should receive this file/content:"
    for admin in get_all_admins():
        try:
            raw_bot.send_message(int(admin["_id"]), text, reply_markup=kb)
        except Exception:
            pass


@bot.callback_query_handler(func=lambda c: c.data.startswith("attachmethodpick|"))
def attach_method_pick_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    from bson import ObjectId
    try:
        _, pending_id, folder_id = c.data.split("|", 2)
        pending = pending_methods_col.find_one({"_id": ObjectId(pending_id), "status": "awaiting_method_selection"})
        folder = folders_col.find_one({"_id": ObjectId(folder_id)})
        if not pending or not folder:
            return bot.answer_callback_query(c.id, "Item already handled or method missing", True)
        file_item = pending.get("file_item")
        if not file_item:
            return bot.answer_callback_query(c.id, "Attachment missing", True)
        folders_col.update_one({"_id": folder["_id"]}, {"$addToSet": {"files": file_item}, "$set": {"updated_at": now_ts()}})
        pending_methods_col.update_one({"_id": pending["_id"]}, {"$set": {"status": "attached", "selected_folder_id": folder["_id"], "reviewed_by": c.from_user.id, "reviewed_at": now_ts()}})
        try:
            _send_source_import_notice(pending.get("source_chat"), f"✅ Added to method: {folder.get('name')}")
        except Exception:
            pass
        send_method_notification("updated", folders_col.find_one({"_id": folder["_id"]}))
        raw_bot.edit_message_text(c.message.text + f"\n\n✅ Added to: {folder.get('name')}", c.message.chat.id, c.message.message_id)
        bot.answer_callback_query(c.id, "Added to method")
    except Exception as exc:
        bot.answer_callback_query(c.id, "Could not attach", True)
        log_event("method_attachment_picker_error", c.from_user.id, details={"error": str(exc)}, level="error")


def _send_source_import_notice(chat_id, text):
    """Send an auto-import status notice to the source chat.

    Admin can disable these replies entirely. When enabled, notices can be
    automatically deleted after the configured delay so source channels stay clean.
    """
    cfg = get_cached_config()
    if not cfg.get("group_import_notify_enabled", True):
        return None
    try:
        sent = raw_bot.send_message(chat_id, text)
    except Exception:
        return None

    if cfg.get("group_import_notify_auto_delete_enabled", True):
        try:
            delay = max(5, int(cfg.get("group_import_notify_auto_delete_seconds", 60) or 60))
        except Exception:
            delay = 60

        def _delete_later():
            try:
                raw_bot.delete_message(chat_id, sent.message_id)
            except Exception:
                pass

        timer = threading.Timer(delay, _delete_later)
        timer.daemon = True
        timer.start()
    return sent


def _queue_auto_import(m, edited=False):
    """
    Queue new/edited source messages for approval.

    Supported:
      #method1 + method name on second line -> main method
      #method1.2 / #method1_2 / #method1-2 -> attach another item
      #add1 / #part1 / #method1add -> simpler additional-file formats
      Tags may be used as a reply or directly in a media caption.
      editing an old group/channel post and adding #method -> queue it
    """
    try:
        raw_command = (m.text or m.caption or "").strip()
        lowered = raw_command.lower()
        if lowered.startswith("/setvipimport") or lowered == "#setvipimport":
            return _set_import_source_from_chat(m, "vip")
        if lowered.startswith("/setfreeimport") or lowered == "#setfreeimport":
            return _set_import_source_from_chat(m, "free")

        cat = _auto_import_category_for_chat(m.chat.id)
        if not cat:
            return

        first = raw_command.splitlines()[0].strip() if raw_command else ""

        # Main method formats:
        #   #method9
        # Additional-file formats (dot-free alternatives are recommended):
        #   #method9.2, #method9_2, #method9-2
        #   #add9, #part9, #method9add
        main_match = re.fullmatch(r"#method(\d+)", first, re.I)
        bare_new_match = re.fullmatch(r"#method", first, re.I)
        part_match = re.fullmatch(r"#method(\d+)[._-](\d+)", first, re.I)
        simple_add_match = re.fullmatch(r"#(?:add|part)(\d+)(?:[._-]?(\d+))?", first, re.I)
        method_add_match = re.fullmatch(r"#method(\d+)add", first, re.I)

        if main_match:
            method_key = int(main_match.group(1))
            # When #methodN is sent as a reply and method N already exists,
            # treat it as an additional file. This is the simplest reliable format.
            replied = getattr(m, "reply_to_message", None)
            existing_for_reply = folders_col.find_one({
                "cat": cat,
                "$or": [{"auto_method_key": method_key}, {"number": method_key}],
            }) if replied is not None else None
            part = 1 if existing_for_reply else 0
        elif bare_new_match:
            # Reply #method to any old untagged post to queue it as a new method.
            if not getattr(m, "reply_to_message", None):
                return
            method_key = int(get_config().get("next_folder_number", 1))
            part = 0
        elif part_match:
            method_key = int(part_match.group(1))
            part = int(part_match.group(2))
        elif simple_add_match:
            method_key = int(simple_add_match.group(1))
            part = int(simple_add_match.group(2) or 1)
        elif method_add_match:
            method_key = int(method_add_match.group(1))
            part = 1
        else:
            return

        # If the tag is sent as a reply, import the replied message. If the tag
        # is written in a media caption, import that media message itself.
        payload = getattr(m, "reply_to_message", None) if bare_new_match else _import_payload_message(m, part)
        file_item = {
            "chat": payload.chat.id,
            "msg": payload.message_id,
            "type": payload.content_type,
        }
        cfg = get_cached_config()
        source_reply_enabled = cfg.get("group_import_notify_enabled", True)

        if part:
            # Automatically attach #methodN_1 / #methodN_2 (and supported aliases)
            # to Method N while preserving every file already stored there.
            approved = folders_col.find_one({
                "cat": cat,
                "$or": [
                    {"auto_method_key": method_key},
                    {"number": method_key},
                ],
            })
            if approved:
                folders_col.update_one(
                    {"_id": approved["_id"]},
                    {"$addToSet": {"files": file_item}, "$set": {"updated_at": now_ts()}},
                )
                if source_reply_enabled:
                    _send_source_import_notice(m.chat.id, f"✅ File added to {approved.get('name') or f'Method {method_key}' }.")
                send_method_notification("updated", folders_col.find_one({"_id": approved["_id"]}))
                return

            # If the main method is still waiting for approval, append the new part
            # to that same pending method instead of creating a separate method.
            pending = pending_methods_col.find_one({
                "cat": cat,
                "auto_method_key": method_key,
                "status": {"$in": ["pending", "pending_update", "pending_replace"]},
            })
            if pending:
                pending_methods_col.update_one(
                    {"_id": pending["_id"]},
                    {"$addToSet": {"files": file_item}, "$set": {"updated_at": now_ts()}},
                )
                if source_reply_enabled:
                    _send_source_import_notice(m.chat.id, f"✅ File added to Method {method_key}. Waiting for admin approval.")
                return

            if source_reply_enabled:
                _send_source_import_notice(m.chat.id, f"❌ Method {method_key} was not found. Add/approve #method{method_key} first.")
            return

        lines = raw_command.splitlines()
        if len(lines) > 1 and lines[1].strip():
            name = lines[1].strip()[:150]
        else:
            # When an old post is edited, use its first meaningful line after the tag.
            payload_text = (getattr(payload, "text", None) or getattr(payload, "caption", None) or "").strip()
            payload_lines = [line.strip() for line in payload_text.splitlines() if line.strip()]
            name = ((payload_lines[0] if bare_new_match and payload_lines else (payload_lines[1] if len(payload_lines) > 1 else f"Method {method_key}")))[:150]

        existing_pending = pending_methods_col.find_one({
            "cat": cat,
            "auto_method_key": method_key,
            "status": {"$in": ["pending", "pending_update", "pending_replace"]},
        })
        existing_live = folders_col.find_one({"cat": cat, "auto_method_key": method_key})

        if existing_pending:
            pending_methods_col.update_one(
                {"_id": existing_pending["_id"]},
                {
                    "$set": {
                        "name": name,
                        "files": [file_item],
                        "updated_at": now_ts(),
                        "edited_source": bool(edited),
                    }
                },
            )
            pending_id = existing_pending["_id"]
            action = "updated in the pending queue"
        elif existing_live:
            doc = {
                "cat": cat,
                "auto_method_key": method_key,
                "name": name,
                "files": [file_item],
                "source_chat": m.chat.id,
                "created_at": now_ts(),
                "updated_at": now_ts(),
                "status": "pending_replace",
                "existing_folder_id": existing_live.get("_id"),
                "submitted_by": getattr(getattr(m, "from_user", None), "id", None),
                "edited_source": bool(edited),
            }
            pending_id = pending_methods_col.insert_one(doc).inserted_id
            action = "queued as an update"
        else:
            doc = {
                "cat": cat,
                "auto_method_key": method_key,
                "name": name,
                "files": [file_item],
                "source_chat": m.chat.id,
                "created_at": now_ts(),
                "updated_at": now_ts(),
                "status": "pending",
                "submitted_by": getattr(getattr(m, "from_user", None), "id", None),
                "edited_source": bool(edited),
            }
            pending_id = pending_methods_col.insert_one(doc).inserted_id
            action = "sent for approval"

        if source_reply_enabled:
            _send_source_import_notice(m.chat.id, f"⏳ {name} has been {action}. It is not visible until admin approval.")

        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("✅ Review & Approve", callback_data=f"pendingview|{pending_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"pendingreject|{pending_id}"),
        )
        raw_bot.send_message(
            ADMIN_ID,
            f"⏳ PENDING METHOD\n\n📁 Category: {cat.upper()}\n🏷 Name: {name}\n🏷 Source: #method{method_key}\n📎 Files: 1\n✏️ Edited old post: {'Yes' if edited else 'No'}",
            reply_markup=kb,
        )
    except Exception as exc:
        try:
            raw_bot.send_message(m.chat.id, f"❌ Auto import failed: {str(exc)[:500]}")
        except Exception:
            pass
        log_event("auto_import_error", target=getattr(m.chat, "id", None), details={"error": str(exc)}, level="error")



# =========================
# 🛡 GROUP MANAGEMENT
# =========================
_vip_member_cache = {}
_vip_badge_cooldown = {}
_group_manage_pending = {}


def _managed_group_ids():
    values = get_cached_config().get("managed_chat_groups", []) or []
    result = []
    for value in values:
        try:
            result.append(int(value))
        except (TypeError, ValueError):
            continue
    return result


def _is_chat_member_status(status):
    return status in ("member", "administrator", "creator", "restricted")


def _is_vip_group_member(user_id):
    """VIP means internal VIP or membership in the configured VIP source chat."""
    try:
        if User(user_id).is_vip():
            return True
    except Exception:
        pass
    source = get_cached_config().get("auto_import_vip_source")
    if not source:
        return False
    key = (str(source), int(user_id))
    cached = _vip_member_cache.get(key)
    if cached and time.time() - cached[1] < 300:
        return cached[0]
    try:
        member = bot.get_chat_member(int(source), int(user_id))
        result = _is_chat_member_status(member.status)
    except Exception:
        result = False
    _vip_member_cache[key] = (result, time.time())
    return result


def _looks_promotional(text):
    text = (text or "").strip()
    if not text:
        return False
    patterns = [
        r"https?://\S+",
        r"(?:t\.me|telegram\.me)/\S+",
        r"(?<!\w)@[A-Za-z0-9_]{5,}",
        r"(?<!\w)[A-Za-z0-9_]{5,}bot(?!\w)",
    ]
    if any(re.search(pattern, text, re.I) for pattern in patterns):
        return True
    promo_words = r"\b(?:buy|sell|selling|available|discount|promo|promotion|offer|dm me|contact me|inbox me|price)\b"
    return bool(re.search(promo_words, text, re.I) and re.search(r"[$€£₹₨]|\d", text))


def _send_vip_badge(message):
    cfg = get_cached_config()
    if not cfg.get("group_vip_badge_enabled", True):
        return
    key = (message.chat.id, message.from_user.id)
    if time.time() - _vip_badge_cooldown.get(key, 0) < 21600:
        return
    _vip_badge_cooldown[key] = time.time()
    try:
        raw_bot.reply_to(message, "👑 VIP MEMBER", disable_notification=True)
    except Exception:
        pass


def _log_managed_group_message(message):
    try:
        if message.chat.id in _managed_group_ids():
            group_message_log_col.update_one(
                {"group_id": int(message.chat.id), "message_id": int(message.message_id)},
                {"$set": {"created_at": now_ts(), "user_id": getattr(getattr(message, "from_user", None), "id", None)}},
                upsert=True,
            )
    except Exception:
        pass


def _apply_warning_action(message, warnings):
    cfg = get_cached_config()
    limit = max(1, int(cfg.get("group_warning_limit", 3)))
    if warnings < limit:
        return None
    action = str(cfg.get("group_warning_action", "mute")).lower()
    try:
        if action == "ban":
            bot.ban_chat_member(message.chat.id, message.from_user.id)
            return "banned"
        minutes = max(1, int(cfg.get("group_mute_minutes", 1440)))
        until = datetime.utcnow() + timedelta(minutes=minutes)
        bot.restrict_chat_member(
            message.chat.id,
            message.from_user.id,
            until_date=until,
            can_send_messages=False,
            can_send_audios=False,
            can_send_documents=False,
            can_send_photos=False,
            can_send_videos=False,
            can_send_video_notes=False,
            can_send_voice_notes=False,
            can_send_polls=False,
            can_send_other_messages=False,
            can_add_web_page_previews=False,
            can_change_info=False,
            can_invite_users=False,
            can_pin_messages=False,
            can_manage_topics=False,
        )
        return f"muted for {minutes} minutes"
    except Exception as exc:
        log_event("group_warning_action_error", target=message.chat.id, details={"error": str(exc)}, level="error")
        return None


def moderate_managed_group_message(message):
    """Delete promotional content from non-VIP members and warn them."""
    try:
        _log_managed_group_message(message)
        if message.chat.id not in _managed_group_ids() or not message.from_user:
            return
        if message.from_user.is_bot or is_admin(message.from_user.id):
            return
        vip = _is_vip_group_member(message.from_user.id)
        if vip:
            _send_vip_badge(message)
            return
        cfg = get_cached_config()
        if not cfg.get("group_moderation_enabled", True):
            return
        text = message.text or message.caption or ""
        if not _looks_promotional(text):
            return
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass
        record = group_warnings_col.find_one_and_update(
            {"group_id": message.chat.id, "user_id": message.from_user.id},
            {"$inc": {"warnings": 1}, "$set": {"updated_at": now_ts(), "name": message.from_user.first_name, "username": message.from_user.username}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        warnings = int((record or {}).get("warnings", 1))
        name = message.from_user.first_name or "Member"
        warning = (
            f"⚠️ {name}, promotional messages, links, usernames and bot mentions are not allowed for free members.\n"
            f"Warning: {warnings}\n\n👑 VIP members are allowed to promote."
        )
        action_taken = _apply_warning_action(message, warnings)
        if action_taken:
            warning += f"\n\n🚫 Limit reached: user was {action_taken}."
            group_warnings_col.update_one(
                {"group_id": message.chat.id, "user_id": message.from_user.id},
                {"$set": {"action_taken": action_taken, "action_at": now_ts()}},
            )
        raw_bot.send_message(message.chat.id, warning, disable_notification=True)
    except Exception as exc:
        log_event("group_moderation_error", target=getattr(message.chat, "id", None), details={"error": str(exc)}, level="error")


def _group_management_keyboard():
    cfg = get_cached_config()
    moderation = cfg.get("group_moderation_enabled", True)
    badge = cfg.get("group_vip_badge_enabled", True)
    user_alerts = cfg.get("user_method_notifications_enabled", True)
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("➕ Add Chat Group", callback_data="groupmgr|add"), InlineKeyboardButton("➖ Remove Group", callback_data="groupmgr|remove"))
    kb.add(InlineKeyboardButton(f"🛡 Moderation: {'ON' if moderation else 'OFF'}", callback_data="groupmgr|togglemod"))
    kb.add(InlineKeyboardButton(f"👑 VIP Badge: {'ON' if badge else 'OFF'}", callback_data="groupmgr|togglebadge"))
    kb.add(InlineKeyboardButton(f"🔔 User Method Alerts: {'ON' if user_alerts else 'OFF'}", callback_data="groupmgr|toggleusers"))
    kb.add(InlineKeyboardButton(f"🎁 Referral Alerts: {'ON' if cfg.get('user_referral_notifications_enabled', True) else 'OFF'}", callback_data="groupmgr|togglerefs"))
    kb.add(InlineKeyboardButton("⚠️ Warning Limit", callback_data="groupmgr|warnlimit"), InlineKeyboardButton("🚫 Ban / Mute", callback_data="groupmgr|warnaction"))
    kb.add(InlineKeyboardButton("📜 Group Rules", callback_data="groupmgr|rules"), InlineKeyboardButton("📤 Send Rules", callback_data="groupmgr|sendrules"))
    kb.add(InlineKeyboardButton("🧹 Clear Tracked Messages", callback_data="groupmgr|clearmessages"))
    kb.add(InlineKeyboardButton("🔓 Unmute / Unban", callback_data="groupmgr|restoreuser"))
    kb.add(InlineKeyboardButton("📋 View Settings", callback_data="groupmgr|view"), InlineKeyboardButton("🧹 Clear Warnings", callback_data="groupmgr|clearwarn"))
    return kb


@bot.message_handler(func=lambda m: m.text == "🛡 Group Management" and is_admin(m.from_user.id))
def group_management_menu(m):
    raw_bot.send_message(m.from_user.id, "🛡 GROUP MANAGEMENT\n\nManage promotion protection, VIP exemptions and method alerts.", reply_markup=_group_management_keyboard())


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupmgr|"))
def group_management_callback(c):
    if not is_admin(c.from_user.id):
        return
    try:
        action = c.data.split("|", 1)[1]
        cfg = get_config()
        if action == "togglemod":
            set_config("group_moderation_enabled", not cfg.get("group_moderation_enabled", True))
        elif action == "togglebadge":
            set_config("group_vip_badge_enabled", not cfg.get("group_vip_badge_enabled", True))
        elif action == "toggleusers":
            set_config("user_method_notifications_enabled", not cfg.get("user_method_notifications_enabled", True))
        elif action == "togglerefs":
            set_config("user_referral_notifications_enabled", not cfg.get("user_referral_notifications_enabled", True))
        elif action == "view":
            groups = _managed_group_ids()
            vip_source = cfg.get("auto_import_vip_source") or "Not set"
            text = "🛡 GROUP MANAGEMENT SETTINGS\n\nManaged groups:\n" + ("\n".join(map(str, groups)) if groups else "None")
            text += f"\n\nVIP membership source: {vip_source}\nModeration: {'ON' if cfg.get('group_moderation_enabled', True) else 'OFF'}\nVIP badge replies: {'ON' if cfg.get('group_vip_badge_enabled', True) else 'OFF'}\nUser method alerts: {'ON' if cfg.get('user_method_notifications_enabled', True) else 'OFF'}\nReferral alerts: {'ON' if cfg.get('user_referral_notifications_enabled', True) else 'OFF'}\nWarning limit: {cfg.get('group_warning_limit', 3)}\nAction: {cfg.get('group_warning_action', 'mute').upper()}\nMute minutes: {cfg.get('group_mute_minutes', 1440)}"
            raw_bot.send_message(c.from_user.id, text)
            return bot.answer_callback_query(c.id)
        elif action == "warnlimit":
            msg = raw_bot.send_message(c.from_user.id, "Send the number of warnings before action (example: 3).")
            bot.register_next_step_handler(msg, save_group_warning_limit)
            return bot.answer_callback_query(c.id, "Continue in chat")
        elif action == "warnaction":
            kb = InlineKeyboardMarkup(row_width=2)
            kb.add(InlineKeyboardButton("🔇 Mute", callback_data="groupwarnaction|mute"), InlineKeyboardButton("🚫 Ban", callback_data="groupwarnaction|ban"))
            raw_bot.send_message(c.from_user.id, "Choose what happens when the warning limit is reached:", reply_markup=kb)
            return bot.answer_callback_query(c.id)
        elif action == "rules":
            msg = raw_bot.send_message(c.from_user.id, "Send the full rules message. You can use multiple lines and emojis.")
            bot.register_next_step_handler(msg, save_group_rules_text)
            return bot.answer_callback_query(c.id, "Continue in chat")
        elif action == "sendrules":
            send_rules_group_picker(c.from_user.id)
            return bot.answer_callback_query(c.id)
        elif action == "clearmessages":
            send_clear_messages_group_picker(c.from_user.id)
            return bot.answer_callback_query(c.id)
        elif action == "restoreuser":
            send_restricted_users_picker(c.from_user.id)
            return bot.answer_callback_query(c.id)
        elif action == "clearwarn":
            group_warnings_col.delete_many({})
            admin_success(c.from_user.id, "All group warnings cleared")
            return bot.answer_callback_query(c.id, "Cleared")
        elif action == "add":
            _group_manage_pending[c.from_user.id] = "add"
            msg = raw_bot.send_message(c.from_user.id, "Send the chat group @username, t.me link, or numeric -100... ID. The bot must be an administrator there.")
            bot.register_next_step_handler(msg, save_managed_group)
            return bot.answer_callback_query(c.id, "Continue in chat")
        elif action == "remove":
            groups = _managed_group_ids()
            if not groups:
                raise ValueError("No managed groups are configured")
            kb = InlineKeyboardMarkup(row_width=1)
            for gid in groups:
                try:
                    title = bot.get_chat(gid).title or str(gid)
                except Exception:
                    title = str(gid)
                kb.add(InlineKeyboardButton(f"➖ {title}", callback_data=f"groupmgrremove|{gid}"))
            raw_bot.send_message(c.from_user.id, "Select a group to remove:", reply_markup=kb)
            return bot.answer_callback_query(c.id)
        bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=_group_management_keyboard())
        bot.answer_callback_query(c.id, "Updated")
    except Exception as exc:
        bot.answer_callback_query(c.id, "Failed", True)
        admin_error(c.from_user.id, exc)


def save_managed_group(m):
    try:
        ref = normalize_chat_reference(m.text or "")
        chat = bot.get_chat(ref)
        if chat.type not in ("group", "supergroup"):
            raise ValueError("The selected chat must be a group or supergroup")
        member = bot.get_chat_member(chat.id, bot.get_me().id)
        if member.status not in ("administrator", "creator"):
            raise ValueError("Make the bot an administrator in that group first")
        groups = _managed_group_ids()
        if int(chat.id) not in groups:
            groups.append(int(chat.id))
            set_config("managed_chat_groups", groups)
        admin_success(m.from_user.id, f"Group management enabled for {chat.title} ({chat.id})")
        raw_bot.send_message(chat.id, "🛡 Group protection is now active. Free members cannot post promotional links, usernames or bot mentions. VIP members are exempt.")
    except Exception as exc:
        admin_error(m.from_user.id, exc)


def save_group_warning_limit(m):
    try:
        limit = int((m.text or "").strip())
        if limit < 1 or limit > 100:
            raise ValueError("Warning limit must be between 1 and 100")
        set_config("group_warning_limit", limit)
        admin_success(m.from_user.id, f"Warning limit set to {limit}")
    except Exception as exc:
        admin_error(m.from_user.id, exc)


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupwarnaction|"))
def group_warning_action_cb(c):
    if not is_admin(c.from_user.id):
        return
    action = c.data.split("|", 1)[1]
    set_config("group_warning_action", action)
    if action == "mute":
        msg = raw_bot.send_message(c.from_user.id, "Send mute duration in minutes (example: 1440 for one day).")
        bot.register_next_step_handler(msg, save_group_mute_minutes)
    else:
        admin_success(c.from_user.id, "Warning action set to BAN")
    bot.answer_callback_query(c.id, "Updated")


def save_group_mute_minutes(m):
    try:
        minutes = int((m.text or "").strip())
        if minutes < 1:
            raise ValueError("Minutes must be at least 1")
        set_config("group_mute_minutes", minutes)
        admin_success(m.from_user.id, f"Warning action set to MUTE for {minutes} minutes")
    except Exception as exc:
        admin_error(m.from_user.id, exc)


def save_group_rules_text(m):
    text = (m.text or m.caption or "").strip()
    if not text:
        return admin_error(m.from_user.id, "Rules message cannot be empty")
    set_config("group_rules_text", text)
    msg = raw_bot.send_message(m.from_user.id, "Now send the button as:\nButton Name | @username-or-link\n\nSend skip for no button.")
    bot.register_next_step_handler(msg, save_group_rules_button)


def save_group_rules_button(m):
    raw = (m.text or "").strip()
    if raw.lower() == "skip":
        set_config("group_rules_button_text", "")
        set_config("group_rules_button_url", "")
        return admin_success(m.from_user.id, "Rules saved without a button")
    if "|" not in raw:
        return admin_error(m.from_user.id, "Use: Button Name | @username-or-link")
    label, target = [x.strip() for x in raw.split("|", 1)]
    if not label or not target:
        return admin_error(m.from_user.id, "Button name and target are required")
    if target.startswith("@"): target = "https://t.me/" + target[1:]
    elif not re.match(r"^https?://", target, re.I): target = "https://t.me/" + target.lstrip("@")
    set_config("group_rules_button_text", label[:64])
    set_config("group_rules_button_url", target)
    admin_success(m.from_user.id, "Rules and button saved")


def send_rules_group_picker(admin_id):
    groups = _managed_group_ids()
    if not groups:
        return admin_error(admin_id, "No managed groups configured")
    kb = InlineKeyboardMarkup(row_width=1)
    for gid in groups:
        try: title = bot.get_chat(gid).title or str(gid)
        except Exception: title = str(gid)
        kb.add(InlineKeyboardButton(f"📤 {title}", callback_data=f"groupsendrules|{gid}"))
    raw_bot.send_message(admin_id, "Select the group where rules should be posted:", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupsendrules|"))
def send_rules_to_group_cb(c):
    if not is_admin(c.from_user.id): return
    try:
        gid = int(c.data.split("|", 1)[1])
        cfg = get_cached_config()
        text = cfg.get("group_rules_text", "").strip()
        if not text: raise ValueError("Set the rules message first")
        kb = None
        if cfg.get("group_rules_button_text") and cfg.get("group_rules_button_url"):
            kb = InlineKeyboardMarkup().add(InlineKeyboardButton(cfg["group_rules_button_text"], url=cfg["group_rules_button_url"]))
        raw_bot.send_message(gid, text, reply_markup=kb)
        bot.answer_callback_query(c.id, "Rules sent")
        admin_success(c.from_user.id, "Rules posted successfully")
    except Exception as exc:
        admin_error(c.from_user.id, exc)


def send_clear_messages_group_picker(admin_id):
    groups = _managed_group_ids()
    if not groups:
        return admin_error(admin_id, "No managed groups configured")
    kb = InlineKeyboardMarkup(row_width=1)
    for gid in groups:
        try: title = bot.get_chat(gid).title or str(gid)
        except Exception: title = str(gid)
        kb.add(InlineKeyboardButton(f"🧹 {title}", callback_data=f"groupcleartracked|{gid}"))
    raw_bot.send_message(admin_id, "Select a group. The bot can delete only messages it has tracked since this feature was enabled:", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupcleartracked|"))
def clear_tracked_group_messages_cb(c):
    if not is_admin(c.from_user.id): return
    gid = int(c.data.split("|", 1)[1])
    rows = list(group_message_log_col.find({"group_id": gid}, {"message_id": 1}).sort("message_id", -1))
    deleted = failed = 0
    for row in rows:
        try:
            bot.delete_message(gid, int(row["message_id"]))
            deleted += 1
        except Exception:
            failed += 1
    group_message_log_col.delete_many({"group_id": gid})
    bot.answer_callback_query(c.id, "Cleanup complete")
    admin_success(c.from_user.id, f"Deleted {deleted} tracked messages. Failed: {failed}")


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupmgrremove|"))
def remove_managed_group_callback(c):
    if not is_admin(c.from_user.id):
        return
    try:
        gid = int(c.data.split("|", 1)[1])
        groups = [x for x in _managed_group_ids() if x != gid]
        set_config("managed_chat_groups", groups)
        bot.answer_callback_query(c.id, "Removed")
        admin_success(c.from_user.id, f"Group removed from management: {gid}")
    except Exception as exc:
        admin_error(c.from_user.id, exc)


def notify_all_users_about_method(folder, action="published"):
    """Notify users about a newly published method with a direct action button."""
    if not get_cached_config().get("user_method_notifications_enabled", True):
        return
    folder = folder or {}
    name = str(folder.get("name") or "New Method")
    cat = str(folder.get("cat") or "free").lower()
    price = float(folder.get("price") or 0)
    oid = str(folder.get("_id") or "")
    if cat == "vip":
        price_line = f"Free for VIP members\nFree users: ${price:g} USDT" if price > 0 else "VIP members only"
        button_text = "🛒 Buy / Open Method"
        title = "💎 NEW VIP METHOD"
    else:
        if price <= 0:
            price_line = "Price: FREE"
            button_text = "▶️ Watch Free"
        else:
            price_line = f"Price: {int(price)} points"
            button_text = "💎 View / Buy with Points"
        title = "🚀 NEW FREE METHOD"
    text = f"{title}\n\n📄 {name}\n{price_line}\n\nTap below to open it."
    kb = InlineKeyboardMarkup(row_width=1)
    if oid:
        kb.add(InlineKeyboardButton(button_text, callback_data=f"openid|{oid}"))
    def worker():
        for row in users_col.find({}, {"_id": 1}):
            try:
                raw_bot.send_message(int(row["_id"]), text, reply_markup=kb if oid else None, disable_notification=False)
                time.sleep(0.04)
            except Exception:
                continue
    threading.Thread(target=worker, daemon=True).start()


def notify_all_users_about_product(product):
    """Notify users when a new shop product is created, with a direct product button."""
    if not get_cached_config().get("user_product_notifications_enabled", True):
        return
    product = product or {}
    oid = str(product.get("_id") or "")
    name = str(product.get("name") or "New Product")
    kind = str(product.get("kind") or "paid")
    stock = _shop_stock_count(product)
    if kind == "paid":
        price = float(product.get("price_usdt", 0) or 0)
        text = f"🛍️ NEW PAID PRODUCT\n\n📦 {name}\n💵 Price: ${price:g} USDT\n📊 Stock: {stock}\n⏳ Duration: {product.get('duration') or 'Not specified'}\n🛡 Warranty: {product.get('warranty') or 'Not specified'}\n\nTap Buy Now to view and purchase from your bot balance."
        button_text = "🛒 Buy Now"
    else:
        pts = int(product.get("points_price", 0) or 0)
        price_line = "FREE" if pts <= 0 else f"{pts} points"
        text = f"🎁 NEW FREE PRODUCT\n\n📦 {name}\n💎 Price: {price_line}\n📊 Stock: {stock}\n⏳ Duration: {product.get('duration') or 'Not specified'}\n🛡 Warranty: {product.get('warranty') or 'Not specified'}\n\nTap below to view it."
        button_text = "🎁 Get / View Product"
    kb = InlineKeyboardMarkup(row_width=1)
    if oid:
        kb.add(InlineKeyboardButton(button_text, callback_data=f"shopview|{oid}"))
    def worker():
        for row in users_col.find({}, {"_id": 1}):
            try:
                raw_bot.send_message(int(row["_id"]), text, reply_markup=kb if oid else None, disable_notification=False)
                time.sleep(0.04)
            except Exception:
                continue
    threading.Thread(target=worker, daemon=True).start()

@bot.message_handler(
    func=lambda m: m.chat.type in ("group", "supergroup"),
    content_types=["text", "photo", "video", "document", "audio", "animation", "voice"],
)
@bot.channel_post_handler(content_types=["text", "photo", "video", "document", "audio", "animation", "voice"])
def auto_import_channel_post(m):
    _queue_auto_import(m, edited=False)
    if m.chat.type in ("group", "supergroup"):
        moderate_managed_group_message(m)


@bot.edited_message_handler(
    func=lambda m: m.chat.type in ("group", "supergroup"),
    content_types=["text", "photo", "video", "document", "audio", "animation", "voice"],
)
@bot.edited_channel_post_handler(content_types=["text", "photo", "video", "document", "audio", "animation", "voice"])
def auto_import_edited_post(m):
    _queue_auto_import(m, edited=True)

_pending_approval_state = {}


def pending_methods_keyboard(page=0, page_size=10):
    rows = list(pending_methods_col.find({"status": {"$in": ["pending", "pending_update", "pending_replace"]}}).sort("created_at", -1))
    kb = InlineKeyboardMarkup(row_width=1)
    chunk = rows[page * page_size:(page + 1) * page_size]
    for row in chunk:
        icon = "💎" if row.get("cat") == "vip" else "📂"
        kb.add(InlineKeyboardButton(f"{icon} {row.get('name')} • {len(row.get('files', []))} file(s)", callback_data=f"pendingview|{row['_id']}"))
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"pendingpage|{page-1}"))
    if (page + 1) * page_size < len(rows):
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"pendingpage|{page+1}"))
    if nav:
        kb.row(*nav)
    return kb, len(rows)


@bot.message_handler(func=lambda m: m.text == "⏳ Pending Methods" and is_admin(m.from_user.id))
def pending_methods_menu(m):
    kb, count = pending_methods_keyboard()
    raw_bot.send_message(m.from_user.id, f"⏳ PENDING METHODS\n\nWaiting for review: {count}\n\nSelect a method:", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("pendingpage|"))
def pending_page_cb(c):
    if not is_admin(c.from_user.id):
        return
    page = int(c.data.split("|", 1)[1])
    kb, count = pending_methods_keyboard(page)
    raw_bot.edit_message_text(f"⏳ PENDING METHODS\n\nWaiting for review: {count}\n\nSelect a method:", c.from_user.id, c.message.message_id, reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("pendingview|"))
def pending_view_cb(c):
    if not is_admin(c.from_user.id):
        return
    from bson import ObjectId
    try:
        pending_id = ObjectId(c.data.split("|", 1)[1])
        row = pending_methods_col.find_one({"_id": pending_id})
        if not row or row.get("status") not in ("pending", "pending_update", "pending_replace"):
            return bot.answer_callback_query(c.id, "This pending method is no longer available.", True)
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("✅ Approve & Set Price", callback_data=f"pendingapprove|{pending_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"pendingreject|{pending_id}"),
        )
        text = (
            "⏳ METHOD REVIEW\n\n"
            f"🏷 Name: {row.get('name')}\n"
            f"📁 Category: {str(row.get('cat')).upper()}\n"
            f"🏷 Source tag: #method{row.get('auto_method_key')}\n"
            f"📎 Files: {len(row.get('files', []))}\n"
            f"📝 Type: {row.get('status')}\n\n"
            "Approve to choose its point price."
        )
        raw_bot.send_message(c.from_user.id, text, reply_markup=kb)
        bot.answer_callback_query(c.id)
    except Exception as exc:
        bot.answer_callback_query(c.id, str(exc)[:180], True)


@bot.callback_query_handler(func=lambda c: c.data.startswith("pendingapprove|"))
def pending_approve_cb(c):
    if not is_admin(c.from_user.id):
        return
    pending_id = c.data.split("|", 1)[1]
    _pending_approval_state[c.from_user.id] = pending_id
    msg = raw_bot.send_message(c.from_user.id, "💰 Send the price in points for this method.\n\nSend 0 for a free method.")
    bot.register_next_step_handler(msg, pending_price_step)
    bot.answer_callback_query(c.id)


def pending_price_step(m):
    from bson import ObjectId
    try:
        pending_id = _pending_approval_state.pop(m.from_user.id, None)
        if not pending_id:
            raise ValueError("Approval session expired")
        price = int((m.text or "").strip())
        if price < 0:
            raise ValueError("Price cannot be negative")
        row = pending_methods_col.find_one({"_id": ObjectId(pending_id)})
        if not row or row.get("status") not in ("pending", "pending_update", "pending_replace"):
            raise ValueError("Pending method was already processed")

        status = row.get("status")
        existing = None
        if row.get("existing_folder_id"):
            existing = folders_col.find_one({"_id": row.get("existing_folder_id")})
        if not existing:
            existing = folders_col.find_one({"cat": row.get("cat"), "auto_method_key": row.get("auto_method_key")})

        if existing and status in ("pending_update", "pending_replace"):
            update = {"updated_at": now_ts()}
            if status == "pending_replace":
                update.update({"name": row.get("name"), "files": row.get("files", []), "price": price})
            else:
                update.update({"price": price})
            if status == "pending_update":
                folders_col.update_one({"_id": existing["_id"]}, {"$push": {"files": {"$each": row.get("files", [])}}, "$set": update})
            else:
                folders_col.update_one({"_id": existing["_id"]}, {"$set": update})
            folder = folders_col.find_one({"_id": existing["_id"]})
            action = "updated"
        else:
            number = fs.add(row.get("cat"), row.get("name"), row.get("files", []), price, at_start=True)
            folders_col.update_one({"number": number}, {"$set": {
                "auto_method_key": row.get("auto_method_key"), "source_chat": row.get("source_chat"),
                "approved_by": m.from_user.id, "approved_at": now_ts(), "pinned": False,
                "parent": None, "sort_priority": -time.time(),
            }})
            folder = fs.get_by_number(number)
            append_to_manual_methods_list(folder or {"name": row.get("name"), "cat": row.get("cat")})
            action = "approved and published"

        pending_methods_col.update_one({"_id": row["_id"]}, {"$set": {"status": "approved", "price": price, "approved_by": m.from_user.id, "approved_at": now_ts()}})
        send_method_notification(action, folder)
        notify_all_users_about_method(folder, action)
        raw_bot.send_message(
            m.from_user.id,
            f"✅ Process Complete\n\n{row.get('name')} has been {action}.\n💰 Price: {price} points\n📌 It appears below pinned methods and above older unpinned methods.",
            reply_markup=admin_menu(),
        )
        try:
            if get_config().get("group_import_notify_enabled", True):
                _send_source_import_notice(row.get("source_chat"), f"✅ {row.get('name')} was approved and published at {price} points.")
        except Exception:
            pass
    except Exception as exc:
        _pending_approval_state.pop(m.from_user.id, None)
        admin_error(m.from_user.id, exc)


@bot.callback_query_handler(func=lambda c: c.data.startswith("pendingreject|"))
def pending_reject_cb(c):
    if not is_admin(c.from_user.id):
        return
    from bson import ObjectId
    try:
        pending_id = ObjectId(c.data.split("|", 1)[1])
        row = pending_methods_col.find_one_and_update(
            {"_id": pending_id, "status": {"$in": ["pending", "pending_update", "pending_replace"]}},
            {"$set": {"status": "rejected", "rejected_by": c.from_user.id, "rejected_at": now_ts()}},
        )
        if not row:
            raise ValueError("Method was already processed")
        bot.answer_callback_query(c.id, "Rejected")
        raw_bot.send_message(c.from_user.id, f"✅ Process Complete\nRejected: {row.get('name')}", reply_markup=admin_menu())
        try:
            if get_config().get("group_import_notify_enabled", True):
                _send_source_import_notice(row.get("source_chat"), f"❌ {row.get('name')} was rejected by admin and was not published.")
        except Exception:
            pass
    except Exception as exc:
        admin_error(c.from_user.id, exc)


def pin_methods_keyboard(action, page=0, page_size=12):
    query = {"parent": None, "pinned": {"$ne": True}} if action == "pin" else {"parent": None, "pinned": True}
    rows = list(folders_col.find(query).sort([("pinned", -1), ("sort_priority", 1), ("number", 1)]))
    kb = InlineKeyboardMarkup(row_width=1)
    for row in rows[page * page_size:(page + 1) * page_size]:
        icon = "📌" if row.get("pinned") else "📄"
        kb.add(InlineKeyboardButton(f"{icon} {row.get('name')}", callback_data=f"methodpinset|{action}|{row['_id']}"))
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"methodpinpage|{action}|{page-1}"))
    if (page + 1) * page_size < len(rows):
        nav.append(InlineKeyboardButton("➡️", callback_data=f"methodpinpage|{action}|{page+1}"))
    if nav:
        kb.row(*nav)
    return kb, len(rows)


@bot.message_handler(func=lambda m: m.text == "📌 Pin Methods" and is_admin(m.from_user.id))
def pin_methods_menu(m):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.row(InlineKeyboardButton("📂 Pin FREE", callback_data="pinv2cat|free|pin"), InlineKeyboardButton("💎 Pin VIP", callback_data="pinv2cat|vip|pin"))
    kb.row(InlineKeyboardButton("📂 Unpin FREE", callback_data="pinv2cat|free|unpin"), InlineKeyboardButton("💎 Unpin VIP", callback_data="pinv2cat|vip|unpin"))
    raw_bot.send_message(m.from_user.id, "📌 METHOD PLACEMENT\n\nFREE and VIP pins are managed separately. You can pin multiple methods in each category.", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("methodpinmenu|"))
def method_pin_menu_cb(c):
    if not is_admin(c.from_user.id):
        return
    action = c.data.split("|", 1)[1]
    kb, count = pin_methods_keyboard(action)
    bot.send_message(c.from_user.id, f"{'📌 Select a method to pin' if action == 'pin' else '📍 Select a method to unpin'}\n\nAvailable: {count}", reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("methodpinpage|"))
def method_pin_page_cb(c):
    if not is_admin(c.from_user.id):
        return
    _, action, page = c.data.split("|")
    kb, count = pin_methods_keyboard(action, int(page))
    bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("methodpinset|"))
def method_pin_set_cb(c):
    if not is_admin(c.from_user.id):
        return
    from bson import ObjectId
    try:
        _, action, object_id = c.data.split("|")
        value = action == "pin"
        row = folders_col.find_one_and_update({"_id": ObjectId(object_id)}, {"$set": {"pinned": value, "pinned_at": now_ts() if value else None, "pinned_by": c.from_user.id if value else None}}, return_document=ReturnDocument.AFTER)
        if not row:
            raise ValueError("Method not found")
        bot.answer_callback_query(c.id, "Pinned" if value else "Unpinned", True)
        raw_bot.send_message(c.from_user.id, f"✅ Process Complete\n{'📌 Pinned' if value else '📍 Unpinned'}: {row.get('name')}", reply_markup=admin_menu())
    except Exception as exc:
        admin_error(c.from_user.id, exc)


def scheduler_loop():
    while not _scheduler_stop.wait(20):
        try:
            now=now_ts()
            for x in broadcasts_col.find({'status':'scheduled','run_at':{'$lte':now}}).limit(10):
                q={} if x['target']=='all' else {'vip':x['target']=='vip'}; sent=failed=0
                for u in users_col.find(q,{'_id':1}):
                    try: bot.copy_message(int(u['_id']),x['source_chat'],x['source_message']); sent+=1
                    except Exception: failed+=1
                broadcasts_col.update_one({'_id':x['_id']},{'$set':{'status':'sent','sent':sent,'failed':failed,'sent_at':now}}); log_event('broadcast_sent',x.get('created_by'),details={'sent':sent,'failed':failed})
            group_auto = _group_auto_config()
            if group_auto.get('active') and group_auto.get('next_run') and group_auto.get('next_run') <= now:
                sent, failed = _send_group_auto_message(group_auto)
                interval_minutes = max(int(group_auto.get('interval_minutes', 60) or 60), 5)
                _save_group_auto(last_run=now, next_run=now + interval_minutes * 60)
                log_event('group_auto_message_sent', details={'sent': sent, 'failed': failed[:10]})
            for x in auto_posts_col.find({'active':True,'next_run':{'$lte':now}}).limit(20):
                channels = x.get('channels') or [x.get('channel')]
                previous = x.get('last_message_ids') or {}
                new_message_ids = dict(previous)
                for target in channels:
                    try:
                        _delete_previous_auto_messages(target, previous.get(str(target), []))
                        if x.get('payload'):
                            new_message_ids[str(target)] = _send_payload(target, x['payload'])
                        else:
                            new_message_ids[str(target)] = [bot.copy_message(target, x['source_chat'], x['source_message']).message_id]
                        log_event('auto_post_sent', target=target)
                    except Exception as exc:
                        log_event('auto_post_error', target=target, details={'error':str(exc)}, level='error')
                if x['schedule'] == 'every_hours':
                    nxt = now + max(float(x['value']), 0.01) * 3600
                else:
                    hh, mm = map(int, str(x['value']).split(':'))
                    local_now = datetime.utcfromtimestamp(now + TZ_OFFSET_SECONDS)
                    local_next = local_now.replace(hour=hh, minute=mm, second=0, microsecond=0)
                    if local_next <= local_now:
                        local_next += timedelta(days=1)
                    nxt = local_next.timestamp() - TZ_OFFSET_SECONDS
                auto_posts_col.update_one({'_id':x['_id']},{'$set':{'next_run':nxt, 'last_run':now, 'last_message_ids':new_message_ids}})
        except Exception as exc: log_event('scheduler_error',details={'error':str(exc)},level='error')

threading.Thread(target=scheduler_loop,name='globexomart-scheduler',daemon=True).start()



@bot.callback_query_handler(func=lambda c: c.data.startswith("buyid|"))
def buy_method_by_id(c):
    from bson import ObjectId
    try:
        _, oid, price_text = c.data.split("|", 2)
        folder = folders_col.find_one({"_id": ObjectId(oid)})
        if not folder:
            return bot.answer_callback_query(c.id, "Method not found", True)
        user = User(c.from_user.id)
        price = int(price_text)
        name = folder.get("name", "Method")
        if user.is_vip() or user.can_access_method(name):
            return bot.answer_callback_query(c.id, "You already have access", True)
        if user.points() < price:
            return bot.answer_callback_query(c.id, f"Need {price} points. You have {user.points()}", True)
        if not user.purchase_method(name, price):
            raise ValueError("Purchase could not be completed")
        raw_bot.edit_message_text(
            f"🎉 PURCHASE COMPLETE\n\n📄 {name}\n💰 Paid: {price} points\n💎 Remaining: {user.points()} points\n\nOpen the method again to receive its content.",
            c.from_user.id, c.message.message_id
        )
        bot.answer_callback_query(c.id, "Purchased successfully")
    except Exception as exc:
        bot.answer_callback_query(c.id, "Purchase failed", True)
        log_event("buy_method_id_error", c.from_user.id, details={"error": str(exc)}, level="error")

# =========================
# ✅ STABILITY FIXES: SAFE METHOD OPEN, BUTTON VISIBILITY, PINS, NOTIFICATIONS
# =========================
@bot.callback_query_handler(func=lambda c: c.data.startswith("openid|"))
def open_folder_by_id(c):
    if force_block(c.from_user.id):
        return
    try:
        from bson import ObjectId
        folder = folders_col.find_one({"_id": ObjectId(c.data.split("|", 1)[1])})
        if not folder:
            return bot.answer_callback_query(c.id, "Method not found", True)
        # Reuse the existing handler with a safe legacy payload.
        fake = type("SafeCallback", (), {})()
        fake.from_user = c.from_user
        fake.id = c.id
        fake.message = c.message
        fake.data = f"open|{folder.get('cat')}|{folder.get('name')}|{folder.get('parent') or ''}"
        open_folder(fake)
    except Exception as exc:
        bot.answer_callback_query(c.id, "Unable to open method", True)
        log_event("open_method_error", c.from_user.id, details={"error": str(exc)}, level="error")

@bot.callback_query_handler(func=lambda c: c.data.startswith("openbyname|"))
def open_folder_by_name(c):
    try:
        _, cat, name = c.data.split("|", 2)
        row = folders_col.find_one({"cat": cat, "name": name})
        if not row:
            return bot.answer_callback_query(c.id, "Method not found", True)
        c.data = f"openid|{row['_id']}"
        return open_folder_by_id(c)
    except Exception as exc:
        bot.answer_callback_query(c.id, "Unable to open method", True)

def _all_visibility_candidates():
    result = list(MAIN_MENU_BUTTONS)
    for item in get_custom_buttons():
        text = str(item.get("text") or "").strip()
        if text and text not in result:
            result.append(text)
    return result

def _visibility_keyboard(mode, page=0, page_size=12):
    hidden = set(get_config().get("hidden_main_buttons", []) or [])
    all_items = _all_visibility_candidates()
    items = [x for x in all_items if (x not in hidden if mode == "hide" else x in hidden)]
    kb = InlineKeyboardMarkup(row_width=1)
    page_items = items[page*page_size:(page+1)*page_size]
    for absolute_index, text in enumerate(page_items, start=page*page_size):
        icon = "🙈" if mode == "hide" else "👁"
        kb.add(InlineKeyboardButton(f"{icon} {text}", callback_data=f"vis2|{mode}|{absolute_index}|{page}"))
    nav=[]
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"vis2page|{mode}|{page-1}"))
    if (page+1)*page_size < len(items):
        nav.append(InlineKeyboardButton("➡️", callback_data=f"vis2page|{mode}|{page+1}"))
    if nav: kb.row(*nav)
    kb.row(InlineKeyboardButton("🔄 Refresh", callback_data=f"vis2page|{mode}|{page}"))
    return kb, len(items)

@bot.message_handler(func=lambda m: m.text in ("🙈 Hide Button", "👁 Show Button") and is_admin(m.from_user.id))
def visibility_menu(m):
    mode = "hide" if m.text.startswith("🙈") else "show"
    kb, count = _visibility_keyboard(mode)
    title = "🙈 HIDE USER BUTTONS" if mode == "hide" else "👁 SHOW USER BUTTONS"
    note = "Choose a visible button to hide." if mode == "hide" else "Choose a hidden button to restore."
    raw_bot.send_message(m.from_user.id, f"{title}\n━━━━━━━━━━━━━━━━━━━━\n{note}\n\nAvailable: {count}", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("vis2page|"))
def visibility_page_callback(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    try:
        _, mode, page = c.data.split("|")
        kb, count = _visibility_keyboard(mode, int(page))
        bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=kb)
        bot.answer_callback_query(c.id, f"{count} available")
    except Exception as exc:
        admin_error(c.from_user.id, exc)

@bot.callback_query_handler(func=lambda c: c.data.startswith("vis2|"))
def visibility_callback_v2(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    try:
        _, mode, index, page = c.data.split("|")
        hidden = list(get_config().get("hidden_main_buttons", []) or [])
        hidden_set = set(hidden)
        candidates = [x for x in _all_visibility_candidates() if (x not in hidden_set if mode == "hide" else x in hidden_set)]
        idx = int(index)
        if idx < 0 or idx >= len(candidates):
            raise ValueError("Button list changed. Open Hide/Show again")
        text = candidates[idx]
        if mode == "hide":
            if text not in hidden:
                hidden.append(text)
        else:
            hidden = [x for x in hidden if x != text]
        set_config("hidden_main_buttons", hidden)
        # Force immediate cache refresh on every worker path.
        global _config_cache, _config_cache_time
        _config_cache = None
        _config_cache_time = 0
        bot.answer_callback_query(c.id, "Button hidden" if mode == "hide" else "Button restored", True)
        kb, _ = _visibility_keyboard(mode, int(page))
        try:
            bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=kb)
        except Exception:
            pass
        raw_bot.send_message(c.from_user.id, f"✅ Button {'hidden' if mode == 'hide' else 'shown'}: {text}\n\nUsers receive the updated menu on their next bot action or /start.", reply_markup=admin_menu())
    except Exception as exc:
        admin_error(c.from_user.id, exc)

def pin_methods_keyboard_v2(cat, action, page=0, page_size=12):
    query = {"parent": None, "cat": cat}
    query["pinned"] = {"$ne": True} if action == "pin" else True
    rows = list(folders_col.find(query).sort([("pinned", -1), ("pinned_at", -1), ("created_at", -1)]))
    kb = InlineKeyboardMarkup(row_width=1)
    for row in rows[page*page_size:(page+1)*page_size]:
        kb.add(InlineKeyboardButton(("📌 " if row.get("pinned") else "📄 ") + row.get("name", "Unnamed"), callback_data=f"pinv2set|{cat}|{action}|{row['_id']}"))
    nav=[]
    if page>0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"pinv2page|{cat}|{action}|{page-1}"))
    if (page+1)*page_size < len(rows): nav.append(InlineKeyboardButton("➡️", callback_data=f"pinv2page|{cat}|{action}|{page+1}"))
    if nav: kb.row(*nav)
    return kb, len(rows)

# Replace the old Pin Methods experience with category-specific controls.
@bot.callback_query_handler(func=lambda c: c.data.startswith("pinv2cat|"))
def pin_v2_category(c):
    if not is_admin(c.from_user.id): return
    _, cat, action = c.data.split("|")
    kb, count = pin_methods_keyboard_v2(cat, action)
    raw_bot.send_message(c.from_user.id, f"{'FREE' if cat == 'free' else 'VIP'} methods — select one to {'pin' if action == 'pin' else 'unpin'} ({count} available):", reply_markup=kb)
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("pinv2page|"))
def pin_v2_page(c):
    _, cat, action, page = c.data.split("|")
    kb, _ = pin_methods_keyboard_v2(cat, action, int(page))
    bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=kb)
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("pinv2set|"))
def pin_v2_set(c):
    if not is_admin(c.from_user.id): return
    try:
        from bson import ObjectId
        _, cat, action, oid = c.data.split("|")
        value = action == "pin"
        row = folders_col.find_one_and_update({"_id": ObjectId(oid), "cat": cat}, {"$set": {"pinned": value, "pinned_at": now_ts() if value else None, "pinned_by": c.from_user.id if value else None}}, return_document=ReturnDocument.AFTER)
        if not row: raise ValueError("Method not found")
        bot.answer_callback_query(c.id, "Pinned" if value else "Unpinned", True)
        admin_success(c.from_user.id, f"{'Pinned' if value else 'Unpinned'} {cat.upper()} method: {row.get('name')}")
    except Exception as exc:
        admin_error(c.from_user.id, exc)

def _notify_settings_keyboard():
    cfg = get_config()
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(f"📣 Admin/group method alerts: {'ON' if cfg.get('method_notify_enabled', True) else 'OFF'}", callback_data="notifyv2|toggle"))
    kb.add(InlineKeyboardButton(f"👥 User method alerts: {'ON' if cfg.get('user_method_notifications_enabled', True) else 'OFF'}", callback_data="notifyv2|toggleusermethods"))
    kb.add(InlineKeyboardButton(f"🛍 User product alerts: {'ON' if cfg.get('user_product_notifications_enabled', True) else 'OFF'}", callback_data="notifyv2|toggleproducts"))
    kb.add(InlineKeyboardButton(f"📥 Source group replies: {'ON' if cfg.get('group_import_notify_enabled', True) else 'OFF'}", callback_data="notifyv2|togglegroup"))
    delete_on = cfg.get('group_import_notify_auto_delete_enabled', True)
    delete_secs = int(cfg.get('group_import_notify_auto_delete_seconds', 60) or 60)
    kb.add(InlineKeyboardButton(f"🗑 Auto-delete source replies: {'ON' if delete_on else 'OFF'} ({delete_secs}s)", callback_data="notifyv2|toggleautodelete"))
    kb.add(InlineKeyboardButton("🧪 Send test notification", callback_data="notifyv2|test"))
    kb.add(InlineKeyboardButton("👥 Notification groups", callback_data="notifyv2|groups"))
    return kb

def _send_notify_settings(uid):
    cfg = get_config()
    target = cfg.get("method_notify_group") or cfg.get("join_notify_group") or "Not set"
    raw_bot.send_message(uid, f"🔔 NOTIFICATION SETTINGS\n\nMethod upload/update alerts can be switched on or off.\nTarget group: {target}", reply_markup=_notify_settings_keyboard())

@bot.message_handler(func=lambda m: m.text == "🔔 Notify" and is_admin(m.from_user.id))
def notify_settings_v2(m):
    _send_notify_settings(m.from_user.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("notifyv2|"))
def notify_v2_callback(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    action = c.data.split("|", 1)[1]
    try:
        if action == "toggle":
            cfg = get_config()
            new = not cfg.get("method_notify_enabled", True)
            set_config("method_notify_enabled", new)
            bot.answer_callback_query(c.id, "Notifications ON" if new else "Notifications OFF", True)
            try:
                bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=_notify_settings_keyboard())
            except Exception:
                _send_notify_settings(c.from_user.id)
            return
        if action == "toggleusermethods":
            cfg = get_config()
            new = not cfg.get("user_method_notifications_enabled", True)
            set_config("user_method_notifications_enabled", new)
            bot.answer_callback_query(c.id, "User method alerts ON" if new else "User method alerts OFF", True)
            try:
                bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=_notify_settings_keyboard())
            except Exception:
                _send_notify_settings(c.from_user.id)
            return
        if action == "toggleproducts":
            cfg = get_config()
            new = not cfg.get("user_product_notifications_enabled", True)
            set_config("user_product_notifications_enabled", new)
            bot.answer_callback_query(c.id, "User product alerts ON" if new else "User product alerts OFF", True)
            try:
                bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=_notify_settings_keyboard())
            except Exception:
                _send_notify_settings(c.from_user.id)
            return
        if action == "togglegroup":
            cfg = get_config()
            new = not cfg.get("group_import_notify_enabled", True)
            set_config("group_import_notify_enabled", new)
            bot.answer_callback_query(c.id, "Source group replies ON" if new else "Source group replies OFF", True)
            try:
                bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=_notify_settings_keyboard())
            except Exception:
                _send_notify_settings(c.from_user.id)
            return
        if action == "toggleautodelete":
            cfg = get_config()
            new = not cfg.get("group_import_notify_auto_delete_enabled", True)
            set_config("group_import_notify_auto_delete_enabled", new)
            bot.answer_callback_query(c.id, "Auto-delete ON (60s)" if new else "Auto-delete OFF", True)
            try:
                bot.edit_message_reply_markup(c.from_user.id, c.message.message_id, reply_markup=_notify_settings_keyboard())
            except Exception:
                _send_notify_settings(c.from_user.id)
            return
        if action == "groups":
            bot.answer_callback_query(c.id)
            raw_bot.send_message(c.from_user.id, "Choose notification settings:", reply_markup=join_notification_keyboard())
            return
        if action == "test":
            cfg = get_config()
            if not cfg.get("method_notify_enabled", True):
                raise ValueError("Method notifications are OFF. Turn them ON first.")
            target = cfg.get("method_notify_group") or cfg.get("join_notify_group")
            if not target:
                raise ValueError("Set a method notification group first")
            raw_bot.send_message(target, "🔔 Test successful! Method upload and update notifications are working.")
            bot.answer_callback_query(c.id, "Test sent", True)
            admin_success(c.from_user.id, "Test notification sent successfully")
            return
    except Exception as exc:
        bot.answer_callback_query(c.id, "Failed", True)
        admin_error(c.from_user.id, exc)



# =========================
# 🔓 USER RESTRICTION MANAGEMENT
# =========================
def send_restricted_users_picker(admin_id):
    records = list(group_warnings_col.find({"action_taken": {"$exists": True, "$ne": None}}).sort("action_at", -1).limit(100))
    if not records:
        return raw_bot.send_message(admin_id, "✅ No muted or banned users are currently recorded.")
    kb = InlineKeyboardMarkup(row_width=1)
    for rec in records:
        gid = int(rec.get("group_id"))
        uid = int(rec.get("user_id"))
        username = rec.get("username")
        name = rec.get("name") or "User"
        action = str(rec.get("action_taken", "restricted"))
        label = f"{'@'+username if username else name} • {action}"
        kb.add(InlineKeyboardButton(label[:60], callback_data=f"restoremember|{gid}|{uid}"))
    raw_bot.send_message(admin_id, "🔓 SELECT A USER\n\nChoose a person to unmute or unban:", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("restoremember|"))
def restore_group_member_cb(c):
    if not is_admin(c.from_user.id):
        return
    try:
        _, gid_s, uid_s = c.data.split("|", 2)
        gid, uid = int(gid_s), int(uid_s)
        rec = group_warnings_col.find_one({"group_id": gid, "user_id": uid}) or {}
        action = str(rec.get("action_taken", "")).lower()
        if "ban" in action:
            bot.unban_chat_member(gid, uid, only_if_banned=True)
            result = "unbanned"
        else:
            bot.restrict_chat_member(
                gid, uid,
                can_send_messages=True,
                can_send_audios=True,
                can_send_documents=True,
                can_send_photos=True,
                can_send_videos=True,
                can_send_video_notes=True,
                can_send_voice_notes=True,
                can_send_polls=True,
                can_send_other_messages=True,
                can_add_web_page_previews=True,
                can_change_info=False,
                can_invite_users=True,
                can_pin_messages=False,
                can_manage_topics=False,
            )
            result = "unmuted"
        group_warnings_col.update_one(
            {"group_id": gid, "user_id": uid},
            {"$unset": {"action_taken": "", "action_at": ""}, "$set": {"warnings": 0, "restored_at": now_ts(), "restored_by": c.from_user.id}},
        )
        username = rec.get("username")
        display = f"@{username}" if username else rec.get("name") or str(uid)
        bot.answer_callback_query(c.id, f"User {result}", True)
        admin_success(c.from_user.id, f"{display} was successfully {result} in group {gid}")
    except Exception as exc:
        bot.answer_callback_query(c.id, "Failed", True)
        admin_error(c.from_user.id, exc)


# =========================
# 🚨 SCAM REPORT SYSTEM
# =========================
_scam_report_sessions = {}

def _clean_username(value):
    value = (value or "").strip()
    if value.startswith("https://t.me/"):
        value = value.split("https://t.me/", 1)[1].split("/", 1)[0]
    return value.lstrip("@").strip().lower()


def _extract_report_target(message):
    # Replying to a user's message is the most reliable option.
    reply = getattr(message, "reply_to_message", None)
    if reply and getattr(reply, "from_user", None):
        u = reply.from_user
        return u.id, (u.username or "").lower(), (u.first_name or "User")
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)
    arg = parts[1].strip() if len(parts) > 1 else ""
    # Also accept /scammer@username as requested, except when suffix is this bot's own username.
    first = parts[0]
    if not arg and first.lower().startswith("/scammer@"):
        suffix = first.split("@", 1)[1]
        try:
            if suffix.lower() != bot.get_me().username.lower():
                arg = "@" + suffix
        except Exception:
            arg = "@" + suffix
    username = _clean_username(arg)
    if not username:
        return None, None, None
    known = users_col.find_one({"username": {"$regex": f"^{re.escape(username)}$", "$options": "i"}})
    return (int(known["_id"]) if known and str(known.get("_id", "")).isdigit() else None), username, (known.get("first_name") if known else username)


def _scam_status_label(approved, pending):
    if approved > 0:
        return "🚨 DECLARED / APPROVED REPORTS"
    if pending > 0:
        return "⏳ REPORTS PENDING ADMIN REVIEW"
    return "✅ NO APPROVED SCAM DECLARATION"


@bot.message_handler(func=lambda m: bool(m.text) and (m.text.lower() == "/scammer" or m.text.lower().startswith("/scammer ") or m.text.lower().startswith("/scammer@")))
def scammer_report_cmd(m):
    try:
        target_id, username, target_name = _extract_report_target(m)
        if not username and not target_id:
            return raw_bot.send_message(
                m.chat.id,
                "🚨 REPORT A POSSIBLE SCAMMER\n\n"
                "Reply to the person's message with /scammer\n"
                "or send /scammer @username\n\n"
                "Then send the reason and any screenshot, video, document, forwarded message, or text proof.",
                reply_to_message_id=m.message_id,
            )
        if target_id == m.from_user.id or (username and m.from_user.username and username == m.from_user.username.lower()):
            return raw_bot.send_message(m.chat.id, "❌ You cannot report yourself.", reply_to_message_id=m.message_id)
        _scam_report_sessions[m.from_user.id] = {
            "target_id": target_id,
            "username": username or "",
            "target_name": target_name or username or "Unknown",
            "chat_id": m.chat.id,
            "started_at": now_ts(),
        }
        raw_bot.send_message(
            m.chat.id,
            "📝 SCAM REPORT DETAILS\n\n"
            f"Target: {'@'+username if username else target_name}\n\n"
            "Now send one evidence message. It can be text, a photo, video, document, audio, or a forwarded message.\n\n"
            "Send /cancel to stop.",
            reply_to_message_id=m.message_id,
        )
    except Exception as exc:
        raw_bot.send_message(m.chat.id, f"❌ Could not start the report: {str(exc)[:500]}")


@bot.message_handler(func=lambda m: m.from_user is not None and m.from_user.id in _scam_report_sessions, content_types=["text", "photo", "video", "document", "audio", "voice", "animation", "video_note", "sticker"])
def save_scam_report_details(m):
    session = _scam_report_sessions.get(m.from_user.id)
    if not session:
        return
    if (m.text or "").strip().lower() == "/cancel":
        _scam_report_sessions.pop(m.from_user.id, None)
        return raw_bot.send_message(m.chat.id, "❌ Scam report cancelled.")
    try:
        details = (m.text or m.caption or f"{m.content_type} evidence attached").strip()
        report = {
            "reporter_id": int(m.from_user.id),
            "reporter_username": (m.from_user.username or "").lower(),
            "target_user_id": session.get("target_id"),
            "target_username": (session.get("username") or "").lower(),
            "target_name": session.get("target_name") or "Unknown",
            "details": details,
            "evidence_chat_id": int(m.chat.id),
            "evidence_message_id": int(m.message_id),
            "evidence_type": m.content_type,
            "status": "pending",
            "created_at": now_ts(),
        }
        result = scam_reports_col.insert_one(report)
        rid = str(result.inserted_id)
        _scam_report_sessions.pop(m.from_user.id, None)
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("✅ Approve Report", callback_data=f"scamapprove|{rid}"),
            InlineKeyboardButton("❌ Reject Report", callback_data=f"scamreject|{rid}"),
        )
        target_display = f"@{report['target_username']}" if report.get("target_username") else str(report.get("target_user_id") or report.get("target_name"))
        reporter_display = f"@{m.from_user.username}" if m.from_user.username else f"ID {m.from_user.id}"
        admin_text = (
            "🚨 NEW SCAM REPORT\n\n"
            f"Target: {target_display}\n"
            f"Reporter: {reporter_display}\n"
            f"Evidence type: {m.content_type}\n"
            f"Details: {details[:1200]}\n"
            f"Report ID: {rid}"
        )
        delivered = 0
        for admin in get_all_admins():
            try:
                admin_id = int(admin["_id"])
                raw_bot.send_message(admin_id, admin_text, reply_markup=kb)
                try:
                    raw_bot.copy_message(admin_id, m.chat.id, m.message_id)
                except Exception:
                    pass
                delivered += 1
            except Exception as exc:
                log_event("scam_report_admin_delivery_error", m.from_user.id, admin.get("_id"), {"error": str(exc)}, level="error")
        if delivered == 0:
            scam_reports_col.delete_one({"_id": result.inserted_id})
            raise RuntimeError("The report could not be delivered to any admin.")
        raw_bot.send_message(
            m.chat.id,
            "✅ REPORT SUBMITTED\n\n"
            "Your evidence was saved and sent to the admins.\n"
            "Status: ⏳ Pending review\n\n"
            "You will receive a message after approval or rejection.",
        )
    except Exception as exc:
        _scam_report_sessions.pop(m.from_user.id, None)
        raw_bot.send_message(m.chat.id, f"❌ Could not submit the report: {str(exc)[:700]}")


@bot.callback_query_handler(func=lambda c: c.data.startswith("scamapprove|") or c.data.startswith("scamreject|"))
def scam_report_review_cb(c):
    if not is_admin(c.from_user.id):
        return
    try:
        action, rid = c.data.split("|", 1)
        from bson import ObjectId
        report = scam_reports_col.find_one({"_id": ObjectId(rid)})
        if not report:
            raise ValueError("Report not found")
        status = "approved" if action == "scamapprove" else "rejected"
        scam_reports_col.update_one(
            {"_id": report["_id"]},
            {"$set": {"status": status, "reviewed_at": now_ts(), "reviewed_by": c.from_user.id}},
        )
        target = f"@{report.get('target_username')}" if report.get("target_username") else str(report.get("target_user_id") or report.get("target_name"))
        bot.answer_callback_query(c.id, f"Report {status}", True)
        raw_bot.edit_message_text(
            f"{'🚨 APPROVED SCAM REPORT' if status == 'approved' else '❌ REJECTED REPORT'}\n\nTarget: {target}\nReviewed by admin.",
            c.message.chat.id,
            c.message.message_id,
        )
        try:
            raw_bot.send_message(int(report["reporter_id"]), f"📋 REPORT UPDATE\n\nYour report about {target} was {status.upper()} by an admin.")
        except Exception:
            pass
    except Exception as exc:
        bot.answer_callback_query(c.id, "Failed", True)
        admin_error(c.from_user.id, exc)


@bot.message_handler(commands=["scammerlist"])
def scammer_list_cmd(m):
    pipeline = [
        {"$match": {"status": {"$in": ["pending", "approved"]}}},
        {"$group": {
            "_id": {"username": "$target_username", "user_id": "$target_user_id", "name": "$target_name"},
            "approved": {"$sum": {"$cond": [{"$eq": ["$status", "approved"]}, 1, 0]}},
            "pending": {"$sum": {"$cond": [{"$eq": ["$status", "pending"]}, 1, 0]}},
            "total": {"$sum": 1},
        }},
        {"$sort": {"approved": -1, "pending": -1, "total": -1}},
        {"$limit": 100},
    ]
    rows = list(scam_reports_col.aggregate(pipeline))
    if not rows:
        return raw_bot.send_message(m.chat.id, "🛡 SCAMMER RECORDS\n\n✅ No scam reports have been submitted yet.")
    chunks, current = [], "🚨 SCAMMER REPORT LIST\n\n"
    for i, row in enumerate(rows, 1):
        key = row["_id"]
        display = f"@{key.get('username')}" if key.get("username") else key.get("name") or str(key.get("user_id"))
        declared = "🚨 ADMIN DECLARED" if row["approved"] else "⏳ NOT DECLARED — PENDING REVIEW"
        block = f"{i}. {display}\n   {declared}\n   Approved: {row['approved']} | Pending: {row['pending']}\n\n"
        if len(current) + len(block) > 3900:
            chunks.append(current); current = ""
        current += block
    if current: chunks.append(current)
    for chunk in chunks:
        raw_bot.send_message(m.chat.id, chunk)


@bot.message_handler(commands=["check"])
def scam_check_standard_cmd(m):
    if getattr(m, "reply_to_message", None) and getattr(m.reply_to_message, "from_user", None):
        username = (m.reply_to_message.from_user.username or "").lower()
        if not username:
            return raw_bot.send_message(m.chat.id, "❌ That user has no public username, so username-based scam records cannot be checked.")
        m.text = f"/check @{username}"
        return scam_check_cmd(m)
    if not (m.text or "").strip().lower().startswith("/check "):
        return raw_bot.send_message(m.chat.id, "🔎 SCAM CHECK\n\nUse /check @username, or reply to a user's message with /check.")
    return scam_check_cmd(m)


@bot.message_handler(func=lambda m: bool(m.text) and (m.text.lower().startswith("/check ") or (m.text.lower().startswith("/check@") and not m.text.lower().startswith("/check@" + (bot.get_me().username or "").lower()))))
def scam_check_cmd(m):
    text = (m.text or "").strip()
    if text.lower().startswith("/check "):
        target = text.split(maxsplit=1)[1]
    else:
        target = "@" + text.split("@", 1)[1].split()[0]
    username = _clean_username(target)
    if not username:
        return raw_bot.send_message(m.chat.id, "🔎 Use /check @username")
    approved = scam_reports_col.count_documents({"target_username": username, "status": "approved"})
    pending = scam_reports_col.count_documents({"target_username": username, "status": "pending"})
    rejected = scam_reports_col.count_documents({"target_username": username, "status": "rejected"})
    if approved == 0 and pending == 0:
        return raw_bot.send_message(
            m.chat.id,
            f"🛡 SCAM CHECK\n\nUser: @{username}\n\n✅ No scam report is currently recorded for this username.\n\nStay careful and always verify payments independently.",
        )
    status = _scam_status_label(approved, pending)
    raw_bot.send_message(
        m.chat.id,
        f"🚨 SCAM CHECK RESULT\n\nUser: @{username}\nStatus: {status}\n\nApproved reports: {approved}\nPending reports: {pending}\nRejected reports: {rejected}\n\nAdmin declaration is based only on reviewed reports stored in this bot.",
    )


# =========================
# 📢 USER CHANNEL PROMOTION
# =========================
_channel_submit_sessions = {}

def _normalize_channel_reference(value):
    value = (value or "").strip()
    if not value:
        return None, None
    # Public t.me links only. Strip query/path noise.
    value = value.replace("https://telegram.me/", "https://t.me/")
    value = value.replace("http://telegram.me/", "https://t.me/")
    value = value.replace("http://t.me/", "https://t.me/")
    if value.startswith("https://t.me/"):
        tail = value.split("https://t.me/", 1)[1].split("?", 1)[0].strip("/")
        if tail.startswith("+") or tail.startswith("joinchat/"):
            return None, None
        username = tail.split("/", 1)[0]
    else:
        username = value.lstrip("@")
    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", username or ""):
        return None, None
    return "@" + username, "https://t.me/" + username

def _channel_label(doc):
    title = (doc.get("title") or doc.get("username") or "Telegram Channel").strip()
    if len(title) > 45:
        title = title[:42] + "..."
    return title

@bot.message_handler(func=lambda m: m.text == "➕ ADD CHANNEL")
def add_promo_channel_start(m):
    if force_block(m.from_user.id):
        return
    _channel_submit_sessions[m.from_user.id] = {"step": "channel"}
    raw_bot.send_message(
        m.from_user.id,
        "➕ ADD YOUR CHANNEL\n\n"
        "1️⃣ Add this bot as an administrator in your channel.\n"
        "2️⃣ Then send the public channel username or link here.\n\n"
        "Examples:\n@mychannel\nhttps://t.me/mychannel\n\n"
        "Your channel will be checked and sent to the admin for approval. It will not appear publicly before approval.\n\n"
        "Send /cancel to stop.",
        reply_markup=main_menu(m.from_user.id),
        disable_web_page_preview=True,
    )

@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in _channel_submit_sessions and _channel_submit_sessions[m.from_user.id].get("step") == "channel", content_types=["text"])
def receive_promo_channel(m):
    uid = m.from_user.id
    if (m.text or "").strip().lower() == "/cancel":
        _channel_submit_sessions.pop(uid, None)
        return raw_bot.send_message(uid, "❌ Channel submission cancelled.", reply_markup=main_menu(uid))
    username, join_url = _normalize_channel_reference(m.text)
    if not username:
        return raw_bot.send_message(uid, "❌ Send a valid public channel username or t.me link.\nExample: @mychannel")
    try:
        chat = bot.get_chat(username)
        if getattr(chat, "type", None) != "channel":
            return raw_bot.send_message(uid, "❌ This is not a Telegram channel. Please send a channel username/link.")
        me = bot.get_me()
        member = bot.get_chat_member(chat.id, me.id)
        if member.status not in ("administrator", "creator"):
            return raw_bot.send_message(uid, "❌ The bot is not an administrator in that channel yet.\n\nMake the bot admin, then send the channel again.")
        owner_member = None
        try:
            owner_member = bot.get_chat_member(chat.id, uid)
        except Exception:
            pass
        if owner_member and owner_member.status not in ("administrator", "creator"):
            return raw_bot.send_message(uid, "❌ You must be an administrator of the submitted channel.")

        existing = promoted_channels_col.find_one({"chat_id": int(chat.id), "status": {"$in": ["pending", "approved"]}})
        if existing:
            _channel_submit_sessions.pop(uid, None)
            status = existing.get("status", "pending")
            return raw_bot.send_message(uid, f"ℹ️ This channel is already {status}.", reply_markup=main_menu(uid))

        doc = {
            "chat_id": int(chat.id),
            "username": "@" + (chat.username or username.lstrip("@")),
            "join_url": "https://t.me/" + (chat.username or username.lstrip("@")),
            "title": getattr(chat, "title", None) or username,
            "submitted_by": str(uid),
            "submitted_by_username": getattr(m.from_user, "username", None),
            "submitted_by_name": " ".join(filter(None, [getattr(m.from_user, "first_name", None), getattr(m.from_user, "last_name", None)])),
            "status": "pending",
            "submitted_at": time.time(),
        }
        result = promoted_channels_col.insert_one(doc)
        _channel_submit_sessions.pop(uid, None)
        raw_bot.send_message(uid, "✅ CHANNEL SUBMITTED\n\nYour channel is pending admin approval. You will be notified after review.", reply_markup=main_menu(uid))
        kb = InlineKeyboardMarkup(row_width=2)
        rid = str(result.inserted_id)
        kb.add(
            InlineKeyboardButton("✅ Approve", callback_data=f"chanapprove|{rid}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"chanreject|{rid}"),
        )
        submitter = "@" + m.from_user.username if m.from_user.username else str(uid)
        for admin in get_all_admins():
            aid = int(admin.get("_id"))
            try:
                raw_bot.send_message(
                    aid,
                    f"📣 CHANNEL APPROVAL REQUEST\n\nChannel: {doc['title']}\nUsername: {doc['username']}\nSubmitted by: {submitter}\nUser ID: {uid}",
                    reply_markup=kb,
                    disable_web_page_preview=True,
                )
            except Exception:
                pass
    except Exception as exc:
        raw_bot.send_message(uid, f"❌ Could not verify that channel.\n\nMake sure the username is correct and the bot is an administrator.\nError: {str(exc)[:300]}")

@bot.message_handler(func=lambda m: m.text == "📢 CHANNELS")
def public_channels_list(m):
    if force_block(m.from_user.id):
        return
    channels = list(promoted_channels_col.find({"status": "approved"}).sort([("approved_at", -1), ("submitted_at", -1)]).limit(100))
    if not channels:
        return raw_bot.send_message(m.from_user.id, "📢 COMMUNITY CHANNELS\n\nNo user channels have been approved yet.", reply_markup=main_menu(m.from_user.id))
    kb = InlineKeyboardMarkup(row_width=1)
    for ch in channels:
        url = ch.get("join_url")
        if url:
            kb.add(InlineKeyboardButton("📢 " + _channel_label(ch), url=url))
    raw_bot.send_message(
        m.from_user.id,
        f"📢 COMMUNITY CHANNELS\n\nExplore {len(channels)} admin-approved channel{'s' if len(channels) != 1 else ''}.\n\nTap a button below to join:",
        reply_markup=kb,
        disable_web_page_preview=True,
    )


# =========================
# 📨 GROUP MESSENGER
# =========================
_group_message_targets = {}


def _group_messenger_picker(admin_id):
    groups = _managed_group_ids()
    if not groups:
        return raw_bot.send_message(admin_id, "❌ No managed groups are configured. Add groups from Group Management first.")
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("📣 Send to ALL managed groups", callback_data="groupmsg|all"))
    for gid in groups:
        try:
            chat = bot.get_chat(gid)
            title = chat.title or str(gid)
        except Exception:
            title = str(gid)
        kb.add(InlineKeyboardButton(f"💬 {title}", callback_data=f"groupmsg|{gid}"))
    raw_bot.send_message(admin_id, "📨 GROUP MESSENGER\n\nChoose one group or send to all managed groups:", reply_markup=kb)


def _group_auto_config():
    doc = config_col.find_one({"_id": "group_auto_message"}) or {}
    return {
        "_id": "group_auto_message",
        "active": bool(doc.get("active", False)),
        "targets": doc.get("targets", []),
        "source_chat": doc.get("source_chat"),
        "source_message": doc.get("source_message"),
        "interval_minutes": int(doc.get("interval_minutes", 60) or 60),
        "next_run": doc.get("next_run"),
        "last_run": doc.get("last_run"),
        "updated_at": doc.get("updated_at"),
    }


def _save_group_auto(**updates):
    updates["updated_at"] = now_ts()
    config_col.update_one({"_id": "group_auto_message"}, {"$set": updates}, upsert=True)


def _group_auto_status_text():
    cfg = _group_auto_config()
    target_count = len(cfg.get("targets") or [])
    interval = cfg.get("interval_minutes", 60)
    hours, minutes = divmod(interval, 60)
    interval_text = []
    if hours:
        interval_text.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes:
        interval_text.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    if not interval_text:
        interval_text = ["not set"]
    next_text = "Not scheduled"
    if cfg.get("next_run"):
        try:
            next_text = datetime.fromtimestamp(cfg["next_run"]).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    return (
        "🤖 AUTO GROUP MESSAGE\n\n"
        f"Status: {'🟢 ON' if cfg.get('active') else '🔴 OFF'}\n"
        f"Message: {'✅ Saved' if cfg.get('source_message') else '❌ Not set'}\n"
        f"Groups: {target_count}\n"
        f"Interval: {' '.join(interval_text)}\n"
        f"Next send: {next_text}"
    )


def _group_messenger_menu_kb():
    cfg = _group_auto_config()
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📨 Send Now", callback_data="groupmsgmenu|sendnow"),
        InlineKeyboardButton("✏️ Set Auto Message", callback_data="groupmsgmenu|setmsg"),
    )
    kb.add(
        InlineKeyboardButton("👥 Select Groups", callback_data="groupmsgmenu|targets"),
        InlineKeyboardButton("⏱ Set Interval", callback_data="groupmsgmenu|interval"),
    )
    kb.add(
        InlineKeyboardButton("🟢 Turn ON" if not cfg.get("active") else "🔴 Turn OFF", callback_data="groupmsgmenu|toggle"),
        InlineKeyboardButton("🧪 Send Test", callback_data="groupmsgmenu|test"),
    )
    kb.add(InlineKeyboardButton("🔄 Refresh", callback_data="groupmsgmenu|refresh"))
    return kb


@bot.message_handler(func=lambda m: m.text == "📨 Group Messenger" and is_admin(m.from_user.id))
def group_messenger_menu(m):
    raw_bot.send_message(m.from_user.id, _group_auto_status_text(), reply_markup=_group_messenger_menu_kb())


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupmsgmenu|"))
def group_messenger_menu_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    action = c.data.split("|", 1)[1]
    try:
        if action == "sendnow":
            bot.answer_callback_query(c.id)
            return _group_messenger_picker(c.from_user.id)
        if action == "setmsg":
            msg = raw_bot.send_message(c.from_user.id, "✏️ Send or forward the message that should be posted automatically to groups.\n\nText, photo, video, document, audio, animation and voice are supported.\nSend /cancel to stop.")
            bot.register_next_step_handler(msg, save_group_auto_message)
            return bot.answer_callback_query(c.id, "Send the auto message")
        if action == "targets":
            groups = _managed_group_ids()
            if not groups:
                return bot.answer_callback_query(c.id, "No managed groups configured", True)
            kb = InlineKeyboardMarkup(row_width=1)
            kb.add(InlineKeyboardButton("📣 ALL managed groups", callback_data="groupautotarget|all"))
            for gid in groups:
                try:
                    title = bot.get_chat(gid).title or str(gid)
                except Exception:
                    title = str(gid)
                kb.add(InlineKeyboardButton(f"💬 {title}", callback_data=f"groupautotarget|{gid}"))
            raw_bot.send_message(c.from_user.id, "👥 Choose the group for automatic messages, or select all groups:", reply_markup=kb)
            return bot.answer_callback_query(c.id)
        if action == "interval":
            msg = raw_bot.send_message(c.from_user.id, "⏱ Enter how often the automatic message should be sent.\n\nExamples:\n30m\n2h\n1d\n90 (minutes)\n\nMinimum: 5 minutes")
            bot.register_next_step_handler(msg, save_group_auto_interval)
            return bot.answer_callback_query(c.id, "Enter the interval")
        if action == "toggle":
            cfg = _group_auto_config()
            new_state = not cfg.get("active")
            if new_state and (not cfg.get("source_message") or not cfg.get("targets")):
                return bot.answer_callback_query(c.id, "Set the message and groups first", True)
            updates = {"active": new_state}
            if new_state:
                updates["next_run"] = now_ts() + max(cfg.get("interval_minutes", 60), 5) * 60
            _save_group_auto(**updates)
            bot.answer_callback_query(c.id, "Auto messages enabled" if new_state else "Auto messages disabled", True)
        elif action == "test":
            cfg = _group_auto_config()
            if not cfg.get("source_message") or not cfg.get("targets"):
                return bot.answer_callback_query(c.id, "Set the message and groups first", True)
            sent, failed = _send_group_auto_message(cfg)
            bot.answer_callback_query(c.id, f"Sent to {sent} group(s)" if sent else "Test failed", True)
            if failed:
                raw_bot.send_message(c.from_user.id, "⚠️ Test failures:\n" + "\n".join(failed[:10]))
        elif action == "refresh":
            bot.answer_callback_query(c.id)
        try:
            bot.edit_message_text(_group_auto_status_text(), c.from_user.id, c.message.message_id, reply_markup=_group_messenger_menu_kb())
        except Exception:
            raw_bot.send_message(c.from_user.id, _group_auto_status_text(), reply_markup=_group_messenger_menu_kb())
    except Exception as exc:
        admin_error(c.from_user.id, exc)


def save_group_auto_message(m):
    if not is_admin(m.from_user.id):
        return
    if (m.text or "").strip().lower() == "/cancel":
        return raw_bot.send_message(m.from_user.id, "❌ Auto-message setup cancelled.", reply_markup=admin_menu())
    _save_group_auto(source_chat=m.chat.id, source_message=m.message_id, active=False)
    raw_bot.send_message(m.from_user.id, "✅ PROCESS COMPLETE\n\nThe automatic group message has been saved. Select groups and interval, then turn it ON.", reply_markup=_group_messenger_menu_kb())


def _parse_interval_minutes(value):
    text = (value or "").strip().lower().replace(" ", "")
    if not text:
        raise ValueError("Interval is required")
    multiplier = 1
    if text.endswith("m"):
        text = text[:-1]
    elif text.endswith("h"):
        multiplier = 60
        text = text[:-1]
    elif text.endswith("d"):
        multiplier = 1440
        text = text[:-1]
    amount = float(text)
    minutes = int(amount * multiplier)
    if minutes < 5:
        raise ValueError("Minimum interval is 5 minutes")
    if minutes > 525600:
        raise ValueError("Maximum interval is 365 days")
    return minutes


def save_group_auto_interval(m):
    if not is_admin(m.from_user.id):
        return
    try:
        minutes = _parse_interval_minutes(m.text)
        _save_group_auto(interval_minutes=minutes, next_run=now_ts() + minutes * 60, active=False)
        raw_bot.send_message(m.from_user.id, f"✅ PROCESS COMPLETE\n\nAutomatic group message interval set to {minutes} minute{'s' if minutes != 1 else ''}.\nTurn it ON when ready.", reply_markup=_group_messenger_menu_kb())
    except Exception as exc:
        raw_bot.send_message(m.from_user.id, f"❌ PROCESS FAILED\n\n{exc}", reply_markup=_group_messenger_menu_kb())


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupautotarget|"))
def group_auto_target_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    target = c.data.split("|", 1)[1]
    if target == "all":
        targets = _managed_group_ids()
    else:
        try:
            targets = [int(target)]
        except Exception:
            return bot.answer_callback_query(c.id, "Invalid group", True)
    if not targets:
        return bot.answer_callback_query(c.id, "No groups configured", True)
    _save_group_auto(targets=targets, active=False)
    bot.answer_callback_query(c.id, f"Selected {len(targets)} group(s)", True)
    raw_bot.send_message(c.from_user.id, "✅ PROCESS COMPLETE\n\nAutomatic message destination updated.", reply_markup=_group_messenger_menu_kb())


def _send_group_auto_message(cfg):
    sent = 0
    failed = []
    for gid in cfg.get("targets") or []:
        try:
            raw_bot.copy_message(int(gid), int(cfg["source_chat"]), int(cfg["source_message"]))
            sent += 1
        except Exception as exc:
            failed.append(f"{gid}: {str(exc)[:120]}")
    return sent, failed


@bot.callback_query_handler(func=lambda c: c.data.startswith("groupmsg|"))
def group_messenger_target_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    target = c.data.split("|", 1)[1]
    if target == "all":
        targets = _managed_group_ids()
    else:
        try:
            targets = [int(target)]
        except Exception:
            return bot.answer_callback_query(c.id, "Invalid group", True)
    if not targets:
        return bot.answer_callback_query(c.id, "No groups configured", True)
    _group_message_targets[c.from_user.id] = targets
    msg = raw_bot.send_message(
        c.from_user.id,
        f"📨 Send or forward the message now.\n\nIt will be delivered to {len(targets)} group{'s' if len(targets) != 1 else ''}.\nYou may send text, photo, video, document, audio, animation, voice, or a forwarded post.\n\nSend /cancel to stop."
    )
    bot.register_next_step_handler(msg, deliver_group_message)
    bot.answer_callback_query(c.id, "Send the message now")


def deliver_group_message(m):
    if not is_admin(m.from_user.id):
        return
    targets = _group_message_targets.pop(m.from_user.id, None)
    if not targets:
        return raw_bot.send_message(m.from_user.id, "❌ Session expired. Open Group Messenger again.")
    if (m.text or "").strip().lower() == "/cancel":
        return raw_bot.send_message(m.from_user.id, "❌ Group message cancelled.", reply_markup=admin_menu())
    sent = 0
    failed = []
    for gid in targets:
        try:
            raw_bot.copy_message(gid, m.chat.id, m.message_id)
            sent += 1
        except Exception as exc:
            failed.append(f"{gid}: {str(exc)[:120]}")
    if sent:
        text = f"✅ PROCESS COMPLETE\n\nMessage delivered to {sent}/{len(targets)} group{'s' if len(targets) != 1 else ''}."
        if failed:
            text += "\n\n⚠️ Failed:\n" + "\n".join(failed[:10])
        raw_bot.send_message(m.from_user.id, text, reply_markup=admin_menu())
    else:
        raw_bot.send_message(m.from_user.id, "❌ PROCESS FAILED\n\nCould not deliver the message.\n" + "\n".join(failed[:10]), reply_markup=admin_menu())


_channel_message_targets = {}
_channel_message_drafts = {}

def _approved_channel_picker(admin_id):
    docs = list(promoted_channels_col.find({"status": "approved"}).sort("approved_at", -1).limit(100))
    if not docs:
        return raw_bot.send_message(admin_id, "❌ No approved channels are available.")
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("📣 Send to ALL approved channels", callback_data="chanmsg|all"))
    for doc in docs:
        kb.add(InlineKeyboardButton("📢 " + _channel_label(doc), callback_data=f"chanmsg|{doc['_id']}"))
    raw_bot.send_message(admin_id, "📨 CHANNEL MESSENGER\n\nChoose one approved channel or send to all approved channels:", reply_markup=kb)


@bot.message_handler(func=lambda m: m.text == "📣 Channel Approvals" and is_admin(m.from_user.id))
def channel_approvals_menu(m):
    pending = list(promoted_channels_col.find({"status": "pending"}).sort("submitted_at", 1).limit(50))
    kb = InlineKeyboardMarkup(row_width=1)
    for ch in pending:
        kb.add(InlineKeyboardButton(f"⏳ {_channel_label(ch)}", callback_data=f"chanreview|{ch['_id']}"))
    kb.add(InlineKeyboardButton("📋 Approved Channels", callback_data="chanapprovedlist"))
    kb.add(InlineKeyboardButton("📨 Channel Messenger", callback_data="chanmessenger"))
    raw_bot.send_message(m.from_user.id, f"📣 CHANNEL APPROVALS\n\nPending: {len(pending)}", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("chanreview|"))
def channel_review_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    from bson import ObjectId
    try:
        doc = promoted_channels_col.find_one({"_id": ObjectId(c.data.split("|", 1)[1])})
    except Exception:
        doc = None
    if not doc:
        return bot.answer_callback_query(c.id, "Channel record not found", True)
    kb = InlineKeyboardMarkup(row_width=2)
    rid = str(doc["_id"])
    if doc.get("status") == "pending":
        kb.add(InlineKeyboardButton("✅ Approve", callback_data=f"chanapprove|{rid}"), InlineKeyboardButton("❌ Reject", callback_data=f"chanreject|{rid}"))
    elif doc.get("status") == "approved":
        kb.add(
            InlineKeyboardButton("📨 Send Message", callback_data=f"chanmsg|{rid}"),
            InlineKeyboardButton("🗑 Remove", callback_data=f"chanremove|{rid}"),
        )
        kb.add(InlineKeyboardButton("❌ Delete Permanently", callback_data=f"chandeleteask|{rid}"))
    if doc.get("join_url"):
        kb.add(InlineKeyboardButton("🔗 Open Channel", url=doc["join_url"]))
    raw_bot.send_message(c.from_user.id, f"📣 CHANNEL REVIEW\n\nTitle: {doc.get('title')}\nUsername: {doc.get('username')}\nStatus: {doc.get('status')}\nSubmitted by: {doc.get('submitted_by_username') or doc.get('submitted_by')}", reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data == "chanmessenger")
def channel_messenger_open_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    bot.answer_callback_query(c.id)
    _approved_channel_picker(c.from_user.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("chanmsg|"))
def channel_message_target_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    from bson import ObjectId
    target = c.data.split("|", 1)[1]
    if target == "all":
        docs = list(promoted_channels_col.find({"status": "approved"}, {"chat_id": 1, "title": 1}))
    else:
        try:
            doc = promoted_channels_col.find_one({"_id": ObjectId(target), "status": "approved"})
        except Exception:
            doc = None
        docs = [doc] if doc else []
    targets = [int(d["chat_id"]) for d in docs if d and d.get("chat_id") is not None]
    if not targets:
        return bot.answer_callback_query(c.id, "No approved channel found", True)
    _channel_message_targets[c.from_user.id] = targets
    _channel_message_drafts.pop(c.from_user.id, None)
    msg = raw_bot.send_message(
        c.from_user.id,
        f"📨 CHANNEL MESSAGE\n\nSend or forward the message now. It will be delivered to {len(targets)} channel{'s' if len(targets) != 1 else ''}.\n\nYou can send text, photo, video, document, audio, voice, animation, or a forwarded post.\n\nSend /cancel to stop."
    )
    bot.register_next_step_handler(msg, receive_channel_message_content)
    bot.answer_callback_query(c.id, "Send the message now")


def receive_channel_message_content(m):
    if not is_admin(m.from_user.id):
        return
    targets = _channel_message_targets.get(m.from_user.id)
    if not targets:
        return raw_bot.send_message(m.from_user.id, "❌ Session expired. Open Channel Messenger again.")
    if (m.text or "").strip().lower() == "/cancel":
        _channel_message_targets.pop(m.from_user.id, None)
        _channel_message_drafts.pop(m.from_user.id, None)
        return raw_bot.send_message(m.from_user.id, "❌ Channel message cancelled.", reply_markup=admin_menu())
    _channel_message_drafts[m.from_user.id] = {
        "source_chat": m.chat.id,
        "source_message": m.message_id,
    }
    msg = raw_bot.send_message(
        m.from_user.id,
        "🔘 ADD BUTTONS\n\nSend buttons using one button per line:\n\nButton Name | https://example.com\nSecond Button | https://t.me/username\n\nSend `skip` to send without buttons, or /cancel to stop.",
        parse_mode=None,
        disable_web_page_preview=True,
    )
    bot.register_next_step_handler(msg, receive_channel_message_buttons)


def _parse_channel_buttons(text):
    value = (text or "").strip()
    if not value or value.lower() == "skip":
        return None, None
    kb = InlineKeyboardMarkup(row_width=1)
    count = 0
    for raw_line in value.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "|" not in line:
            return None, "Use: Button Name | https://example.com"
        label, url = [x.strip() for x in line.split("|", 1)]
        if not label or not re.match(r"^https?://", url, re.I):
            return None, f"Invalid button line: {line[:80]}"
        kb.add(InlineKeyboardButton(label[:64], url=url))
        count += 1
        if count >= 8:
            break
    if count == 0:
        return None, "No valid buttons found."
    return kb, None


def receive_channel_message_buttons(m):
    uid = m.from_user.id
    if not is_admin(uid):
        return
    targets = _channel_message_targets.pop(uid, None)
    draft = _channel_message_drafts.pop(uid, None)
    if not targets or not draft:
        return raw_bot.send_message(uid, "❌ Session expired. Open Channel Messenger again.")
    if (m.text or "").strip().lower() == "/cancel":
        return raw_bot.send_message(uid, "❌ Channel message cancelled.", reply_markup=admin_menu())
    kb, error = _parse_channel_buttons(m.text)
    if error:
        # restore session and ask again
        _channel_message_targets[uid] = targets
        _channel_message_drafts[uid] = draft
        msg = raw_bot.send_message(uid, f"❌ {error}\n\nTry again, send `skip`, or /cancel.", parse_mode=None)
        bot.register_next_step_handler(msg, receive_channel_message_buttons)
        return
    sent = 0
    failed = []
    for chat_id in targets:
        try:
            raw_bot.copy_message(
                int(chat_id),
                int(draft["source_chat"]),
                int(draft["source_message"]),
                reply_markup=kb,
            )
            sent += 1
        except Exception as exc:
            failed.append(f"{chat_id}: {str(exc)[:120]}")
    text = f"✅ PROCESS COMPLETE\n\nMessage delivered to {sent}/{len(targets)} channel{'s' if len(targets) != 1 else ''}."
    if failed:
        text += "\n\n⚠️ Failed:\n" + "\n".join(failed[:10])
    raw_bot.send_message(uid, text, reply_markup=admin_menu())


@bot.callback_query_handler(func=lambda c: c.data.startswith("chandeleteask|"))
def channel_delete_ask_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    rid = c.data.split("|", 1)[1]
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Yes, Delete", callback_data=f"chandelete|{rid}"),
        InlineKeyboardButton("↩️ Cancel", callback_data=f"chanreview|{rid}"),
    )
    raw_bot.send_message(c.from_user.id, "⚠️ PERMANENT DELETE\n\nThis removes the channel record completely from the bot and public Channels list. Continue?", reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("chandelete|"))
def channel_delete_confirm_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    from bson import ObjectId
    try:
        oid = ObjectId(c.data.split("|", 1)[1])
    except Exception:
        return bot.answer_callback_query(c.id, "Invalid record", True)
    doc = promoted_channels_col.find_one({"_id": oid})
    if not doc:
        return bot.answer_callback_query(c.id, "Channel already deleted", True)
    promoted_channels_col.delete_one({"_id": oid})
    try:
        raw_bot.send_message(int(doc.get("submitted_by")), f"🗑 CHANNEL REMOVED\n\n{doc.get('title') or doc.get('username')} was permanently removed from the bot by an admin.", reply_markup=main_menu(int(doc.get("submitted_by"))))
    except Exception:
        pass
    try:
        bot.edit_message_text("✅ Channel permanently deleted from the bot.", c.message.chat.id, c.message.message_id)
    except Exception:
        raw_bot.send_message(c.from_user.id, "✅ Channel permanently deleted from the bot.")
    bot.answer_callback_query(c.id, "Deleted")


def send_approved_channel_promo(channel_doc):
    """Post the bot promotion in a newly approved channel."""
    chat_id = int(channel_doc["chat_id"])
    try:
        bot_username = bot.get_me().username or "globexomartxbot"
    except Exception:
        bot_username = "globexomartxbot"
    bot_url = f"https://t.me/{bot_username}"
    text = (
        "🚀 JOIN GLOBEXOMART VIP BOT\n\n"
        "Discover the best channels, premium methods, accounts, private material, "
        "exclusive updates and useful digital resources — all in one place.\n\n"
        f"🤖 @{bot_username}"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🚀 Open GLOBEXOMART VIP Bot", url=bot_url))
    message = raw_bot.send_message(chat_id, text, reply_markup=kb, disable_web_page_preview=True)
    promoted_channels_col.update_one(
        {"_id": channel_doc["_id"]},
        {"$set": {"promo_message_id": getattr(message, "message_id", None), "promo_sent_at": time.time()}}
    )
    return message


@bot.callback_query_handler(func=lambda c: c.data.startswith(("chanapprove|", "chanreject|", "chanremove|")))
def channel_decision_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    from bson import ObjectId
    action, rid = c.data.split("|", 1)
    try:
        oid = ObjectId(rid)
    except Exception:
        return bot.answer_callback_query(c.id, "Invalid record", True)
    doc = promoted_channels_col.find_one({"_id": oid})
    if not doc:
        return bot.answer_callback_query(c.id, "Channel record not found", True)
    now = time.time()
    if action == "chanapprove":
        # Re-check that the bot remains an admin before approval.
        try:
            member = bot.get_chat_member(int(doc["chat_id"]), bot.get_me().id)
            if member.status not in ("administrator", "creator"):
                return bot.answer_callback_query(c.id, "Bot is no longer admin in this channel", True)
        except Exception as exc:
            return bot.answer_callback_query(c.id, f"Verification failed: {str(exc)[:100]}", True)
        promoted_channels_col.update_one({"_id": oid}, {"$set": {"status": "approved", "approved_at": now, "approved_by": str(c.from_user.id)}})
        doc = promoted_channels_col.find_one({"_id": oid}) or doc
        try:
            send_approved_channel_promo(doc)
            promo_note = " Promotional post sent successfully."
        except Exception as exc:
            promo_note = f" Channel approved, but promotional post failed: {str(exc)[:140]}"
        status_text = "approved"
        admin_text = "✅ Channel approved and added to CHANNELS." + promo_note
    elif action == "chanreject":
        promoted_channels_col.update_one({"_id": oid}, {"$set": {"status": "rejected", "reviewed_at": now, "reviewed_by": str(c.from_user.id)}})
        status_text = "rejected"
        admin_text = "❌ Channel rejected."
    else:
        promoted_channels_col.update_one({"_id": oid}, {"$set": {"status": "removed", "removed_at": now, "removed_by": str(c.from_user.id)}})
        status_text = "removed"
        admin_text = "🗑 Channel removed from public list."
    try:
        raw_bot.send_message(int(doc["submitted_by"]), f"📢 CHANNEL REVIEW RESULT\n\n{doc.get('title')} has been {status_text} by the admin.", reply_markup=main_menu(int(doc["submitted_by"])))
    except Exception:
        pass
    try:
        bot.edit_message_text(admin_text, c.message.chat.id, c.message.message_id)
    except Exception:
        raw_bot.send_message(c.from_user.id, admin_text)
    bot.answer_callback_query(c.id, "Process complete")

@bot.callback_query_handler(func=lambda c: c.data == "chanapprovedlist")
def approved_channels_admin_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    docs = list(promoted_channels_col.find({"status": "approved"}).sort("approved_at", -1).limit(100))
    kb = InlineKeyboardMarkup(row_width=1)
    for doc in docs:
        kb.add(InlineKeyboardButton("✅ " + _channel_label(doc), callback_data=f"chanreview|{doc['_id']}"))
    raw_bot.send_message(c.from_user.id, f"📋 APPROVED CHANNELS\n\nTotal: {len(docs)}", reply_markup=kb)
    bot.answer_callback_query(c.id)



# =========================
# 💎 GLOBEXOMART SUBSCRIPTIONS / TIMED ACCESS
# =========================
SUBSCRIPTION_PLANS = {
    "1M": {"days": 30, "price": 25},
    "2M": {"days": 60, "price": 40},
    "4M": {"days": 120, "price": 60},
    "1Y": {"days": 365, "price": 100},
}


def get_subscription_plans():
    cfg = get_cached_config()
    plans = cfg.get("subscription_plans") or SUBSCRIPTION_PLANS
    normalized = {}
    for code, row in plans.items():
        try:
            key = str(code).upper()[:24]
            if row.get("duration_minutes") is not None:
                duration_minutes = max(1, int(row.get("duration_minutes")))
            else:
                duration_minutes = max(1, int(row.get("days", 1)) * 1440)
            normalized[key] = {
                "duration_minutes": duration_minutes,
                "days": duration_minutes / 1440.0,
                "price": max(0.0, float(row["price"])),
                "name": str(row.get("name") or key)[:40],
                "active": bool(row.get("active", True)),
                "discount_percent": max(0.0, min(100.0, float(row.get("discount_percent", 0) or 0))),
            }
        except Exception:
            continue
    if normalized:
        return normalized
    return {k: {**v, "duration_minutes": int(v.get("days", 1))*1440, "name": k, "active": True, "discount_percent": 0.0} for k, v in SUBSCRIPTION_PLANS.items()}


def _format_duration_minutes(minutes):
    minutes = max(1, int(minutes or 1))
    if minutes < 60:
        return f"{minutes} min"
    if minutes < 1440:
        hours = minutes / 60.0
        return f"{int(hours)} hour" + ("" if hours == 1 else "s") if hours.is_integer() else f"{hours:g} hours"
    days = minutes / 1440.0
    return f"{int(days)} day" + ("" if days == 1 else "s") if days.is_integer() else f"{days:g} days"


def _plan_rank(row):
    return int(row.get("duration_minutes", max(1, int(float(row.get("days", 1))*1440))) or 1)


def _sorted_active_plans(highest_first=True):
    rows = [(code, row) for code, row in get_subscription_plans().items() if row.get("active", True)]
    return sorted(rows, key=lambda item: (_plan_rank(item[1]), float(item[1].get("price", 0))), reverse=highest_first)


def get_access_chats():
    cfg = get_cached_config()
    out = []
    for item in cfg.get("access_chats", []) or []:
        try:
            out.append(normalize_chat_reference(item))
        except Exception:
            pass
    return list(dict.fromkeys(out))


def _plan_text():
    plans = get_subscription_plans()
    order = ["1M", "2M", "4M", "1Y"]
    labels = {"1M": "1 Month", "2M": "2 Months", "4M": "4 Months", "1Y": "1 Year"}
    lines = []
    for code in order:
        if code in plans:
            price = plans[code]["price"]
            price_text = str(int(price)) if float(price).is_integer() else str(price)
            lines.append(f"• {labels[code]} — ${price_text}")
    return "\n".join(lines)


def _active_subscription(uid):
    uid = int(uid)
    now = time.time()
    return subscriptions_col.find_one({
        "user_id": uid,
        "status": "active",
        "$or": [{"expires_at": None}, {"expires_at": {"$gt": now}}],
    }, sort=[("expires_at", -1)])


def _sync_user_vip(uid, expires_at):
    uid_s = str(uid)
    users_col.update_one(
        {"_id": uid_s},
        {"$set": {"vip": True, "vip_expiry": expires_at, "last_active": time.time()}},
        upsert=True,
    )
    User._cache.pop(uid_s, None)
    User._cache_time.pop(uid_s, None)


def _grant_chat_access(uid):
    results = []
    for chat in get_access_chats():
        try:
            # If the user was previously removed, unban allows them to join again.
            try:
                bot.unban_chat_member(chat, int(uid), only_if_banned=True)
            except Exception:
                pass
            invite = bot.create_chat_invite_link(
                chat,
                name=f"Globexomart-VIP-{uid}-{int(time.time())}",
                member_limit=1,
                expire_date=int(time.time() + 86400),
            )
            logs_col.insert_one({"event":"vip_one_time_invite_created","user_id":int(uid),"chat":str(chat),"invite_link":invite.invite_link,"created_at":time.time(),"expires_at":time.time()+86400})
            results.append((chat, invite.invite_link, None))
        except Exception as exc:
            results.append((chat, None, str(exc)))
    return results


def _remove_chat_access(uid):
    results = []
    for chat in get_access_chats():
        try:
            bot.ban_chat_member(chat, int(uid), revoke_messages=False)
            results.append((chat, True, None))
        except Exception as exc:
            results.append((chat, False, str(exc)))
    return results


def activate_subscription(uid, plan_code, payment_mode="manual", amount=None, payment_ref=None, added_by=None):
    uid = int(uid)
    code = str(plan_code).upper().strip()
    plans = get_subscription_plans()
    if code not in plans:
        raise ValueError("Selected VIP plan is not available")
    plan = plans[code]
    now = time.time()
    current = _active_subscription(uid)
    start_at = max(now, float(current.get("expires_at", 0) or 0)) if current else now
    expires_at = start_at + int(plan.get("duration_minutes", 1440)) * 60
    charged = float(plan["price"] if amount is None else amount)

    subscriptions_col.update_many(
        {"user_id": uid, "status": "active"},
        {"$set": {"status": "superseded", "superseded_at": now}},
    )
    sub_doc = {
        "user_id": uid,
        "plan": code,
        "days": float(plan.get("duration_minutes", 1440)) / 1440.0,
        "duration_minutes": int(plan.get("duration_minutes", 1440)),
        "price_usd": charged,
        "payment_mode": str(payment_mode).lower(),
        "payment_ref": payment_ref,
        "status": "active",
        "starts_at": start_at,
        "expires_at": expires_at,
        "created_at": now,
        "added_by": added_by,
    }
    sub_id = subscriptions_col.insert_one(sub_doc).inserted_id
    payments_col.insert_one({
        "user_id": uid,
        "subscription_id": str(sub_id),
        "plan": code,
        "amount": charged,
        "currency": "USD",
        "mode": str(payment_mode).lower(),
        "reference": payment_ref,
        "status": "paid",
        "created_at": now,
        "approved_by": added_by,
    })
    _sync_user_vip(uid, expires_at)
    links = _grant_chat_access(uid)
    try:
        _record_vip_activation(uid, sub_doc)
        _process_referral_vip_sale(uid, charged, sub_doc)
        _send_vip_rules(uid)
        _notify_owner_vip_join(uid, sub_doc)
    except Exception as exc:
        log_event("vip_post_activation_error", uid, details={"error": str(exc)}, level="error")
    return sub_doc, links


def expire_subscription(uid, reason="expired", removed_by=None):
    uid = int(uid)
    now = time.time()
    subscriptions_col.update_many(
        {"user_id": uid, "status": "active"},
        {"$set": {"status": reason, "ended_at": now, "removed_by": removed_by}},
    )
    users_col.update_one({"_id": str(uid)}, {"$set": {"vip": False, "vip_expiry": None}})
    User._cache.pop(str(uid), None)
    User._cache_time.pop(str(uid), None)
    return _remove_chat_access(uid)


def _subscription_status_text(uid):
    sub = _active_subscription(uid)
    if not sub:
        return "No active subscription."
    exp = datetime.fromtimestamp(sub["expires_at"]).strftime("%Y-%m-%d %H:%M")
    return f"Plan: {sub.get('plan')}\nPaid: ${sub.get('price_usd')}\nExpires: {exp}"


@bot.message_handler(commands=["plans"])
def plans_cmd(m):
    raw_bot.send_message(m.chat.id, "💎 GLOBEXOMART ACCESS\n\n" + _plan_text() + "\n\nUse /mystatus to check your access.")


@bot.message_handler(commands=["mystatus"])
def my_subscription_status(m):
    raw_bot.send_message(m.chat.id, "💎 YOUR ACCESS\n\n" + _subscription_status_text(m.from_user.id))


@bot.callback_query_handler(func=lambda c: c.data.startswith("subplan|"))
def choose_subscription_plan_cb(c):
    try:
        _, code = c.data.split("|", 1)
        plans = get_subscription_plans()
        code = code.upper()
        if code not in plans:
            return bot.answer_callback_query(c.id, "Invalid plan", True)
        plan = plans[code]
        cfg = get_cached_config()
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(InlineKeyboardButton("✅ I Paid — Submit Proof", callback_data=f"submanual|{code}"))
        contact = cfg.get("vip_contact") or cfg.get("contact_link") or cfg.get("contact_username")
        if contact:
            if str(contact).startswith("@"):
                url = "https://t.me/" + str(contact).lstrip("@")
            elif str(contact).startswith("http"):
                url = str(contact)
            else:
                url = "https://t.me/" + str(contact).lstrip("@")
            kb.add(InlineKeyboardButton("📞 Contact Admin", url=url))
        price = _effective_plan_price(c.from_user.id, code)
        raw_bot.edit_message_text(
            f"💎 {plan.get('name', code)}\n\nDuration: {_format_duration_minutes(plan.get('duration_minutes', 1440))}\nPrice: ${price:g} USDT\n\n{_usdt_instructions(price)}\n\nAfter paying, tap **I Paid — Submit Proof** and send your transaction ID + screenshot.",
            c.from_user.id,
            c.message.message_id,
            reply_markup=kb,
        )
        bot.answer_callback_query(c.id)
    except Exception as exc:
        bot.answer_callback_query(c.id, "Could not open plan", True)
        log_event("subscription_plan_open_error", c.from_user.id, details={"error": str(exc)}, level="error")


_vip_payment_state = {}

@bot.callback_query_handler(func=lambda c: c.data.startswith("submanual|"))
def request_manual_subscription_payment_cb(c):
    try:
        _, code = c.data.split("|", 1)
        code = code.upper()
        plans = get_subscription_plans()
        if code not in plans:
            return bot.answer_callback_query(c.id, "Invalid plan", True)
        pending = payments_col.find_one({"user_id": c.from_user.id, "plan": code, "status": "pending"})
        if pending:
            return bot.answer_callback_query(c.id, "Payment proof already pending review", True)
        _vip_payment_state[c.from_user.id] = {
            "plan": code,
            "amount": float(_effective_plan_price(c.from_user.id, code)),
            "username": c.from_user.username,
            "first_name": c.from_user.first_name,
            "chat_id": c.message.chat.id,
        }
        msg = raw_bot.send_message(
            c.from_user.id,
            f"🧾 VIP PAYMENT PROOF\n\nPlan: {code}\n{_usdt_instructions(_effective_plan_price(c.from_user.id, code))}\n\nStep 1/2: Send the transaction ID / TxID exactly as shown by your wallet or exchange."
        )
        bot.register_next_step_handler(msg, vip_payment_txid_step)
        bot.answer_callback_query(c.id, "Send transaction ID")
    except Exception as exc:
        bot.answer_callback_query(c.id, "Could not start payment proof", True)
        log_event("manual_payment_request_error", c.from_user.id, details={"error": str(exc)}, level="error")


def vip_payment_txid_step(m):
    state = _vip_payment_state.get(m.from_user.id)
    if not state:
        return raw_bot.send_message(m.chat.id, "Payment session expired. Open Buy VIP again.")
    txid = (m.text or "").strip()
    if len(txid) < 6:
        msg = raw_bot.send_message(m.chat.id, "❌ Transaction ID looks too short. Send the complete TxID.")
        bot.register_next_step_handler(msg, vip_payment_txid_step)
        return
    state["transaction_id"] = txid[:300]
    msg = raw_bot.send_message(m.chat.id, "📸 Step 2/2: Now send the payment screenshot as a photo.")
    bot.register_next_step_handler(msg, vip_payment_screenshot_step)


def vip_payment_screenshot_step(m):
    state = _vip_payment_state.get(m.from_user.id)
    if not state:
        return raw_bot.send_message(m.chat.id, "Payment session expired. Open Buy VIP again.")
    if m.content_type != "photo":
        msg = raw_bot.send_message(m.chat.id, "❌ Please send the payment screenshot as a photo.")
        bot.register_next_step_handler(msg, vip_payment_screenshot_step)
        return
    state = _vip_payment_state.pop(m.from_user.id)
    username = m.from_user.username or state.get("username")
    display = f"@{username}" if username else (m.from_user.first_name or state.get("first_name") or str(m.from_user.id))
    doc = {
        "user_id": int(m.from_user.id),
        "chat_id": int(m.chat.id),
        "username": username,
        "first_name": m.from_user.first_name or state.get("first_name"),
        "plan": state["plan"],
        "amount": float(state["amount"]),
        "currency": "USDT",
        "mode": "manual",
        "transaction_id": state["transaction_id"],
        "screenshot_chat_id": int(m.chat.id),
        "screenshot_message_id": int(m.message_id),
        "status": "pending",
        "created_at": time.time(),
    }
    pid = payments_col.insert_one(doc).inserted_id
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Confirm Payment", callback_data=f"payapprove|{pid}"),
        InlineKeyboardButton("❌ Reject", callback_data=f"payreject|{pid}"),
    )
    admin_text = (
        "🧾 VIP PAYMENT REVIEW\n\n"
        f"👤 User: {display}\n"
        f"🆔 User ID: {m.from_user.id}\n"
        f"💬 Chat ID: {m.chat.id}\n"
        f"💎 Plan: {state['plan']}\n"
        f"💰 Amount: ${state['amount']:g} USDT\n"
        f"🔗 Transaction ID: {state['transaction_id']}\n\n"
        "Check the transaction and screenshot, then confirm or reject."
    )
    for admin in get_all_admins():
        try:
            aid = int(admin["_id"])
            raw_bot.send_message(aid, admin_text, reply_markup=kb)
            raw_bot.copy_message(aid, m.chat.id, m.message_id)
        except Exception as exc:
            log_event("vip_payment_admin_delivery_error", m.from_user.id, details={"admin": str(admin.get('_id')), "error": str(exc)}, level="error")
    raw_bot.send_message(m.chat.id, "✅ Payment proof submitted. Admin will confirm or reject it. Your VIP activates automatically after approval.")


@bot.callback_query_handler(func=lambda c: c.data.startswith(("payapprove|", "payreject|")))
def review_manual_subscription_payment_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    try:
        from bson import ObjectId
        action, pid = c.data.split("|", 1)
        pay = payments_col.find_one({"_id": ObjectId(pid)})
        if not pay:
            return bot.answer_callback_query(c.id, "Payment not found", True)
        if pay.get("status") != "pending":
            return bot.answer_callback_query(c.id, f"Already {pay.get('status')}", True)
        uid = int(pay["user_id"])
        if action == "payreject":
            payments_col.update_one({"_id": pay["_id"]}, {"$set": {"status": "rejected", "reviewed_at": time.time(), "reviewed_by": c.from_user.id}})
            bot.answer_callback_query(c.id, "Rejected")
            raw_bot.edit_message_text(c.message.text + "\n\n❌ REJECTED", c.message.chat.id, c.message.message_id)
            try:
                raw_bot.send_message(uid, "❌ Your manual payment request was rejected. Contact the admin if you believe this is a mistake.")
            except Exception:
                pass
            return

        # Mark this request approved, then activate without adding a duplicate paid payment row.
        payments_col.update_one({"_id": pay["_id"]}, {"$set": {"status": "approved", "reviewed_at": time.time(), "reviewed_by": c.from_user.id}})
        sub, links = activate_subscription(uid, pay["plan"], "manual", amount=pay.get("amount"), payment_ref=str(pay["_id"]), added_by=c.from_user.id)
        _publish_proof("VIP", pay, screenshot_chat_id=pay.get("screenshot_chat_id"), screenshot_message_id=pay.get("screenshot_message_id"))
        # The activation function records the final paid transaction; this request remains as the approval audit entry.
        exp = datetime.fromtimestamp(sub["expires_at"]).strftime("%Y-%m-%d %H:%M")
        good_links = [link for _, link, err in links if link and not err]
        bot.answer_callback_query(c.id, "Approved and activated")
        raw_bot.edit_message_text(c.message.text + f"\n\n✅ APPROVED\nExpires: {exp}", c.message.chat.id, c.message.message_id)
        msg = f"✅ GLOBEXOMART ACCESS ACTIVE\n\nPlan: {pay['plan']}\nExpires: {exp}"
        if good_links:
            msg += "\n\nPrivate access link(s):\n" + "\n".join(good_links)
        try:
            raw_bot.send_message(uid, msg, disable_web_page_preview=True)
        except Exception:
            pass
    except Exception as exc:
        bot.answer_callback_query(c.id, "Review failed", True)
        admin_error(c.from_user.id, exc)


def process_verified_auto_payment(user_id, plan_code, provider_reference, amount=None):
    """Integration hook for a trusted payment provider/webhook after payment is independently verified."""
    if not provider_reference:
        raise ValueError("provider_reference is required")
    existing = payments_col.find_one({"mode": "auto", "reference": str(provider_reference), "status": "paid"})
    if existing:
        return None, []
    return activate_subscription(
        int(user_id), str(plan_code).upper(), "auto", amount=amount,
        payment_ref=str(provider_reference), added_by="payment_provider"
    )


@bot.message_handler(commands=["subadd"])
def admin_subadd_cmd(m):
    if not is_admin(m.from_user.id):
        return
    try:
        parts = (m.text or "").split()
        if len(parts) < 3:
            return raw_bot.send_message(m.chat.id, "Usage: /subadd USER_ID 1M|2M|4M|1Y [manual|auto] [payment_ref]")
        uid = int(parts[1])
        plan = parts[2].upper()
        mode = parts[3].lower() if len(parts) > 3 else "manual"
        ref = parts[4] if len(parts) > 4 else None
        if mode not in ("manual", "auto"):
            raise ValueError("Payment mode must be manual or auto")
        sub, links = activate_subscription(uid, plan, mode, payment_ref=ref, added_by=m.from_user.id)
        exp = datetime.fromtimestamp(sub["expires_at"]).strftime("%Y-%m-%d %H:%M")
        good_links = [link for _, link, err in links if link and not err]
        text = f"✅ ACCESS ACTIVATED\n\nUser: {uid}\nPlan: {plan}\nPaid: ${sub['price_usd']}\nExpires: {exp}"
        if good_links:
            text += "\n\nInvite link(s) were sent to the user."
        raw_bot.send_message(m.chat.id, text, reply_markup=admin_menu())
        user_text = f"✅ GLOBEXOMART ACCESS ACTIVE\n\nPlan: {plan}\nExpires: {exp}"
        if good_links:
            user_text += "\n\nYour private access link(s):\n" + "\n".join(good_links)
        try:
            raw_bot.send_message(uid, user_text, disable_web_page_preview=True)
        except Exception:
            pass
    except Exception as exc:
        admin_error(m.from_user.id, exc)


@bot.message_handler(commands=["subremove"])
def admin_subremove_cmd(m):
    if not is_admin(m.from_user.id):
        return
    try:
        parts = (m.text or "").split()
        if len(parts) != 2:
            return raw_bot.send_message(m.chat.id, "Usage: /subremove USER_ID")
        uid = int(parts[1])
        expire_subscription(uid, reason="removed", removed_by=m.from_user.id)
        raw_bot.send_message(m.chat.id, f"✅ Removed subscription and channel/group access for {uid}.", reply_markup=admin_menu())
        try:
            raw_bot.send_message(uid, "⛔ Your Globexomart access has been removed.")
        except Exception:
            pass
    except Exception as exc:
        admin_error(m.from_user.id, exc)


@bot.message_handler(commands=["substatus"])
def admin_substatus_cmd(m):
    if not is_admin(m.from_user.id):
        return
    try:
        parts = (m.text or "").split()
        if len(parts) != 2:
            return raw_bot.send_message(m.chat.id, "Usage: /substatus USER_ID")
        uid = int(parts[1])
        raw_bot.send_message(m.chat.id, f"User: {uid}\n\n" + _subscription_status_text(uid))
    except Exception as exc:
        admin_error(m.from_user.id, exc)


@bot.message_handler(func=lambda m: m.text in ("🧾 Subscriptions", "🧾 VIP Manager") and is_admin(m.from_user.id))
def subscriptions_admin_menu(m):
    rows = list(subscriptions_col.find({"status": "active", "expires_at": {"$gt": time.time()}}).sort("expires_at", 1).limit(50))
    if not rows:
        return raw_bot.send_message(m.from_user.id, "🧾 VIP MANAGER\n\nNo active VIP users.\n\n/subadd USER_ID 1M manual\n/subremove USER_ID", reply_markup=admin_menu())
    kb = InlineKeyboardMarkup(row_width=1)
    lines = ["🧾 VIP MANAGER", ""]
    now = time.time()
    for sub in rows:
        uid = int(sub["user_id"])
        u = users_col.find_one({"_id": str(uid)}, {"username":1,"first_name":1}) or {}
        name = ("@" + u["username"]) if u.get("username") else (u.get("first_name") or str(uid))
        days = max(0, int((float(sub.get("expires_at", now))-now + 86399)//86400))
        paid = float(sub.get("price_usd",0) or 0)
        lines.append(f"{name} • {sub.get('plan')} • {days}d • ${paid:g}")
        kb.add(InlineKeyboardButton(f"👤 {name[:20]} • {days}d", callback_data=f"vipmanage|{uid}"))
    lines.append("")
    lines.append("Tap a user below for remove / ban / mute controls.")
    raw_bot.send_message(m.from_user.id, "\n".join(lines), reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("vipmanage|"))
def vip_manage_user_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    uid = int(c.data.split("|",1)[1])
    sub = _active_subscription(uid)
    u = users_col.find_one({"_id":str(uid)}) or {}
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("❌ Remove VIP", callback_data=f"vipaction|remove|{uid}"),
        InlineKeyboardButton("🚫 Ban", callback_data=f"vipaction|ban|{uid}"),
        InlineKeyboardButton("🔇 Mute", callback_data=f"vipaction|mute|{uid}"),
        InlineKeyboardButton("🔊 Unmute", callback_data=f"vipaction|unmute|{uid}"),
    )
    status = _subscription_status_text(uid)
    uname = f"@{u.get('username')}" if u.get("username") else "No username"
    raw_bot.send_message(c.from_user.id, f"👤 VIP USER\n\nID: {uid}\nUsername: {uname}\n\n{status}", reply_markup=kb)
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("vipaction|"))
def vip_action_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    _, action, uid_s = c.data.split("|",2)
    uid = int(uid_s)
    if action == "remove":
        expire_subscription(uid, reason="removed", removed_by=c.from_user.id)
        msg = "VIP removed and channel/group access revoked."
    elif action == "ban":
        users_col.update_one({"_id":str(uid)}, {"$set":{"banned":True}})
        expire_subscription(uid, reason="banned", removed_by=c.from_user.id)
        msg = "User banned and VIP access revoked."
    elif action == "mute":
        users_col.update_one({"_id":str(uid)}, {"$set":{"muted":True}})
        msg = "User muted in bot records."
    else:
        users_col.update_one({"_id":str(uid)}, {"$set":{"muted":False}})
        msg = "User unmuted."
    try:
        raw_bot.send_message(uid, f"ℹ️ Globexomart account update: {msg}")
    except Exception:
        pass
    bot.answer_callback_query(c.id, msg, True)
@bot.message_handler(func=lambda m: m.text in ("🎯 Access Chats", "🎯 VIP Channel") and is_admin(m.from_user.id))
def access_chats_admin_menu(m):
    chats = get_access_chats()
    text = "🎯 ACCESS CHANNELS / GROUPS\n\n"
    text += ("\n".join(map(str, chats)) if chats else "No access chats configured.")
    pending = list(source_chats_col.find({"vip_access_status":"pending"}).sort("vip_detected_at",-1).limit(20))
    if pending:
        text += "\n\n⏳ Detected chats waiting for VIP approval:\n" + "\n".join(f"• {x.get('title') or x.get('_id')} — {x.get('_id')}" for x in pending)
    text += "\n\nManual commands:\n/access_add @channel_or_group\n/vipchannel_add @channel_or_group\n/access_remove @channel_or_group\n/access_list\n\nAuto mode: simply add/promote the bot as admin in a channel/group. Admins will receive an approval button automatically."
    raw_bot.send_message(m.from_user.id, text, reply_markup=admin_menu())


@bot.message_handler(commands=["access_add", "vipchannel_add"])
def access_add_cmd(m):
    if not is_admin(m.from_user.id):
        return
    try:
        raw = (m.text or "").split(maxsplit=1)
        if len(raw) != 2:
            return raw_bot.send_message(m.chat.id, "Usage: /access_add @channel_or_group\nOr: /vipchannel_add @channel_or_group")
        ref = normalize_chat_reference(raw[1])
        cfg = get_config()
        chats = cfg.get("access_chats", []) or []
        canonical = str(ref)
        if canonical not in [str(x) for x in chats]:
            chats.append(ref)
            set_config("access_chats", chats)
        raw_bot.send_message(m.chat.id, f"✅ Added access chat: {ref}\n\nBot must be admin there with invite + ban permissions.")
    except Exception as exc:
        admin_error(m.from_user.id, exc)


@bot.message_handler(commands=["access_remove"])
def access_remove_cmd(m):
    if not is_admin(m.from_user.id):
        return
    try:
        raw = (m.text or "").split(maxsplit=1)
        if len(raw) != 2:
            return raw_bot.send_message(m.chat.id, "Usage: /access_remove @channel_or_group")
        ref = normalize_chat_reference(raw[1])
        cfg = get_config()
        chats = [x for x in (cfg.get("access_chats", []) or []) if str(x) != str(ref)]
        set_config("access_chats", chats)
        raw_bot.send_message(m.chat.id, f"✅ Removed access chat: {ref}")
    except Exception as exc:
        admin_error(m.from_user.id, exc)


@bot.message_handler(commands=["access_list"])
def access_list_cmd(m):
    if not is_admin(m.from_user.id):
        return
    chats = get_access_chats()
    raw_bot.send_message(m.chat.id, "🎯 ACCESS CHATS\n\n" + ("\n".join(map(str, chats)) if chats else "None configured."))


@bot.message_handler(commands=["payments_today"])
def payments_today_cmd(m):
    if not is_admin(m.from_user.id):
        return
    start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    docs = list(payments_col.find({"created_at": {"$gte": start}, "status": "paid"}).sort("created_at", -1))
    total = sum(float(x.get("amount", 0) or 0) for x in docs)
    manual = sum(1 for x in docs if x.get("mode") == "manual")
    auto = sum(1 for x in docs if x.get("mode") == "auto")
    raw_bot.send_message(m.chat.id, f"💰 TODAY'S PAYMENTS\n\nCount: {len(docs)}\nTotal: ${total:.2f}\nManual: {manual}\nAuto: {auto}")



# =========================
# 💵 INDIVIDUAL USDT ITEM PURCHASES
# =========================
_item_payment_state = {}
_wallet_state = {}

def _effective_usdt_price(folder):
    base = float(folder.get("price", 0) or 0)
    cfg = get_cached_config()
    if cfg.get("discount_enabled") and base > 0:
        pct = max(0.0, min(100.0, float(cfg.get("discount_percent", 0) or 0)))
        return round(base * (100.0 - pct) / 100.0, 2)
    return base

def _usdt_instructions(amount):
    cfg = get_cached_config()
    address = cfg.get("usdt_address") or cfg.get("binance_address") or "Not configured"
    network = cfg.get("usdt_network") or cfg.get("binance_network") or "TRC20"
    amount_line = f"Amount: ${float(amount):g} USDT\n" if float(amount) > 0 else ""
    return f"{amount_line}Network: {network}\nAddress: {address}"

def _deliver_digital_product(uid, folder, prefix="✅ PURCHASE COMPLETE"):
    raw_bot.send_message(int(uid), prefix + "\n\n" + _service_card(folder) + "\n\nYour digital product is below:")
    if folder.get("text_content"):
        raw_bot.send_message(int(uid), str(folder.get("text_content")))
    for f in folder.get("files", []) or []:
        try:
            raw_bot.copy_message(int(uid), f["chat"], f["msg"])
        except Exception as exc:
            log_event("product_delivery_copy_error", uid, details={"folder_id": str(folder.get("_id")), "error": str(exc)}, level="error")
    if folder.get("service_msg") and folder.get("service_msg") != folder.get("text_content"):
        raw_bot.send_message(int(uid), str(folder.get("service_msg")))

@bot.callback_query_handler(func=lambda c: c.data.startswith("buyitem|"))
def buy_item_usdt_cb(c):
    if force_block(c.from_user.id):
        return
    try:
        from bson import ObjectId
        folder = folders_col.find_one({"_id": ObjectId(c.data.split("|", 1)[1])})
        if not folder or folder.get("cat") not in ("vip", "paid_service"):
            raise ValueError("Item not found")
        price = _effective_usdt_price(folder)
        if price <= 0:
            raise ValueError("Price is not configured")

        # Paid PRODUCTS are bought only from the user's approved bot USDT balance.
        if folder.get("cat") == "paid_service":
            if _service_status(folder) != "in_stock":
                return bot.answer_callback_query(c.id, "Out of stock", True)
            already = item_purchases_col.find_one({"user_id": int(c.from_user.id), "folder_id": str(folder["_id"]), "status": "paid"})
            if already:
                bot.answer_callback_query(c.id, "Already purchased", True)
                _deliver_digital_product(c.from_user.id, folder, "✅ ALREADY PURCHASED")
                return
            balance = _user_usdt_balance(c.from_user.id)
            if balance + 1e-9 < price:
                kb = InlineKeyboardMarkup()
                kb.add(InlineKeyboardButton("💳 Deposit USDT", callback_data="open_deposit_help"))
                raw_bot.send_message(c.from_user.id, _service_card(folder) + f"\n\n💰 Price: ${price:g} USDT\n👛 Your balance: ${balance:.2f} USDT\n\n❌ Insufficient account balance. Deposit USDT first, wait for admin approval, then buy this product.", reply_markup=kb)
                return bot.answer_callback_query(c.id, "Insufficient balance", True)
            # Atomic debit prevents double-spending if the user taps twice.
            debit = users_col.update_one(
                {"_id": str(c.from_user.id), "usdt_balance": {"$gte": price}},
                {"$inc": {"usdt_balance": -price}},
            )
            if debit.modified_count != 1:
                return bot.answer_callback_query(c.id, "Insufficient balance", True)
            try:
                doc = {
                    "user_id": int(c.from_user.id), "chat_id": int(c.message.chat.id),
                    "username": c.from_user.username, "folder_id": str(folder["_id"]),
                    "item_name": folder.get("name"), "category": "paid_service",
                    "amount": float(price), "currency": "USDT", "status": "paid",
                    "payment_source": "account_balance", "created_at": time.time(),
                }
                item_purchases_col.insert_one(doc)
                payments_col.insert_one({"user_id": int(c.from_user.id), "type": "product", "folder_id": str(folder["_id"]), "amount": float(price), "currency": "USDT", "mode": "balance", "status": "paid", "created_at": time.time()})
                _deliver_digital_product(c.from_user.id, folder)
                bot.answer_callback_query(c.id, f"Purchased for ${price:g}", True)
            except Exception:
                users_col.update_one({"_id": str(c.from_user.id)}, {"$inc": {"usdt_balance": price}})
                raise
            return

        # VIP methods still use the existing transaction-proof/manual-review flow.
        if User(c.from_user.id).is_vip():
            return bot.answer_callback_query(c.id, "Already included with VIP", True)
        _item_payment_state[c.from_user.id] = {"folder_id": str(folder["_id"]), "price": price}
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("✅ I Paid — Submit Proof", callback_data="itempay_screenshot"))
        raw_bot.send_message(c.from_user.id, f"💎 {folder.get('name')}\n\n{_usdt_instructions(price)}\n\nAfter payment, submit your transaction ID and screenshot.", reply_markup=kb)
        bot.answer_callback_query(c.id)
    except Exception as exc:
        bot.answer_callback_query(c.id, str(exc)[:150], True)

@bot.callback_query_handler(func=lambda c: c.data == "itempay_screenshot")
def item_payment_screenshot_cb(c):
    state = _item_payment_state.get(c.from_user.id)
    if not state:
        return bot.answer_callback_query(c.id, "Purchase session expired", True)
    msg = raw_bot.send_message(c.from_user.id, "🧾 Step 1/2: Send the transaction ID / TxID.")
    bot.register_next_step_handler(msg, item_payment_receive_txid)
    bot.answer_callback_query(c.id, "Send transaction ID")


def item_payment_receive_txid(m):
    state = _item_payment_state.get(m.from_user.id)
    if not state:
        return raw_bot.send_message(m.chat.id, "Purchase session expired.")
    txid = (m.text or "").strip()
    if len(txid) < 6:
        msg = raw_bot.send_message(m.chat.id, "❌ Transaction ID looks too short. Send the complete TxID.")
        bot.register_next_step_handler(msg, item_payment_receive_txid)
        return
    state["transaction_id"] = txid[:300]
    msg = raw_bot.send_message(m.chat.id, "📸 Step 2/2: Send the payment screenshot as a photo.")
    bot.register_next_step_handler(msg, item_payment_receive_screenshot)


def item_payment_receive_screenshot(m):
    state = _item_payment_state.get(m.from_user.id)
    if not state:
        return raw_bot.send_message(m.chat.id, "Purchase session expired.")
    if m.content_type != "photo":
        msg = raw_bot.send_message(m.chat.id, "Please send a photo screenshot.")
        bot.register_next_step_handler(msg, item_payment_receive_screenshot)
        return
    state = _item_payment_state.pop(m.from_user.id)
    from bson import ObjectId
    folder = folders_col.find_one({"_id": ObjectId(state["folder_id"])})
    if not folder:
        return raw_bot.send_message(m.chat.id, "Item no longer exists.")
    if _service_status(folder) != "in_stock":
        return raw_bot.send_message(m.chat.id, "❌ This product went out of stock before review. Contact admin for help.")
    doc = {
        "user_id": int(m.from_user.id),
        "chat_id": int(m.chat.id),
        "username": m.from_user.username,
        "folder_id": state["folder_id"],
        "item_name": folder.get("name"),
        "category": folder.get("cat"),
        "amount": float(state["price"]),
        "currency": "USDT",
        "transaction_id": state["transaction_id"],
        "status": "pending",
        "screenshot_chat_id": m.chat.id,
        "screenshot_message_id": m.message_id,
        "created_at": time.time(),
    }
    rid = item_purchases_col.insert_one(doc).inserted_id
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("✅ Approve", callback_data=f"itempayapprove|{rid}"),
           InlineKeyboardButton("❌ Reject", callback_data=f"itempayreject|{rid}"))
    display = f"@{m.from_user.username}" if m.from_user.username else (m.from_user.first_name or str(m.from_user.id))
    for admin in get_all_admins():
        try:
            aid = int(admin["_id"])
            raw_bot.send_message(
                aid,
                f"💵 DIGITAL PRODUCT PAYMENT\n\n👤 User: {display}\n🆔 User ID: {m.from_user.id}\n💬 Chat ID: {m.chat.id}\n🛍 Item: {folder.get('name')}\n💰 Amount: ${state['price']:g} USDT\n🔗 Transaction ID: {state['transaction_id']}\n\nConfirm only after verifying the payment.",
                reply_markup=kb,
            )
            raw_bot.copy_message(aid, m.chat.id, m.message_id)
        except Exception:
            pass
    raw_bot.send_message(m.chat.id, "✅ Payment proof submitted. You will be notified after admin review.")

@bot.callback_query_handler(func=lambda c: c.data.startswith("itempayapprove|") or c.data.startswith("itempayreject|"))
def review_item_payment(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    from bson import ObjectId
    action, rid = c.data.split("|", 1)
    row = item_purchases_col.find_one({"_id": ObjectId(rid)})
    if not row or row.get("status") != "pending":
        return bot.answer_callback_query(c.id, "Already reviewed", True)
    approved = action == "itempayapprove"
    status = "paid" if approved else "rejected"
    item_purchases_col.update_one({"_id": row["_id"]}, {"$set": {"status": status, "reviewed_at": time.time(), "reviewed_by": c.from_user.id}})
    if approved:
        payments_col.insert_one({"user_id": row["user_id"], "type": "item", "folder_id": row["folder_id"], "amount": row["amount"], "currency": "USDT", "mode": "manual", "transaction_id": row.get("transaction_id"), "status": "paid", "created_at": time.time(), "approved_by": c.from_user.id})
        _publish_proof("METHOD" if row.get("category") == "vip" else "PRODUCT", row, screenshot_chat_id=row.get("screenshot_chat_id"), screenshot_message_id=row.get("screenshot_message_id"))
        try:
            folder = folders_col.find_one({"_id": ObjectId(row["folder_id"])})
            if folder:
                raw_bot.send_message(row["user_id"], "✅ PAYMENT APPROVED\n\n" + _service_card(folder) + "\n\nYour digital product is below:")
                if folder.get("text_content"):
                    raw_bot.send_message(row["user_id"], str(folder.get("text_content")))
                for f in folder.get("files", []) or []:
                    try:
                        raw_bot.copy_message(int(row["user_id"]), f["chat"], f["msg"])
                    except Exception:
                        pass
                if folder.get("service_msg") and folder.get("service_msg") != folder.get("text_content"):
                    raw_bot.send_message(row["user_id"], str(folder.get("service_msg")))
            else:
                raw_bot.send_message(row["user_id"], "✅ Payment approved. Your product purchase is complete.")
        except Exception as exc:
            log_event("service_delivery_error", row.get("user_id"), details={"folder_id": row.get("folder_id"), "error": str(exc)}, level="error")
    else:
        try:
            raw_bot.send_message(row["user_id"], "❌ Payment rejected. Please contact admin if needed.")
        except Exception:
            pass
    bot.answer_callback_query(c.id, "Approved" if approved else "Rejected", True)

# =========================
# 💳 USER USDT WALLET
# =========================
def _user_usdt_balance(uid):
    row = users_col.find_one({"_id": str(uid)}, {"usdt_balance": 1}) or {}
    return float(row.get("usdt_balance", 0) or 0)

@bot.callback_query_handler(func=lambda c: c.data == "open_deposit_help")
def open_deposit_help_cb(c):
    bot.answer_callback_query(c.id)
    raw_bot.send_message(c.from_user.id, f"💳 DEPOSIT USDT\n\nCurrent balance: ${_user_usdt_balance(c.from_user.id):.2f}\n\nUse the 💳 Deposit button from the main menu. A transaction ID and payment screenshot are required before admin approval.\n\n{_usdt_instructions(0)}")

@bot.message_handler(func=lambda m: m.text == "💳 Deposit")
@force_join_handler
def deposit_menu(m):
    if not get_cached_config().get("deposit_enabled", True):
        return raw_bot.send_message(m.chat.id, "Deposits are currently disabled.")
    msg = raw_bot.send_message(m.chat.id, f"💳 DEPOSIT USDT\n\nBalance: ${_user_usdt_balance(m.from_user.id):.2f}\n\nSend the USDT amount you deposited.\n\n{_usdt_instructions(0)}")
    bot.register_next_step_handler(msg, deposit_amount_step)

def deposit_amount_step(m):
    try:
        amount = float((m.text or "").replace("$", "").strip())
        if amount <= 0:
            raise ValueError()
        _wallet_state[m.from_user.id] = {"type": "deposit", "amount": amount}
        msg = raw_bot.send_message(m.chat.id, "🧾 Step 1/2: Send the transaction ID / TxID for this deposit.")
        bot.register_next_step_handler(msg, deposit_txid_step)
    except Exception:
        raw_bot.send_message(m.chat.id, "Invalid amount. Start Deposit again.")

def deposit_txid_step(m):
    st = _wallet_state.get(m.from_user.id)
    if not st:
        return raw_bot.send_message(m.chat.id, "Deposit session expired. Start Deposit again.")
    txid = (m.text or "").strip()
    if len(txid) < 6:
        msg = raw_bot.send_message(m.chat.id, "❌ Transaction ID looks too short. Send the complete TxID.")
        bot.register_next_step_handler(msg, deposit_txid_step)
        return
    st["transaction_id"] = txid[:300]
    msg = raw_bot.send_message(m.chat.id, "📸 Step 2/2: Now send the payment screenshot as a photo.")
    bot.register_next_step_handler(msg, deposit_screenshot_step)

def deposit_screenshot_step(m):
    st = _wallet_state.get(m.from_user.id)
    if not st:
        return raw_bot.send_message(m.chat.id, "Deposit session expired. Start Deposit again.")
    if m.content_type != "photo":
        msg = raw_bot.send_message(m.chat.id, "❌ A screenshot photo is required. Please send the payment screenshot.")
        bot.register_next_step_handler(msg, deposit_screenshot_step)
        return
    st = _wallet_state.pop(m.from_user.id)
    doc = {
        "user_id": int(m.from_user.id), "chat_id": int(m.chat.id),
        "username": m.from_user.username, "type": "deposit",
        "amount": st["amount"], "transaction_id": st["transaction_id"],
        "status": "pending", "created_at": time.time(),
        "screenshot_chat_id": m.chat.id, "screenshot_message_id": m.message_id,
    }
    rid = wallet_tx_col.insert_one(doc).inserted_id
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("✅ Approve Deposit", callback_data=f"walletapprove|{rid}"),
           InlineKeyboardButton("❌ Reject", callback_data=f"walletreject|{rid}"))
    display = f"@{m.from_user.username}" if m.from_user.username else (m.from_user.first_name or str(m.from_user.id))
    for admin in get_all_admins():
        try:
            aid = int(admin["_id"])
            raw_bot.send_message(aid, f"💳 DEPOSIT REQUEST\n\n👤 User: {display}\n🆔 User ID: {m.from_user.id}\n💬 Chat ID: {m.chat.id}\n💰 Amount: ${st['amount']:.2f} USDT\n🔗 TxID: {st['transaction_id']}\n\nApprove only after verifying the transaction.", reply_markup=kb)
            raw_bot.copy_message(aid, m.chat.id, m.message_id)
        except Exception:
            pass
    raw_bot.send_message(m.chat.id, "✅ Deposit submitted. Your balance will increase only after admin approval.")

@bot.message_handler(func=lambda m: m.text == "💸 Withdraw")
@force_join_handler
def withdraw_menu(m):
    if not get_cached_config().get("withdraw_enabled", True):
        return raw_bot.send_message(m.chat.id, "Withdrawals are currently disabled.")
    bal = _user_usdt_balance(m.from_user.id)
    msg = raw_bot.send_message(m.chat.id, f"💸 WITHDRAW USDT\n\nAvailable balance: ${bal:.2f}\n\nSend amount to withdraw.")
    bot.register_next_step_handler(msg, withdraw_amount_step)

def withdraw_amount_step(m):
    try:
        amount = float((m.text or "").replace("$", "").strip())
        bal = _user_usdt_balance(m.from_user.id)
        if amount <= 0 or amount > bal:
            raise ValueError()
        _wallet_state[m.from_user.id] = {"type": "withdraw", "amount": amount}
        msg = raw_bot.send_message(m.chat.id, "Send your USDT wallet address.")
        bot.register_next_step_handler(msg, withdraw_address_step)
    except Exception:
        raw_bot.send_message(m.chat.id, "Invalid amount or insufficient balance. Start Withdraw again.")

def withdraw_address_step(m):
    st = _wallet_state.pop(m.from_user.id, None)
    address = (m.text or "").strip()
    if not st or len(address) < 10:
        return raw_bot.send_message(m.chat.id, "Withdraw cancelled. Invalid address.")
    result = users_col.update_one({"_id": str(m.from_user.id), "usdt_balance": {"$gte": st["amount"]}}, {"$inc": {"usdt_balance": -st["amount"]}})
    if result.modified_count != 1:
        return raw_bot.send_message(m.chat.id, "Insufficient balance.")
    doc = {"user_id": int(m.from_user.id), "type": "withdraw", "amount": st["amount"], "address": address, "status": "pending", "created_at": time.time()}
    rid = wallet_tx_col.insert_one(doc).inserted_id
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("✅ Mark Paid", callback_data=f"walletapprove|{rid}"),
           InlineKeyboardButton("❌ Reject + Refund", callback_data=f"walletreject|{rid}"))
    for admin in get_all_admins():
        try:
            raw_bot.send_message(int(admin["_id"]), f"💸 WITHDRAW REQUEST\nUser: {m.from_user.id}\nAmount: ${st['amount']:.2f} USDT\nAddress: {address}", reply_markup=kb)
        except Exception:
            pass
    raw_bot.send_message(m.chat.id, "✅ Withdrawal requested. Funds are reserved until admin review.")

@bot.callback_query_handler(func=lambda c: c.data.startswith("walletapprove|") or c.data.startswith("walletreject|"))
def wallet_review(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    from bson import ObjectId
    action, rid = c.data.split("|", 1)
    row = wallet_tx_col.find_one({"_id": ObjectId(rid)})
    if not row or row.get("status") != "pending":
        return bot.answer_callback_query(c.id, "Already reviewed", True)
    approve = action == "walletapprove"
    status = "approved" if approve else "rejected"
    if row["type"] == "deposit" and approve:
        users_col.update_one({"_id": str(row["user_id"])}, {"$inc": {"usdt_balance": float(row["amount"])}})
    elif row["type"] == "withdraw" and not approve:
        users_col.update_one({"_id": str(row["user_id"])}, {"$inc": {"usdt_balance": float(row["amount"])}})
    wallet_tx_col.update_one({"_id": row["_id"]}, {"$set": {"status": status, "reviewed_at": time.time(), "reviewed_by": c.from_user.id}})
    try:
        raw_bot.send_message(row["user_id"], f"{'✅' if approve else '❌'} {row['type'].title()} {status}. Amount: ${float(row['amount']):.2f} USDT")
    except Exception:
        pass
    bot.answer_callback_query(c.id, status.title(), True)

@bot.message_handler(func=lambda m: m.text in ("💳 Deposits", "💸 Withdrawals") and is_admin(m.from_user.id))
def wallet_admin_list(m):
    tx_type = "deposit" if m.text.startswith("💳") else "withdraw"
    rows = list(wallet_tx_col.find({"type": tx_type}).sort("created_at", -1).limit(30))
    lines = [f"{tx_type.upper()}S - LAST 30"]
    for r in rows:
        lines.append(f"{r.get('status','?').upper()} • {r.get('user_id')} • ${float(r.get('amount',0)):.2f}")
    raw_bot.send_message(m.chat.id, "\n".join(lines) if len(lines) > 1 else f"No {tx_type}s yet.", reply_markup=admin_menu())

# =========================
# 🏷 DISCOUNTS
# =========================
@bot.message_handler(func=lambda m: m.text == "🏷 Discounts" and is_admin(m.from_user.id))
def discount_admin(m):
    cfg = get_config()
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🌐 Discount All VIP Plans", callback_data="vipdiscountall"))
    for code, row in _sorted_active_plans(highest_first=True):
        kb.add(InlineKeyboardButton(f"🏷 {row.get('name',code)} • {row.get('discount_percent',0):g}%", callback_data=f"vipplandiscount|{code}"))
    kb.add(InlineKeyboardButton("⬆️ Upgrade Discount", callback_data="vipupgradediscount"))
    raw_bot.send_message(m.chat.id, f"🏷 VIP DISCOUNTS\n\nGlobal VIP discount: {'ON' if cfg.get('discount_enabled') else 'OFF'} • {cfg.get('discount_percent',0)}%\nUpgrade discount: {cfg.get('vip_upgrade_discount_percent',0)}%", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data == "vipdiscountall")
def vip_discount_all_cb(c):
    if not is_admin(c.from_user.id): return
    msg = raw_bot.send_message(c.from_user.id, "Send discount % for ALL VIP plans (0-100). Send 0 to disable.")
    bot.register_next_step_handler(msg, vip_discount_all_save)
    bot.answer_callback_query(c.id)


def vip_discount_all_save(m):
    try:
        pct = float(m.text)
        if pct < 0 or pct > 100: raise ValueError()
        set_config("discount_percent", pct)
        set_config("discount_enabled", pct > 0)
        msg = raw_bot.send_message(m.chat.id, f"✅ Global VIP discount set to {pct:g}%.\n\nSend the discount broadcast message, or type SKIP.")
        bot.register_next_step_handler(msg, vip_discount_all_broadcast)
    except Exception:
        raw_bot.send_message(m.chat.id, "❌ Invalid percent.", reply_markup=admin_menu())


def vip_discount_all_broadcast(m):
    text = (m.text or "").strip()
    if text.upper() == "SKIP":
        return raw_bot.send_message(m.chat.id, "✅ Global discount saved without broadcast.", reply_markup=admin_menu())
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🛒 Buy VIP Now", callback_data="get_vip"))
    sent = 0
    for u in users_col.find({}, {"_id": 1}):
        try:
            raw_bot.send_message(int(u["_id"]), text, reply_markup=kb)
            sent += 1
            time.sleep(0.02)
        except Exception:
            pass
    raw_bot.send_message(m.chat.id, f"✅ Discount broadcast sent to {sent} users.", reply_markup=admin_menu())


@bot.callback_query_handler(func=lambda c: c.data == "vipupgradediscount")
def vip_upgrade_discount_cb(c):
    if not is_admin(c.from_user.id): return
    msg = raw_bot.send_message(c.from_user.id, "Send: DISCOUNT_PERCENT | VALID_DAYS\nExample: 20 | 7\nUse 0 | 0 to disable.")
    bot.register_next_step_handler(msg, vip_upgrade_discount_save)
    bot.answer_callback_query(c.id)


def vip_upgrade_discount_save(m):
    try:
        parts = [x.strip() for x in (m.text or "").split("|")]
        if len(parts) != 2: raise ValueError("Use PERCENT | DAYS")
        pct, days = float(parts[0]), int(parts[1])
        if pct < 0 or pct > 100 or days < 0 or (pct > 0 and days < 1): raise ValueError("Discounted upgrades need at least 1 valid day")
        set_config("vip_upgrade_discount_percent", pct)
        set_config("vip_upgrade_discount_until", time.time() + days*86400 if pct > 0 and days > 0 else 0)
        raw_bot.send_message(m.chat.id, f"✅ Upgrade discount set to {pct:g}%" + (f" for {days} day(s)." if days else "."), reply_markup=admin_menu())
    except Exception as exc:
        raw_bot.send_message(m.chat.id, f"❌ {exc}", reply_markup=admin_menu())


@bot.message_handler(func=lambda m: m.text == "🧾 Payments" and is_admin(m.from_user.id))
def payment_overview(m):
    paid = list(payments_col.find({"status": "paid"}).sort("created_at", -1).limit(50))
    total = sum(float(x.get("amount", 0) or 0) for x in paid)
    raw_bot.send_message(m.chat.id, f"🧾 PAYMENTS\n\nPaid records (latest 50): {len(paid)}\nTotal: ${total:.2f}", reply_markup=admin_menu())

# =========================
# 👑 VIP REMINDERS + MODERATION
# =========================
def _send_vip_expiry_reminders():
    cfg = get_cached_config()
    notice_days = int(cfg.get("vip_expiry_notice_days", 5) or 5)
    now = time.time()
    cutoff = now + notice_days * 86400
    day_key = datetime.utcfromtimestamp(now + 5*3600).strftime("%Y-%m-%d")
    rows = list(subscriptions_col.find({"status": "active", "expires_at": {"$gt": now, "$lte": cutoff}}).limit(500))
    for sub in rows:
        if sub.get("last_notice_day") == day_key:
            continue
        uid = int(sub["user_id"])
        days_left = max(1, int((float(sub["expires_at"]) - now + 86399) // 86400))
        try:
            raw_bot.send_message(uid, f"⏳ VIP EXPIRY NOTICE\n\nYour Globexomart VIP expires in about {days_left} day(s).\nExpiry: {datetime.fromtimestamp(sub['expires_at']).strftime('%Y-%m-%d %H:%M')}")
            subscriptions_col.update_one({"_id": sub["_id"]}, {"$set": {"last_notice_day": day_key}})
        except Exception:
            pass

@bot.message_handler(commands=["vipban", "vipmute", "vipunmute"])
def vip_moderation_cmd(m):
    if not is_admin(m.from_user.id):
        return
    try:
        parts = (m.text or "").split()
        if len(parts) != 2:
            raise ValueError("Usage: /vipban USER_ID or /vipmute USER_ID or /vipunmute USER_ID")
        uid = int(parts[1])
        cmd = parts[0].lower()
        if cmd == "/vipban":
            users_col.update_one({"_id": str(uid)}, {"$set": {"banned": True}})
            expire_subscription(uid, reason="banned", removed_by=m.from_user.id)
            msg = "User banned and VIP access removed."
        elif cmd == "/vipmute":
            users_col.update_one({"_id": str(uid)}, {"$set": {"muted": True}})
            msg = "User muted in bot records."
        else:
            users_col.update_one({"_id": str(uid)}, {"$set": {"muted": False}})
            msg = "User unmuted."
        raw_bot.send_message(m.chat.id, "✅ " + msg, reply_markup=admin_menu())
    except Exception as exc:
        admin_error(m.from_user.id, exc)


def _expire_due_subscriptions():
    now = time.time()
    due = list(subscriptions_col.find({"status": "active", "expires_at": {"$lte": now}}).limit(200))
    for sub in due:
        uid = int(sub["user_id"])
        try:
            expire_subscription(uid, reason="expired", removed_by="system")
            try:
                raw_bot.send_message(uid, "⏳ Your Globexomart subscription has expired. Your private channel/group access was removed.")
            except Exception:
                pass
            log_event("subscription_expired", uid, details={"plan": sub.get("plan")})
        except Exception as exc:
            log_event("subscription_expiry_error", uid, details={"error": str(exc)}, level="error")


def _send_daily_owner_report():
    if not get_cached_config().get("daily_owner_report", True):
        return
    now=time.time()
    start_day=datetime.fromtimestamp(now).replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
    active_docs=list(subscriptions_col.find({"status":"active","$or":[{"expires_at":None},{"expires_at":{"$gt":now}}]}).sort("created_at",-1))
    today_docs=list(vip_events_col.find({"created_at":{"$gte":start_day}}).sort("created_at",-1))
    old_docs=[x for x in active_docs if float(x.get("created_at",0) or 0)<start_day]
    pay_docs=list(payments_col.find({"created_at":{"$gte":start_day},"status":"paid"}))
    revenue=sum(float(x.get("amount",0) or 0) for x in pay_docs)
    lines=["📊 GLOBEXOMART DAILY VIP REPORT","",f"👥 Total bot users: {users_col.count_documents({})}",f"💎 Active VIP users: {len(active_docs)}",f"🆕 VIP added today: {len(today_docs)}",f"🗂 Older active VIP users: {len(old_docs)}",f"💰 Revenue recorded today: ${revenue:.2f}","","🆕 TODAY'S VIP USERS"]
    if today_docs:
        for ev in today_docs[:50]:
            u=users_col.find_one({"_id":str(ev.get("user_id"))}) or {}
            uname="@"+u.get("username") if u.get("username") else str(ev.get("user_id"))
            lines.append(f"• {uname} | {ev.get('interval','?')} | ${float(ev.get('amount',0) or 0):g}")
    else: lines.append("• None")
    lines+=["","🗂 OLDER ACTIVE VIP USERS"]
    if old_docs:
        for sub in old_docs[:50]:
            exp=sub.get("expires_at")
            remain="Lifetime" if exp is None else f"{max(0,int(exp-now))//86400}d {(max(0,int(exp-now))%86400)//3600}h"
            u=users_col.find_one({"_id":str(sub.get("user_id"))}) or {}
            uname="@"+u.get("username") if u.get("username") else str(sub.get("user_id"))
            lines.append(f"• {uname} | {sub.get('plan','CUSTOM')} | {remain}")
    else: lines.append("• None")
    raw_bot.send_message(ADMIN_ID,"\n".join(lines)[:4000])


def subscription_maintenance_loop():
    last_report_day = None
    while True:
        try:
            _expire_due_subscriptions()
            _send_vip_expiry_reminders()
            _run_due_auto_broadcasts()
            now_dt = datetime.utcfromtimestamp(time.time() + globals().get("TZ_OFFSET_SECONDS", 5 * 3600))
            # One report per calendar day, after 09:00 Asia/Karachi time.
            day_key = now_dt.strftime("%Y-%m-%d")
            if now_dt.hour >= 9 and last_report_day != day_key:
                _send_daily_owner_report()
                last_report_day = day_key
        except Exception as exc:
            try:
                log_event("subscription_maintenance_error", details={"error": str(exc)}, level="error")
            except Exception:
                pass
        time.sleep(60)


threading.Thread(target=subscription_maintenance_loop, name="globexomart-subscriptions", daemon=True).start()


# =========================
# 🚀 GLOBEXOMART ADVANCED VIP / REFERRAL / ANALYTICS / AUTO BROADCAST
# =========================

def _record_vip_activation(uid, sub):
    interval = "Lifetime" if sub.get("expires_at") is None else str(sub.get("plan") or sub.get("interval") or f"{sub.get('days','?')} days")
    vip_events_col.insert_one({
        "user_id": int(uid), "plan": sub.get("plan"), "interval": interval,
        "amount": float(sub.get("price_usd",0) or 0), "payment_mode": sub.get("payment_mode"),
        "expires_at": sub.get("expires_at"), "created_at": time.time()
    })

def _notify_owner_vip_join(uid, sub):
    u=users_col.find_one({"_id":str(uid)}) or {}
    name=" ".join(x for x in [u.get("first_name"),u.get("last_name")] if x) or "Unknown"
    username="@"+u.get("username") if u.get("username") else "No username"
    exp=sub.get("expires_at")
    expiry="Lifetime" if exp is None else datetime.fromtimestamp(exp).strftime("%Y-%m-%d %H:%M")
    interval="Lifetime" if exp is None else str(sub.get("plan") or f"{sub.get('days','?')} days")
    msg=(f"💎 NEW VIP MEMBER\n\n👤 Name: {name}\n🔗 Username: {username}\n🆔 Chat ID: {uid}\n"
         f"📦 Plan/Interval: {interval}\n💰 Paid: ${float(sub.get('price_usd',0) or 0):g}\n"
         f"💳 Mode: {sub.get('payment_mode','manual')}\n⏳ Expiry: {expiry}")
    for adm in get_all_admins():
        try: raw_bot.send_message(int(adm["_id"]),msg)
        except Exception: pass

def _send_vip_rules(uid):
    rules=get_cached_config().get("vip_rules_message") or ""
    if rules:
        try: raw_bot.send_message(int(uid),rules)
        except Exception: pass

def _send_vip_welcome_bundle(uid, sub, links):
    try:
        exp=sub.get("expires_at")
        expiry="Lifetime" if exp is None else datetime.fromtimestamp(exp).strftime("%Y-%m-%d %H:%M")
        raw_bot.send_message(uid,f"🎉 VIP ACTIVATED\n\nPlan: {sub.get('plan','CUSTOM')}\nExpires: {expiry}")
        for chat,link,err in links or []:
            if link:
                raw_bot.send_message(uid,f"🔐 Your private VIP invite link:\n{link}\n\nThis link is for one user and expires in 24 hours.")
        _send_vip_rules(uid)
    except Exception: pass

def grant_custom_vip(uid, seconds, label, added_by=None):
    now=time.time()
    expires_at=None if seconds is None else now+int(seconds)
    subscriptions_col.update_many({"user_id":int(uid),"status":"active"},{"$set":{"status":"superseded","superseded_at":now}})
    sub={"user_id":int(uid),"plan":"LIFETIME" if seconds is None else "CUSTOM","interval":label,
         "days":None if seconds is None else float(seconds)/86400,"price_usd":0.0,"payment_mode":"admin",
         "payment_ref":None,"status":"active","starts_at":now,"expires_at":expires_at,"created_at":now,"added_by":added_by}
    subscriptions_col.insert_one(sub)
    _sync_user_vip(uid,expires_at)
    links=_grant_chat_access(uid)
    _record_vip_activation(uid,sub)
    _notify_owner_vip_join(uid,sub)
    return sub,links

def _process_referral_vip_sale(buyer_id, amount, sub):
    # Only paid VIP activations generate affiliate commission.
    if float(amount or 0)<=0 or not sub.get("payment_ref") or str(sub.get("payment_mode","")).lower() in ("admin","free","referral"):
        return
    buyer=users_col.find_one({"_id":str(buyer_id)}) or {}
    ref=buyer.get("ref")
    if not ref or not str(ref).isdigit() or str(ref)==str(buyer_id):
        return
    referrer_id=int(ref)
    pct=max(0.0,min(15.0,float(get_cached_config().get("referral_commission_percent",15) or 0)))
    commission=round(float(amount)*pct/100.0,2)
    sale_key=str(sub.get("payment_ref"))
    if referral_sales_col.find_one({"payment_ref":sale_key}):
        return
    referral_sales_col.insert_one({"payment_ref":sale_key,"referrer_id":referrer_id,"buyer_id":int(buyer_id),"amount":float(amount),"percent":pct,"commission":commission,"created_at":time.time()})
    if commission>0:
        users_col.update_one({"_id":str(referrer_id)},{"$inc":{"referral_balance_usdt":commission,"referral_total_earned_usdt":commission}},upsert=True)
    unique_buyers=len(referral_sales_col.distinct("buyer_id",{"referrer_id":referrer_id}))
    cfg=get_cached_config(); target=int(cfg.get("referral_vip_bonus_target",10) or 10); bonus=float(cfg.get("referral_vip_bonus_usdt",10) or 10)
    row=users_col.find_one({"_id":str(referrer_id)}) or {}
    bonus_text=""
    if unique_buyers>=target and not row.get("referral_bonus_awarded"):
        result=users_col.update_one({"_id":str(referrer_id),"referral_bonus_awarded":{"$ne":True}},
            {"$set":{"referral_bonus_awarded":True},"$inc":{"referral_balance_usdt":bonus,"referral_total_earned_usdt":bonus,"referral_bonus_earned_usdt":bonus}})
        if result.modified_count: bonus_text=f"\n🏆 Milestone prize: +${bonus:g} USDT"
    try:
        raw_bot.send_message(referrer_id,f"💸 REFERRAL VIP SALE\n\nYour referral bought VIP for ${float(amount):g}.\nCommission ({pct:g}%): +${commission:g} USDT\nPaid VIP referrals: {unique_buyers}/{target}{bonus_text}")
    except Exception: pass

# Replace referral card with affiliate earnings information.
def send_referral_card(uid):
    user=User(uid); cfg=get_cached_config(); username=bot.get_me().username
    link=f"https://t.me/{username}?start={uid}"
    row=users_col.find_one({"_id":str(uid)}) or {}
    refs=int(user.get_refs_count() or 0)
    buyers=len(referral_sales_col.distinct("buyer_id",{"referrer_id":int(uid)}))
    pct=max(0.0,min(15.0,float(cfg.get("referral_commission_percent",15) or 0)))
    target=int(cfg.get("referral_vip_bonus_target",10) or 10)
    prize=float(cfg.get("referral_vip_bonus_usdt",10) or 10)
    bal=float(row.get("referral_balance_usdt",0) or 0)
    earned=float(row.get("referral_total_earned_usdt",0) or 0)
    minimum=float(cfg.get("referral_min_withdraw_usdt",10) or 10)
    progress=min(100,int((buyers/max(1,target))*100)); filled=min(10,progress//10); bar="🟩"*filled+"⬜"*(10-filled)
    text=(f"🎁 GLOBEXOMART REFER & EARN\n━━━━━━━━━━━━━━━━━━━━\n\n"
          f"Turn your network into earnings. Share your personal link and earn up to {pct:g}% every time one of your referred users buys a VIP plan.\n\n"
          f"🏆 BONUS: Get ${prize:g} when {target} unique referrals become paid VIP members — and you still keep your {pct:g}% commission on every sale.\n\n"
          f"🔗 Your personal link:\n{link}\n\n📊 YOUR RESULTS\n{bar} {progress}%\n"
          f"👥 Verified referrals: {refs}\n💎 Paid VIP referrals: {buyers}/{target}\n"
          f"💰 Total referral earnings: ${earned:.2f}\n💵 Withdrawable balance: ${bal:.2f}\n"
          f"🏧 Minimum withdrawal: ${minimum:g}\n\nShare your link with friends, communities and customers. The more paid VIP members you refer, the more you can earn.")
    kb=InlineKeyboardMarkup(row_width=2)
    share=f"https://t.me/share/url?url={link}&text=Join%20GLOBEXOMART%20VIP"
    kb.row(InlineKeyboardButton("📤 Share Link",url=share),InlineKeyboardButton("💸 Withdraw Earnings",callback_data="refwithdraw|start"))
    raw_bot.send_message(uid,text,reply_markup=kb,disable_web_page_preview=True)

_ref_withdraw_state={}
@bot.callback_query_handler(func=lambda c: c.data=="refwithdraw|start")
def refwithdraw_start(c):
    cfg=get_cached_config(); row=users_col.find_one({"_id":str(c.from_user.id)}) or {}
    bal=float(row.get("referral_balance_usdt",0) or 0); minimum=float(cfg.get("referral_min_withdraw_usdt",10) or 10)
    if bal<minimum: return bot.answer_callback_query(c.id,f"Minimum withdrawal is ${minimum:g}",True)
    msg=raw_bot.send_message(c.from_user.id,f"Referral balance: ${bal:.2f}\nMinimum: ${minimum:g}\n\nSend withdrawal amount in USDT:")
    bot.register_next_step_handler(msg,refwithdraw_amount); bot.answer_callback_query(c.id)

def refwithdraw_amount(m):
    try:
        amount=float((m.text or "").strip()); cfg=get_cached_config(); minimum=float(cfg.get("referral_min_withdraw_usdt",10) or 10)
        row=users_col.find_one({"_id":str(m.from_user.id)}) or {}; bal=float(row.get("referral_balance_usdt",0) or 0)
        if amount<minimum or amount>bal: raise ValueError(f"Amount must be between ${minimum:g} and ${bal:.2f}")
        _ref_withdraw_state[m.from_user.id]={"amount":amount}
        msg=raw_bot.send_message(m.chat.id,"Send your USDT payout address/details:")
        bot.register_next_step_handler(msg,refwithdraw_details)
    except Exception as exc: raw_bot.send_message(m.chat.id,f"❌ {exc}")

def refwithdraw_details(m):
    st=_ref_withdraw_state.pop(m.from_user.id,None)
    if not st: return
    details=(m.text or "").strip()
    result=users_col.update_one({"_id":str(m.from_user.id),"referral_balance_usdt":{"$gte":st["amount"]}},{"$inc":{"referral_balance_usdt":-st["amount"]}})
    if result.modified_count!=1: return raw_bot.send_message(m.chat.id,"❌ Balance changed. Try again.")
    doc={"user_id":m.from_user.id,"amount":st["amount"],"details":details,"status":"pending","created_at":time.time()}
    oid=referral_withdrawals_col.insert_one(doc).inserted_id
    kb=InlineKeyboardMarkup(row_width=2); kb.add(InlineKeyboardButton("✅ Mark Paid",callback_data=f"refwd|paid|{oid}"),InlineKeyboardButton("❌ Reject",callback_data=f"refwd|reject|{oid}"))
    for adm in get_all_admins():
        try: raw_bot.send_message(int(adm["_id"]),f"💸 REFERRAL WITHDRAWAL\n\nUser: {m.from_user.id}\nAmount: ${st['amount']:g}\nDetails: {details}",reply_markup=kb)
        except Exception: pass
    raw_bot.send_message(m.chat.id,"✅ Withdrawal request submitted.")

@bot.callback_query_handler(func=lambda c:c.data.startswith("refwd|"))
def refwd_review(c):
    if not is_admin(c.from_user.id): return
    try:
        from bson import ObjectId
        _,action,oid=c.data.split("|",2); row=referral_withdrawals_col.find_one({"_id":ObjectId(oid)})
        if not row or row.get("status")!="pending": return bot.answer_callback_query(c.id,"Already reviewed",True)
        if action=="reject":
            referral_withdrawals_col.update_one({"_id":row["_id"],"status":"pending"},{"$set":{"status":"rejected","reviewed_at":time.time(),"reviewed_by":c.from_user.id}})
            users_col.update_one({"_id":str(row["user_id"])},{"$inc":{"referral_balance_usdt":float(row["amount"])}})
            raw_bot.send_message(int(row["user_id"]),f"❌ Referral withdrawal rejected. ${float(row['amount']):g} returned to your referral balance.")
        else:
            referral_withdrawals_col.update_one({"_id":row["_id"],"status":"pending"},{"$set":{"status":"paid","reviewed_at":time.time(),"reviewed_by":c.from_user.id}})
            raw_bot.send_message(int(row["user_id"]),f"✅ Referral withdrawal marked paid: ${float(row['amount']):g}")
        bot.answer_callback_query(c.id,"Updated",True)
    except Exception as exc: admin_error(c.from_user.id,exc)

@bot.message_handler(func=lambda m:m.text=="🎁 Referral Settings" and is_admin(m.from_user.id))
def referral_settings_menu(m):
    cfg=get_cached_config(); kb=InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(f"💹 Commission: {float(cfg.get('referral_commission_percent',15)):g}%",callback_data="refsettings|commission"))
    kb.add(InlineKeyboardButton(f"🏧 Min Withdraw: ${float(cfg.get('referral_min_withdraw_usdt',10)):g}",callback_data="refsettings|min"))
    kb.add(InlineKeyboardButton(f"🏆 Bonus: {int(cfg.get('referral_vip_bonus_target',10))} VIP = ${float(cfg.get('referral_vip_bonus_usdt',10)):g}",callback_data="refsettings|bonus"))
    raw_bot.send_message(m.chat.id,"🎁 REFERRAL SETTINGS",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith("refsettings|"))
def referral_settings_cb(c):
    if not is_admin(c.from_user.id): return
    action=c.data.split("|",1)[1]
    if action=="open":
        fake=type("M",(),{"text":"🎁 Referral Settings","from_user":c.from_user,"chat":c.message.chat})()
        referral_settings_menu(fake); return bot.answer_callback_query(c.id)
    prompts={"commission":"Send referral commission percent (0-15):","min":"Send minimum referral withdrawal in USDT:","bonus":"Send bonus as target,amount  Example: 10,10"}
    msg=raw_bot.send_message(c.from_user.id,prompts[action]); bot.register_next_step_handler(msg,lambda m:referral_setting_save(m,action)); bot.answer_callback_query(c.id)

def referral_setting_save(m,action):
    try:
        if action=="commission":
            v=float(m.text); 
            if not 0<=v<=15: raise ValueError("Commission must be 0-15")
            set_config("referral_commission_percent",v)
        elif action=="min":
            v=float(m.text); 
            if v<0: raise ValueError("Invalid minimum")
            set_config("referral_min_withdraw_usdt",v)
        else:
            a,b=[x.strip() for x in m.text.split(",",1)]; target=int(a); prize=float(b)
            if target<1 or prize<0: raise ValueError("Invalid bonus")
            set_config("referral_vip_bonus_target",target); set_config("referral_vip_bonus_usdt",prize)
        admin_success(m.from_user.id,"Referral settings updated.")
    except Exception as exc: admin_error(m.from_user.id,exc)

@bot.message_handler(func=lambda m:m.text=="💸 Ref Withdrawals" and is_admin(m.from_user.id))
def referral_withdrawals_admin(m):
    rows=list(referral_withdrawals_col.find({}).sort("created_at",-1).limit(30))
    txt=["💸 REFERRAL WITHDRAWALS",""]
    for x in rows: txt.append(f"• {x.get('user_id')} | ${float(x.get('amount',0)):g} | {x.get('status')}")
    raw_bot.send_message(m.chat.id,"\n".join(txt) if rows else "No referral withdrawals.")

@bot.message_handler(func=lambda m:m.text=="📝 VIP Messages" and is_admin(m.from_user.id))
def vip_messages_menu(m):
    kb=InlineKeyboardMarkup(row_width=1); kb.add(InlineKeyboardButton("📝 Edit Buy VIP Explanation",callback_data="vipmsg|buy"),InlineKeyboardButton("📜 Edit Instructions & Rules",callback_data="vipmsg|rules"))
    raw_bot.send_message(m.chat.id,"📝 VIP MESSAGES",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith("vipmsg|"))
def vipmsg_cb(c):
    if not is_admin(c.from_user.id): return
    kind=c.data.split("|",1)[1]; current=get_cached_config().get("vip_buy_message" if kind=="buy" else "vip_rules_message","")
    msg=raw_bot.send_message(c.from_user.id,f"Current message:\n\n{current}\n\nSend the new message:")
    bot.register_next_step_handler(msg,lambda m:vipmsg_save(m,kind)); bot.answer_callback_query(c.id)

def vipmsg_save(m,kind):
    key="vip_buy_message" if kind=="buy" else "vip_rules_message"; val=(m.text or m.caption or "").strip()
    if not val: return admin_error(m.from_user.id,"Message cannot be empty")
    set_config(key,val); admin_success(m.from_user.id,"VIP message updated.")

def record_force_join_verification(uid, targets):
    now=time.time()
    for target in targets or []:
        key=str(target)
        force_join_stats_col.update_one({"chat_id":key,"user_id":int(uid)},
            {"$setOnInsert":{"chat_id":key,"user_id":int(uid),"first_verified_at":now},"$set":{"last_verified_at":now}},upsert=True)

@bot.message_handler(func=lambda m:m.text=="📊 Force Join Stats" and is_admin(m.from_user.id))
def force_join_stats_admin(m):
    cfg=get_cached_config(); targets=list(dict.fromkeys((cfg.get("force_channels") or [])+(cfg.get("force_groups") or [])))
    start=datetime.now().replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
    lines=["📊 FORCE JOIN ANALYTICS",""]
    if not targets: lines.append("No force-join chats configured.")
    for t in targets:
        total=force_join_stats_col.count_documents({"chat_id":str(t)})
        today=force_join_stats_col.count_documents({"chat_id":str(t),"first_verified_at":{"$gte":start}})
        try: members=bot.get_chat_member_count(t)
        except Exception: members="Unavailable"
        lines.append(f"• {t}\n  Telegram members: {members}\n  Verified today: {today}\n  Verified total: {total}")
    raw_bot.send_message(m.chat.id,"\n".join(lines)[:4000])

@bot.message_handler(func=lambda m:m.text=="📈 VIP Analytics" and is_admin(m.from_user.id))
def vip_analytics_admin(m):
    now=time.time(); start=datetime.now().replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
    active=list(subscriptions_col.find({"status":"active","$or":[{"expires_at":None},{"expires_at":{"$gt":now}}]}).sort("created_at",-1))
    today=list(vip_events_col.find({"created_at":{"$gte":start}}).sort("created_at",-1))
    lines=["📈 VIP ANALYTICS","",f"Active VIP: {len(active)}",f"Added today: {len(today)}",f"Historical VIP activations: {vip_events_col.count_documents({})}",""]
    for ev in today[:30]:
        lines.append(f"• {ev.get('user_id')} | {ev.get('interval')} | ${float(ev.get('amount',0) or 0):g}")
    raw_bot.send_message(m.chat.id,"\n".join(lines)[:4000])

_auto_broadcast_state={}
def _capture_broadcast_source(m):
    return {"chat_id":m.chat.id,"message_id":m.message_id,"content_type":m.content_type}

def _calc_next_daily_run(hhmm):
    # Schedule using Pakistan Standard Time (UTC+5), independent of Railway server timezone.
    h,mi=map(int,hhmm.split(":"))
    now_utc=datetime.utcnow()
    now_pk=now_utc+timedelta(hours=5)
    run_pk=now_pk.replace(hour=h,minute=mi,second=0,microsecond=0)
    if run_pk<=now_pk:
        run_pk+=timedelta(days=1)
    run_utc=run_pk-timedelta(hours=5)
    return run_utc.timestamp()

@bot.message_handler(func=lambda m:m.text=="📣 Auto Broadcast" and is_admin(m.from_user.id))
def auto_broadcast_menu(m):
    rows=list(auto_broadcasts_col.find({}).sort("created_at",-1).limit(20))
    kb=InlineKeyboardMarkup(row_width=1); kb.add(InlineKeyboardButton("➕ Add Daily Broadcast",callback_data="autobc|add"))
    for r in rows:
        state="🟢" if r.get("active",True) else "🔴"; kb.add(InlineKeyboardButton(f"{state} {r.get('time','--:--')} • {r.get('target','all')}",callback_data=f"autobc|view|{r['_id']}"))
    raw_bot.send_message(m.chat.id,"📣 AUTO BROADCAST\n\nCreate unlimited daily scheduled broadcasts. Text, photos, videos, documents, audio, animation and voice are supported.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith("autobc|"))
def autobc_cb(c):
    if not is_admin(c.from_user.id): return
    try:
        parts=c.data.split("|"); action=parts[1]
        if action=="add":
            _auto_broadcast_state[c.from_user.id]={}
            kb=InlineKeyboardMarkup(row_width=1)
            for t,label in [("all","👥 All Users"),("vip","💎 VIP Users"),("free","🆓 Free Users")]: kb.add(InlineKeyboardButton(label,callback_data=f"autobc|target|{t}"))
            raw_bot.send_message(c.from_user.id,"Choose recipients:",reply_markup=kb)
        elif action=="target":
            _auto_broadcast_state[c.from_user.id]={"target":parts[2]}
            msg=raw_bot.send_message(c.from_user.id,"Send daily time as HH:MM (24-hour, Pakistan time). Example: 21:30")
            bot.register_next_step_handler(msg,autobc_time_step)
        elif action=="view":
            from bson import ObjectId
            r=auto_broadcasts_col.find_one({"_id":ObjectId(parts[2])})
            if not r: return bot.answer_callback_query(c.id,"Not found",True)
            kb=InlineKeyboardMarkup(row_width=2); kb.add(InlineKeyboardButton("⏯ Toggle",callback_data=f"autobc|toggle|{r['_id']}"),InlineKeyboardButton("🧪 Test",callback_data=f"autobc|test|{r['_id']}"),InlineKeyboardButton("🗑 Delete",callback_data=f"autobc|delete|{r['_id']}"))
            raw_bot.send_message(c.from_user.id,f"📣 Broadcast\nTime: {r.get('time')}\nTarget: {r.get('target')}\nStatus: {'ON' if r.get('active') else 'OFF'}",reply_markup=kb)
        elif action in ("toggle","delete","test"):
            from bson import ObjectId
            oid=ObjectId(parts[2]); r=auto_broadcasts_col.find_one({"_id":oid})
            if action=="toggle":
                auto_broadcasts_col.update_one({"_id":oid},{"$set":{"active":not r.get("active",True)}})
            elif action=="delete": auto_broadcasts_col.delete_one({"_id":oid})
            else: raw_bot.copy_message(c.from_user.id,int(r["source_chat_id"]),int(r["source_message_id"]))
        bot.answer_callback_query(c.id)
    except Exception as exc: admin_error(c.from_user.id,exc)

def autobc_time_step(m):
    try:
        raw=(m.text or "").strip()
        if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d",raw): raise ValueError("Use HH:MM, e.g. 21:30")
        st=_auto_broadcast_state.setdefault(m.from_user.id,{})
        st["time"]=raw
        msg=raw_bot.send_message(m.chat.id,"Now send/forward the broadcast message. You can send text, photo, video, document/file, audio, animation or voice.")
        bot.register_next_step_handler(msg,autobc_message_step)
    except Exception as exc: raw_bot.send_message(m.chat.id,f"❌ {exc}")

def autobc_message_step(m):
    st=_auto_broadcast_state.pop(m.from_user.id,None)
    if not st: return
    doc={"target":st.get("target","all"),"time":st["time"],"source_chat_id":m.chat.id,"source_message_id":m.message_id,"content_type":m.content_type,
         "active":True,"next_run":_calc_next_daily_run(st["time"]),"created_at":time.time(),"created_by":m.from_user.id}
    auto_broadcasts_col.insert_one(doc)
    admin_success(m.from_user.id,f"Daily broadcast saved for {st['time']} Pakistan time.")

def _run_due_auto_broadcasts():
    now=time.time()
    for r in auto_broadcasts_col.find({"active":True,"next_run":{"$lte":now}}):
        sent=failed=0
        target=r.get("target","all")
        for u in users_col.find({},{"_id":1,"vip":1,"vip_expiry":1}):
            try:
                is_vip=bool(u.get("vip")) and (not u.get("vip_expiry") or float(u.get("vip_expiry"))>now)
                if target=="vip" and not is_vip: continue
                if target=="free" and is_vip: continue
                raw_bot.copy_message(int(u["_id"]),int(r["source_chat_id"]),int(r["source_message_id"]))
                sent+=1; time.sleep(0.03)
            except Exception: failed+=1
        auto_broadcasts_col.update_one({"_id":r["_id"]},{"$set":{"last_run":now,"last_sent":sent,"last_failed":failed,"next_run":_calc_next_daily_run(r["time"])}})
        try: raw_bot.send_message(ADMIN_ID,f"📣 Auto broadcast completed\nSent: {sent}\nFailed: {failed}\nNext: {r.get('time')} tomorrow")
        except Exception: pass


# =========================
# 🧠 FALLBACK
# =========================
# 🧩 CONFIGURABLE PLANS / CHANNEL OFFERS / SUPPORT / PROOFS / RESELLER APIs
# =========================

def _safe_member(chat, uid):
    try:
        member = bot.get_chat_member(normalize_chat_reference(chat), int(uid))
        return member.status not in ("left", "kicked")
    except Exception:
        return False


def _matching_channel_offers(uid, kind=None):
    rows = []
    for offer in get_cached_config().get("channel_offers", []) or []:
        if not offer.get("active", True):
            continue
        if kind and offer.get("type") != kind:
            continue
        if _safe_member(offer.get("chat"), uid):
            rows.append(offer)
    return rows


def _effective_plan_price(uid, code):
    code = str(code).upper()
    row = get_subscription_plans().get(code) or {}
    price = float(row.get("price", 0) or 0)

    # Per-plan discount overrides the global VIP discount for that plan.
    plan_pct = max(0.0, min(100.0, float(row.get("discount_percent", 0) or 0)))
    cfg = get_cached_config()
    if plan_pct:
        price *= (100.0 - plan_pct) / 100.0
    elif cfg.get("discount_enabled"):
        global_pct = max(0.0, min(100.0, float(cfg.get("discount_percent", 0) or 0)))
        price *= (100.0 - global_pct) / 100.0

    # Existing channel-member discounts.
    discounts = _matching_channel_offers(uid, "discount")
    if discounts:
        percent = max(float(x.get("percent", 0) or 0) for x in discounts)
        price *= max(0.0, 100.0 - min(percent, 100.0)) / 100.0

    # Upgrade-only discount. Valid until configured timestamp when present.
    current = _active_subscription(uid) if "_active_subscription" in globals() else None
    if current:
        plans = get_subscription_plans()
        current_row = plans.get(str(current.get("plan", "")).upper()) or {}
        if _plan_rank(row) > _plan_rank(current_row):
            until = float(cfg.get("vip_upgrade_discount_until", 0) or 0)
            pct = max(0.0, min(100.0, float(cfg.get("vip_upgrade_discount_percent", 0) or 0)))
            if pct and (until <= 0 or time.time() <= until):
                price *= (100.0 - pct) / 100.0
    return round(price, 2)


def _eligible_trial_offer(uid):
    for offer in _matching_channel_offers(uid, "trial"):
        oid = str(offer.get("id"))
        if oid and not channel_offer_usage_col.find_one({"user_id": int(uid), "offer_id": oid}):
            return offer
    return None


@bot.callback_query_handler(func=lambda c: c.data == "offertrial|claim")
def claim_channel_trial(c):
    offer = _eligible_trial_offer(c.from_user.id)
    if not offer:
        return bot.answer_callback_query(c.id, "No unused channel trial is available", True)
    days = max(1, int(offer.get("days", 1)))
    code = "TRIAL_" + str(offer.get("id"))[-8:].upper()
    now = time.time()
    subscriptions_col.update_many({"user_id": int(c.from_user.id), "status": "active"}, {"$set": {"status": "superseded", "superseded_at": now}})
    sub = {"user_id": int(c.from_user.id), "plan": code, "days": days, "price_usd": 0.0, "payment_mode": "channel_trial", "status": "active", "starts_at": now, "expires_at": now + days*86400, "created_at": now}
    subscriptions_col.insert_one(sub)
    _sync_user_vip(c.from_user.id, sub["expires_at"])
    links = _grant_chat_access(c.from_user.id)
    channel_offer_usage_col.insert_one({"user_id": int(c.from_user.id), "offer_id": str(offer.get("id")), "used_at": now})
    good = [link for _, link, err in links if link and not err]
    text = f"🎁 TRIAL ACTIVATED\n\nDuration: {days} day(s)\nExpires: {datetime.fromtimestamp(sub['expires_at']).strftime('%Y-%m-%d %H:%M')}"
    if good: text += "\n\nPrivate access:\n" + "\n".join(good)
    raw_bot.send_message(c.from_user.id, text, disable_web_page_preview=True)
    bot.answer_callback_query(c.id, "Trial activated")


# ---- VIP plan names / custom plans ----
@bot.callback_query_handler(func=lambda c: c.data.startswith("vipplanname|"))
def vip_plan_name_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id, "Admin only", True)
    code = c.data.split("|",1)[1]
    row = get_subscription_plans().get(code, {})
    msg = raw_bot.send_message(c.from_user.id, f"Send new button/package name.\nCurrent: {row.get('name',code)}")
    bot.register_next_step_handler(msg, lambda m: _vip_plan_name_save(m, code))
    bot.answer_callback_query(c.id)


def _vip_plan_name_save(m, code):
    name = (m.text or "").strip()
    if not name or len(name) > 40:
        return raw_bot.send_message(m.chat.id, "❌ Name must be 1-40 characters.", reply_markup=admin_menu())
    plans = get_config().get("subscription_plans") or {}
    row = dict(plans.get(code) or get_subscription_plans().get(code) or {"days":30,"price":0})
    row["name"] = name
    plans[code] = row
    set_config("subscription_plans", plans)
    raw_bot.send_message(m.chat.id, f"✅ Plan renamed to {name}.", reply_markup=admin_menu())


@bot.callback_query_handler(func=lambda c: c.data.startswith("vipplantoggle|"))
def vip_plan_toggle_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id, "Admin only", True)
    code = c.data.split("|",1)[1]
    plans = get_config().get("subscription_plans") or {}
    row = dict(plans.get(code) or get_subscription_plans().get(code) or {})
    row["active"] = not bool(row.get("active", True))
    plans[code] = row
    set_config("subscription_plans", plans)
    bot.answer_callback_query(c.id, "Plan enabled" if row["active"] else "Plan hidden", True)


@bot.callback_query_handler(func=lambda c: c.data == "vipplanadd")
def vip_plan_add_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id, "Admin only", True)
    msg = raw_bot.send_message(c.from_user.id, "➕ ADD VIP PLAN\n\nSend: NAME | DURATION | PRICE\nExamples: Trial | 30m | 1\nHourly | 2h | 5\nMonthly | 30d | 25")
    bot.register_next_step_handler(msg, _vip_plan_add_save)
    bot.answer_callback_query(c.id)


def _vip_plan_add_save(m):
    try:
        parts = [x.strip() for x in (m.text or "").split("|")]
        if len(parts) != 3: raise ValueError("Use NAME | DAYS | PRICE")
        name, duration_text, price = parts[0][:40], parts[1], float(parts[2])
        minutes = _parse_duration_minutes(duration_text)
        if not name or price < 0: raise ValueError("Invalid values")
        plans = get_config().get("subscription_plans") or {}
        code = "P" + str(int(time.time()*1000))[-10:]
        plans[code] = {"name": name, "duration_minutes": minutes, "days": minutes / 1440.0, "price": price, "active": True, "discount_percent": 0.0}
        set_config("subscription_plans", plans)
        raw_bot.send_message(m.chat.id, f"✅ Added {name}: {_format_duration_minutes(minutes)}, ${price:g} USDT.", reply_markup=admin_menu())
    except Exception as exc:
        raw_bot.send_message(m.chat.id, f"❌ {exc}", reply_markup=admin_menu())


# ---- Proof channel ----
def _publish_proof(kind, record, screenshot_chat_id=None, screenshot_message_id=None):
    target = get_cached_config().get("proof_channel")
    if not target:
        return
    try:
        target = normalize_chat_reference(target)
        uid = record.get("user_id")
        username = record.get("username") or ""
        amount = record.get("amount", record.get("total", 0))
        title = {"VIP":"PROOF VIP", "METHOD":"PROOF METHOD", "PRODUCT":"PRODUCT PROOF"}.get(str(kind).upper(), "PURCHASE PROOF")
        text = f"✅ {title}\n\n👤 User: @{username}" if username else f"✅ {title}\n\n👤 User ID: {uid}"
        text += f"\n🆔 User ID: {uid}\n💰 Amount: ${float(amount or 0):g} USDT\n🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        if screenshot_chat_id and screenshot_message_id:
            try:
                raw_bot.copy_message(target, int(screenshot_chat_id), int(screenshot_message_id), caption=text)
                return
            except Exception:
                pass
        raw_bot.send_message(target, text)
    except Exception as exc:
        log_event("proof_publish_error", record.get("user_id"), details={"error":str(exc)}, level="error")


@bot.message_handler(func=lambda m: m.text == "🧾 Proof Settings" and is_admin(m.from_user.id))
def proof_settings_menu(m):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("📢 Set Proof Channel", callback_data="proofset|channel"))
    kb.add(InlineKeyboardButton("🗑 Disable Proof Channel", callback_data="proofset|off"))
    raw_bot.send_message(m.from_user.id, f"🧾 PROOF SETTINGS\n\nCurrent: {get_config().get('proof_channel') or 'Not set'}", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("proofset|"))
def proof_settings_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    action=c.data.split("|",1)[1]
    if action=="off":
        set_config("proof_channel",None); bot.answer_callback_query(c.id,"Disabled",True); return
    msg=raw_bot.send_message(c.from_user.id,"Send proof channel @username, t.me link, or numeric ID. Bot must be admin there.")
    bot.register_next_step_handler(msg,_proof_channel_save); bot.answer_callback_query(c.id)


def _proof_channel_save(m):
    try:
        ref=normalize_chat_reference(m.text); bot.get_chat(ref); set_config("proof_channel",ref)
        raw_bot.send_message(m.chat.id,f"✅ Proof channel set to {ref}.",reply_markup=admin_menu())
    except Exception as exc: admin_error(m.from_user.id,exc)


# ---- Channel-member discounts and trials ----
@bot.message_handler(func=lambda m: m.text == "🏷 Channel Offers" and is_admin(m.from_user.id))
def channel_offers_admin(m):
    offers=get_config().get("channel_offers",[]) or []
    lines=["🏷 CHANNEL MEMBER OFFERS",""]
    for x in offers:
        detail=f"{x.get('percent')}% discount" if x.get('type')=='discount' else f"{x.get('days')} day trial"
        lines.append(f"• {x.get('id')} | {x.get('chat')} | {detail} | {'ON' if x.get('active',True) else 'OFF'}")
    kb=InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("➕ Add Discount",callback_data="choffer|discount"))
    kb.add(InlineKeyboardButton("🎁 Add Trial",callback_data="choffer|trial"))
    kb.add(InlineKeyboardButton("🗑 Clear All Offers",callback_data="choffer|clear"))
    raw_bot.send_message(m.from_user.id,"\n".join(lines) if offers else "🏷 CHANNEL MEMBER OFFERS\n\nNo offers yet.",reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("choffer|"))
def channel_offer_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admin only",True)
    action=c.data.split("|",1)[1]
    if action=="clear": set_config("channel_offers",[]); bot.answer_callback_query(c.id,"All offers removed",True); return
    if action=="discount": prompt="Send: CHANNEL | DISCOUNT_PERCENT\nExample: @mychannel | 20"
    else: prompt="Send: CHANNEL | TRIAL_DAYS\nExample: @mychannel | 7"
    msg=raw_bot.send_message(c.from_user.id,prompt); bot.register_next_step_handler(msg,lambda m:_channel_offer_save(m,action)); bot.answer_callback_query(c.id)


def _channel_offer_save(m, kind):
    try:
        parts=[x.strip() for x in (m.text or '').split('|')]
        if len(parts)!=2: raise ValueError('Use CHANNEL | VALUE')
        chat=normalize_chat_reference(parts[0]); bot.get_chat(chat); value=float(parts[1]) if kind=='discount' else int(parts[1])
        if kind=='discount' and not (0<=value<=100): raise ValueError('Discount must be 0-100')
        if kind=='trial' and not (1<=value<=3650): raise ValueError('Trial days must be 1-3650')
        rows=get_config().get('channel_offers',[]) or []
        offer={'id':''.join(random.choices(string.ascii_uppercase+string.digits,k=8)),'chat':chat,'type':kind,'active':True}
        if kind=='discount': offer['percent']=float(value)
        else: offer['days']=int(value)
        rows.append(offer); set_config('channel_offers',rows)
        raw_bot.send_message(m.chat.id,'✅ Channel offer added.',reply_markup=admin_menu())
    except Exception as exc: admin_error(m.from_user.id,exc)


# ---- About us links ----
@bot.message_handler(func=lambda m: m.text == "ℹ️ About Us")
def about_us_user(m):
    links=get_cached_config().get('about_links',[]) or []
    kb=InlineKeyboardMarkup(row_width=1)
    for x in links:
        try: kb.add(InlineKeyboardButton(str(x.get('name','Link'))[:60],url=str(x.get('url'))))
        except Exception: pass
    raw_bot.send_message(m.from_user.id,'ℹ️ ABOUT US\n\nOfficial websites, accounts and community links:',reply_markup=kb if links else None)


@bot.message_handler(func=lambda m: m.text == "ℹ️ About Us Setup" and is_admin(m.from_user.id))
def about_us_admin(m):
    links=get_config().get('about_links',[]) or []
    text='ℹ️ ABOUT US LINKS\n\n'+('\n'.join(f"• {i+1}. {x.get('name')} — {x.get('url')}" for i,x in enumerate(links)) if links else 'No links yet.')
    kb=InlineKeyboardMarkup(row_width=1); kb.add(InlineKeyboardButton('➕ Add Link',callback_data='aboutcfg|add')); kb.add(InlineKeyboardButton('🗑 Clear Links',callback_data='aboutcfg|clear'))
    raw_bot.send_message(m.from_user.id,text,reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith('aboutcfg|'))
def about_cfg_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,'Admin only',True)
    action=c.data.split('|',1)[1]
    if action=='clear': set_config('about_links',[]); bot.answer_callback_query(c.id,'Cleared',True); return
    msg=raw_bot.send_message(c.from_user.id,'Send: BUTTON NAME | https://link.example'); bot.register_next_step_handler(msg,_about_link_save); bot.answer_callback_query(c.id)


def _about_link_save(m):
    try:
        name,url=[x.strip() for x in (m.text or '').split('|',1)]
        if not url.startswith(('https://','http://','tg://')): raise ValueError('URL must start with http://, https:// or tg://')
        rows=get_config().get('about_links',[]) or []; rows.append({'name':name[:60],'url':url}); set_config('about_links',rows)
        raw_bot.send_message(m.chat.id,'✅ About link added.',reply_markup=admin_menu())
    except Exception as exc: admin_error(m.from_user.id,exc)


# ---- User ↔ admin support chats ----
def _support_user_row(uid):
    return support_chats_col.find_one({'user_id':int(uid)}) or {}


def _support_send_prompt(uid):
    kb=ReplyKeyboardMarkup(resize_keyboard=True); kb.row('❌ End Chat')
    msg=raw_bot.send_message(uid,'💬 CHAT WITH ADMIN\n\nSend any text, photo, video, file, document, voice note or other Telegram message. Admin can reply through the bot.\n\nTap ❌ End Chat when finished.',reply_markup=kb)
    bot.register_next_step_handler(msg,_support_user_message)


@bot.message_handler(func=lambda m: m.text == '💬 Chat Admin')
def support_chat_start(m):
    if not get_cached_config().get('support_chat_enabled',True): return raw_bot.send_message(m.from_user.id,'Support chat is currently unavailable.')
    support_chats_col.update_one({'user_id':int(m.from_user.id)},{'$set':{'user_id':int(m.from_user.id),'username':m.from_user.username,'first_name':m.from_user.first_name,'updated_at':time.time(),'open':True},'$setOnInsert':{'created_at':time.time(),'unread_admin':0}},upsert=True)
    _support_send_prompt(m.from_user.id)


def _support_user_message(m):
    uid=int(m.from_user.id)
    if (m.text or '').strip()=='❌ End Chat':
        support_chats_col.update_one({'user_id':uid},{'$set':{'open':False,'updated_at':time.time()}})
        return raw_bot.send_message(uid,'✅ Chat closed.',reply_markup=main_menu(uid))
    row=support_chats_col.find_one_and_update({'user_id':uid},{'$set':{'username':m.from_user.username,'first_name':m.from_user.first_name,'updated_at':time.time(),'open':True,'last_user_chat':m.chat.id,'last_user_msg':m.message_id},'$inc':{'unread_admin':1}},upsert=True,return_document=ReturnDocument.AFTER)
    if get_cached_config().get('support_chat_notifications',True):
        for adm in get_all_admins():
            try: raw_bot.send_message(int(adm['_id']),f"💬 New support message from @{m.from_user.username or 'NoUsername'} ({uid}). Open 💬 Chats to reply.")
            except Exception: pass
    raw_bot.send_message(uid,'✅ Sent to admin.')
    _support_send_prompt(uid)


@bot.message_handler(func=lambda m: (m.text or '').startswith('💬 Chats') and is_admin(m.from_user.id))
def support_chats_admin(m):
    rows=list(support_chats_col.find({}).sort([('unread_admin',-1),('updated_at',-1)]).limit(50))
    kb=InlineKeyboardMarkup(row_width=1)
    for x in rows:
        unread=int(x.get('unread_admin',0) or 0); label=('🔴 ' if unread else '⚪ ')+f"@{x.get('username') or x.get('first_name') or x.get('user_id')}"+(f" • {unread} unread" if unread else '')
        kb.add(InlineKeyboardButton(label[:64],callback_data=f"supportopen|{x.get('user_id')}"))
    kb.add(InlineKeyboardButton('🔔 Toggle Chat Notifications',callback_data='supportnotify|toggle'))
    raw_bot.send_message(m.from_user.id,f"💬 CHATS\n\n{len(rows)} recent conversation(s).",reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith('supportopen|'))
def support_open_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,'Admin only',True)
    uid=int(c.data.split('|',1)[1]); row=_support_user_row(uid); support_chats_col.update_one({'user_id':uid},{'$set':{'unread_admin':0}})
    kb=InlineKeyboardMarkup(row_width=1); kb.add(InlineKeyboardButton('↩️ Reply',callback_data=f'supportreply|{uid}'))
    text=f"💬 SUPPORT CHAT\n\nName: {row.get('first_name') or '-'}\nUsername: @{row.get('username') or 'None'}\nUser ID: {uid}\nStatus: {'OPEN' if row.get('open') else 'CLOSED'}"
    raw_bot.send_message(c.from_user.id,text,reply_markup=kb)
    if row.get('last_user_chat') and row.get('last_user_msg'):
        try: raw_bot.copy_message(c.from_user.id,int(row['last_user_chat']),int(row['last_user_msg']))
        except Exception: pass
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith('supportreply|'))
def support_reply_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,'Admin only',True)
    uid=int(c.data.split('|',1)[1]); msg=raw_bot.send_message(c.from_user.id,f'Send reply to user {uid}. Any copyable Telegram message is supported.')
    bot.register_next_step_handler(msg,lambda m:_support_admin_reply(m,uid)); bot.answer_callback_query(c.id)


def _support_admin_reply(m,uid):
    try:
        raw_bot.copy_message(int(uid),m.chat.id,m.message_id)
        support_chats_col.update_one({'user_id':int(uid)},{'$set':{'updated_at':time.time(),'last_admin_reply_at':time.time()}})
        raw_bot.send_message(m.chat.id,'✅ Reply sent.',reply_markup=admin_menu())
    except Exception as exc: admin_error(m.from_user.id,exc)


@bot.callback_query_handler(func=lambda c: c.data=='supportnotify|toggle')
def support_notify_toggle(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,'Admin only',True)
    new=not get_config().get('support_chat_notifications',True); set_config('support_chat_notifications',new); bot.answer_callback_query(c.id,'Notifications ON' if new else 'Notifications OFF',True)


# ---- Generic reseller API bridge ----
def _api_request_json(url, method='GET', headers=None, payload=None, timeout=20):
    data=None if payload is None else json.dumps(payload).encode('utf-8')
    req=Request(url,data=data,method=method.upper(),headers={'Accept':'application/json','Content-Type':'application/json',**(headers or {})})
    with urlopen(req,timeout=timeout) as resp:
        raw=resp.read(2_000_000).decode('utf-8','replace')
    return json.loads(raw)


def _reseller_headers(provider):
    headers=dict(provider.get('headers') or {})
    if provider.get('api_key'):
        headers[str(provider.get('api_key_header') or 'Authorization')]=str(provider.get('api_key_prefix') or '')+str(provider.get('api_key'))
    return headers


@bot.message_handler(func=lambda m: m.text == '🌐 Reseller APIs' and is_admin(m.from_user.id))
def reseller_admin(m):
    rows=get_config().get('reseller_apis',[]) or []
    text='🌐 RESELLER APIS\n\n'+('\n'.join(f"• {x.get('id')} | {x.get('name')} | {x.get('list_url')} | {'ON' if x.get('active',True) else 'OFF'}" for x in rows) if rows else 'No providers configured.')
    kb=InlineKeyboardMarkup(row_width=1); kb.add(InlineKeyboardButton('➕ Add JSON API',callback_data='resellercfg|add')); kb.add(InlineKeyboardButton('🗑 Clear Providers',callback_data='resellercfg|clear'))
    raw_bot.send_message(m.from_user.id,text,reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith('resellercfg|'))
def reseller_cfg_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,'Admin only',True)
    action=c.data.split('|',1)[1]
    if action=='clear': set_config('reseller_apis',[]); bot.answer_callback_query(c.id,'Cleared',True); return
    msg=raw_bot.send_message(c.from_user.id,'Send provider JSON with at least: {"name":"Supplier","list_url":"https://..."}. Optional: api_key, api_key_header, api_key_prefix, products_path, id_field, name_field, price_field, buy_url, buy_method, buy_product_field, buy_quantity_field.'); bot.register_next_step_handler(msg,_reseller_add_save); bot.answer_callback_query(c.id)


def _reseller_add_save(m):
    try:
        row=json.loads(m.text or '{}')
        if not row.get('name') or not str(row.get('list_url','')).startswith('http'): raise ValueError('name and https list_url are required')
        row['id']='R'+''.join(random.choices(string.ascii_uppercase+string.digits,k=7)); row['active']=True
        rows=get_config().get('reseller_apis',[]) or []; rows.append(row); set_config('reseller_apis',rows)
        raw_bot.send_message(m.chat.id,f"✅ Provider {row['name']} added.",reply_markup=admin_menu())
    except Exception as exc: admin_error(m.from_user.id,exc)


def _reseller_provider(pid):
    return next((x for x in (get_cached_config().get('reseller_apis',[]) or []) if x.get('id')==pid and x.get('active',True)),None)


def _dig(obj,path):
    cur=obj
    for part in str(path or '').split('.') if path else []:
        if isinstance(cur,dict): cur=cur.get(part)
        else: return None
    return cur


@bot.callback_query_handler(func=lambda c: c.data=='reseller|providers')
def reseller_providers_cb(c):
    rows=[x for x in (get_cached_config().get('reseller_apis',[]) or []) if x.get('active',True)]
    kb=InlineKeyboardMarkup(row_width=1)
    for x in rows: kb.add(InlineKeyboardButton('🌐 '+str(x.get('name'))[:55],callback_data=f"resellerlist|{x.get('id')}"))
    raw_bot.send_message(c.from_user.id,'🌐 OTHER PRODUCTS\n\nChoose a supplier:',reply_markup=kb if rows else None); bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith('resellerlist|'))
def reseller_list_cb(c):
    pid=c.data.split('|',1)[1]; p=_reseller_provider(pid)
    if not p: return bot.answer_callback_query(c.id,'Provider unavailable',True)
    try:
        data=_api_request_json(str(p['list_url']),headers=_reseller_headers(p)); items=_dig(data,p.get('products_path')) if p.get('products_path') else data
        if isinstance(items,dict): items=items.get('products') or items.get('data') or list(items.values())
        if not isinstance(items,list): raise ValueError('API product response is not a list')
        kb=InlineKeyboardMarkup(row_width=1); cache=[]
        for item in items[:40]:
            if not isinstance(item,dict): continue
            ext=str(item.get(p.get('id_field','id'))); name=str(item.get(p.get('name_field','name')) or ext); price=float(item.get(p.get('price_field','price'),0) or 0)
            cache.append({'id':ext,'name':name,'price':price})
            kb.add(InlineKeyboardButton(f"{name[:40]} • ${price:g}",callback_data=f"resellerbuy|{pid}|{len(cache)-1}"))
        users_col.update_one({'_id':str(c.from_user.id)},{'$set':{f'reseller_cache.{pid}':cache,f'reseller_cache_time.{pid}':time.time()}},upsert=True)
        raw_bot.send_message(c.from_user.id,f"🌐 {p.get('name')} PRODUCTS\n\nLive list from supplier API:",reply_markup=kb); bot.answer_callback_query(c.id)
    except Exception as exc:
        bot.answer_callback_query(c.id,'Supplier API error',True); raw_bot.send_message(c.from_user.id,f"❌ Could not load supplier products: {str(exc)[:300]}")


@bot.callback_query_handler(func=lambda c: c.data.startswith('resellerbuy|'))
def reseller_buy_cb(c):
    try:
        _,pid,idxs=c.data.split('|',2); p=_reseller_provider(pid); idx=int(idxs); u=users_col.find_one({'_id':str(c.from_user.id)}) or {}; cache=((u.get('reseller_cache') or {}).get(pid) or []); item=cache[idx]
        price=float(item.get('price',0)); balance=float(u.get('usdt_balance',0) or 0)
        if balance<price: return bot.answer_callback_query(c.id,f'Need ${price:g} USDT balance',True)
        users_col.update_one({'_id':str(c.from_user.id),'usdt_balance':{'$gte':price}},{'$inc':{'usdt_balance':-price}})
        try:
            if not p.get('buy_url'): raise ValueError('Provider is browse-only: buy_url is not configured')
            payload={str(p.get('buy_product_field') or 'product_id'):item['id'],str(p.get('buy_quantity_field') or 'quantity'):1}
            result=_api_request_json(str(p['buy_url']),method=str(p.get('buy_method') or 'POST'),headers=_reseller_headers(p),payload=payload)
            order={'user_id':int(c.from_user.id),'provider_id':pid,'provider_name':p.get('name'),'product':item,'amount':price,'provider_response':result,'status':'submitted','created_at':time.time()}; oid=reseller_orders_col.insert_one(order).inserted_id
            payments_col.insert_one({'user_id':int(c.from_user.id),'type':'reseller_product','amount':price,'currency':'USDT','mode':'balance','status':'paid','created_at':time.time(),'reference':str(oid)})
            _publish_proof('PRODUCT',{'user_id':int(c.from_user.id),'username':c.from_user.username,'amount':price})
            raw_bot.send_message(c.from_user.id,f"✅ Order submitted\n\nProduct: {item.get('name')}\nAmount: ${price:g}\nOrder: {str(oid)[-8:]}\n\nSupplier response:\n{json.dumps(result,ensure_ascii=False)[:1500]}")
            bot.answer_callback_query(c.id,'Order submitted',True)
        except Exception:
            users_col.update_one({'_id':str(c.from_user.id)},{'$inc':{'usdt_balance':price}})
            raise
    except Exception as exc:
        bot.answer_callback_query(c.id,'Order failed',True); raw_bot.send_message(c.from_user.id,f"❌ Reseller order failed; your balance was restored if charged.\n{str(exc)[:300]}")

# =========================
@bot.message_handler(func=lambda m: True)
def fallback(m):
    if not validate_request(m):
        return
    
    uid = m.from_user.id
    
    if force_block(uid):
        return
    
    # Check custom buttons
    for btn in get_custom_buttons():
        if m.text == btn["text"]:
            if btn["type"] == "link":
                kb = InlineKeyboardMarkup()
                kb.add(InlineKeyboardButton("🔗 Open", url=btn["data"]))
                bot.send_message(uid, f"🔗 {btn['text']}", reply_markup=kb)
            elif btn["type"] == "folder":
                f = fs.get_by_number(int(btn["data"]))
                if f:
                    fake = type('obj', (object,), {'from_user': m.from_user, 'id': m.message_id, 'data': f"open|{f['cat']}|{f['name']}|"})
                    open_folder(fake)
            return
    
    known = MAIN_MENU_BUTTONS + [
        "⚙️ ADMIN PANEL", "🔎 Search", "📣 Auto Posts", "📥 Auto Import",
        "🧾 Logs", "💾 Backup/Export", "🙈 Hide Button", "👁 Show Button", "📋 METHODS LIST", "🛡 Group Management", "📢 CHANNELS", "➕ ADD CHANNEL", "📣 Channel Approvals"
    ]
    if m.text and m.text not in known:
        bot.send_message(uid, "❌ Use menu buttons", reply_markup=main_menu(uid))

# =========================
# 🚀 RUN BOT
# =========================
def run_bot():
    print("=" * 50, flush=True)
    print("🚀 GLOBEXOMART FRESH BOT - RAILWAY READY", flush=True)
    print(f"👑 Owner ID: {ADMIN_ID}", flush=True)
    print("🆕 Database: globexomart_fresh_v1 (fresh data only)", flush=True)
    print("=" * 50, flush=True)

    # Remove any old webhook before long polling.
    bot.remove_webhook()
    time.sleep(1)

    me = bot.get_me()
    print(f"✅ Logged in as @{me.username}", flush=True)

    while True:
        try:
            bot.infinity_polling(
                timeout=30,
                long_polling_timeout=30,
                skip_pending=True,
                allowed_updates=["message", "edited_message", "channel_post", "edited_channel_post", "callback_query", "my_chat_member", "chat_member"],
                restart_on_change=False,
            )
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            log_event("polling_restart", details={"error": str(exc), "trace": traceback.format_exc()}, level="error")
            print(f"⚠️ Polling error; restarting: {exc}", flush=True)
            time.sleep(5)

if __name__ == "__main__":
    run_bot()
