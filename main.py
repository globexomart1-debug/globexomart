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
# Stores the full user/admin support conversation history.
support_messages_col = db["support_messages"]
reseller_orders_col = db["reseller_orders"]
channel_offer_usage_col = db["channel_offer_usage"]
# Growth, retention and support modules. Kept separate so existing collections remain untouched.
daily_rewards_col = db["daily_rewards"]
deals_col = db["limited_deals"]
coupons_col = db["coupons"]
method_favorites_col = db["method_favorites"]
method_reviews_col = db["method_reviews"]
method_views_col = db["method_views"]
support_tickets_col = db["support_tickets"]
smart_broadcasts_col = db["smart_broadcasts"]
growth_events_col = db["growth_events"]
campaigns_col = db["growth_campaigns"]
affiliates_col = db["growth_affiliates"]
checkout_intents_col = db["checkout_intents"]
method_update_events_col = db["method_update_events"]
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
        (support_messages_col, [("user_id", 1), ("created_at", 1)], {}),
        (support_messages_col, [("user_id", 1), ("_id", -1)], {}),
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
            "reseller_apis": [],
            "revenue_subtracted_total": 0.0
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
    try:
        _notify_users_method_update(action, folder)
    except Exception as exc:
        log_event("user_method_update_error", details={"error": str(exc)}, level="error")

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


def is_submission_blocked(uid):
    """Block only payment/application submissions; normal bot access stays available."""
    try:
        row = users_col.find_one({"_id": str(uid)}, {"submission_blocked": 1}) or {}
        return bool(row.get("submission_blocked", False))
    except Exception:
        return False


def submission_block_notice(uid):
    return raw_bot.send_message(
        int(uid),
        "🚫 Submissions are restricted for your account. You can still use the bot, but new VIP/payment/deposit/withdraw/application requests are disabled. Contact support if you believe this is a mistake.",
        reply_markup=main_menu(uid) if 'main_menu' in globals() else None,
    )

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
                "muted": False,
                "submission_blocked": False
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

def get_scanner_main_visibility():
    """Visibility for the main Scanners button: all, vip, or hidden."""
    value = str(get_cached_config().get("scanner_main_visibility", "all") or "all").lower()
    return value if value in {"all", "vip", "hidden"} else "all"

def scanner_main_visible_for(uid):
    # Admins can always access Scanners for testing/management.
    if is_admin(uid):
        return True
    vis = get_scanner_main_visibility()
    if vis == "hidden":
        return False
    if vis == "vip":
        try:
            return User(uid).is_vip()
        except Exception:
            return False
    return True

MAIN_MENU_ROWS = [
    ("🎯 For You", "📚 Methods"),
    ("🛍 Products", "⭐ Buy VIP"),
    ("💰 Points", "🎁 Referral"),
    ("💼 Earn", "📝 Post Maker"),
    ("👤 Account", "ℹ️ About Us"),
    ("💬 Chat Admin", "🆔 Chat ID"),
    ("🏆 Redeem", "💳 Deposit"),
    ("💸 Withdraw", "🎁 Daily Reward"),
    ("🔥 Deals", "🎟 Coupon"),
    ("🏆 Ref Leaderboard", "❤️ Favorites"),
    ("🔍 Search All", "🎫 Support"),
    ("🎓 Start Here",),
    ("🛒 Cart", "🧾 My Orders"),
    ("🔔 Watchlist", "🎖 Loyalty"),
    ("❓ FAQ", "🌐 Language"),
    ("🔎 Scanners",),
]

MAIN_MENU_BUTTONS = [button for row in MAIN_MENU_ROWS for button in row]

def get_main_menu_order():
    """Return admin-configured order while automatically including newly added buttons."""
    cfg = get_cached_config()
    saved = cfg.get("main_menu_order") or []
    if not isinstance(saved, list):
        saved = []
    valid = []
    seen = set()
    for button in saved:
        if button in MAIN_MENU_BUTTONS and button not in seen:
            valid.append(button)
            seen.add(button)
    for button in MAIN_MENU_BUTTONS:
        if button not in seen:
            valid.append(button)
            seen.add(button)
    return valid

def save_main_menu_order(order):
    clean = []
    seen = set()
    for button in order:
        if button in MAIN_MENU_BUTTONS and button not in seen:
            clean.append(button)
            seen.add(button)
    for button in MAIN_MENU_BUTTONS:
        if button not in seen:
            clean.append(button)
            seen.add(button)
    set_config("main_menu_order", clean)
    return clean

def get_main_menu_columns():
    try:
        return max(1, min(3, int(get_cached_config().get("main_menu_columns", 2) or 2)))
    except Exception:
        return 2

def main_menu(uid):
    columns = get_main_menu_columns()
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=columns)
    hidden = get_hidden_main_buttons()

    visible_buttons = []
    for button in get_main_menu_order():
        if button in hidden:
            continue
        if button == "🔎 Scanners" and not scanner_main_visible_for(uid):
            continue
        visible_buttons.append(button)

    for i in range(0, len(visible_buttons), columns):
        kb.row(*visible_buttons[i:i + columns])

    custom_btns = get_custom_buttons()
    if custom_btns:
        row = []
        for btn in custom_btns:
            if btn["text"] in hidden:
                continue
            row.append(btn["text"])
            if len(row) == columns:
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
        payload = args[1].strip()[:100]
        if payload.startswith("camp_"):
            code = payload[5:].strip().lower()
            campaign = campaigns_col.find_one({"code": code, "active": {"$ne": False}})
            if campaign:
                users_col.update_one({"_id": str(uid)}, {"$set": {"acquisition_campaign": code, "acquisition_source": campaign.get("name") or code}})
                growth_events_col.insert_one({"event":"start","user_id":int(uid),"campaign":code,"created_at":time.time(),"is_new":bool(is_new_user)})
        elif payload.startswith("aff_"):
            code = payload[4:].strip().lower()
            affiliate = affiliates_col.find_one({"code": code, "active": {"$ne": False}})
            if affiliate and int(affiliate.get("user_id",0) or 0) != int(uid):
                users_col.update_one({"_id": str(uid)}, {"$set": {"affiliate_code": code, "acquisition_source": "affiliate:"+code}})
                growth_events_col.insert_one({"event":"start","user_id":int(uid),"affiliate":code,"created_at":time.time(),"is_new":bool(is_new_user)})
        else:
            ref_id = payload
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
    welcome_msg = ((_adv_cfg().get("welcome_custom") if "_adv_cfg" in globals() else None) or cfg.get("welcome", "Welcome to GLOBEXOMART BOT!"))
    
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
        if is_submission_blocked(c.from_user.id):
            bot.answer_callback_query(c.id, "Submissions are restricted for your account", True)
            return submission_block_notice(c.from_user.id)
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

    # Active VIP members bypass point charges for FREE METHODS for the full
    # duration of their subscription. Product pricing remains unchanged.
    if cat in ("free", "free_service") and price > 0:
        vip_method_bypass = (cat == "free" and user.is_vip())
        purchase_key = f"{cat}:{folder_id}"
        if not vip_method_bypass and purchase_key not in user.purchased_methods():
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

    # Record a lightweight method view and show save/rating controls without changing delivery.
    if cat in ("free", "vip"):
        try:
            method_views_col.update_one(
                {"user_id": int(uid), "folder_id": folder_id},
                {"$set": {"last_viewed_at": time.time(), "cat": cat, "name": name}, "$inc": {"views": 1}},
                upsert=True,
            )
            fav = method_favorites_col.find_one({"user_id": int(uid), "folder_id": folder_id}) is not None
            extra_kb = InlineKeyboardMarkup(row_width=2)
            extra_kb.add(
                InlineKeyboardButton("💔 Remove Favorite" if fav else "❤️ Save Favorite", callback_data=f"favmethod|{folder_id}"),
                InlineKeyboardButton("⭐ Rate Method", callback_data=f"ratemethod|{folder_id}"),
            )
            raw_bot.send_message(uid, "✨ Method tools", reply_markup=extra_kb)
        except Exception as exc:
            log_event("method_tools_error", uid, details={"error": str(exc)}, level="error")

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
    
    if not force_block(uid):
        # A pending referral becomes valid only after Telegram confirms that
        # the invited user joined every configured force-join channel/group.
        finalize_pending_referral(uid, c.from_user)
        user = User(uid)
        try:
            bot.edit_message_text("✅ **Access Granted!**", uid, c.message.message_id, parse_mode="Markdown")
        except:
            pass
        bot.send_message(uid, f"🎉 Welcome!\n\n💰 Points: {user.points()}", reply_markup=main_menu(uid))
    else:
        bot.answer_callback_query(c.id, "❌ Join all required channels and groups first!", True)

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
    if is_submission_blocked(uid):
        bot.answer_callback_query(c.id, "Applications are restricted for your account", True)
        return submission_block_notice(uid)
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
    if current:
        kb.add(InlineKeyboardButton("🔄 Renew Current VIP", callback_data="viprenew|current"))
    kb.add(InlineKeyboardButton("🎁 Gift VIP", callback_data="vipgift|start"))
    if edit_message_id:
        raw_bot.edit_message_text(text, int(uid), int(edit_message_id), reply_markup=kb if kb.keyboard else None)
    else:
        raw_bot.send_message(int(uid), text, reply_markup=kb if kb.keyboard else None)


@bot.message_handler(func=lambda m: m.text in ("⭐ BUY VIP", "⭐ Buy VIP"))
@force_join_handler
def buy_vip_button(m):
    if "_require_terms" in globals() and _require_terms(m.from_user.id, "vip"):
        return
    if is_submission_blocked(m.from_user.id):
        return submission_block_notice(m.from_user.id)
    _growth_event("vip_view", m.from_user.id)
    _vip_plan_selection(m.from_user.id)


@bot.callback_query_handler(func=lambda c: c.data == "get_vip")
def get_vip_callback_v2(c):
    try:
        if "_require_terms" in globals() and _require_terms(c.from_user.id, "vip"):
            bot.answer_callback_query(c.id, "Accept purchase terms first", True)
            return
        if is_submission_blocked(c.from_user.id):
            bot.answer_callback_query(c.id, "VIP submissions are restricted for your account", True)
            return submission_block_notice(c.from_user.id)
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
    kb.row("🧾 Proof Settings", "✨ Premium Emojis")
    kb.row("📝 Admin Post Maker")
    kb.row("🏷 Channel Offers")
    kb.row("🌐 Reseller APIs")
    kb.row("💳 Payment Methods", "🏦 Binance Settings")
    kb.row("🧾 VIP Manager", "🎯 VIP Channel")
    kb.row("📈 VIP Analytics", "📊 Force Join Stats")
    kb.row("🎁 Referral Settings", "💸 Ref Withdrawals")
    kb.row("🤝 Earn Applications")
    kb.row("📣 Auto Broadcast", "📝 VIP Messages")
    kb.row("⏳ Pending Orders")
    kb.row("💳 Deposits", "💸 Withdrawals")
    kb.row("🏷 Discounts", "🧾 Payments")
    kb.row("📸 Screenshot", "🔘 Button Manager")
    kb.row("↕️ Arrange Main Buttons")
    kb.row("🙈 Hide Button", "👁 Show Button")
    kb.row("📢 Force Join", "👥 Join Notifications")
    kb.row("⚙️ Settings")
    kb.row("📊 Stats", "📢 Broadcast")
    kb.row("💰 Revenue Manager")
    kb.row("🧹 Test Data & User Controls")
    kb.row("🔎 Scanner Manager")
    kb.row("📨 Invite Bot Users")
    kb.row("🔔 Notify", "🛡 Group Management")
    kb.row("📊 Leaderboard")
    kb.row("🔎 Search", "📣 Auto Posts")
    kb.row("📥 Auto Import", "⏳ Pending Methods")
    kb.row("📌 Pin Methods", "📝 Edit Methods List")
    kb.row("📣 Channel Approvals", "📨 Group Messenger")
    kb.row("🔥 Deal Manager", "🎟 Coupon Manager")
    kb.row("📈 Method Analytics", "⭐ Reviews")
    kb.row("🎫 Tickets", "📢 Smart Broadcast")
    kb.row("🛡 Risk Panel", "📊 Business Dashboard")
    kb.row("🎓 Roadmap Setup", "🔔 Method Update Alerts")
    kb.row("🎯 Growth Center", "🤝 Affiliate Manager")
    kb.row("🔗 Campaign Manager", "📈 Funnel Analytics")
    kb.row("🧰 Operations Center", "👥 Staff Roles")
    kb.row("📝 Admin Activity", "📊 Retention")
    kb.row("💾 Backup/Restore", "🔧 Maintenance")
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
        if product.get("kind")=="paid":
            kb.add(InlineKeyboardButton("➕ Add to Cart", callback_data=f"cartadd|{product['_id']}"))
            kb.add(InlineKeyboardButton("🎁 Gift Product", callback_data=f"giftprod|{product['_id']}"))
    else:
        kb.add(InlineKeyboardButton("❌ OUT OF STOCK", callback_data="shopnoop"))
        kb.add(InlineKeyboardButton("🔔 Notify Me When Restocked", callback_data=f"stockwatch|{product['_id']}"))
    kb.add(InlineKeyboardButton("🔔 Watch Product", callback_data=f"watchprod|{product['_id']}"))
    kb.add(InlineKeyboardButton("🧾 My Orders", callback_data="shoporders|mine"))
    raw_bot.send_message(c.from_user.id,_shop_card(product),reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data == "shopnoop")
def shop_noop(c):
    bot.answer_callback_query(c.id,"Out of stock",True)


@bot.callback_query_handler(func=lambda c: c.data.startswith("shopbuyask|"))
def shop_buy_ask(c):
    if "_require_terms" in globals() and _require_terms(c.from_user.id, "products"):
        try: bot.answer_callback_query(c.id, "Accept purchase terms first", True)
        except Exception: pass
        return
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
        disc=max(_shop_discount(qty), _active_product_deal_discount(str(product.get("_id"))), _active_coupon_discount(uid, "product", str(product.get("_id"))))
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


_GLOBEXOMART_INVOICE_LOGO_B64 = "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"

def _invoice_pdf_escape(value):
    """Escape text for a PDF text object using built-in fonts."""
    text = str(value if value is not None else "")
    text = text.encode("latin-1", "replace").decode("latin-1")
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _make_invoice_pdf_bytes(title, invoice_id, rows, status="Paid / Delivered Successfully"):
    """Create a premium black-and-gold one-page GLOBEXOMART invoice."""
    from io import BytesIO
    import base64

    # Embedded Logo 01 so deployment still needs only this Python file.
    logo_jpeg = base64.b64decode(_GLOBEXOMART_INVOICE_LOGO_B64)
    logo_w, logo_h = 505, 380

    content = []
    def rgb(r,g,b, stroke=False):
        content.append(f"{r} {g} {b} {'RG' if stroke else 'rg'}")
    def txt(x,y,size,text,font="F1", color=(0.94,0.94,0.94)):
        rgb(*color)
        content.append(f"BT /{font} {size} Tf {x} {y} Td ({_invoice_pdf_escape(text)}) Tj ET")
    def line(x1,y1,x2,y2,width=1,color=(0.75,0.52,0.12)):
        rgb(*color, stroke=True); content.append(f"{width} w {x1} {y1} m {x2} {y2} l S")
    def fill_rect(x,y,w,h,color):
        rgb(*color); content.append(f"{x} {y} {w} {h} re f")
    def stroke_rect(x,y,w,h,width=1,color=(0.75,0.52,0.12)):
        rgb(*color, stroke=True); content.append(f"{width} w {x} {y} {w} {h} re S")

    # Full premium canvas.
    fill_rect(0,0,595,842,(0.018,0.018,0.022))
    fill_rect(22,22,551,798,(0.035,0.035,0.043))
    stroke_rect(22,22,551,798,1.4,(0.88,0.63,0.16))
    stroke_rect(28,28,539,786,0.35,(0.45,0.32,0.09))

    # Logo 01 image.
    content.append("q 155 0 0 117 42 682 cm /Im1 Do Q")

    # Brand/title area.
    txt(225,770,24,"GLOBEXOMART","F2",(0.96,0.73,0.25))
    txt(226,748,8.5,"PREMIUM METHODS. PREMIUM RESULTS.","F1",(0.84,0.84,0.84))
    line(225,738,538,738,0.7,(0.76,0.54,0.15))
    txt(424,713,22,"INVOICE","F2",(0.96,0.73,0.25))
    txt(225,713,9,str(title).upper(),"F2",(0.92,0.92,0.92))

    # Invoice identity bar.
    fill_rect(42,650,511,45,(0.065,0.065,0.075))
    stroke_rect(42,650,511,45,0.7,(0.73,0.51,0.13))
    txt(58,678,7.5,"INVOICE ID",color=(0.72,0.72,0.72))
    txt(58,659,12,f"#{invoice_id}","F2",(0.97,0.76,0.30))
    txt(360,678,7.5,"ISSUED",color=(0.72,0.72,0.72))
    txt(360,659,10,datetime.now().strftime("%d/%m/%Y  %I:%M %p"),"F2")

    # Details rows.
    y=620
    gold=(0.96,0.73,0.25); muted=(0.72,0.72,0.72); white=(0.95,0.95,0.95)
    for idx,(label,value) in enumerate(rows):
        if y < 250: break
        value=str(value)
        if len(value)>54: value=value[:51]+"..."
        if idx % 2 == 0: fill_rect(42,y-8,511,26,(0.052,0.052,0.061))
        txt(58,y,8.5,str(label).upper(),"F2",gold)
        txt(225,y,9.2,value,"F1",white)
        y-=28

    # Status and total-like emphasis.
    y=max(y-8,190)
    fill_rect(42,y-4,511,42,(0.07,0.06,0.035))
    stroke_rect(42,y-4,511,42,0.8,(0.88,0.63,0.16))
    txt(58,y+20,7.5,"STATUS","F2",muted)
    txt(58,y+3,12,str(status).upper(),"F2",gold)

    # Trust/footer.
    fy=82
    line(42,fy+72,553,fy+72,0.8,(0.76,0.54,0.15))
    txt(42,fy+50,10,"THANK YOU FOR TRUSTING GLOBEXOMART","F2",gold)
    txt(42,fy+32,8.5,"Your trusted source for premium methods, digital products, VIP access & guidance.","F1",white)
    txt(42,fy+14,8.5,"Official Bot: @globexomartbot","F2",(0.90,0.90,0.90))
    txt(383,fy+14,8,"SECURE  •  PREMIUM  •  TRUSTED","F2",gold)
    fill_rect(22,22,551,34,(0.11,0.075,0.02))
    txt(143,34,8.5,"GLOBEXOMART  •  PREMIUM TOOLS. PREMIUM RESULTS.","F2",(0.98,0.78,0.33))

    stream="\n".join(content).encode("latin-1","replace")
    objects=[]
    objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objects.append(b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    objects.append(b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 5 0 R /F2 6 0 R >> /XObject << /Im1 7 0 R >> >> /Contents 4 0 R >>")
    objects.append(b"<< /Length %d >>\nstream\n"%len(stream)+stream+b"\nendstream")
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>")
    objects.append((f"<< /Type /XObject /Subtype /Image /Width {logo_w} /Height {logo_h} /ColorSpace /DeviceRGB /BitsPerComponent 8 /Filter /DCTDecode /Length {len(logo_jpeg)} >>\nstream\n").encode()+logo_jpeg+b"\nendstream")
    out=BytesIO(); out.write(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n"); offsets=[0]
    for i,obj in enumerate(objects,1):
        offsets.append(out.tell()); out.write(f"{i} 0 obj\n".encode()); out.write(obj); out.write(b"\nendobj\n")
    xref=out.tell(); out.write(f"xref\n0 {len(objects)+1}\n".encode()); out.write(b"0000000000 65535 f \n")
    for off in offsets[1:]: out.write(f"{off:010d} 00000 n \n".encode())
    out.write(f"trailer\n<< /Size {len(objects)+1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF".encode())
    return out.getvalue()


def _send_globexomart_invoice(uid, invoice_type, invoice_id, rows, status="Paid / Delivered Successfully"):
    """Generate and send the premium official GLOBEXOMART invoice."""
    try:
        pdf=io.BytesIO(_make_invoice_pdf_bytes(invoice_type,invoice_id,rows,status=status))
        pdf.name=f"Globexomart_Invoice_{str(invoice_id).replace(' ','_')}.pdf"
        raw_bot.send_message(uid,"✨ Preparing your premium GLOBEXOMART invoice...")
        raw_bot.send_document(uid,pdf,caption=f"👑 GLOBEXOMART {invoice_type} INVOICE\nInvoice: #{invoice_id}\n\n✨ Thank you for trusting GLOBEXOMART.\n🤖 @globexomartbot")
        return True
    except Exception as exc:
        log_event("invoice_send_error",uid,details={"type":invoice_type,"invoice_id":str(invoice_id),"error":str(exc)},level="error")
        return False

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
            disc=max(_shop_discount(qty), _active_product_deal_discount(str(product.get("_id"))), _active_coupon_discount(uid, "product", str(product.get("_id"))))
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
                "duration":product.get("duration") or "Not specified",
                "warranty":product.get("warranty") or "Not specified",
                "created_at":time.time(),
            }
            result=shop_orders_col.insert_one(order)
            if kind == "paid":
                _consume_coupon(uid)
            _publish_proof("PRODUCT", order)
            if kind=="paid":
                payments_col.insert_one({"user_id":uid,"type":"product","product_id":str(product["_id"]),"amount":float(total),"currency":"USDT","mode":"balance","status":"paid","created_at":time.time()})
                try:
                    _loyalty_after_purchase(uid, float(total), "product", str(product["_id"]))
                except Exception:
                    pass
            _shop_deliver(uid,product,delivered,result.inserted_id)
            order_short = str(result.inserted_id)[-8:].upper()
            product_total_text = (_shop_money(total) if kind == "paid" else (f"{total} points" if total else "FREE"))
            _send_globexomart_invoice(
                uid,
                "PRODUCT PURCHASE",
                order_short,
                [
                    ("Order ID", f"#{order_short}"),
                    ("Product", product.get("name") or "Digital Product"),
                    ("Quantity", qty),
                    ("Purchase Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                    ("Duration", product.get("duration") or "Not specified"),
                    ("Warranty", product.get("warranty") or "Not specified"),
                    ("Price Each", _shop_money(each) if kind == "paid" else (f"{each} points" if each else "FREE")),
                    ("Subtotal", _shop_money(subtotal) if kind == "paid" else (f"{subtotal} points" if subtotal else "FREE")),
                    ("Discount", f"{disc}%" if disc else "No discount"),
                    ("Price Paid", product_total_text),
                    ("Paid With", paid_with),
                    ("Bot", "@globexomartbot"),
                ],
                status="Activated / Delivered Successfully",
            )
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
            try:
                _notify_product_restock(str(p["_id"]), p.get("name") or "Product")
            except Exception:
                pass
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
        ("⛔ Select the method to patch:" if mode == "patch" else "✅ Select the method to restore:")
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
        if mode == "patch":
            current_price = float(row.get("price", 0) or 0)
            kb = InlineKeyboardMarkup(row_width=1)
            price_unit = "USDT" if str(row.get("cat", "")).lower() == "vip" else "points"
            kb.add(InlineKeyboardButton(f"💰 Keep Same Price ({current_price:g} {price_unit})", callback_data=f"patchprice|same|{oid}"))
            kb.add(InlineKeyboardButton("🆓 Make Free", callback_data=f"patchprice|free|{oid}"))
            kb.add(InlineKeyboardButton("✏️ Set New Price", callback_data=f"patchprice|new|{oid}"))
            kb.add(InlineKeyboardButton("❌ Cancel", callback_data="methodstatuscancel"))
            raw_bot.send_message(
                c.from_user.id,
                f"⛔ PATCH METHOD\n\n{row.get('name')}\nCurrent price: {current_price:g} {price_unit}\n\nChoose what price users should see while this method is patched:",
                reply_markup=kb,
            )
        else:
            kb = InlineKeyboardMarkup(row_width=2)
            kb.add(
                InlineKeyboardButton("✅ Yes, Unpatch", callback_data=f"methodstatusapply|unpatch|{oid}"),
                InlineKeyboardButton("❌ Cancel", callback_data="methodstatuscancel"),
            )
            raw_bot.send_message(c.from_user.id, f"✅ UNPATCH METHOD\n\n{row.get('name')}\n\nUsers will be able to access this method again.", reply_markup=kb)
        bot.answer_callback_query(c.id)
    except Exception as exc:
        admin_error(c.from_user.id, exc)


_patch_price_state = {}

@bot.callback_query_handler(func=lambda c: c.data.startswith("patchprice|"))
def patch_price_choice_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    from bson import ObjectId
    try:
        _, choice, oid = c.data.split("|", 2)
        row = folders_col.find_one({"_id": ObjectId(oid)})
        if not row:
            return bot.answer_callback_query(c.id, "Method not found", True)
        if choice == "new":
            _patch_price_state[c.from_user.id] = oid
            msg = raw_bot.send_message(c.from_user.id, f"✏️ Send the new price for {row.get('name')}.\n\nUse 0 to make it free.")
            bot.register_next_step_handler(msg, patch_new_price_step)
            return bot.answer_callback_query(c.id, "Send new price")
        new_price = float(row.get("price", 0) or 0) if choice == "same" else 0.0
        _apply_patch_with_price(c.from_user.id, oid, new_price)
        bot.answer_callback_query(c.id, "Patched")
    except Exception as exc:
        admin_error(c.from_user.id, exc)


def patch_new_price_step(m):
    oid = _patch_price_state.pop(m.from_user.id, None)
    if not oid:
        return raw_bot.send_message(m.chat.id, "Session expired. Open Patch Method again.", reply_markup=admin_menu())
    try:
        value = float((m.text or "").strip())
        if value < 0:
            raise ValueError("Price cannot be negative")
        _apply_patch_with_price(m.from_user.id, oid, value)
    except Exception as exc:
        admin_error(m.from_user.id, exc)


def _apply_patch_with_price(uid, oid, new_price):
    from bson import ObjectId
    row = folders_col.find_one({"_id": ObjectId(oid)})
    if not row:
        raise ValueError("Method not found")
    update = {
        "patched": True,
        "active": True,
        "price": new_price,
        "updated_at": now_ts(),
        "patched_at": now_ts(),
        "patched_by": uid,
    }
    if "pre_patch_price" not in row:
        update["pre_patch_price"] = float(row.get("price", 0) or 0)
    folders_col.update_one({"_id": row["_id"]}, {"$set": update})
    row = folders_col.find_one({"_id": row["_id"]})
    send_method_notification("patched", row)
    price_text = "FREE" if float(new_price or 0) == 0 else f"{float(new_price):g}"
    admin_success(uid, f"{row.get('name')} patched successfully. Price: {price_text}")


@bot.callback_query_handler(func=lambda c: c.data.startswith("methodstatusapply|"))
def method_status_apply_cb(c):
    if not is_admin(c.from_user.id):
        return
    from bson import ObjectId
    try:
        _, mode, oid = c.data.split("|", 2)
        if mode != "unpatch":
            return bot.answer_callback_query(c.id, "Use the patch price options", True)
        result = folders_col.update_one(
            {"_id": ObjectId(oid)},
            {"$set": {
                "patched": False,
                "active": True,
                "updated_at": now_ts(),
                "patched_at": None,
                "patched_by": None,
            }},
        )
        if result.matched_count != 1:
            raise ValueError("Method was not found or could not be updated")
        row = folders_col.find_one({"_id": ObjectId(oid)})
        send_method_notification("unpatched", row)
        admin_success(c.from_user.id, f"{row.get('name')} unpatched successfully")
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
        label = f"{str(f.get('cat','')).upper()} • {f.get('name')}"
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
            bot.send_message(c.from_user.id,f"⚠️ Delete **{folder['name']}** from **{folder['cat'].upper()}**?\nThis also deletes its subfolders.",reply_markup=kb,parse_mode="Markdown")
        elif action=="price":
            msg=bot.send_message(c.from_user.id,f"Current price: `{folder.get('price',0)}`\nSend the new price:",parse_mode="Markdown");bot.register_next_step_handler(msg,folder_price_step)
        elif action=="name":
            msg=bot.send_message(c.from_user.id,f"Current name: **{folder['name']}**\nSend the new name:",parse_mode="Markdown");bot.register_next_step_handler(msg,folder_name_step)
        else:
            edit_sessions[c.from_user.id]={"cat":folder['cat'],"name":folder['name'],"parent":folder.get('parent'),"number":int(num)}
            kb=InlineKeyboardMarkup(row_width=2);kb.add(InlineKeyboardButton("📝 Text",callback_data="edit_text"),InlineKeyboardButton("📁 Files",callback_data="edit_files"),InlineKeyboardButton("❌ Cancel",callback_data="edit_cancel"))
            bot.send_message(c.from_user.id,f"📝 **Edit {folder['name']}**\nWhat do you want to update?",reply_markup=kb,parse_mode="Markdown")
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
# ↕️ MAIN MENU BUTTON ORDER
# =========================
def _main_order_markup(page=0, selected=None):
    order = get_main_menu_order()
    per_page = 8
    total_pages = max(1, (len(order) + per_page - 1) // per_page)
    page = max(0, min(int(page or 0), total_pages - 1))
    start = page * per_page
    end = min(len(order), start + per_page)
    kb = InlineKeyboardMarkup(row_width=1)
    for idx in range(start, end):
        prefix = "✅ " if selected == idx else ""
        kb.add(InlineKeyboardButton(
            f"{idx + 1}. {prefix}{order[idx]}",
            callback_data=f"mmord|sel|{idx}|{page}"
        ))
    if selected is not None and 0 <= selected < len(order):
        kb.row(
            InlineKeyboardButton("⬆️ Up", callback_data=f"mmord|mv|{selected}|up|{page}"),
            InlineKeyboardButton("⬇️ Down", callback_data=f"mmord|mv|{selected}|down|{page}")
        )
        kb.row(
            InlineKeyboardButton("⏫ Top", callback_data=f"mmord|mv|{selected}|top|{page}"),
            InlineKeyboardButton("⏬ Bottom", callback_data=f"mmord|mv|{selected}|bottom|{page}")
        )
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"mmord|page|{page-1}"))
    if page + 1 < total_pages:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"mmord|page|{page+1}"))
    if nav:
        kb.row(*nav)
    kb.row(
        InlineKeyboardButton("🔄 Reset Default", callback_data="mmord|reset"),
        InlineKeyboardButton("✅ Done", callback_data="mmord|done")
    )
    return kb, page, total_pages


def _main_order_text(page=0, selected=None):
    order = get_main_menu_order()
    chosen = order[selected] if selected is not None and 0 <= selected < len(order) else None
    text = "↕️ MAIN MENU BUTTON ORDER\n\nSelect a button, then move it where you want. The order is saved immediately and applies to users automatically."
    if chosen:
        text += f"\n\nSelected: {chosen}"
    text += "\n\nHidden buttons stay hidden, and Scanners visibility (All/VIP/Hidden) is still respected."
    return text


@bot.message_handler(func=lambda m: m.text == "↕️ Arrange Main Buttons" and is_admin(m.from_user.id))
def arrange_main_buttons_menu(m):
    kb, page, total_pages = _main_order_markup(0, None)
    raw_bot.send_message(m.from_user.id, _main_order_text(), reply_markup=kb)


@bot.callback_query_handler(func=lambda c: str(c.data or "").startswith("mmord|"))
def main_button_order_callback(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    try:
        parts = c.data.split("|")
        action = parts[1]
        if action == "done":
            bot.answer_callback_query(c.id, "Saved")
            try:
                bot.delete_message(c.message.chat.id, c.message.message_id)
            except Exception:
                pass
            return raw_bot.send_message(c.from_user.id, "✅ Main menu button order saved.", reply_markup=admin_menu())

        if action == "reset":
            save_main_menu_order(list(MAIN_MENU_BUTTONS))
            kb, _, _ = _main_order_markup(0, None)
            raw_bot.edit_message_text(_main_order_text(), c.message.chat.id, c.message.message_id, reply_markup=kb)
            return bot.answer_callback_query(c.id, "Default order restored")

        if action == "page":
            page = int(parts[2])
            kb, _, _ = _main_order_markup(page, None)
            raw_bot.edit_message_text(_main_order_text(page), c.message.chat.id, c.message.message_id, reply_markup=kb)
            return bot.answer_callback_query(c.id)

        if action == "sel":
            idx = int(parts[2]); page = int(parts[3])
            order = get_main_menu_order()
            if idx < 0 or idx >= len(order):
                return bot.answer_callback_query(c.id, "Button not found", True)
            kb, _, _ = _main_order_markup(page, idx)
            raw_bot.edit_message_text(_main_order_text(page, idx), c.message.chat.id, c.message.message_id, reply_markup=kb)
            return bot.answer_callback_query(c.id, order[idx])

        if action == "mv":
            idx = int(parts[2]); direction = parts[3]; page = int(parts[4])
            order = get_main_menu_order()
            if idx < 0 or idx >= len(order):
                return bot.answer_callback_query(c.id, "Button not found", True)
            new_idx = idx
            if direction == "up" and idx > 0:
                order[idx - 1], order[idx] = order[idx], order[idx - 1]
                new_idx = idx - 1
            elif direction == "down" and idx < len(order) - 1:
                order[idx + 1], order[idx] = order[idx], order[idx + 1]
                new_idx = idx + 1
            elif direction == "top" and idx > 0:
                item = order.pop(idx); order.insert(0, item); new_idx = 0
            elif direction == "bottom" and idx < len(order) - 1:
                item = order.pop(idx); order.append(item); new_idx = len(order) - 1
            save_main_menu_order(order)
            new_page = new_idx // 8
            kb, _, _ = _main_order_markup(new_page, new_idx)
            raw_bot.edit_message_text(_main_order_text(new_page, new_idx), c.message.chat.id, c.message.message_id, reply_markup=kb)
            return bot.answer_callback_query(c.id, "Position updated")

        bot.answer_callback_query(c.id)
    except Exception as exc:
        log_event("main_button_order_error", c.from_user.id, details={"error": str(exc)}, level="error")
        bot.answer_callback_query(c.id, "Could not update button order", True)


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
    kb.add(InlineKeyboardButton("↔️ Main Menu Columns", callback_data="btnmgr|columns"))
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
        if action == "columns":
            kb = InlineKeyboardMarkup(row_width=3)
            current = get_main_menu_columns()
            for n in (1, 2, 3):
                label = f"✅ {n} per row" if n == current else f"{n} per row"
                kb.add(InlineKeyboardButton(label, callback_data=f"btnmgr|setcols|{n}"))
            bot.send_message(c.from_user.id, "↔️ **MAIN MENU BUTTONS PER ROW**\n\nChoose 1, 2, or 3 buttons in each row. Telegram automatically fits the button widths to the row.", reply_markup=kb, parse_mode="Markdown")
            return bot.answer_callback_query(c.id)
        if action == "setcols":
            n = max(1, min(3, int(parts[2])))
            set_config("main_menu_columns", n)
            bot.answer_callback_query(c.id, f"Main menu set to {n} per row", True)
            return bot.send_message(c.from_user.id, f"✅ Main menu now uses **{n} button{'s' if n != 1 else ''} per row**. Button widths auto-fit in Telegram.", reply_markup=admin_menu(), parse_mode="Markdown")
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
# 💰 REVENUE MANAGER
# =========================
def _gross_bot_revenue():
    """Return gross revenue from completed/paid bot sales only."""
    rows = payments_col.aggregate([
        {"$match": {"status": "paid"}},
        {"$group": {"_id": None, "total": {"$sum": "$amount"}}},
    ])
    row = next(rows, None) or {}
    return float(row.get("total", 0) or 0)


def _revenue_subtracted_total():
    return max(0.0, float(get_cached_config().get("revenue_subtracted_total", 0) or 0))


def _net_bot_revenue():
    return _gross_bot_revenue() - _revenue_subtracted_total()


@bot.message_handler(func=lambda m: m.text == "💰 Revenue Manager" and is_admin(m.from_user.id))
def revenue_manager_cmd(m):
    gross = _gross_bot_revenue()
    subtracted = _revenue_subtracted_total()
    net = gross - subtracted
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("➖ Subtract Revenue", callback_data="revenue_subtract"))
    raw_bot.send_message(
        m.from_user.id,
        "💰 REVENUE MANAGER\n\n"
        f"📈 Gross revenue: ${gross:.2f}\n"
        f"➖ Manually subtracted: ${subtracted:.2f}\n"
        f"✅ Net revenue: ${net:.2f}\n\n"
        "Use Subtract Revenue for expenses, refunds or any amount you want removed from the bot's total revenue figure.",
        reply_markup=kb,
    )


@bot.callback_query_handler(func=lambda c: c.data == "revenue_subtract")
def revenue_subtract_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    bot.answer_callback_query(c.id)
    current = _net_bot_revenue()
    msg = raw_bot.send_message(
        c.from_user.id,
        f"➖ SUBTRACT REVENUE\n\nCurrent net revenue: ${current:.2f}\n\nSend the amount in USDT/USD to subtract.\nExample: 25 or 12.50"
    )
    bot.register_next_step_handler(msg, revenue_subtract_amount_step)


def revenue_subtract_amount_step(m):
    if not is_admin(m.from_user.id):
        return
    try:
        amount = float((m.text or "").strip().replace("$", "").replace(",", ""))
    except Exception:
        amount = 0
    if amount <= 0:
        msg = raw_bot.send_message(m.from_user.id, "❌ Invalid amount. Send a number greater than 0.")
        bot.register_next_step_handler(msg, revenue_subtract_amount_step)
        return
    net_before = _net_bot_revenue()
    if amount > net_before:
        msg = raw_bot.send_message(
            m.from_user.id,
            f"❌ You cannot subtract ${amount:.2f}. Current net revenue is ${net_before:.2f}.\n\nSend a smaller amount."
        )
        bot.register_next_step_handler(msg, revenue_subtract_amount_step)
        return
    cfg = get_config()
    old_total = max(0.0, float(cfg.get("revenue_subtracted_total", 0) or 0))
    new_total = old_total + amount
    set_config("revenue_subtracted_total", new_total)
    log_event(
        "revenue_subtracted",
        m.from_user.id,
        details={"amount": amount, "previous_subtracted": old_total, "new_subtracted": new_total},
    )
    gross = _gross_bot_revenue()
    net = gross - new_total
    raw_bot.send_message(
        m.from_user.id,
        "✅ REVENUE UPDATED\n\n"
        f"➖ Subtracted now: ${amount:.2f}\n"
        f"📈 Gross revenue: ${gross:.2f}\n"
        f"➖ Total subtracted: ${new_total:.2f}\n"
        f"✅ Net revenue: ${net:.2f}",
        reply_markup=admin_menu(),
    )


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

    gross_revenue = _gross_bot_revenue()
    subtracted_revenue = _revenue_subtracted_total()
    net_revenue = gross_revenue - subtracted_revenue
    text += f"💵 **REVENUE:**\n"
    text += f"┌ Gross Revenue: `${gross_revenue:.2f}`\n"
    text += f"├ Subtracted: `${subtracted_revenue:.2f}`\n"
    text += f"└ Net Revenue: `${net_revenue:.2f}`\n\n"
    
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
# 📨 INVITE BOT USERS
# =========================
_invite_bot_users_state = {}

@bot.message_handler(func=lambda m: m.text == "📨 Invite Bot Users" and is_admin(m.from_user.id))
def invite_bot_users_menu(m):
    kb = InlineKeyboardMarkup(row_width=3)
    kb.add(
        InlineKeyboardButton("👥 All Users", callback_data="inviteusers|all"),
        InlineKeyboardButton("👑 VIP", callback_data="inviteusers|vip"),
        InlineKeyboardButton("🆓 Free", callback_data="inviteusers|free"),
    )
    raw_bot.send_message(
        m.from_user.id,
        "📨 INVITE BOT USERS\n\nChoose which bot users should receive the group/channel invitation.\n\nTelegram bots cannot force-add users; this sends them a Join button so they can join themselves.",
        reply_markup=kb,
    )

@bot.callback_query_handler(func=lambda c: c.data.startswith("inviteusers|"))
def invite_bot_users_target_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    target = c.data.split("|", 1)[1]
    if target not in {"all", "vip", "free"}:
        return bot.answer_callback_query(c.id, "Invalid target", True)
    _invite_bot_users_state[c.from_user.id] = {"target": target}
    msg = raw_bot.send_message(
        c.from_user.id,
        "🔗 Send the public group/channel @username, t.me link, or private Telegram invite link you want users to join."
    )
    bot.register_next_step_handler(msg, invite_bot_users_link_step)
    bot.answer_callback_query(c.id)


def invite_bot_users_link_step(m):
    if not is_admin(m.from_user.id):
        return
    state = _invite_bot_users_state.get(m.from_user.id)
    if not state:
        return raw_bot.send_message(m.chat.id, "❌ Invite session expired. Open Invite Bot Users again.", reply_markup=admin_menu())
    value = (m.text or "").strip()
    try:
        invite_url = normalize_url_or_username(value)
    except Exception as exc:
        msg = raw_bot.send_message(m.chat.id, f"❌ Invalid Telegram link/username: {exc}\n\nSend @username, a t.me link, or a private Telegram invite link.")
        bot.register_next_step_handler(msg, invite_bot_users_link_step)
        return
    state["invite_url"] = invite_url
    msg = raw_bot.send_message(
        m.chat.id,
        "✍️ Send the invitation message users should receive.\n\nType SKIP to use the default message."
    )
    bot.register_next_step_handler(msg, invite_bot_users_message_step)


def invite_bot_users_message_step(m):
    if not is_admin(m.from_user.id):
        return
    state = _invite_bot_users_state.pop(m.from_user.id, None)
    if not state:
        return raw_bot.send_message(m.chat.id, "❌ Invite session expired. Open Invite Bot Users again.", reply_markup=admin_menu())

    custom_text = (m.text or "").strip()
    if custom_text.upper() == "SKIP" or not custom_text:
        custom_text = (
            "🚀 Join our official community!\n\n"
            "Get the latest methods, updates, announcements and opportunities directly from us.\n\n"
            "Tap the button below to join 👇"
        )

    query = {}
    target = state.get("target", "all")
    if target == "vip":
        query = {"vip": True}
    elif target == "free":
        query = {"vip": False}

    users = list(users_col.find(query, {"_id": 1}))
    if not users:
        return raw_bot.send_message(m.chat.id, "❌ No matching users found.", reply_markup=admin_menu())

    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✅ Join Now", url=state["invite_url"]))
    status = raw_bot.send_message(m.chat.id, f"📨 Sending invitation to {len(users)} {target.upper()} users...")
    sent = failed = 0
    for u in users:
        try:
            uid = int(u["_id"])
            raw_bot.send_message(uid, custom_text, reply_markup=kb, disable_web_page_preview=True)
            sent += 1
            if sent % 20 == 0:
                time.sleep(0.3)
        except Exception:
            failed += 1

    try:
        raw_bot.edit_message_text(
            f"✅ Invitation completed.\n\n👥 Target: {target.upper()}\n📤 Sent: {sent}\n❌ Failed: {failed}",
            m.chat.id,
            status.message_id,
            reply_markup=admin_menu(),
        )
    except Exception:
        raw_bot.send_message(
            m.chat.id,
            f"✅ Invitation completed.\n\n👥 Target: {target.upper()}\n📤 Sent: {sent}\n❌ Failed: {failed}",
            reply_markup=admin_menu(),
        )

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
            sent_ids.append(_premium_send_text(target, chunk, reply_markup=markup).message_id)
    elif typ == "photo":
        sent_ids.append(raw_bot.send_photo(target, payload["file_id"], caption=payload.get("caption") or None, caption_entities=_premium_entities_for_text(payload.get("caption") or ""), reply_markup=reply_markup).message_id)
    elif typ == "video":
        sent_ids.append(raw_bot.send_video(target, payload["file_id"], caption=payload.get("caption") or None, caption_entities=_premium_entities_for_text(payload.get("caption") or ""), reply_markup=reply_markup).message_id)
    elif typ == "document":
        sent_ids.append(raw_bot.send_document(target, payload["file_id"], caption=payload.get("caption") or None, caption_entities=_premium_entities_for_text(payload.get("caption") or ""), reply_markup=reply_markup).message_id)
    elif typ == "animation":
        sent_ids.append(raw_bot.send_animation(target, payload["file_id"], caption=payload.get("caption") or None, caption_entities=_premium_entities_for_text(payload.get("caption") or ""), reply_markup=reply_markup).message_id)
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
    _growth_conversion(uid, "vip", code, charged)
    try:
        if str(payment_mode).lower() != "gift_balance":
            _loyalty_after_purchase(uid, charged, "vip", code)
    except Exception:
        pass
    links = _grant_chat_access(uid)
    try:
        _record_vip_activation(uid, sub_doc)
        _process_referral_vip_sale(uid, charged, sub_doc)
        _consume_coupon(uid)
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
        _growth_event("checkout_started", c.from_user.id, kind="vip", item=code, amount=price)
        _record_checkout_intent(c.from_user.id, "vip", code, price)
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
        if is_submission_blocked(c.from_user.id):
            bot.answer_callback_query(c.id, "Payment submissions are restricted for your account", True)
            return submission_block_notice(c.from_user.id)
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
        plan_code = str(pay.get("plan") or "VIP").upper()
        plan_info = (get_subscription_plans() or {}).get(plan_code) or {}
        plan_name = plan_info.get("name") or plan_code
        duration_minutes = plan_info.get("duration_minutes")
        if duration_minutes is None and plan_info.get("days") is not None:
            try:
                duration_minutes = float(plan_info.get("days")) * 1440
            except Exception:
                duration_minutes = None
        duration_label = _format_duration_minutes(duration_minutes) if duration_minutes else "VIP subscription"
        paid_amount = float(pay.get("amount", 0) or 0)
        try:
            base_price = float(plan_info.get("price", paid_amount) or paid_amount)
        except Exception:
            base_price = paid_amount
        saved = max(0.0, base_price - paid_amount)
        discount_pct = (saved / base_price * 100.0) if base_price > 0 else 0.0
        vip_invoice_id = str(pay.get("_id"))[-8:].upper()
        _send_globexomart_invoice(
            uid,
            "VIP ACCESS",
            vip_invoice_id,
            [
                ("Payment ID", f"#{vip_invoice_id}"),
                ("VIP Plan", plan_name),
                ("Access Duration", duration_label),
                ("Activated At", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                ("Expires At", exp),
                ("Regular Price", f"${base_price:g} USDT"),
                ("Discount", f"{discount_pct:g}% (-${saved:g})" if saved > 0.000001 else "No discount"),
                ("Price Paid", f"${paid_amount:g} USDT"),
                ("Payment Method", str(pay.get("mode") or "Manual").title()),
                ("Transaction ID", pay.get("transaction_id") or str(pay.get("_id"))),
                ("Bot", "@globexomartbot"),
            ],
            status="VIP Activated Successfully",
        )
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
    if is_submission_blocked(c.from_user.id):
        bot.answer_callback_query(c.id, "Payment submissions are restricted for your account", True)
        return submission_block_notice(c.from_user.id)
    try:
        from bson import ObjectId
        folder = folders_col.find_one({"_id": ObjectId(c.data.split("|", 1)[1])})
        if not folder or folder.get("cat") not in ("vip", "paid_service"):
            raise ValueError("Item not found")
        price = _effective_usdt_price(folder)
        _growth_event("product_view", c.from_user.id, kind="product", item=str(folder.get("_id")), amount=price)
        _record_checkout_intent(c.from_user.id, "product", str(folder.get("_id")), price, title=folder.get("name"))
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
                _growth_conversion(c.from_user.id, "product", str(folder["_id"]), price)
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
    if is_submission_blocked(c.from_user.id):
        bot.answer_callback_query(c.id, "Payment submissions are restricted for your account", True)
        return submission_block_notice(c.from_user.id)
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
    if is_submission_blocked(m.from_user.id):
        return submission_block_notice(m.from_user.id)
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
    if is_submission_blocked(m.from_user.id):
        return submission_block_notice(m.from_user.id)
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

def _pending_order_user_label(row):
    username = str(row.get("username") or "").strip().lstrip("@")
    first_name = str(row.get("first_name") or "").strip()
    if username:
        return "@" + username
    if first_name:
        return first_name
    try:
        u = users_col.find_one({"_id": str(row.get("user_id"))}, {"username": 1, "first_name": 1}) or {}
        if u.get("username"):
            return "@" + str(u.get("username")).lstrip("@")
        if u.get("first_name"):
            return str(u.get("first_name"))
    except Exception:
        pass
    return str(row.get("user_id") or "Unknown User")


def _get_pending_orders():
    orders = []
    try:
        for row in wallet_tx_col.find({"status": "pending", "type": {"$in": ["deposit", "withdraw"]}}).sort("created_at", -1).limit(100):
            orders.append((float(row.get("created_at", 0) or 0), str(row.get("type")), row))
    except Exception:
        pass
    try:
        # Manual VIP purchases are stored in payments with a plan field.
        for row in payments_col.find({"status": "pending", "plan": {"$exists": True}}).sort("created_at", -1).limit(100):
            orders.append((float(row.get("created_at", 0) or 0), "vip", row))
    except Exception:
        pass
    orders.sort(key=lambda x: x[0], reverse=True)
    return orders[:100]


@bot.message_handler(func=lambda m: m.text == "⏳ Pending Orders" and is_admin(m.from_user.id))
def pending_orders_admin(m):
    orders = _get_pending_orders()
    if not orders:
        return raw_bot.send_message(m.chat.id, "✅ No pending deposits, withdrawals or VIP purchases.", reply_markup=admin_menu())

    kb = InlineKeyboardMarkup(row_width=1)
    dep = wd = vip = 0
    for _, kind, row in orders:
        if kind == "deposit":
            dep += 1
            icon, title = "💳", "Deposit"
        elif kind == "withdraw":
            wd += 1
            icon, title = "💸", "Withdraw"
        else:
            vip += 1
            icon, title = "👑", "VIP"
        label = _pending_order_user_label(row)
        amount = float(row.get("amount", 0) or 0)
        kb.add(InlineKeyboardButton(
            f"{icon} {label} • {title} • ${amount:g}",
            callback_data=f"pendord|{kind}|{row['_id']}"
        ))
    raw_bot.send_message(
        m.chat.id,
        f"⏳ PENDING ORDERS\n\n💳 Deposits: {dep}\n💸 Withdrawals: {wd}\n👑 VIP Purchases: {vip}\n\nTap a user to review the order and proof.",
        reply_markup=kb,
    )


@bot.callback_query_handler(func=lambda c: c.data.startswith("pendord|"))
def pending_order_view_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    try:
        from bson import ObjectId
        _, kind, rid = c.data.split("|", 2)
        oid = ObjectId(rid)
        if kind in ("deposit", "withdraw"):
            row = wallet_tx_col.find_one({"_id": oid, "status": "pending", "type": kind})
        elif kind == "vip":
            row = payments_col.find_one({"_id": oid, "status": "pending", "plan": {"$exists": True}})
        else:
            row = None
        if not row:
            return bot.answer_callback_query(c.id, "Order already reviewed or not found", True)

        label = _pending_order_user_label(row)
        uid = row.get("user_id")
        amount = float(row.get("amount", 0) or 0)
        created = datetime.fromtimestamp(float(row.get("created_at", 0) or 0)).strftime("%Y-%m-%d %H:%M") if row.get("created_at") else "Unknown"

        if kind == "deposit":
            title = "💳 DEPOSIT REVIEW"
            details = (
                f"{title}\n\n👤 User: {label}\n🆔 User ID: {uid}\n"
                f"💰 Amount: ${amount:g} USDT\n🔗 TxID: {row.get('transaction_id','Not provided')}\n"
                f"🕒 Submitted: {created}"
            )
            kb = InlineKeyboardMarkup(row_width=2)
            kb.add(
                InlineKeyboardButton("✅ Approve Deposit", callback_data=f"walletapprove|{rid}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"walletreject|{rid}"),
            )
        elif kind == "withdraw":
            title = "💸 WITHDRAWAL REVIEW"
            details = (
                f"{title}\n\n👤 User: {label}\n🆔 User ID: {uid}\n"
                f"💰 Amount: ${amount:g} USDT\n🏦 Address: {row.get('address','Not provided')}\n"
                f"🕒 Submitted: {created}"
            )
            kb = InlineKeyboardMarkup(row_width=2)
            kb.add(
                InlineKeyboardButton("✅ Mark Paid", callback_data=f"walletapprove|{rid}"),
                InlineKeyboardButton("❌ Reject + Refund", callback_data=f"walletreject|{rid}"),
            )
        else:
            title = "👑 VIP PAYMENT REVIEW"
            details = (
                f"{title}\n\n👤 User: {label}\n🆔 User ID: {uid}\n"
                f"💎 Plan: {row.get('plan','Unknown')}\n💰 Amount: ${amount:g} USDT\n"
                f"🔗 TxID: {row.get('transaction_id','Not provided')}\n🕒 Submitted: {created}"
            )
            kb = InlineKeyboardMarkup(row_width=2)
            kb.add(
                InlineKeyboardButton("✅ Approve VIP", callback_data=f"payapprove|{rid}"),
                InlineKeyboardButton("❌ Reject", callback_data=f"payreject|{rid}"),
            )

        kb.add(InlineKeyboardButton("⬅️ Pending Orders", callback_data="pendordlist"))
        raw_bot.send_message(c.from_user.id, details, reply_markup=kb)

        # Deposits and VIP purchases include screenshot proof. Show it directly under the order.
        proof_chat = row.get("screenshot_chat_id")
        proof_msg = row.get("screenshot_message_id")
        if proof_chat and proof_msg:
            try:
                raw_bot.copy_message(c.from_user.id, int(proof_chat), int(proof_msg))
            except Exception as exc:
                raw_bot.send_message(c.from_user.id, f"⚠️ Could not load proof screenshot: {str(exc)[:200]}")
        elif kind != "withdraw":
            raw_bot.send_message(c.from_user.id, "⚠️ No screenshot proof is attached to this order.")
        bot.answer_callback_query(c.id)
    except Exception as exc:
        bot.answer_callback_query(c.id, "Could not open order", True)
        log_event("pending_order_view_error", c.from_user.id, details={"error": str(exc)}, level="error")


@bot.callback_query_handler(func=lambda c: c.data == "pendordlist")
def pending_orders_list_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    orders = _get_pending_orders()
    if not orders:
        bot.answer_callback_query(c.id, "No pending orders", True)
        return raw_bot.send_message(c.from_user.id, "✅ No pending deposits, withdrawals or VIP purchases.", reply_markup=admin_menu())
    kb = InlineKeyboardMarkup(row_width=1)
    dep = wd = vip = 0
    for _, kind, row in orders:
        if kind == "deposit":
            dep += 1; icon, title = "💳", "Deposit"
        elif kind == "withdraw":
            wd += 1; icon, title = "💸", "Withdraw"
        else:
            vip += 1; icon, title = "👑", "VIP"
        kb.add(InlineKeyboardButton(
            f"{icon} {_pending_order_user_label(row)} • {title} • ${float(row.get('amount',0) or 0):g}",
            callback_data=f"pendord|{kind}|{row['_id']}"
        ))
    raw_bot.send_message(c.from_user.id, f"⏳ PENDING ORDERS\n\n💳 Deposits: {dep}\n💸 Withdrawals: {wd}\n👑 VIP Purchases: {vip}", reply_markup=kb)
    bot.answer_callback_query(c.id)


# =========================
# 🧹 TEST DATA + SUBMISSION CONTROLS
# =========================
_cleanup_state = {}


def _active_vip_rows(limit=100):
    now = time.time()
    rows = []
    try:
        for u in users_col.find({"vip": True}).limit(limit):
            expiry = u.get("vip_expiry")
            if expiry is None or float(expiry or 0) > now:
                rows.append(u)
    except Exception:
        pass
    return rows


def _admin_user_label_from_userdoc(u):
    username = (u.get("username") or "").strip()
    name = " ".join(x for x in [u.get("first_name"), u.get("last_name")] if x).strip()
    if username:
        return "@" + username.lstrip("@")
    if name:
        return name[:40]
    return str(u.get("_id", "Unknown"))


@bot.message_handler(func=lambda m: m.text == "🧹 Test Data & User Controls" and is_admin(m.from_user.id))
def test_data_user_controls_admin(m):
    pay_count = payments_col.count_documents({})
    wallet_count = wallet_tx_col.count_documents({})
    item_count = item_purchases_col.count_documents({})
    shop_count = shop_orders_col.count_documents({})
    vip_count = len(_active_vip_rows(10000))
    blocked_count = users_col.count_documents({"submission_blocked": True})
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🧾 Clear Payment/Test Order History", callback_data="cleanup|payments|ask"))
    kb.add(InlineKeyboardButton("👑 Manage Test VIP Users", callback_data="cleanup|vip|list"))
    kb.add(InlineKeyboardButton("🚫 Submission Blocks", callback_data="cleanup|blocks|menu"))
    raw_bot.send_message(
        m.chat.id,
        "🧹 TEST DATA & USER CONTROLS\n\n"
        f"🧾 Payment records: {pay_count}\n"
        f"💳 Wallet transactions: {wallet_count}\n"
        f"🛍 Purchase/order records: {item_count + shop_count}\n"
        f"👑 Active VIP users: {vip_count}\n"
        f"🚫 Submission-blocked users: {blocked_count}\n\n"
        "Use these tools for test cleanup and abuse control. Normal bot users are not deleted.",
        reply_markup=kb,
    )


@bot.callback_query_handler(func=lambda c: c.data == "cleanup|payments|ask")
def cleanup_payments_ask_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("⚠️ YES — Clear Test Payment History", callback_data="cleanup|payments|confirm"))
    kb.add(InlineKeyboardButton("❌ Cancel", callback_data="cleanup|cancel"))
    raw_bot.send_message(
        c.from_user.id,
        "⚠️ CLEAR PAYMENT/TEST HISTORY?\n\n"
        "This permanently deletes stored payment proofs/order records from:\n"
        "• VIP/points/product payment history\n• Deposit & withdrawal history\n• Item purchase history\n• Shop order history\n\n"
        "It does NOT delete users or remove their current VIP status. Use VIP cleanup separately for test VIP accounts.",
        reply_markup=kb,
    )
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data == "cleanup|payments|confirm")
def cleanup_payments_confirm_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    counts = {}
    for name, col in (
        ("payments", payments_col),
        ("wallet transactions", wallet_tx_col),
        ("item purchases", item_purchases_col),
        ("shop orders", shop_orders_col),
    ):
        try:
            counts[name] = col.delete_many({}).deleted_count
        except Exception:
            counts[name] = 0
    log_event("admin_test_payment_history_cleared", c.from_user.id, details=counts)
    raw_bot.send_message(
        c.from_user.id,
        "✅ TEST PAYMENT HISTORY CLEARED\n\n" + "\n".join(f"• {k}: {v}" for k, v in counts.items()),
        reply_markup=admin_menu(),
    )
    bot.answer_callback_query(c.id, "Cleared", True)


@bot.callback_query_handler(func=lambda c: c.data == "cleanup|vip|list")
def cleanup_vip_list_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    rows = _active_vip_rows(100)
    if not rows:
        bot.answer_callback_query(c.id, "No active VIP users", True)
        return raw_bot.send_message(c.from_user.id, "✅ No active VIP users to reset.")
    kb = InlineKeyboardMarkup(row_width=1)
    for u in rows:
        uid = str(u.get("_id"))
        exp = u.get("vip_expiry")
        exp_text = "Lifetime" if not exp else datetime.fromtimestamp(float(exp)).strftime("%Y-%m-%d")
        kb.add(InlineKeyboardButton(f"👑 {_admin_user_label_from_userdoc(u)} • {exp_text}", callback_data=f"cleanup|vipuser|{uid}"))
    kb.add(InlineKeyboardButton("⚠️ Reset ALL Active VIP Users", callback_data="cleanup|vipall|ask"))
    raw_bot.send_message(c.from_user.id, "👑 ACTIVE VIP USERS\n\nTap a test user to remove VIP access, or use the bulk reset only if all current VIPs are test accounts.", reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("cleanup|vipuser|"))
def cleanup_vip_user_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    uid = c.data.rsplit("|", 1)[1]
    u = users_col.find_one({"_id": str(uid)}) or {}
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✅ Remove VIP From This User", callback_data=f"cleanup|vipremove|{uid}"))
    kb.add(InlineKeyboardButton("⬅️ Back", callback_data="cleanup|vip|list"))
    raw_bot.send_message(c.from_user.id, f"👤 {_admin_user_label_from_userdoc(u)}\n🆔 {uid}\n\nRemove this user's VIP/subscription access?", reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("cleanup|vipremove|"))
def cleanup_vip_remove_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    uid = int(c.data.rsplit("|", 1)[1])
    users_col.update_one({"_id": str(uid)}, {"$set": {"vip": False, "vip_expiry": None}})
    subscriptions_col.update_many({"user_id": int(uid), "status": "active"}, {"$set": {"status": "removed", "removed_at": time.time(), "removed_by": c.from_user.id, "remove_reason": "test_cleanup"}})
    try:
        User._cache.pop(uid, None); User._cache.pop(str(uid), None)
    except Exception:
        pass
    log_event("admin_test_vip_removed", c.from_user.id, uid, {"reason": "test_cleanup"})
    raw_bot.send_message(c.from_user.id, f"✅ VIP removed from user {uid}.")
    bot.answer_callback_query(c.id, "VIP removed", True)


@bot.callback_query_handler(func=lambda c: c.data == "cleanup|vipall|ask")
def cleanup_vip_all_ask_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("⚠️ CONFIRM — Reset ALL VIP Users", callback_data="cleanup|vipall|confirm"))
    kb.add(InlineKeyboardButton("❌ Cancel", callback_data="cleanup|cancel"))
    raw_bot.send_message(c.from_user.id, "⚠️ This removes VIP from EVERY currently active VIP user. Only use this if they are all test accounts.", reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data == "cleanup|vipall|confirm")
def cleanup_vip_all_confirm_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    rows = _active_vip_rows(10000)
    uids = [int(u["_id"]) for u in rows if str(u.get("_id", "")).isdigit()]
    if uids:
        users_col.update_many({"_id": {"$in": [str(x) for x in uids]}}, {"$set": {"vip": False, "vip_expiry": None}})
        subscriptions_col.update_many({"user_id": {"$in": uids}, "status": "active"}, {"$set": {"status": "removed", "removed_at": time.time(), "removed_by": c.from_user.id, "remove_reason": "bulk_test_cleanup"}})
    try:
        User._cache.clear(); User._cache_time.clear()
    except Exception:
        pass
    log_event("admin_all_test_vip_removed", c.from_user.id, details={"count": len(uids)})
    raw_bot.send_message(c.from_user.id, f"✅ Reset complete. VIP removed from {len(uids)} user(s).", reply_markup=admin_menu())
    bot.answer_callback_query(c.id, "VIP reset complete", True)


@bot.callback_query_handler(func=lambda c: c.data == "cleanup|blocks|menu")
def submission_blocks_menu_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    blocked = list(users_col.find({"submission_blocked": True}).limit(100))
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🔎 Block / Unblock a User", callback_data="cleanup|blocks|search"))
    for u in blocked:
        uid = str(u.get("_id"))
        kb.add(InlineKeyboardButton(f"🚫 {_admin_user_label_from_userdoc(u)}", callback_data=f"cleanup|blockuser|{uid}"))
    text = f"🚫 SUBMISSION BLOCKS\n\nBlocked users: {len(blocked)}\n\nBlocked users can still browse/use the bot, but cannot create VIP payment proofs, deposits, withdrawals, points payments, paid-method payment requests, referral withdrawals, or work applications."
    raw_bot.send_message(c.from_user.id, text, reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data == "cleanup|blocks|search")
def submission_block_search_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    msg = raw_bot.send_message(c.from_user.id, "🔎 Send the user's Telegram ID or @username.")
    bot.register_next_step_handler(msg, submission_block_search_step)
    bot.answer_callback_query(c.id)


def submission_block_search_step(m):
    if not is_admin(m.from_user.id):
        return
    q = (m.text or "").strip()
    if not q:
        return raw_bot.send_message(m.chat.id, "❌ Empty search.", reply_markup=admin_menu())
    if q.lstrip("-").isdigit():
        u = users_col.find_one({"_id": str(int(q))})
    else:
        uname = q.lstrip("@")
        u = users_col.find_one({"username": {"$regex": f"^{re.escape(uname)}$", "$options": "i"}})
    if not u:
        return raw_bot.send_message(m.chat.id, "❌ User not found in bot database.", reply_markup=admin_menu())
    uid = str(u.get("_id"))
    blocked = bool(u.get("submission_blocked", False))
    kb = InlineKeyboardMarkup(row_width=1)
    action = "✅ Unblock Submissions" if blocked else "🚫 Block Submissions"
    kb.add(InlineKeyboardButton(action, callback_data=f"cleanup|blocktoggle|{uid}"))
    raw_bot.send_message(m.chat.id, f"👤 {_admin_user_label_from_userdoc(u)}\n🆔 {uid}\nStatus: {'🚫 BLOCKED' if blocked else '✅ Allowed'}", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("cleanup|blockuser|"))
def submission_block_user_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    uid = c.data.rsplit("|", 1)[1]
    u = users_col.find_one({"_id": str(uid)}) or {"_id": uid}
    blocked = bool(u.get("submission_blocked", False))
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✅ Unblock Submissions" if blocked else "🚫 Block Submissions", callback_data=f"cleanup|blocktoggle|{uid}"))
    raw_bot.send_message(c.from_user.id, f"👤 {_admin_user_label_from_userdoc(u)}\n🆔 {uid}\nStatus: {'🚫 BLOCKED' if blocked else '✅ Allowed'}", reply_markup=kb)
    bot.answer_callback_query(c.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("cleanup|blocktoggle|"))
def submission_block_toggle_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    uid = c.data.rsplit("|", 1)[1]
    u = users_col.find_one({"_id": str(uid)}) or {}
    new_value = not bool(u.get("submission_blocked", False))
    users_col.update_one({"_id": str(uid)}, {"$set": {"submission_blocked": new_value}}, upsert=False)
    _clear_known_flow_state(int(uid) if str(uid).isdigit() else uid)
    log_event("submission_block_changed", c.from_user.id, int(uid) if str(uid).isdigit() else uid, {"blocked": new_value})
    raw_bot.send_message(c.from_user.id, f"{'🚫 Submissions blocked' if new_value else '✅ Submissions unblocked'} for user {uid}.", reply_markup=admin_menu())
    try:
        raw_bot.send_message(int(uid), "🚫 Your payment/application submissions have been restricted by an administrator." if new_value else "✅ Your payment/application submission access has been restored.")
    except Exception:
        pass
    bot.answer_callback_query(c.id, "Updated", True)


@bot.callback_query_handler(func=lambda c: c.data == "cleanup|cancel")
def cleanup_cancel_cb(c):
    if is_admin(c.from_user.id):
        raw_bot.send_message(c.from_user.id, "❌ Cleanup cancelled.", reply_markup=admin_menu())
    bot.answer_callback_query(c.id, "Cancelled")

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
    if is_submission_blocked(c.from_user.id):
        bot.answer_callback_query(c.id, "Withdrawal submissions are restricted for your account", True)
        return submission_block_notice(c.from_user.id)
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
    # Time-limited deals and a user's redeemed coupon are applied last.
    try:
        deal_pct = _active_vip_deal_discount(code)
        coupon_pct = _active_coupon_discount(uid, "vip", code)
        extra_pct = max(deal_pct, coupon_pct)
        if extra_pct:
            price *= (100.0 - extra_pct) / 100.0
    except Exception:
        pass
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


# ---- Premium custom emoji system ----
def _premium_emoji_map():
    data = get_cached_config().get("premium_emoji_map") or {}
    return {str(k): str(v) for k, v in data.items() if k and v}


def _utf16_slice(text, offset, length):
    raw = str(text).encode("utf-16-le")
    return raw[offset * 2:(offset + length) * 2].decode("utf-16-le", errors="ignore")


def _premium_entities_for_text(text):
    mapping = _premium_emoji_map()
    if not mapping or not text:
        return None
    from telebot.types import MessageEntity
    entities = []
    # Longest glyph first so multi-codepoint emoji win over shorter variants.
    glyphs = sorted(mapping.keys(), key=len, reverse=True)
    occupied = []
    for glyph in glyphs:
        start = 0
        while True:
            idx = text.find(glyph, start)
            if idx < 0:
                break
            before = text[:idx]
            chunk = text[idx:idx + len(glyph)]
            off = len(before.encode("utf-16-le")) // 2
            ln = len(chunk.encode("utf-16-le")) // 2
            if not any(not (off + ln <= a or off >= b) for a, b in occupied):
                entities.append(MessageEntity(type="custom_emoji", offset=off, length=ln, custom_emoji_id=mapping[glyph]))
                occupied.append((off, off + ln))
            start = idx + len(glyph)
    return sorted(entities, key=lambda e: e.offset) or None


def _premium_send_text(chat_id, text, **kwargs):
    """Send text with Telegram custom-emoji entities when available.

    Telegram currently allows bots to use custom emoji in directly sent messages
    when the bot owner has Telegram Premium (or the bot has an eligible
    collectible/additional username). If Telegram rejects custom emoji, keep the
    message usable by falling back to the normal emoji glyphs.
    """
    kwargs.pop("parse_mode", None)
    entities = _premium_entities_for_text(text)
    if not entities:
        return raw_bot.send_message(chat_id, text, **kwargs)

    try:
        kwargs["entities"] = entities
        return raw_bot.send_message(chat_id, text, **kwargs)
    except Exception as exc:
        # Never break posts/proofs just because Telegram does not allow this bot
        # to send custom emoji. Retry once with normal Unicode emoji.
        kwargs.pop("entities", None)
        log_event(
            "premium_emoji_send_fallback",
            target=chat_id,
            details={"error": str(exc)[:500]},
            level="warning",
        )
        return raw_bot.send_message(chat_id, text, **kwargs)


def _premium_copy_with_caption(target, source_chat, source_message, caption, **kwargs):
    kwargs.pop("parse_mode", None)
    ents = _premium_entities_for_text(caption)
    if ents:
        kwargs["caption_entities"] = ents
    return raw_bot.copy_message(target, source_chat, source_message, caption=caption, **kwargs)


@bot.message_handler(func=lambda m: m.text == "✨ Premium Emojis" and is_admin(m.from_user.id))
def premium_emoji_menu(m):
    count = len(_premium_emoji_map())
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🔗 Add Emoji Pack Links", callback_data="premiumemoji|pack"))
    kb.add(InlineKeyboardButton("✨ Send Premium Emojis", callback_data="premiumemoji|direct"))
    kb.add(InlineKeyboardButton("📚 View All Emojis", callback_data="premiumemoji|view"))
    kb.add(InlineKeyboardButton("⭐ Favorite Emojis", callback_data="premiumemoji|favorites"))
    kb.add(InlineKeyboardButton("🧪 Test Premium Emoji", callback_data="premiumemoji|test"))
    kb.add(InlineKeyboardButton("🧹 Clear Saved Emojis", callback_data="premiumemoji|clear"))
    raw_bot.send_message(m.from_user.id, f"✨ PREMIUM EMOJIS\n\nSaved mappings: {count}\n\nAdd one or multiple Telegram custom emoji pack links (t.me/addemoji/...) or send custom premium emojis directly. The bot maps each premium emoji to its normal emoji meaning and automatically uses it in generated proof/status messages. Auto Posts preserve the exact premium emojis from the post you send or forward.\n\nℹ️ Telegram requirement: for directly generated bot messages, the bot owner must have Telegram Premium (or the bot must otherwise be eligible for custom emoji). Use 🧪 Test Premium Emoji to verify this bot.", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith("premiumemoji|"))
def premium_emoji_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admin only", True)
    action = c.data.split("|", 1)[1]
    if action == "view":
        mapping = _premium_emoji_map()
        favs = set(str(x) for x in (get_cached_config().get("premium_emoji_favorites") or []))
        if not mapping:
            bot.answer_callback_query(c.id)
            return raw_bot.send_message(c.from_user.id, "📚 No premium emojis saved yet.")
        lines = ["📚 ALL PREMIUM EMOJIS\n"]
        for i, glyph in enumerate(mapping.keys(), 1):
            lines.append(f"{'⭐ ' if glyph in favs else ''}{i}. {glyph}")
        bot.answer_callback_query(c.id)
        return _premium_send_text(c.from_user.id, "\n".join(lines)[:4000])
    if action == "test":
        mapping = _premium_emoji_map()
        if not mapping:
            bot.answer_callback_query(c.id, "Add premium emojis first", True)
            return
        glyph = next(iter(mapping.keys()))
        test_text = f"{glyph} Premium emoji test — if this icon is animated/custom, setup is working."
        try:
            ents = _premium_entities_for_text(test_text)
            if not ents:
                raise ValueError("No custom emoji entity could be built")
            raw_bot.send_message(c.from_user.id, test_text, entities=ents)
            bot.answer_callback_query(c.id, "Test sent")
        except Exception as exc:
            bot.answer_callback_query(c.id, "Telegram rejected custom emoji", True)
            raw_bot.send_message(
                c.from_user.id,
                "❌ Telegram did not allow this bot to send the custom emoji.\n\n"
                "The emoji pack is saved correctly, but Telegram requires the bot owner to have an active Telegram Premium subscription for directly generated custom-emoji messages (unless the bot has other eligible custom-emoji rights).\n\n"
                f"Error: {str(exc)[:500]}",
                reply_markup=admin_menu(),
            )
        return
    if action == "favorites":
        mapping = _premium_emoji_map()
        if not mapping:
            bot.answer_callback_query(c.id, "Add emojis first", True)
            return
        lines = [f"{i}. {g}" for i, g in enumerate(mapping.keys(), 1)]
        msg = _premium_send_text(c.from_user.id, "⭐ FAVORITE EMOJIS\n\nSend the numbers you want the bot to prefer, separated by commas.\nExample: 1,3,5\n\n" + "\n".join(lines)[:3300])
        bot.register_next_step_handler(msg, premium_emoji_favorites_step)
        bot.answer_callback_query(c.id)
        return
    if action == "clear":
        set_config("premium_emoji_map", {})
        set_config("premium_emoji_favorites", [])
        bot.answer_callback_query(c.id, "Cleared")
        return raw_bot.send_message(c.from_user.id, "✅ Premium emoji mappings cleared.", reply_markup=admin_menu())
    prompt = "Send one or multiple Telegram custom emoji pack links. You can put each link on a new line or separate them with spaces.\n\nExample:\nhttps://t.me/addemoji/PackOne\nhttps://t.me/addemoji/PackTwo" if action == "pack" else "Send a message containing the premium custom emojis you want the bot to learn."
    msg = raw_bot.send_message(c.from_user.id, prompt)
    bot.register_next_step_handler(msg, premium_emoji_pack_step if action == "pack" else premium_emoji_direct_step)
    bot.answer_callback_query(c.id)


def premium_emoji_pack_step(m):
    try:
        value = (m.text or "").strip()
        pack_names = re.findall(r"(?:https?://)?t\.me/addemoji/([A-Za-z0-9_]+)", value, re.I)
        # Preserve order while ignoring duplicate links in the same submission.
        pack_names = list(dict.fromkeys(pack_names))
        if not pack_names:
            raise ValueError("Send at least one valid t.me/addemoji/... link")

        mapping = _premium_emoji_map()
        total_added = 0
        packs_added = 0
        failed = []
        for pack_name in pack_names:
            try:
                sticker_set = bot.get_sticker_set(pack_name)
                pack_added = 0
                for sticker in getattr(sticker_set, "stickers", []) or []:
                    glyph = getattr(sticker, "emoji", None)
                    custom_id = getattr(sticker, "custom_emoji_id", None)
                    if glyph and custom_id:
                        mapping[str(glyph)] = str(custom_id)
                        pack_added += 1
                if pack_added:
                    total_added += pack_added
                    packs_added += 1
                else:
                    failed.append(pack_name)
            except Exception:
                failed.append(pack_name)

        if not total_added:
            raise ValueError("No custom emoji mappings were found in the supplied packs")
        set_config("premium_emoji_map", mapping)
        text = f"✅ Premium emoji packs added: {packs_added}/{len(pack_names)}\n✨ Learned {total_added} emoji entries.\n📚 Total saved: {len(mapping)}"
        if failed:
            text += "\n\n⚠️ Could not read: " + ", ".join(failed[:10])
        raw_bot.send_message(m.chat.id, text, reply_markup=admin_menu())
    except Exception as exc:
        admin_error(m.from_user.id, exc)


def premium_emoji_direct_step(m):
    try:
        text = m.text or m.caption or ""
        entities = list(getattr(m, "entities", None) or []) + list(getattr(m, "caption_entities", None) or [])
        mapping = _premium_emoji_map()
        added = 0
        for ent in entities:
            if getattr(ent, "type", None) != "custom_emoji" or not getattr(ent, "custom_emoji_id", None):
                continue
            glyph = _utf16_slice(text, int(ent.offset), int(ent.length))
            if glyph:
                mapping[glyph] = str(ent.custom_emoji_id)
                added += 1
        if not added:
            raise ValueError("No Telegram custom premium emojis were detected in that message")
        set_config("premium_emoji_map", mapping)
        raw_bot.send_message(m.chat.id, f"✅ Learned {added} premium emoji mappings. Total saved: {len(mapping)}", reply_markup=admin_menu())
    except Exception as exc:
        admin_error(m.from_user.id, exc)



def premium_emoji_favorites_step(m):
    try:
        mapping = _premium_emoji_map()
        glyphs = list(mapping.keys())
        nums = []
        for part in re.split(r"[,\s]+", (m.text or "").strip()):
            if part.isdigit():
                n = int(part)
                if 1 <= n <= len(glyphs) and n not in nums:
                    nums.append(n)
        if not nums:
            raise ValueError("Send valid emoji numbers, for example: 1,3,5")
        favorites = [glyphs[n - 1] for n in nums]
        set_config("premium_emoji_favorites", favorites)
        _premium_send_text(m.chat.id, "✅ Favorite emojis saved: " + " ".join(favorites) + "\n\nThe Post Maker will prioritize these emojis when they fit the post.", reply_markup=admin_menu())
    except Exception as exc:
        admin_error(m.from_user.id, exc)


def _favorite_post_emoji(default_glyph):
    mapping = _premium_emoji_map()
    favorites = [str(x) for x in (get_cached_config().get("premium_emoji_favorites") or []) if str(x) in mapping]
    if not favorites:
        return default_glyph
    # Prefer favorites that naturally match the role of the requested emoji; otherwise rotate favorites.
    groups = {
        "🔥": "🔥🚨💥⚡", "🚨": "🚨🔥💥⚡", "✨": "✨💎⭐👑", "💎": "💎✨👑⭐",
        "📚": "📚📌💡✅", "🚀": "🚀⚡🔥💥", "👑": "👑💎⭐✨", "✅": "✅📌✨⭐",
        "💡": "💡📚✨📌", "📌": "📌✅📚💡", "⚡": "⚡🔥🚀💥", "💥": "💥🔥🚨⚡"
    }
    preferred = groups.get(default_glyph, default_glyph)
    for glyph in favorites:
        if glyph in preferred:
            return glyph
    return favorites[sum(ord(ch) for ch in default_glyph) % len(favorites)]


# ---- User Post Maker ----
_post_maker_state = {}

def _post_maker_clean_input(value):
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:1200]

def _post_maker_title(details):
    clean = _post_maker_clean_input(details)
    if not clean:
        return "NEW UPDATE"
    first = re.split(r"[.!?\n]", clean, 1)[0].strip()
    words = first.split()[:9]
    title = " ".join(words).strip(" -:,.!")
    return (title or "NEW UPDATE").upper()

def _post_maker_body(details):
    clean = _post_maker_clean_input(details)
    # Telegram's native Copy Text button supports up to 256 characters, so
    # generated posts intentionally stay compact enough for true one-tap copy.
    if len(clean) <= 120:
        return clean
    return clean[:117].rstrip() + "..."

def _build_post_maker_text(details, variant=0):
    details = _post_maker_clean_input(details)
    title = _post_maker_title(details)[:52]
    body = _post_maker_body(details)
    e = _favorite_post_emoji
    styles = [
        f"{e('🔥')} {title} {e('🔥')}\n\n{body}\n\n{e('✨')} Clear. Useful. Easy to follow.\n{e('🚀')} Don't miss the update!",
        f"{e('🚨')} {title} {e('🚨')}\n\n{body}\n\n{e('💎')} Everything you need in one place.\n{e('📌')} Check it out now!",
        f"{e('✨')} {title} {e('✨')}\n\n{body}\n\n{e('📚')} Simple guidance • {e('⚡')} Quick steps\n{e('🔥')} Stay updated with GLOBEXOMART.",
        f"{e('💥')} {title} {e('💥')}\n\n{body}\n\n{e('✅')} Useful info • {e('✅')} Full guidance\n{e('🚀')} Take action while it's available.",
        f"{e('👑')} {title} {e('👑')}\n\n{body}\n\n{e('💡')} Learn it. Use it. Grow.\n{e('🔥')} More updates from GLOBEXOMART.",
    ]
    text = styles[int(variant) % len(styles)]
    return text if len(text) <= 256 else text[:253].rstrip() + "..."

def _post_maker_keyboard(uid, text):
    kb = InlineKeyboardMarkup(row_width=2)
    kb.row(
        InlineKeyboardButton("🔄 Change", callback_data="postmaker|change"),
        InlineKeyboardButton("📋 Copy", callback_data="postmaker|copy"),
    )
    kb.row(InlineKeyboardButton("✏️ New Topic", callback_data="postmaker|new"))
    return kb

def _send_post_maker_result(uid, text, edit_message_id=None):
    kb = _post_maker_keyboard(uid, text)
    # Premium emoji mappings are supplied and controlled only by admins.
    if edit_message_id is not None:
        try:
            ents = _premium_entities_for_text(text)
            raw_bot.edit_message_text(text, uid, edit_message_id, reply_markup=kb, entities=ents)
            return
        except Exception:
            pass
    _premium_send_text(uid, text, reply_markup=kb)

@bot.message_handler(func=lambda m: m.text == "📝 Admin Post Maker" and is_admin(m.from_user.id))
def admin_post_maker_open(m):
    uid = m.from_user.id
    msg = raw_bot.send_message(
        uid,
        "📝 ADMIN POST MAKER\n\nSend the topic, offer, update, method, product or information you want to post. The bot will create a ready-to-use post with the premium emojis configured by admin.",
        reply_markup=_cancel_inline_markup(),
    )
    bot.register_next_step_handler(msg, post_maker_topic_step)

@bot.message_handler(func=lambda m: m.text == "📝 Post Maker")
@force_join_handler
def post_maker_open(m):
    uid = m.from_user.id
    if "📝 Post Maker" in get_hidden_main_buttons() and not is_admin(uid):
        return raw_bot.send_message(uid, "❌ Post Maker is currently unavailable.", reply_markup=main_menu(uid))
    msg = raw_bot.send_message(
        uid,
        "📝 POST MAKER\n\nSend the topic, offer, update, method, product or information you want to post. The bot will turn it into an attractive ready-to-use post using the premium emojis added by admin.",
        reply_markup=_cancel_inline_markup(),
    )
    bot.register_next_step_handler(msg, post_maker_topic_step)

def post_maker_topic_step(m):
    uid = m.from_user.id
    details = _post_maker_clean_input(m.text or m.caption)
    if not details:
        msg = raw_bot.send_message(uid, "❌ Send some text describing what the post should be about.")
        bot.register_next_step_handler(msg, post_maker_topic_step)
        return
    _post_maker_state[uid] = {"details": details, "variant": 0}
    text = _build_post_maker_text(details, 0)
    _post_maker_state[uid]["text"] = text
    _send_post_maker_result(uid, text)

@bot.callback_query_handler(func=lambda c: c.data.startswith("postmaker|"))
def post_maker_callback(c):
    uid = c.from_user.id
    action = c.data.split("|", 1)[1]
    state = _post_maker_state.get(uid)
    if action == "new":
        msg = raw_bot.send_message(uid, "✏️ Send the new topic/details for your post:", reply_markup=_cancel_inline_markup())
        bot.register_next_step_handler(msg, post_maker_topic_step)
        return bot.answer_callback_query(c.id)
    if not state:
        bot.answer_callback_query(c.id, "Post session expired", True)
        return raw_bot.send_message(uid, "Open 📝 Post Maker again.", reply_markup=main_menu(uid))
    if action == "change":
        state["variant"] = int(state.get("variant", 0)) + 1
        state["text"] = _build_post_maker_text(state["details"], state["variant"])
        _send_post_maker_result(uid, state["text"], c.message.message_id)
        return bot.answer_callback_query(c.id, "Post changed")
    if action == "copy":
        text = state.get("text") or _build_post_maker_text(state["details"], state.get("variant", 0))
        # Try Telegram's native Copy Text button when supported by the installed library.
        try:
            from telebot.types import CopyTextButton
            copy_kb = InlineKeyboardMarkup(row_width=1)
            copy_kb.add(InlineKeyboardButton("📋 Copy Post", copy_text=CopyTextButton(text=text)))
            _premium_send_text(uid, text, reply_markup=copy_kb)
            bot.answer_callback_query(c.id, "Tap Copy Post")
        except Exception:
            _premium_send_text(uid, text)
            bot.answer_callback_query(c.id, "Post sent separately — long-press to copy", True)
        return


# ---- Proof channel ----
def _mask_public_name(username=None, first_name=None):
    """Return a privacy-safe public display name for proof posts."""
    raw = (username or first_name or "Member").strip().lstrip("@")
    if not raw:
        return "Member"
    if len(raw) <= 2:
        return raw[:1] + "***"
    visible = max(2, min(5, (len(raw) + 1) // 2))
    return raw[:visible] + "***"

def _publish_proof(kind, record, screenshot_chat_id=None, screenshot_message_id=None):
    target = get_cached_config().get("proof_channel")
    if not target:
        return
    try:
        target = normalize_chat_reference(target)
        kind = str(kind).upper()
        amount = float(record.get("amount", record.get("total", 0)) or 0)
        title = {"VIP":"PROOF VIP", "METHOD":"PROOF METHOD", "PRODUCT":"PRODUCT PROOF"}.get(kind, "PURCHASE PROOF")
        masked_user = _mask_public_name(record.get("username"), record.get("first_name"))

        lines = [
            f"✅ {title}",
            "",
            f"👤 Member: @{masked_user}" if record.get("username") else f"👤 Member: {masked_user}",
        ]

        if kind == "VIP":
            plan_code = str(record.get("plan") or "").upper()
            plan = (get_subscription_plans() or {}).get(plan_code) or {}
            plan_name = plan.get("name") or plan_code or "VIP"
            duration_minutes = plan.get("duration_minutes")
            if duration_minutes is None and plan.get("days") is not None:
                try:
                    duration_minutes = float(plan.get("days")) * 1440
                except Exception:
                    duration_minutes = None
            if duration_minutes:
                lines.append(f"⏳ Access: {_format_duration_minutes(duration_minutes)}")
            lines.append(f"💎 Plan: {plan_name}")

            try:
                base_price = float(plan.get("price", amount) or amount)
            except Exception:
                base_price = amount
            discount_amount = max(0.0, base_price - amount)
            discount_percent = (discount_amount / base_price * 100.0) if base_price > 0 else 0.0
            if discount_amount > 0.000001:
                lines.append(f"🏷 Discount: {discount_percent:g}% (-${discount_amount:g})")
            else:
                lines.append("🏷 Discount: No discount")

        lines.append(f"💰 Paid: ${amount:g} USDT")

        created_at = record.get("created_at")
        try:
            paid_time = datetime.fromtimestamp(float(created_at)) if created_at else datetime.now()
        except Exception:
            paid_time = datetime.now()
        lines.append(f"🕒 Paid at: {paid_time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.extend(["", "💛 Thanks for trusting GLOBEXOMART. We appreciate every member who chooses to grow with us! 🚀"])
        text = "\n".join(lines)

        # Privacy rule: proof-channel posts never expose raw user IDs or chat IDs.
        if screenshot_chat_id and screenshot_message_id:
            try:
                _premium_copy_with_caption(target, int(screenshot_chat_id), int(screenshot_message_id), text)
                return
            except Exception:
                pass
        _premium_send_text(target, text)
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


def _support_save_message(uid, sender, source_chat_id, source_message_id, admin_id=None):
    """Save a Telegram message reference so the complete support conversation can be replayed later."""
    try:
        support_messages_col.insert_one({
            'user_id': int(uid),
            'sender': str(sender),   # user | admin
            'admin_id': int(admin_id) if admin_id is not None else None,
            'source_chat_id': int(source_chat_id),
            'source_message_id': int(source_message_id),
            'created_at': time.time(),
        })
    except Exception as exc:
        log_event('support_history_save_error', uid, details={'error': str(exc)}, level='error')


def _support_send_prompt(uid):
    kb=ReplyKeyboardMarkup(resize_keyboard=True,row_width=2)
    kb.row('⬅️ Back','❌ End Chat')
    msg=raw_bot.send_message(uid,'💬 CHAT WITH ADMIN\n\nSend any text, photo, video, file, document, voice note or other Telegram message. Admin can reply through the bot.\n\n⬅️ Back returns to the main menu without closing this conversation.\n❌ End Chat closes it.',reply_markup=kb)
    bot.register_next_step_handler(msg,_support_user_message)


@bot.message_handler(func=lambda m: m.text == '💬 Chat Admin')
def support_chat_start(m):
    if not get_cached_config().get('support_chat_enabled',True):
        return raw_bot.send_message(m.from_user.id,'Support chat is currently unavailable.')
    support_chats_col.update_one(
        {'user_id':int(m.from_user.id)},
        {'$set':{'user_id':int(m.from_user.id),'username':m.from_user.username,'first_name':m.from_user.first_name,'updated_at':time.time(),'open':True},
         '$setOnInsert':{'created_at':time.time(),'unread_admin':0}},upsert=True)
    _support_send_prompt(m.from_user.id)


def _support_user_message(m):
    uid=int(m.from_user.id)
    text=(m.text or '').strip()
    if text=='⬅️ Back':
        # Keep the support thread OPEN; only leave the active input flow.
        support_chats_col.update_one({'user_id':uid},{'$set':{'open':True,'updated_at':time.time()}})
        return raw_bot.send_message(uid,'💬 Support chat is still open. Tap 💬 Chat Admin anytime to continue.',reply_markup=main_menu(uid))
    if text=='❌ End Chat':
        support_chats_col.update_one({'user_id':uid},{'$set':{'open':False,'updated_at':time.time()}})
        return raw_bot.send_message(uid,'✅ Chat closed.',reply_markup=main_menu(uid))

    support_chats_col.find_one_and_update(
        {'user_id':uid},
        {'$set':{'username':m.from_user.username,'first_name':m.from_user.first_name,'updated_at':time.time(),'open':True,'last_user_chat':m.chat.id,'last_user_msg':m.message_id},'$inc':{'unread_admin':1}},
        upsert=True,return_document=ReturnDocument.AFTER)
    _support_save_message(uid,'user',m.chat.id,m.message_id)
    if get_cached_config().get('support_chat_notifications',True):
        for adm in get_all_admins():
            try:
                raw_bot.send_message(int(adm['_id']),f"💬 New support message from @{m.from_user.username or 'NoUsername'} ({uid}). Open 💬 Chats to reply.")
            except Exception:
                pass
    raw_bot.send_message(uid,'✅ Sent to admin.')
    _support_send_prompt(uid)


def _support_chats_markup(rows):
    kb=InlineKeyboardMarkup(row_width=1)
    for x in rows:
        unread=int(x.get('unread_admin',0) or 0)
        label=('🔴 ' if unread else '⚪ ')+f"@{x.get('username') or x.get('first_name') or x.get('user_id')}"+(f" • {unread} unread" if unread else '')
        kb.add(InlineKeyboardButton(label[:64],callback_data=f"supportopen|{x.get('user_id')}|0"))
    kb.add(InlineKeyboardButton('🔔 Toggle Chat Notifications',callback_data='supportnotify|toggle'))
    return kb


@bot.message_handler(func=lambda m: (m.text or '').startswith('💬 Chats') and is_admin(m.from_user.id))
def support_chats_admin(m):
    rows=list(support_chats_col.find({}).sort([('unread_admin',-1),('updated_at',-1)]).limit(100))
    raw_bot.send_message(m.from_user.id,f"💬 CHATS\n\n{len(rows)} recent conversation(s).\nTap a username to open the full conversation.",reply_markup=_support_chats_markup(rows))


def _support_show_history(admin_chat_id, uid, page=0, page_size=20):
    row=_support_user_row(uid)
    total=int(support_messages_col.count_documents({'user_id':int(uid)}))
    pages=max(1,(total+page_size-1)//page_size)
    page=max(0,min(int(page),pages-1))
    # Page 0 is the newest page. Fetch newest first, then display that page chronologically.
    docs=list(support_messages_col.find({'user_id':int(uid)}).sort([('created_at',-1),('_id',-1)]).skip(page*page_size).limit(page_size))
    docs.reverse()

    header=(
        f"💬 SUPPORT CHAT\n\n"
        f"Name: {row.get('first_name') or '-'}\n"
        f"Username: @{row.get('username') or 'None'}\n"
        f"User ID: {uid}\n"
        f"Status: {'OPEN' if row.get('open') else 'CLOSED'}\n"
        f"Messages: {total}\n"
        f"History page: {page+1}/{pages}"
    )
    raw_bot.send_message(admin_chat_id,header)
    if not docs:
        raw_bot.send_message(admin_chat_id,'No messages have been saved in this conversation yet.')
    else:
        for d in docs:
            who='👤 USER' if d.get('sender')=='user' else '🛡 ADMIN'
            stamp=datetime.fromtimestamp(float(d.get('created_at') or time.time())).strftime('%Y-%m-%d %H:%M')
            try:
                raw_bot.send_message(admin_chat_id,f'{who} • {stamp}')
                raw_bot.copy_message(admin_chat_id,int(d['source_chat_id']),int(d['source_message_id']))
            except Exception:
                raw_bot.send_message(admin_chat_id,f'{who} • {stamp}\n⚠️ This older Telegram message can no longer be copied.')

    kb=InlineKeyboardMarkup(row_width=2)
    nav=[]
    if page+1<pages:
        nav.append(InlineKeyboardButton('⬅️ Older',callback_data=f'supportopen|{uid}|{page+1}'))
    if page>0:
        nav.append(InlineKeyboardButton('Newer ➡️',callback_data=f'supportopen|{uid}|{page-1}'))
    if nav:
        kb.row(*nav)
    kb.row(InlineKeyboardButton('↩️ Reply',callback_data=f'supportreply|{uid}'),InlineKeyboardButton('🔄 Refresh',callback_data=f'supportopen|{uid}|0'))
    kb.add(InlineKeyboardButton('⬅️ Back to Chats',callback_data='supportlist|open'))
    raw_bot.send_message(admin_chat_id,'Choose an action:',reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data.startswith('supportopen|'))
def support_open_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id,'Admin only',True)
    parts=c.data.split('|')
    uid=int(parts[1]); page=int(parts[2]) if len(parts)>2 and parts[2].isdigit() else 0
    support_chats_col.update_one({'user_id':uid},{'$set':{'unread_admin':0}})
    bot.answer_callback_query(c.id)
    _support_show_history(c.from_user.id,uid,page)


@bot.callback_query_handler(func=lambda c: c.data=='supportlist|open')
def support_list_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id,'Admin only',True)
    rows=list(support_chats_col.find({}).sort([('unread_admin',-1),('updated_at',-1)]).limit(100))
    bot.answer_callback_query(c.id)
    raw_bot.send_message(c.from_user.id,f"💬 CHATS\n\n{len(rows)} recent conversation(s).",reply_markup=_support_chats_markup(rows))


@bot.callback_query_handler(func=lambda c: c.data.startswith('supportreply|'))
def support_reply_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id,'Admin only',True)
    uid=int(c.data.split('|',1)[1])
    msg=raw_bot.send_message(c.from_user.id,f'Send reply to @{_support_user_row(uid).get("username") or uid}. Any copyable Telegram message is supported.')
    bot.register_next_step_handler(msg,lambda m:_support_admin_reply(m,uid))
    bot.answer_callback_query(c.id)


def _support_admin_reply(m,uid):
    try:
        raw_bot.copy_message(int(uid),m.chat.id,m.message_id)
        _support_save_message(uid,'admin',m.chat.id,m.message_id,admin_id=m.from_user.id)
        support_chats_col.update_one({'user_id':int(uid)},{'$set':{'updated_at':time.time(),'last_admin_reply_at':time.time(),'open':True}})
        kb=InlineKeyboardMarkup(row_width=2)
        kb.row(InlineKeyboardButton('💬 Open Chat',callback_data=f'supportopen|{uid}|0'),InlineKeyboardButton('⬅️ Chats',callback_data='supportlist|open'))
        raw_bot.send_message(m.chat.id,'✅ Reply sent.',reply_markup=kb)
    except Exception as exc:
        admin_error(m.from_user.id,exc)


@bot.callback_query_handler(func=lambda c: c.data=='supportnotify|toggle')
def support_notify_toggle(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id,'Admin only',True)
    new=not get_config().get('support_chat_notifications',True)
    set_config('support_chat_notifications',new)
    bot.answer_callback_query(c.id,'Notifications ON' if new else 'Notifications OFF',True)


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


# =========================
# 🔎 SCANNERS MARKETPLACE
# =========================
_scanner_admin_state = {}

def _scanner_rows():
    """Return scanner listings stored by admin."""
    rows = get_config().get("scanner_listings", []) or []
    return [x for x in rows if isinstance(x, dict)]

def _save_scanner_rows(rows):
    set_config("scanner_listings", rows)

def _scanner_visibility_allowed(row, uid):
    # Individual scanner rows are not access-controlled. Visibility belongs
    # only to the main Scanners button. Keep this helper for old stored rows.
    return bool(row.get("active", True))

def _scanner_chat_url(value):
    value = str(value or "").strip()
    if not value:
        return None
    if value.startswith(("https://t.me/", "http://t.me/", "tg://")):
        return value
    if value.startswith("@"):
        return "https://t.me/" + value[1:]
    if re.fullmatch(r"[A-Za-z0-9_]{5,}", value):
        return "https://t.me/" + value
    return None

def _scanner_platforms_for(uid):
    grouped = {}
    for row in _scanner_rows():
        if not _scanner_visibility_allowed(row, uid):
            continue
        platform = str(row.get("platform") or "Other").strip() or "Other"
        grouped.setdefault(platform, []).append(row)
    return grouped

@bot.message_handler(func=lambda m: m.text == "🔎 Scanners")
@force_join_handler
def scanners_user_menu(m):
    uid = m.from_user.id
    if not scanner_main_visible_for(uid):
        return raw_bot.send_message(uid, "🔒 Scanners are not available for your account right now.", reply_markup=main_menu(uid))
    grouped = _scanner_platforms_for(uid)
    if not grouped:
        return raw_bot.send_message(uid, "🔎 SCANNERS\n\nNo scanner services are available right now.", reply_markup=main_menu(uid))
    kb = InlineKeyboardMarkup(row_width=1)
    platforms = list(grouped.keys())
    for i, platform in enumerate(platforms):
        kb.add(InlineKeyboardButton(f"📱 {platform}", callback_data=f"scanplat|{i}"))
    raw_bot.send_message(uid, "🔎 SCANNERS\n━━━━━━━━━━━━━━━━━━━━\nChoose a platform to see available scanners and prices.", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("scanplat|"))
def scanners_platform_cb(c):
    if force_block(c.from_user.id):
        return
    if not scanner_main_visible_for(c.from_user.id):
        return bot.answer_callback_query(c.id, "Scanners are not available for your account", True)
    try:
        idx = int(c.data.split("|",1)[1])
        grouped = _scanner_platforms_for(c.from_user.id)
        platforms = list(grouped.keys())
        if idx < 0 or idx >= len(platforms):
            return bot.answer_callback_query(c.id, "List changed. Open Scanners again.", True)
        platform = platforms[idx]
        rows = grouped[platform]
        kb = InlineKeyboardMarkup(row_width=1)
        for row in rows:
            username = str(row.get("username") or "Scanner").strip()
            price = float(row.get("price", 0) or 0)
            label = f"👤 {username} — ${price:g}"
            url = _scanner_chat_url(row.get("chat_link") or username)
            if url:
                kb.add(InlineKeyboardButton(label, url=url))
        kb.add(InlineKeyboardButton("⬅️ Back", callback_data="scanback"))
        raw_bot.edit_message_text(
            f"🔎 {platform} SCANNERS\n━━━━━━━━━━━━━━━━━━━━\nTap a scanner to open Telegram chat and get your scan.",
            c.from_user.id, c.message.message_id, reply_markup=kb
        )
        bot.answer_callback_query(c.id)
    except Exception as exc:
        bot.answer_callback_query(c.id, "Unable to open scanners", True)
        log_event("scanner_platform_error", c.from_user.id, details={"error": str(exc)}, level="error")

@bot.callback_query_handler(func=lambda c: c.data == "scanback")
def scanners_back_cb(c):
    if not scanner_main_visible_for(c.from_user.id):
        return bot.answer_callback_query(c.id, "Scanners are not available for your account", True)
    grouped = _scanner_platforms_for(c.from_user.id)
    kb = InlineKeyboardMarkup(row_width=1)
    for i, platform in enumerate(grouped.keys()):
        kb.add(InlineKeyboardButton(f"📱 {platform}", callback_data=f"scanplat|{i}"))
    raw_bot.edit_message_text("🔎 SCANNERS\n━━━━━━━━━━━━━━━━━━━━\nChoose a platform:", c.from_user.id, c.message.message_id, reply_markup=kb)
    bot.answer_callback_query(c.id)


def _scanner_admin_keyboard():
    rows = _scanner_rows()
    kb = InlineKeyboardMarkup(row_width=1)
    vis = get_scanner_main_visibility()
    vis_label = {"all": "🌍 All Users", "vip": "👑 VIP Only", "hidden": "🙈 Hidden"}.get(vis, "🌍 All Users")
    kb.add(InlineKeyboardButton(f"👁 Scanners Visibility: {vis_label}", callback_data="scanmainvis|menu"))
    kb.add(InlineKeyboardButton("➕ Add Scanner", callback_data="scanadm|add"))
    if rows:
        kb.add(InlineKeyboardButton("📋 Manage Scanners", callback_data="scanadm|manage"))
    return kb

@bot.message_handler(func=lambda m: m.text == "🔎 Scanner Manager" and is_admin(m.from_user.id))
def scanner_admin_menu(m):
    rows = _scanner_rows()
    raw_bot.send_message(m.from_user.id, f"🔎 SCANNER MANAGER\n━━━━━━━━━━━━━━━━━━━━\nListings: {len(rows)}\n\nAdd scanner profiles with platform, username/chat and price. Visibility is controlled for the main Scanners button.", reply_markup=_scanner_admin_keyboard())

@bot.callback_query_handler(func=lambda c: c.data == "scanmainvis|menu")
def scanner_main_visibility_menu_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    current = get_scanner_main_visibility()
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(("✅ " if current == "all" else "") + "🌍 Visible to All Users", callback_data="scanmainvis|set|all"))
    kb.add(InlineKeyboardButton(("✅ " if current == "vip" else "") + "👑 VIP Users Only", callback_data="scanmainvis|set|vip"))
    kb.add(InlineKeyboardButton(("✅ " if current == "hidden" else "") + "🙈 Hidden from Users", callback_data="scanmainvis|set|hidden"))
    kb.add(InlineKeyboardButton("⬅️ Back", callback_data="scanmainvis|back"))
    raw_bot.edit_message_text(
        "👁 SCANNERS MAIN BUTTON VISIBILITY\n\nChoose who can see and access the main 🔎 Scanners button. Individual scanner listings use this same access level.",
        c.from_user.id, c.message.message_id, reply_markup=kb
    )
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("scanmainvis|set|"))
def scanner_main_visibility_set_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    vis = c.data.rsplit("|", 1)[-1]
    if vis not in {"all", "vip", "hidden"}:
        return bot.answer_callback_query(c.id, "Invalid visibility", True)
    set_config("scanner_main_visibility", vis)
    bot.answer_callback_query(c.id, "Scanners visibility updated", True)
    c.data = "scanmainvis|menu"
    return scanner_main_visibility_menu_cb(c)

@bot.callback_query_handler(func=lambda c: c.data == "scanmainvis|back")
def scanner_main_visibility_back_cb(c):
    if not is_admin(c.from_user.id):
        return bot.answer_callback_query(c.id, "Admins only", True)
    rows = _scanner_rows()
    raw_bot.edit_message_text(
        f"🔎 SCANNER MANAGER\n━━━━━━━━━━━━━━━━━━━━\nListings: {len(rows)}\n\nAdd scanner profiles with platform, username/chat and price. Visibility is controlled for the main Scanners button.",
        c.from_user.id, c.message.message_id, reply_markup=_scanner_admin_keyboard()
    )
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data == "scanadm|add")
def scanner_admin_add_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id, "Admins only", True)
    _scanner_admin_state[c.from_user.id] = {}
    msg = raw_bot.send_message(c.from_user.id, "➕ ADD SCANNER\n\nStep 1/4: Send the platform name.\nExample: Telegram, Instagram, TikTok, Facebook")
    bot.register_next_step_handler(msg, scanner_add_platform_step)
    bot.answer_callback_query(c.id)

def scanner_add_platform_step(m):
    if not is_admin(m.from_user.id): return
    state = _scanner_admin_state.setdefault(m.from_user.id,{})
    platform = str(m.text or "").strip()
    if not platform:
        msg=raw_bot.send_message(m.chat.id,"❌ Platform cannot be empty. Send platform name."); bot.register_next_step_handler(msg,scanner_add_platform_step); return
    state["platform"] = platform[:60]
    msg=raw_bot.send_message(m.chat.id,"Step 2/4: Send scanner username.\nExample: @scannername")
    bot.register_next_step_handler(msg,scanner_add_username_step)

def scanner_add_username_step(m):
    state = _scanner_admin_state.get(m.from_user.id)
    if not state: return raw_bot.send_message(m.chat.id,"Session expired.",reply_markup=admin_menu())
    username=str(m.text or "").strip()
    if not username:
        msg=raw_bot.send_message(m.chat.id,"❌ Username cannot be empty."); bot.register_next_step_handler(msg,scanner_add_username_step); return
    state["username"] = username[:80]
    msg=raw_bot.send_message(m.chat.id,"Step 3/4: Send Telegram public username/link users should open to chat.\nExample: @scannername or https://t.me/scannername")
    bot.register_next_step_handler(msg,scanner_add_link_step)

def scanner_add_link_step(m):
    state=_scanner_admin_state.get(m.from_user.id)
    if not state: return raw_bot.send_message(m.chat.id,"Session expired.",reply_markup=admin_menu())
    link=str(m.text or "").strip()
    if not _scanner_chat_url(link):
        msg=raw_bot.send_message(m.chat.id,"❌ Send a valid public Telegram @username or t.me link."); bot.register_next_step_handler(msg,scanner_add_link_step); return
    state["chat_link"] = link
    msg=raw_bot.send_message(m.chat.id,"Step 4/4: Send scan price in USD/USDT.\nExample: 5 or 2.5")
    bot.register_next_step_handler(msg,scanner_add_price_step)

def scanner_add_price_step(m):
    state=_scanner_admin_state.get(m.from_user.id)
    if not state: return raw_bot.send_message(m.chat.id,"Session expired.",reply_markup=admin_menu())
    try:
        price=float(str(m.text or "").replace("$","").strip())
        if price < 0: raise ValueError()
    except Exception:
        msg=raw_bot.send_message(m.chat.id,"❌ Invalid price. Send a number such as 5 or 2.5."); bot.register_next_step_handler(msg,scanner_add_price_step); return
    state["price"] = price
    state = _scanner_admin_state.pop(m.from_user.id, None)
    if not state:
        return raw_bot.send_message(m.chat.id, "Session expired.", reply_markup=admin_menu())
    row={
        "id": ''.join(random.choices(string.ascii_lowercase+string.digits,k=8)),
        "platform": state["platform"], "username": state["username"], "chat_link": state["chat_link"],
        "price": float(state["price"]), "active": True,
        "created_at": time.time(), "created_by": int(m.from_user.id)
    }
    rows=_scanner_rows(); rows.append(row); _save_scanner_rows(rows)
    raw_bot.send_message(m.chat.id,f"✅ SCANNER ADDED\n\nPlatform: {row['platform']}\nScanner: {row['username']}\nPrice: ${row['price']:g}",reply_markup=admin_menu())

@bot.callback_query_handler(func=lambda c: c.data == "scanadm|manage")
def scanner_manage_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admins only",True)
    rows=_scanner_rows(); kb=InlineKeyboardMarkup(row_width=1)
    for i,row in enumerate(rows):
        kb.add(InlineKeyboardButton(f"🔎 {row.get('platform')} • {row.get('username')} • ${float(row.get('price',0) or 0):g}",callback_data=f"scanedit|{i}"))
    raw_bot.edit_message_text("📋 MANAGE SCANNERS\n\nChoose a scanner:",c.from_user.id,c.message.message_id,reply_markup=kb)
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("scanedit|"))
def scanner_edit_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admins only",True)
    try:
        idx=int(c.data.split("|",1)[1]); rows=_scanner_rows(); row=rows[idx]
    except Exception: return bot.answer_callback_query(c.id,"Scanner not found",True)
    kb=InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🗑 Delete Scanner",callback_data=f"scandel|{idx}"))
    kb.add(InlineKeyboardButton("⬅️ Back",callback_data="scanadm|manage"))
    raw_bot.edit_message_text(f"🔎 SCANNER\n\nPlatform: {row.get('platform')}\nUsername: {row.get('username')}\nPrice: ${float(row.get('price',0) or 0):g}\nChat: {row.get('chat_link')}",c.from_user.id,c.message.message_id,reply_markup=kb)
    bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("scandel|"))
def scanner_delete_cb(c):
    if not is_admin(c.from_user.id): return bot.answer_callback_query(c.id,"Admins only",True)
    try:
        idx=int(c.data.split("|",1)[1]); rows=_scanner_rows(); row=rows.pop(idx); _save_scanner_rows(rows)
        bot.answer_callback_query(c.id,"Deleted",True)
        raw_bot.edit_message_text(f"✅ Scanner deleted: {row.get('username')}",c.from_user.id,c.message.message_id,reply_markup=_scanner_admin_keyboard())
    except Exception:
        bot.answer_callback_query(c.id,"Scanner not found",True)



# ============================================================
# 🚀 GROWTH / RETENTION / SUPPORT SUITE
# ============================================================
_FEATURE_FLOW = {}

def _fmt_money(v):
    try: return f"${float(v):g}"
    except Exception: return "$0"

def _utc_local(ts=None):
    return datetime.fromtimestamp(float(ts or time.time())).strftime("%Y-%m-%d %H:%M")

def _active_deals(target_type=None, target_id=None):
    now=time.time(); q={"active":True,"starts_at":{"$lte":now},"expires_at":{"$gt":now}}
    if target_type: q["target_type"]=target_type
    rows=list(deals_col.find(q).sort("expires_at",1))
    if target_id is not None:
        rows=[r for r in rows if str(r.get("target_id") or "all").lower() in ("all",str(target_id).lower())]
    return rows

def _active_vip_deal_discount(code):
    rows=_active_deals("vip",code)
    return max([float(x.get("discount_percent",0) or 0) for x in rows] or [0.0])

def _active_product_deal_discount(pid):
    rows=_active_deals("product",pid)
    return max([float(x.get("discount_percent",0) or 0) for x in rows] or [0.0])

def _active_coupon_discount(uid, target_type, target_id=None):
    u=users_col.find_one({"_id":str(uid)},{"active_coupon":1}) or {}
    code=str(u.get("active_coupon") or "").upper()
    if not code: return 0.0
    row=coupons_col.find_one({"code":code,"active":True})
    if not row: return 0.0
    now=time.time()
    if row.get("expires_at") and float(row["expires_at"])<=now: return 0.0
    if int(row.get("max_uses",0) or 0)>0 and int(row.get("used_count",0) or 0)>=int(row.get("max_uses")): return 0.0
    typ=str(row.get("target_type") or "all")
    if typ not in ("all",target_type): return 0.0
    tid=str(row.get("target_id") or "all")
    if tid.lower() not in ("all",str(target_id or "").lower()): return 0.0
    return max(0.0,min(100.0,float(row.get("discount_percent",0) or 0)))

def _consume_coupon(uid):
    u=users_col.find_one({"_id":str(uid)},{"active_coupon":1}) or {}; code=u.get("active_coupon")
    if not code: return
    coupons_col.update_one({"code":str(code).upper()},{"$inc":{"used_count":1},"$addToSet":{"used_by":int(uid)}})
    users_col.update_one({"_id":str(uid)},{"$unset":{"active_coupon":""}})

# 1) Daily reward
@bot.message_handler(func=lambda m: m.text == "🎁 Daily Reward")
@force_join_handler
def daily_reward_button(m):
    uid=int(m.from_user.id); now=time.time(); row=daily_rewards_col.find_one({"user_id":uid}) or {}
    last=float(row.get("last_claim",0) or 0)
    if now-last < 86400:
        left=int(86400-(now-last)); h=left//3600; mi=(left%3600)//60
        return raw_bot.send_message(uid,f"🎁 DAILY REWARD\n\nAlready claimed today. Come back in about {h}h {mi}m.\nCurrent streak: {int(row.get('streak',0) or 0)} days.")
    prev_streak=int(row.get("streak",0) or 0)
    streak=prev_streak+1 if now-last<172800 else 1
    base=int(get_cached_config().get("daily_reward_points",5) or 5)
    bonus=10 if streak%7==0 else (50 if streak%30==0 else 0)
    reward=base+bonus
    User(uid).add_points(reward)
    daily_rewards_col.update_one({"user_id":uid},{"$set":{"last_claim":now,"streak":streak},"$inc":{"total_claimed":reward}},upsert=True)
    raw_bot.send_message(uid,f"🎁 DAILY REWARD CLAIMED\n\n+{reward} points\n🔥 Streak: {streak} day(s)\n💰 Balance: {User(uid).points()} points"+(f"\n🎉 Streak bonus: +{bonus}" if bonus else ""))

# 2) Deals
@bot.message_handler(func=lambda m: m.text == "🔥 Deals")
def user_deals(m):
    rows=_active_deals()
    if not rows: return raw_bot.send_message(m.chat.id,"🔥 LIMITED-TIME DEALS\n\nNo active deals right now. Check again later.")
    kb=InlineKeyboardMarkup(row_width=1); lines=["🔥 LIMITED-TIME DEALS\n"]
    for r in rows[:20]:
        title=r.get("title") or "Special Deal"; pct=float(r.get("discount_percent",0) or 0); exp=_utc_local(r.get("expires_at"))
        lines.append(f"• {title} — {pct:g}% OFF — until {exp}")
        if r.get("target_type")=="vip": kb.add(InlineKeyboardButton(f"👑 {title}",callback_data="get_vip"))
        elif r.get("target_type")=="product" and str(r.get("target_id"))!="all": kb.add(InlineKeyboardButton(f"🛍 {title}",callback_data=f"shopview|{r.get('target_id')}"))
    raw_bot.send_message(m.chat.id,"\n".join(lines),reply_markup=kb if kb.keyboard else None)

@bot.message_handler(func=lambda m: m.text == "🔥 Deal Manager" and is_admin(m.from_user.id))
def deal_manager(m):
    kb=InlineKeyboardMarkup(row_width=2); kb.add(InlineKeyboardButton("➕ Add Deal",callback_data="dealadm|add"),InlineKeyboardButton("📋 Active Deals",callback_data="dealadm|list")); kb.add(InlineKeyboardButton("🗑 Clear Expired",callback_data="dealadm|clean"))
    raw_bot.send_message(m.chat.id,"🔥 DEAL MANAGER\n\nCreate time-limited VIP or product discounts.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("dealadm|") and is_admin(c.from_user.id))
def deal_admin_cb(c):
    act=c.data.split("|",1)[1]; bot.answer_callback_query(c.id)
    if act=="add":
        _FEATURE_FLOW[c.from_user.id]={"type":"deal"}; msg=raw_bot.send_message(c.from_user.id,"Send deal as:\nTITLE | vip/product | TARGET(all, plan code, or product id) | DISCOUNT% | HOURS\n\nExample:\nWeekend VIP | vip | all | 20 | 6")
        return bot.register_next_step_handler(msg,deal_add_step)
    if act=="clean":
        n=deals_col.delete_many({"expires_at":{"$lte":time.time()}}).deleted_count; return raw_bot.send_message(c.from_user.id,f"✅ Removed {n} expired deal(s).",reply_markup=admin_menu())
    rows=_active_deals(); text="🔥 ACTIVE DEALS\n\n"+"\n".join(f"• {r.get('title')} | {r.get('target_type')}:{r.get('target_id')} | {r.get('discount_percent')}% | {_utc_local(r.get('expires_at'))}" for r in rows[:50]) if rows else "No active deals."
    raw_bot.send_message(c.from_user.id,text,reply_markup=admin_menu())

def deal_add_step(m):
    try:
        title,typ,target,pct,hours=[x.strip() for x in (m.text or "").split("|",4)]; pct=float(pct); hours=float(hours)
        if typ not in ("vip","product") or not(0<pct<=100) or hours<=0: raise ValueError()
        deals_col.insert_one({"title":title[:80],"target_type":typ,"target_id":target,"discount_percent":pct,"starts_at":time.time(),"expires_at":time.time()+hours*3600,"active":True,"created_by":m.from_user.id,"created_at":time.time()})
        raw_bot.send_message(m.chat.id,"✅ Limited-time deal created.",reply_markup=admin_menu())
    except Exception: raw_bot.send_message(m.chat.id,"❌ Invalid format. Open Deal Manager and try again.",reply_markup=admin_menu())

# 3) Coupons
@bot.message_handler(func=lambda m: m.text == "🎟 Coupon")
def coupon_button(m):
    msg=raw_bot.send_message(m.chat.id,"🎟 COUPON\n\nSend your coupon code to activate it for your next eligible purchase.")
    bot.register_next_step_handler(msg,coupon_redeem_step)

def coupon_redeem_step(m):
    code=(m.text or "").strip().upper(); row=coupons_col.find_one({"code":code,"active":True})
    if not row: return raw_bot.send_message(m.chat.id,"❌ Invalid or inactive coupon.",reply_markup=main_menu(m.from_user.id))
    if row.get("expires_at") and float(row["expires_at"])<=time.time(): return raw_bot.send_message(m.chat.id,"❌ This coupon has expired.",reply_markup=main_menu(m.from_user.id))
    if int(row.get("max_uses",0) or 0)>0 and int(row.get("used_count",0) or 0)>=int(row.get("max_uses")): return raw_bot.send_message(m.chat.id,"❌ This coupon has reached its usage limit.",reply_markup=main_menu(m.from_user.id))
    if int(m.from_user.id) in (row.get("used_by") or []): return raw_bot.send_message(m.chat.id,"❌ You already used this coupon.",reply_markup=main_menu(m.from_user.id))
    users_col.update_one({"_id":str(m.from_user.id)},{"$set":{"active_coupon":code}},upsert=True)
    raw_bot.send_message(m.chat.id,f"✅ COUPON ACTIVATED\n\nCode: {code}\nDiscount: {float(row.get('discount_percent',0)):g}%\nIt will apply to your next eligible purchase.",reply_markup=main_menu(m.from_user.id))

@bot.message_handler(func=lambda m: m.text == "🎟 Coupon Manager" and is_admin(m.from_user.id))
def coupon_manager(m):
    kb=InlineKeyboardMarkup(row_width=2); kb.add(InlineKeyboardButton("➕ Add Coupon",callback_data="couponadm|add"),InlineKeyboardButton("📋 Coupons",callback_data="couponadm|list")); raw_bot.send_message(m.chat.id,"🎟 COUPON MANAGER",reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("couponadm|") and is_admin(c.from_user.id))
def coupon_admin_cb(c):
    act=c.data.split("|",1)[1]; bot.answer_callback_query(c.id)
    if act=="add":
        msg=raw_bot.send_message(c.from_user.id,"Send:\nCODE | all/vip/product | TARGET(all/plan/product-id) | DISCOUNT% | MAX USES(0=unlimited) | DAYS\nExample: VIP20 | vip | all | 20 | 100 | 7")
        return bot.register_next_step_handler(msg,coupon_add_step)
    rows=list(coupons_col.find({}).sort("created_at",-1).limit(50)); text="🎟 COUPONS\n\n"+"\n".join(f"• {r.get('code')} — {r.get('discount_percent')}% — {r.get('target_type')}:{r.get('target_id')} — uses {r.get('used_count',0)}/{r.get('max_uses',0) or '∞'}" for r in rows) if rows else "No coupons yet."
    raw_bot.send_message(c.from_user.id,text,reply_markup=admin_menu())

def coupon_add_step(m):
    try:
        code,typ,target,pct,maxuses,days=[x.strip() for x in (m.text or "").split("|",5)]; pct=float(pct); maxuses=int(maxuses); days=float(days)
        if typ not in ("all","vip","product") or not(0<pct<=100) or maxuses<0 or days<=0: raise ValueError()
        coupons_col.update_one({"code":code.upper()},{"$set":{"code":code.upper(),"target_type":typ,"target_id":target,"discount_percent":pct,"max_uses":maxuses,"expires_at":time.time()+days*86400,"active":True,"created_by":m.from_user.id,"created_at":time.time()},"$setOnInsert":{"used_count":0,"used_by":[]}},upsert=True)
        raw_bot.send_message(m.chat.id,"✅ Coupon saved.",reply_markup=admin_menu())
    except Exception: raw_bot.send_message(m.chat.id,"❌ Invalid coupon format.",reply_markup=admin_menu())

# 4) Referral leaderboard
@bot.message_handler(func=lambda m: m.text == "🏆 Ref Leaderboard")
def ref_leaderboard_user(m):
    rows=list(users_col.find({"refs":{"$gt":0}},{"username":1,"first_name":1,"refs":1,"referral_total_earned_usdt":1}).sort("refs",-1).limit(10)); lines=["🏆 REFERRAL LEADERBOARD\n"]
    for i,r in enumerate(rows,1):
        nm=("@"+r.get("username")) if r.get("username") else (r.get("first_name") or "Member"); lines.append(f"{i}. {nm} — {int(r.get('refs',0))} referrals — ${float(r.get('referral_total_earned_usdt',0) or 0):g}")
    raw_bot.send_message(m.chat.id,"\n".join(lines) if rows else "🏆 No referral activity yet.")

# 5) VIP expiry reminders handled by periodic worker below.

def _vip_expiry_reminders():
    now=time.time(); windows=[(7,"7d"),(3,"3d"),(1,"1d")]
    for sub in subscriptions_col.find({"status":"active","expires_at":{"$gt":now}}):
        left=float(sub.get("expires_at",0))-now; uid=int(sub.get("user_id"))
        for days,key in windows:
            if (days-0.05)*86400 <= left <= days*86400:
                marker=f"vip_expiry_notice_{key}_{str(sub.get('_id'))}"
                u=users_col.find_one({"_id":str(uid)},{marker:1}) or {}
                if not u.get(marker):
                    kb=InlineKeyboardMarkup(); kb.add(InlineKeyboardButton("🔄 Renew VIP",callback_data="get_vip"))
                    try: raw_bot.send_message(uid,f"⏳ VIP EXPIRY REMINDER\n\nYour {sub.get('plan')} access expires in about {days} day(s). Renew now to keep uninterrupted access.",reply_markup=kb)
                    except Exception: pass
                    users_col.update_one({"_id":str(uid)},{"$set":{marker:True}})

# 6) Favorites + 9) ratings/reviews
@bot.callback_query_handler(func=lambda c: c.data.startswith("favmethod|"))
def favorite_method_cb(c):
    fid=c.data.split("|",1)[1]; uid=int(c.from_user.id); row=method_favorites_col.find_one({"user_id":uid,"folder_id":fid})
    if row: method_favorites_col.delete_one({"_id":row["_id"]}); bot.answer_callback_query(c.id,"Removed from favorites")
    else:
        from bson import ObjectId
        f=folders_col.find_one({"_id":ObjectId(fid)})
        if not f: return bot.answer_callback_query(c.id,"Method not found",True)
        method_favorites_col.insert_one({"user_id":uid,"folder_id":fid,"name":f.get("name"),"cat":f.get("cat"),"created_at":time.time()}); bot.answer_callback_query(c.id,"Saved to favorites")

@bot.message_handler(func=lambda m: m.text == "❤️ Favorites")
def favorites_button(m):
    rows=list(method_favorites_col.find({"user_id":int(m.from_user.id)}).sort("created_at",-1).limit(50)); kb=InlineKeyboardMarkup(row_width=1)
    for r in rows: kb.add(InlineKeyboardButton(f"❤️ {r.get('name')}",callback_data=f"openid|{r.get('folder_id')}"))
    raw_bot.send_message(m.chat.id,f"❤️ FAVORITE METHODS\n\n{len(rows)} saved method(s).",reply_markup=kb if rows else None)

@bot.callback_query_handler(func=lambda c: c.data.startswith("ratemethod|"))
def rate_method_open(c):
    fid=c.data.split("|",1)[1]; kb=InlineKeyboardMarkup(row_width=5)
    for n in range(1,6): kb.add(InlineKeyboardButton("⭐"*n,callback_data=f"ratepick|{fid}|{n}"))
    raw_bot.send_message(c.from_user.id,"⭐ RATE THIS METHOD\n\nChoose 1–5 stars:",reply_markup=kb); bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("ratepick|"))
def rate_method_pick(c):
    _,fid,n=c.data.split("|",2); uid=int(c.from_user.id); n=int(n)
    method_reviews_col.update_one({"user_id":uid,"folder_id":fid},{"$set":{"rating":n,"updated_at":time.time(),"status":"approved"},"$setOnInsert":{"created_at":time.time()}},upsert=True)
    kb=InlineKeyboardMarkup(); kb.add(InlineKeyboardButton("✍️ Add Short Review",callback_data=f"reviewtext|{fid}")); raw_bot.send_message(uid,f"✅ Rated {n}/5.",reply_markup=kb); bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("reviewtext|"))
def review_text_start(c):
    fid=c.data.split("|",1)[1]; _FEATURE_FLOW[c.from_user.id]={"review_fid":fid}; msg=raw_bot.send_message(c.from_user.id,"Send a short review (max 500 characters). It will be sent to admin for moderation."); bot.register_next_step_handler(msg,review_text_step); bot.answer_callback_query(c.id)

def review_text_step(m):
    st=_FEATURE_FLOW.pop(m.from_user.id,{}) ; fid=st.get("review_fid"); text=(m.text or "").strip()[:500]
    if not fid or not text: return raw_bot.send_message(m.chat.id,"❌ Review cancelled.")
    method_reviews_col.update_one({"user_id":int(m.from_user.id),"folder_id":fid},{"$set":{"review":text,"status":"pending","updated_at":time.time()}},upsert=True); raw_bot.send_message(m.chat.id,"✅ Review submitted for approval.")

@bot.message_handler(func=lambda m: m.text == "⭐ Reviews" and is_admin(m.from_user.id))
def admin_reviews(m):
    rows=list(method_reviews_col.find({"review":{"$exists":True}}).sort("updated_at",-1).limit(30)); kb=InlineKeyboardMarkup(row_width=1)
    for r in rows: kb.add(InlineKeyboardButton(f"⭐ {r.get('rating',0)}/5 • {str(r.get('review',''))[:35]}",callback_data=f"reviewadm|{r['_id']}"))
    raw_bot.send_message(m.chat.id,"⭐ METHOD REVIEWS\n\nOpen a review to approve/reject.",reply_markup=kb if rows else None)

@bot.callback_query_handler(func=lambda c: c.data.startswith("reviewadm|") and is_admin(c.from_user.id))
def review_admin_open(c):
    from bson import ObjectId
    r=method_reviews_col.find_one({"_id":ObjectId(c.data.split("|",1)[1])});
    if not r:return bot.answer_callback_query(c.id,"Not found",True)
    kb=InlineKeyboardMarkup(row_width=2); kb.add(InlineKeyboardButton("✅ Approve",callback_data=f"reviewset|{r['_id']}|approved"),InlineKeyboardButton("❌ Reject",callback_data=f"reviewset|{r['_id']}|rejected")); raw_bot.send_message(c.from_user.id,f"⭐ {r.get('rating')}/5\nUser: {r.get('user_id')}\n\n{r.get('review')}",reply_markup=kb);bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("reviewset|") and is_admin(c.from_user.id))
def review_admin_set(c):
    from bson import ObjectId
    _,oid,status=c.data.split("|",2); method_reviews_col.update_one({"_id":ObjectId(oid)},{"$set":{"status":status,"reviewed_by":c.from_user.id,"reviewed_at":time.time()}}); bot.answer_callback_query(c.id,status.title(),True)

# 7) Global user search
@bot.message_handler(func=lambda m: m.text == "🔍 Search All")
def global_search_start(m):
    msg=raw_bot.send_message(m.chat.id,"🔍 SEARCH ALL\n\nSend a keyword to search methods, products and scanners."); bot.register_next_step_handler(msg,global_search_step)

def global_search_step(m):
    q=(m.text or "").strip();
    if not q:return
    rx={"$regex":re.escape(q),"$options":"i"}; kb=InlineKeyboardMarkup(row_width=1); count=0
    for f in folders_col.find({"name":rx}).limit(15): kb.add(InlineKeyboardButton(f"📚 {f.get('name')}",callback_data=f"openid|{f['_id']}")); count+=1
    for p in shop_products_col.find({"name":rx,"active":True}).limit(10): kb.add(InlineKeyboardButton(f"🛍 {p.get('name')}",callback_data=f"shopview|{p['_id']}")); count+=1
    scanners=get_cached_config().get("scanner_listings",[]) or []
    for sc in scanners:
        if q.lower() in (str(sc.get("platform",""))+" "+str(sc.get("username",""))).lower(): kb.add(InlineKeyboardButton(f"🔎 {sc.get('platform')} • {sc.get('username')}",url=sc.get("chat_link"))); count+=1
    raw_bot.send_message(m.chat.id,f"🔍 SEARCH RESULTS\n\n{count} result(s) for: {q}",reply_markup=kb if count else None)

# 8) Method analytics
@bot.message_handler(func=lambda m: m.text == "📈 Method Analytics" and is_admin(m.from_user.id))
def method_analytics(m):
    rows=[]
    for f in folders_col.find({"cat":{"$in":["free","vip"]}}):
        fid=str(f["_id"]); views=sum(int(x.get("views",0) or 0) for x in method_views_col.find({"folder_id":fid},{"views":1})); uniq=method_views_col.count_documents({"folder_id":fid}); favs=method_favorites_col.count_documents({"folder_id":fid}); rev=list(method_reviews_col.find({"folder_id":fid,"status":"approved"},{"rating":1})); avg=(sum(float(x.get("rating",0)) for x in rev)/len(rev)) if rev else 0
        rows.append((views,f.get("name"),uniq,favs,avg))
    rows.sort(reverse=True); lines=["📈 METHOD ANALYTICS\n"]+[f"• {name}: {views} views / {uniq} users / ❤️ {favs} / ⭐ {avg:.1f}" for views,name,uniq,favs,avg in rows[:30]]
    raw_bot.send_message(m.chat.id,"\n".join(lines) if len(lines)>1 else "No method analytics yet.",reply_markup=admin_menu())

# 10) Support tickets
@bot.message_handler(func=lambda m: m.text == "🎫 Support")
def support_ticket_menu(m):
    kb=InlineKeyboardMarkup(row_width=2)
    for key,label in [("payment","💳 Payment"),("method","📚 Method Help"),("product","🛍 Product"),("vip","👑 VIP"),("other","💬 Other")]: kb.add(InlineKeyboardButton(label,callback_data=f"ticketnew|{key}"))
    kb.add(InlineKeyboardButton("📋 My Tickets",callback_data="ticketmine")); raw_bot.send_message(m.chat.id,"🎫 SUPPORT TICKETS\n\nChoose a category:",reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("ticketnew|"))
def ticket_new_cb(c):
    cat=c.data.split("|",1)[1]; _FEATURE_FLOW[c.from_user.id]={"ticket_cat":cat}; msg=raw_bot.send_message(c.from_user.id,"Describe your issue clearly. You can include order IDs or method names."); bot.register_next_step_handler(msg,ticket_new_step); bot.answer_callback_query(c.id)

def ticket_new_step(m):
    cat=_FEATURE_FLOW.pop(m.from_user.id,{}).get("ticket_cat") or "other"; text=(m.text or "").strip()[:2000]
    if not text:return
    doc={"user_id":int(m.from_user.id),"username":m.from_user.username,"category":cat,"message":text,"status":"open","created_at":time.time(),"updated_at":time.time(),"replies":[]}; rid=support_tickets_col.insert_one(doc).inserted_id
    raw_bot.send_message(m.chat.id,f"✅ Ticket #{str(rid)[-8:].upper()} opened. Admin can reply inside the bot.",reply_markup=main_menu(m.from_user.id))

@bot.callback_query_handler(func=lambda c: c.data=="ticketmine")
def ticket_mine(c):
    rows=list(support_tickets_col.find({"user_id":int(c.from_user.id)}).sort("updated_at",-1).limit(20)); text="🎫 MY TICKETS\n\n"+"\n".join(f"• #{str(r['_id'])[-8:].upper()} • {r.get('category')} • {r.get('status')}" for r in rows) if rows else "No tickets yet."; raw_bot.send_message(c.from_user.id,text);bot.answer_callback_query(c.id)

@bot.message_handler(func=lambda m: m.text == "🎫 Tickets" and is_admin(m.from_user.id))
def admin_tickets(m):
    rows=list(support_tickets_col.find({"status":{"$in":["open","answered"]}}).sort("updated_at",-1).limit(50)); kb=InlineKeyboardMarkup(row_width=1)
    for r in rows: kb.add(InlineKeyboardButton(f"🎫 @{r.get('username') or r.get('user_id')} • {r.get('category')} • {r.get('status')}",callback_data=f"ticketadm|{r['_id']}"))
    raw_bot.send_message(m.chat.id,"🎫 SUPPORT TICKETS",reply_markup=kb if rows else None)

@bot.callback_query_handler(func=lambda c: c.data.startswith("ticketadm|") and is_admin(c.from_user.id))
def ticket_admin_open(c):
    from bson import ObjectId
    r=support_tickets_col.find_one({"_id":ObjectId(c.data.split("|",1)[1])});
    if not r:return bot.answer_callback_query(c.id,"Not found",True)
    history="\n".join(f"{x.get('by','Admin')}: {x.get('text')}" for x in (r.get('replies') or [])[-10:]); kb=InlineKeyboardMarkup(row_width=2); kb.add(InlineKeyboardButton("↩️ Reply",callback_data=f"ticketreply|{r['_id']}"),InlineKeyboardButton("✅ Close",callback_data=f"ticketclose|{r['_id']}")); raw_bot.send_message(c.from_user.id,f"🎫 TICKET #{str(r['_id'])[-8:].upper()}\nUser: @{r.get('username') or 'None'} ({r.get('user_id')})\nCategory: {r.get('category')}\nStatus: {r.get('status')}\n\n{r.get('message')}\n\n{history}",reply_markup=kb);bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("ticketreply|") and is_admin(c.from_user.id))
def ticket_reply_start(c):
    oid=c.data.split("|",1)[1]; _FEATURE_FLOW[c.from_user.id]={"ticket_reply":oid}; msg=raw_bot.send_message(c.from_user.id,"Send your reply to the user:");bot.register_next_step_handler(msg,ticket_reply_step);bot.answer_callback_query(c.id)

def ticket_reply_step(m):
    from bson import ObjectId
    oid=_FEATURE_FLOW.pop(m.from_user.id,{}).get("ticket_reply");
    if not oid:return
    r=support_tickets_col.find_one({"_id":ObjectId(oid)}); text=(m.text or "").strip()[:2000]
    if not r or not text:return
    support_tickets_col.update_one({"_id":r["_id"]},{"$push":{"replies":{"by":"Admin","text":text,"at":time.time()}},"$set":{"status":"answered","updated_at":time.time()}})
    try: raw_bot.send_message(int(r["user_id"]),f"🎫 SUPPORT REPLY\n\nTicket #{str(r['_id'])[-8:].upper()}\n\n{text}")
    except Exception: pass
    raw_bot.send_message(m.chat.id,"✅ Reply sent.",reply_markup=admin_menu())

@bot.callback_query_handler(func=lambda c: c.data.startswith("ticketclose|") and is_admin(c.from_user.id))
def ticket_close(c):
    from bson import ObjectId
    oid=ObjectId(c.data.split("|",1)[1]);support_tickets_col.update_one({"_id":oid},{"$set":{"status":"closed","updated_at":time.time(),"closed_by":c.from_user.id}});bot.answer_callback_query(c.id,"Closed",True)

# 11) Smart Broadcast
@bot.message_handler(func=lambda m: m.text == "📢 Smart Broadcast" and is_admin(m.from_user.id))
def smart_broadcast_menu(m):
    kb=InlineKeyboardMarkup(row_width=2)
    for k,l in [("all","👥 All"),("vip","👑 VIP"),("free","🆓 Free"),("expiring","⏳ Expiring VIP"),("buyers","🛍 Buyers"),("nonbuyers","🌱 Non-buyers"),("active","⚡ Active 7d")]: kb.add(InlineKeyboardButton(l,callback_data=f"smartbc|{k}"))
    raw_bot.send_message(m.chat.id,"📢 SMART BROADCAST\n\nChoose audience:",reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("smartbc|") and is_admin(c.from_user.id))
def smart_broadcast_target(c):
    target=c.data.split("|",1)[1];_FEATURE_FLOW[c.from_user.id]={"smart_target":target};msg=raw_bot.send_message(c.from_user.id,"Send the message you want to broadcast. Text, photo, video, document and other copyable messages are supported.");bot.register_next_step_handler(msg,smart_broadcast_send);bot.answer_callback_query(c.id)

def _smart_user_ids(target):
    now=time.time()
    if target=="vip": q={"vip":True}
    elif target=="free": q={"vip":{"$ne":True}}
    elif target=="active": q={"last_active":{"$gte":now-7*86400}}
    else:q={}
    ids=[int(x["_id"]) for x in users_col.find(q,{"_id":1})]
    if target=="expiring": ids=[u for u in ids if (lambda s: s and 0<float(s.get('expires_at',0))-now<=7*86400)(_active_subscription(u))]
    if target=="buyers": ids=[u for u in ids if payments_col.find_one({"user_id":u,"status":{"$in":["paid","approved"]}})]
    if target=="nonbuyers": ids=[u for u in ids if not payments_col.find_one({"user_id":u,"status":{"$in":["paid","approved"]}})]
    return ids

def smart_broadcast_send(m):
    target=_FEATURE_FLOW.pop(m.from_user.id,{}).get("smart_target") or "all"
    _FEATURE_FLOW[m.from_user.id]={"smart_preview":{"target":target,"source_chat":int(m.chat.id),"source_message":int(m.message_id)}}
    raw_bot.send_message(m.chat.id,"👀 SMART BROADCAST PREVIEW\n\nThe message below is exactly what users will receive:")
    try: raw_bot.copy_message(m.chat.id,m.chat.id,m.message_id)
    except Exception: pass
    kb=InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("✅ Send Now",callback_data="smartbcsendnow"),InlineKeyboardButton("🕒 Schedule",callback_data="smartbcschedule"))
    kb.add(InlineKeyboardButton("❌ Cancel",callback_data="smartbccancel"))
    raw_bot.send_message(m.chat.id,f"Audience: {target}\nEstimated recipients: {len(_smart_user_ids(target))}",reply_markup=kb)

def _execute_smart_broadcast(target, source_chat, source_message, created_by=None):
    sent=fail=0
    for uid in _smart_user_ids(target):
        try: raw_bot.copy_message(uid,int(source_chat),int(source_message)); sent+=1
        except Exception: fail+=1
    return sent,fail

@bot.callback_query_handler(func=lambda c: c.data in ("smartbcsendnow","smartbcschedule","smartbccancel") and is_admin(c.from_user.id))
def smart_broadcast_preview_action(c):
    st=_FEATURE_FLOW.get(c.from_user.id,{}).get("smart_preview")
    if c.data=="smartbccancel":
        _FEATURE_FLOW.pop(c.from_user.id,None); bot.answer_callback_query(c.id,"Cancelled"); return raw_bot.send_message(c.from_user.id,"❌ Smart broadcast cancelled.",reply_markup=admin_menu())
    if not st:return bot.answer_callback_query(c.id,"Preview expired",True)
    if c.data=="smartbcschedule":
        msg=raw_bot.send_message(c.from_user.id,"Send delay in minutes (for example 30, 120, 1440).")
        bot.register_next_step_handler(msg,smart_broadcast_schedule_step); return bot.answer_callback_query(c.id,"Send delay")
    sent,fail=_execute_smart_broadcast(st["target"],st["source_chat"],st["source_message"],c.from_user.id)
    smart_broadcasts_col.insert_one({"target":st["target"],"source_chat":st["source_chat"],"source_message":st["source_message"],"status":"sent","sent":sent,"failed":fail,"created_by":c.from_user.id,"created_at":time.time(),"sent_at":time.time()})
    _FEATURE_FLOW.pop(c.from_user.id,None); bot.answer_callback_query(c.id,"Sent",True); raw_bot.send_message(c.from_user.id,f"✅ Smart broadcast complete.\nSent: {sent}\nFailed: {fail}",reply_markup=admin_menu())

def smart_broadcast_schedule_step(m):
    st=_FEATURE_FLOW.pop(m.from_user.id,{}).get("smart_preview")
    if not st:return raw_bot.send_message(m.chat.id,"❌ Preview expired.",reply_markup=admin_menu())
    try:
        minutes=float((m.text or "").strip());
        if minutes<1:raise ValueError()
    except Exception:return raw_bot.send_message(m.chat.id,"❌ Invalid delay. Scheduling cancelled.",reply_markup=admin_menu())
    run_at=time.time()+minutes*60
    smart_broadcasts_col.insert_one({"target":st["target"],"source_chat":st["source_chat"],"source_message":st["source_message"],"status":"scheduled","run_at":run_at,"created_by":m.from_user.id,"created_at":time.time()})
    raw_bot.send_message(m.chat.id,f"✅ Smart broadcast scheduled for {_utc_local(run_at)}.",reply_markup=admin_menu())

# 12) Anti-abuse risk panel
@bot.message_handler(func=lambda m: m.text == "🛡 Risk Panel" and is_admin(m.from_user.id))
def risk_panel(m):
    cutoff=time.time()-7*86400; risks=[]
    for u in users_col.find({}, {"_id":1,"username":1,"submissions_blocked":1}):
        uid=int(u["_id"]); pending=payments_col.count_documents({"user_id":uid,"status":"pending","created_at":{"$gte":cutoff}}); rejected=payments_col.count_documents({"user_id":uid,"status":"rejected","created_at":{"$gte":cutoff}})+wallet_tx_col.count_documents({"user_id":uid,"status":"rejected","created_at":{"$gte":cutoff}}); tickets=support_tickets_col.count_documents({"user_id":uid,"created_at":{"$gte":cutoff}})
        score=rejected*3+pending+tickets
        if score>=3 or u.get("submissions_blocked"): risks.append((score,u,rejected,pending,tickets))
    risks.sort(key=lambda x:x[0],reverse=True); lines=["🛡 RISK PANEL — LAST 7 DAYS\n"]+[f"• @{u.get('username') or u['_id']} • score {score} • rejected {rej} • pending {pen} • tickets {tic} • {'BLOCKED' if u.get('submissions_blocked') else 'watch'}" for score,u,rej,pen,tic in risks[:30]]
    raw_bot.send_message(m.chat.id,"\n".join(lines) if len(lines)>1 else "🛡 No high-risk users detected.",reply_markup=admin_menu())

# 13) Business dashboard
@bot.message_handler(func=lambda m: m.text == "📊 Business Dashboard" and is_admin(m.from_user.id))
def business_dashboard(m):
    now=time.time(); lines=["📊 GLOBEXOMART BUSINESS DASHBOARD\n"]
    for label,days in [("Today",1),("7 Days",7),("30 Days",30)]:
        since=now-days*86400; pay=list(payments_col.find({"status":{"$in":["paid","approved"]},"created_at":{"$gte":since}},{"amount":1,"type":1,"plan":1})); revenue=sum(float(x.get("amount",0) or 0) for x in pay); vip=sum(1 for x in pay if x.get("plan")); products=shop_orders_col.count_documents({"created_at":{"$gte":since},"status":"completed"}); newusers=users_col.count_documents({"created_at":{"$gte":since}}); refs=referrals_col.count_documents({"created_at":{"$gte":since}}) if "referrals_col" in globals() else 0
        lines.append(f"{label}: ${revenue:.2f} revenue • {vip} VIP • {products} products • {newusers} new users • {refs} referrals")
    raw_bot.send_message(m.chat.id,"\n".join(lines),reply_markup=admin_menu())

# 14) Method update alerts

def _notify_users_method_update(action, folder):
    if not get_cached_config().get("user_method_update_alerts",True): return
    cat=str(folder.get("cat") or "");
    if cat not in ("free","vip"): return
    fid=str(folder.get("_id") or ""); name=str(folder.get("name") or "Method"); event={"folder_id":fid,"name":name,"action":str(action),"created_at":time.time()}; method_update_events_col.insert_one(event)
    # Notify VIP users for VIP updates and users who saved/viewed/purchased the method.
    ids=set()
    if cat=="vip": ids.update(int(x["_id"]) for x in users_col.find({"vip":True},{"_id":1}))
    ids.update(int(x["user_id"]) for x in method_favorites_col.find({"folder_id":fid},{"user_id":1}))
    ids.update(int(x["user_id"]) for x in method_views_col.find({"folder_id":fid},{"user_id":1}))
    kb=InlineKeyboardMarkup(); kb.add(InlineKeyboardButton("📚 View Updated Method",callback_data=f"openid|{fid}"))
    for uid in list(ids)[:5000]:
        try: raw_bot.send_message(uid,f"🔔 METHOD UPDATED\n\n{name} has a new update. Open it to see the latest files/instructions.",reply_markup=kb)
        except Exception: pass

@bot.message_handler(func=lambda m: m.text == "🔔 Method Update Alerts" and is_admin(m.from_user.id))
def method_alert_admin(m):
    cur=bool(get_cached_config().get("user_method_update_alerts",True)); kb=InlineKeyboardMarkup(); kb.add(InlineKeyboardButton(f"{'🔕 Disable' if cur else '🔔 Enable'} User Alerts",callback_data="methodalerttoggle")); raw_bot.send_message(m.chat.id,f"🔔 METHOD UPDATE ALERTS\n\nStatus: {'ON' if cur else 'OFF'}",reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data=="methodalerttoggle" and is_admin(c.from_user.id))
def method_alert_toggle(c):
    new=not bool(get_cached_config().get("user_method_update_alerts",True));set_config("user_method_update_alerts",new);bot.answer_callback_query(c.id,f"Alerts {'ON' if new else 'OFF'}",True)

# 15) Beginner roadmap
@bot.message_handler(func=lambda m: m.text == "🎓 Start Here")
def beginner_roadmap(m):
    cfg=get_cached_config(); custom=(cfg.get("beginner_roadmap") or "").strip()
    if custom:return raw_bot.send_message(m.chat.id,"🎓 BEGINNER ROADMAP\n\n"+custom)
    rows=list(folders_col.find({"cat":"free","parent":None}).sort("created_at",1).limit(5)); lines=["🎓 BEGINNER ROADMAP\n","1️⃣ Start with the free methods below and read every instruction before using them.","2️⃣ Use 💬 Chat Admin or 🎫 Support whenever you get stuck.","3️⃣ Join VIP when you want the full library, live guidance and advanced methods.\n"]
    for i,r in enumerate(rows,1): lines.append(f"{i}. {r.get('name')}")
    raw_bot.send_message(m.chat.id,"\n".join(lines))

@bot.message_handler(func=lambda m: m.text == "🎓 Roadmap Setup" and is_admin(m.from_user.id))
def roadmap_setup(m):
    msg=raw_bot.send_message(m.chat.id,"Send the beginner roadmap text. This will replace the automatic starter roadmap.");bot.register_next_step_handler(msg,roadmap_save)

def roadmap_save(m):
    set_config("beginner_roadmap",(m.text or "")[:4000]);raw_bot.send_message(m.chat.id,"✅ Beginner roadmap updated.",reply_markup=admin_menu())

# Periodic maintenance for expiry reminders and stale deals. Uses a separate lightweight worker.
def _growth_worker():
    while True:
        try:
            _vip_expiry_reminders(); deals_col.update_many({"active":True,"expires_at":{"$lte":time.time()}},{"$set":{"active":False,"ended_at":time.time()}})
            globals().get("_abandoned_checkout_worker_once", lambda: None)()
            for row in smart_broadcasts_col.find({"status":"scheduled","run_at":{"$lte":time.time()}}).limit(10):
                try:
                    sent,fail=_execute_smart_broadcast(row.get("target","all"),row.get("source_chat"),row.get("source_message"),row.get("created_by"))
                    smart_broadcasts_col.update_one({"_id":row["_id"]},{"$set":{"status":"sent","sent":sent,"failed":fail,"sent_at":time.time()}})
                except Exception as exc:
                    smart_broadcasts_col.update_one({"_id":row["_id"]},{"$set":{"status":"failed","error":str(exc),"failed_at":time.time()}})
        except Exception as exc: log_event("growth_worker_error",details={"error":str(exc)},level="error")
        time.sleep(1800)
threading.Thread(target=_growth_worker,name="globexomart-growth",daemon=True).start()


# =========================
# 🎯 GROWTH & SALES ENGINE
# Personalized menu, abandoned checkout recovery, funnel analytics,
# campaign tracking and affiliate/KOL attribution.
# =========================
_GROWTH_FLOW = {}

def _growth_event(event, uid, **extra):
    try:
        doc={"event":str(event),"user_id":int(uid),"created_at":time.time()}
        doc.update(extra)
        growth_events_col.insert_one(doc)
    except Exception:
        pass

def _record_checkout_intent(uid, kind, item, amount=0, title=None):
    try:
        now=time.time()
        checkout_intents_col.update_one(
            {"user_id":int(uid),"kind":str(kind),"item":str(item),"status":"open"},
            {"$set":{"amount":float(amount or 0),"title":title,"updated_at":now},"$setOnInsert":{"created_at":now,"reminders_sent":0}},
            upsert=True,
        )
    except Exception:
        pass

def _growth_conversion(uid, kind, item, amount=0):
    try:
        now=time.time()
        checkout_intents_col.update_many({"user_id":int(uid),"kind":str(kind),"status":"open"},{"$set":{"status":"converted","converted_at":now}})
        u=users_col.find_one({"_id":str(uid)},{"acquisition_campaign":1,"affiliate_code":1}) or {}
        doc={"event":"purchase","user_id":int(uid),"kind":str(kind),"item":str(item),"amount":float(amount or 0),"created_at":now}
        if u.get("acquisition_campaign"):doc["campaign"]=u.get("acquisition_campaign")
        if u.get("affiliate_code"):doc["affiliate"]=u.get("affiliate_code")
        growth_events_col.insert_one(doc)
        if u.get("affiliate_code"):
            affiliates_col.update_one({"code":u["affiliate_code"]},{"$inc":{"sales":1,"revenue":float(amount or 0)},"$set":{"last_sale_at":now}})
        if u.get("acquisition_campaign"):
            campaigns_col.update_one({"code":u["acquisition_campaign"]},{"$inc":{"sales":1,"revenue":float(amount or 0)},"$set":{"last_sale_at":now}})
    except Exception as exc:
        try: log_event("growth_conversion_error",uid,details={"error":str(exc)},level="error")
        except Exception: pass

@bot.message_handler(func=lambda m: m.text == "🎯 For You")
@force_join_handler
def personalized_home(m):
    uid=m.from_user.id; user=User(uid); now=time.time()
    paid=payments_col.count_documents({"user_id":int(uid),"status":{"$in":["paid","approved"]}})
    sub=_active_subscription(uid) if "_active_subscription" in globals() else None
    kb=InlineKeyboardMarkup(row_width=1)
    if sub:
        left=max(0,int((float(sub.get("expires_at",now))-now)/86400))
        title=f"👑 Your VIP is active • about {left} day(s) remaining"
        kb.add(InlineKeyboardButton("🔥 Latest VIP Methods",callback_data="catalog|vip"))
        kb.add(InlineKeyboardButton("❤️ My Favorites",callback_data="favorites|open"))
        if left<=7: kb.add(InlineKeyboardButton("🔄 Renew / Upgrade VIP",callback_data="get_vip"))
        text=f"🎯 FOR YOU\n\n{title}\n\nYour best next step is to check recently updated VIP methods and saved content."
    elif paid:
        text="🎯 FOR YOU\n\nWelcome back. You already know GLOBEXOMART — unlock VIP for the full method library, updates, classes and private guidance."
        kb.add(InlineKeyboardButton("💎 View VIP Plans",callback_data="get_vip"))
        kb.add(InlineKeyboardButton("🔥 Current Deals",callback_data="deals|open"))
    else:
        text="🎯 FOR YOU\n\nNew here? Start with free methods, the beginner roadmap and current deals. When you're ready, VIP unlocks the complete library and private guidance."
        kb.add(InlineKeyboardButton("🎓 Start Here",callback_data="roadmap|open"))
        kb.add(InlineKeyboardButton("📚 Free Methods",callback_data="catalog|free"))
        kb.add(InlineKeyboardButton("💎 See VIP",callback_data="get_vip"))
    _growth_event("personalized_home",uid,segment=("vip" if sub else "buyer" if paid else "new_free"))
    raw_bot.send_message(uid,text,reply_markup=kb)


@bot.callback_query_handler(func=lambda c: c.data=="favorites|open")
def growth_open_favorites(c):
    bot.answer_callback_query(c.id)
    fake=type('obj',(object,),{'from_user':c.from_user,'chat':c.message.chat,'text':'❤️ Favorites'})
    favorites_button(fake)

@bot.callback_query_handler(func=lambda c: c.data=="roadmap|open")
def growth_open_roadmap(c):
    bot.answer_callback_query(c.id)
    fake=type('obj',(object,),{'from_user':c.from_user,'chat':c.message.chat,'text':'🎓 Start Here'})
    beginner_roadmap(fake)

@bot.callback_query_handler(func=lambda c: c.data=="deals|open")
def growth_open_deals(c):
    bot.answer_callback_query(c.id)
    fake=type('obj',(object,),{'from_user':c.from_user,'chat':c.message.chat,'text':'🔥 Deals'})
    try: user_deals(fake)
    except Exception: raw_bot.send_message(c.from_user.id,"Open 🔥 Deals from the main menu.")

@bot.callback_query_handler(func=lambda c: c.data=="growthoptout")
def growth_reminder_optout(c):
    users_col.update_one({"_id":str(c.from_user.id)},{"$set":{"checkout_reminders_disabled":True}})
    checkout_intents_col.update_many({"user_id":int(c.from_user.id),"status":"open"},{"$set":{"status":"opted_out"}})
    bot.answer_callback_query(c.id,"Checkout reminders disabled",True)


def _abandoned_checkout_worker_once():
    if not bool(get_cached_config().get("checkout_recovery_enabled",True)): return
    now=time.time()
    for row in checkout_intents_col.find({"status":"open","created_at":{"$lte":now-2*3600},"reminders_sent":{"$lt":2}}).sort("created_at",1).limit(100):
        uid=int(row.get("user_id")); u=users_col.find_one({"_id":str(uid)},{"checkout_reminders_disabled":1}) or {}
        if u.get("checkout_reminders_disabled"):
            checkout_intents_col.update_one({"_id":row["_id"]},{"$set":{"status":"opted_out"}}); continue
        sent=int(row.get("reminders_sent",0) or 0)
        age=now-float(row.get("created_at",now))
        if sent==1 and age<24*3600: continue
        kind=row.get("kind"); amount=float(row.get("amount",0) or 0); title=row.get("title") or row.get("item")
        kb=InlineKeyboardMarkup(row_width=1)
        if kind=="vip": kb.add(InlineKeyboardButton("💎 Continue VIP Purchase",callback_data="get_vip"))
        else: kb.add(InlineKeyboardButton("🛍 Open Products",callback_data="catalog|paid_service"))
        kb.add(InlineKeyboardButton("🔕 Don't remind me",callback_data="growthoptout"))
        text=("🛒 YOU LEFT SOMETHING OPEN\n\n"+f"{title}\n"+(f"Price: ${amount:g} USDT\n" if amount else "")+"\nIf you still want it, you can continue from where you stopped. No pressure — this is only a reminder.")
        try:
            raw_bot.send_message(uid,text,reply_markup=kb)
            checkout_intents_col.update_one({"_id":row["_id"]},{"$inc":{"reminders_sent":1},"$set":{"last_reminder_at":now}})
        except Exception:
            checkout_intents_col.update_one({"_id":row["_id"]},{"$set":{"last_send_failed_at":now}})

@bot.message_handler(func=lambda m: m.text == "🎯 Growth Center" and is_admin(m.from_user.id))
def growth_center_admin(m):
    cfg=get_cached_config(); recovery=bool(cfg.get("checkout_recovery_enabled",True))
    kb=InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton(f"🛒 Checkout Recovery: {'ON' if recovery else 'OFF'}",callback_data="growth|toggle_recovery"))
    kb.add(InlineKeyboardButton("📈 Open Funnel Analytics",callback_data="growth|funnel"))
    kb.add(InlineKeyboardButton("🔗 Campaign Manager",callback_data="growth|campaigns"))
    kb.add(InlineKeyboardButton("🤝 Affiliate Manager",callback_data="growth|affiliates"))
    raw_bot.send_message(m.chat.id,"🎯 GROWTH CENTER\n\nManage conversion tracking, abandoned checkout reminders, campaign attribution and affiliate/KOL links.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("growth|") and is_admin(c.from_user.id))
def growth_center_cb(c):
    action=c.data.split("|",1)[1]
    if action=="toggle_recovery":
        new=not bool(get_cached_config().get("checkout_recovery_enabled",True)); set_config("checkout_recovery_enabled",new); bot.answer_callback_query(c.id,f"Recovery {'ON' if new else 'OFF'}",True); return
    bot.answer_callback_query(c.id)
    fake=type('obj',(object,),{'from_user':c.from_user,'chat':c.message.chat,'text':''})
    if action=="funnel": return funnel_analytics(fake)
    if action=="campaigns": return campaign_manager(fake)
    if action=="affiliates": return affiliate_manager(fake)

@bot.message_handler(func=lambda m: m.text == "📈 Funnel Analytics" and is_admin(m.from_user.id))
def funnel_analytics(m):
    now=time.time(); lines=["📈 SALES FUNNEL ANALYTICS\n"]
    for label,days in [("7 Days",7),("30 Days",30),("All Time",3650)]:
        since=now-days*86400
        starts=growth_events_col.count_documents({"event":"start","created_at":{"$gte":since}})
        vipviews=growth_events_col.count_documents({"event":"vip_view","created_at":{"$gte":since}})
        checkouts=growth_events_col.count_documents({"event":"checkout_started","created_at":{"$gte":since}})
        purchases=list(growth_events_col.find({"event":"purchase","created_at":{"$gte":since}},{"amount":1}))
        buyers=len(purchases); revenue=sum(float(x.get("amount",0) or 0) for x in purchases)
        checkout_rate=(buyers/checkouts*100) if checkouts else 0
        start_rate=(buyers/starts*100) if starts else 0
        lines.append(f"{label}: {starts} starts → {vipviews} VIP views → {checkouts} checkouts → {buyers} purchases\nConversion: {start_rate:.1f}% from tracked starts • {checkout_rate:.1f}% checkout-to-sale • ${revenue:.2f} tracked revenue\n")
    raw_bot.send_message(m.chat.id,"\n".join(lines),reply_markup=admin_menu())

# Campaign tracking
@bot.message_handler(func=lambda m: m.text == "🔗 Campaign Manager" and is_admin(m.from_user.id))
def campaign_manager(m):
    rows=list(campaigns_col.find({}).sort("created_at",-1).limit(25)); kb=InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("➕ Create Campaign",callback_data="camp|new"))
    for r in rows: kb.add(InlineKeyboardButton(f"🔗 {r.get('name')} • {r.get('sales',0)} sales • ${float(r.get('revenue',0) or 0):.2f}",callback_data=f"camp|view|{r.get('code')}"))
    raw_bot.send_message(m.chat.id,"🔗 CAMPAIGN MANAGER\n\nCreate separate tracked bot links for X, Telegram, ads, creators or any traffic source.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("camp|") and is_admin(c.from_user.id))
def campaign_cb(c):
    parts=c.data.split("|"); action=parts[1]
    if action=="new":
        _GROWTH_FLOW[c.from_user.id]={"type":"campaign"}; msg=raw_bot.send_message(c.from_user.id,"Send campaign name. Example: X August Ads"); bot.register_next_step_handler(msg,campaign_name_step); return bot.answer_callback_query(c.id,"Send name")
    if action=="view":
        r=campaigns_col.find_one({"code":parts[2]}); bot.answer_callback_query(c.id)
        if not r:return
        starts=growth_events_col.count_documents({"event":"start","campaign":r["code"]}); sales=growth_events_col.count_documents({"event":"purchase","campaign":r["code"]})
        link=f"https://t.me/globexomartbot?start=camp_{r['code']}"
        return raw_bot.send_message(c.from_user.id,f"🔗 {r.get('name')}\n\nTracked link:\n{link}\n\nStarts: {starts}\nSales: {sales}\nRevenue: ${float(r.get('revenue',0) or 0):.2f}")

def campaign_name_step(m):
    import secrets
    name=(m.text or '').strip()[:80]
    if not name:return raw_bot.send_message(m.chat.id,"❌ Invalid name.",reply_markup=admin_menu())
    code=secrets.token_hex(4)
    campaigns_col.insert_one({"code":code,"name":name,"active":True,"sales":0,"revenue":0.0,"created_by":m.from_user.id,"created_at":time.time()})
    link=f"https://t.me/globexomartbot?start=camp_{code}"
    raw_bot.send_message(m.chat.id,f"✅ Campaign created\n\n{name}\n{link}",reply_markup=admin_menu())

# Affiliate/KOL tracking
@bot.message_handler(func=lambda m: m.text == "🤝 Affiliate Manager" and is_admin(m.from_user.id))
def affiliate_manager(m):
    rows=list(affiliates_col.find({}).sort("created_at",-1).limit(30)); kb=InlineKeyboardMarkup(row_width=1);kb.add(InlineKeyboardButton("➕ Add Affiliate/KOL",callback_data="aff|new"))
    for r in rows: kb.add(InlineKeyboardButton(f"🤝 {r.get('name')} • {r.get('sales',0)} sales • ${float(r.get('revenue',0) or 0):.2f}",callback_data=f"aff|view|{r.get('code')}"))
    raw_bot.send_message(m.chat.id,"🤝 AFFILIATE / KOL MANAGER\n\nCreate tracked links for approved promoters and see their joins, sales and attributed revenue.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data.startswith("aff|") and is_admin(c.from_user.id))
def affiliate_cb(c):
    parts=c.data.split('|'); action=parts[1]
    if action=='new':
        _GROWTH_FLOW[c.from_user.id]={"type":"affiliate"};msg=raw_bot.send_message(c.from_user.id,"Send affiliate Telegram user ID or @username.");bot.register_next_step_handler(msg,affiliate_user_step);return bot.answer_callback_query(c.id,"Send user")
    r=affiliates_col.find_one({"code":parts[2]});bot.answer_callback_query(c.id)
    if not r:return
    starts=growth_events_col.count_documents({"event":"start","affiliate":r['code']});link=f"https://t.me/globexomartbot?start=aff_{r['code']}"
    raw_bot.send_message(c.from_user.id,f"🤝 {r.get('name')}\nUser ID: {r.get('user_id')}\n\nTracked link:\n{link}\n\nStarts: {starts}\nSales: {r.get('sales',0)}\nAttributed revenue: ${float(r.get('revenue',0) or 0):.2f}")

def affiliate_user_step(m):
    import secrets
    raw=(m.text or '').strip(); u=None
    if raw.lstrip('-').isdigit():u=users_col.find_one({'_id':str(int(raw))})
    else:u=users_col.find_one({'username':{'$regex':'^'+re.escape(raw.lstrip('@'))+'$','$options':'i'}})
    if not u:return raw_bot.send_message(m.chat.id,"❌ User not found in bot database.",reply_markup=admin_menu())
    uid=int(u['_id']); name='@'+u['username'] if u.get('username') else (u.get('first_name') or str(uid)); code=secrets.token_hex(4)
    affiliates_col.update_one({'user_id':uid},{'$set':{'code':code,'name':name,'active':True,'updated_at':time.time()},'$setOnInsert':{'created_at':time.time(),'sales':0,'revenue':0.0}},upsert=True)
    link=f"https://t.me/globexomartbot?start=aff_{code}"
    try: raw_bot.send_message(uid,f"🤝 GLOBEXOMART AFFILIATE ACCESS\n\nYour tracked promotion link:\n{link}\n\nSales are attributed when users first join through this link and later complete a tracked purchase.")
    except Exception: pass
    raw_bot.send_message(m.chat.id,f"✅ Affiliate added\n{name}\n{link}",reply_markup=admin_menu())




# =========================
# 🚀 V24 OPERATIONS + RETENTION + COMMERCE SUITE
# =========================
# Added as a self-contained layer so existing features remain unchanged.
cart_col = db["user_carts"]
watchlist_col = db["watchlists"]
stock_watch_col = db["stock_watchers"]
loyalty_col = db["loyalty"]
admin_activity_col = db["admin_activity"]
staff_roles_col = db["staff_roles"]
faq_col = db["faq"]
restore_jobs_col = db["restore_jobs"]

try:
    cart_col.create_index([("user_id",1),("product_id",1)], unique=True)
    watchlist_col.create_index([("user_id",1),("target_type",1),("target_id",1)], unique=True)
    stock_watch_col.create_index([("user_id",1),("product_id",1)], unique=True)
    loyalty_col.create_index("user_id", unique=True)
    staff_roles_col.create_index("user_id", unique=True)
except Exception:
    pass

_V24_FLOW = {}

# ---------- Loyalty / cashback ----------
def _loyalty_level(total_spend):
    total=float(total_spend or 0)
    if total >= 500: return "💎 Diamond"
    if total >= 250: return "🥇 Gold"
    if total >= 100: return "🥈 Silver"
    return "🥉 Bronze"

def _loyalty_after_purchase(uid, amount, source, source_id):
    amount=max(0.0,float(amount or 0))
    if amount <= 0: return
    cfg=get_cached_config()
    pct=max(0.0,min(20.0,float(cfg.get("cashback_percent",2.0) or 0)))
    cashback=round(amount*pct/100.0,2)
    row=loyalty_col.find_one_and_update(
        {"user_id":int(uid)},
        {"$inc":{"total_spend":amount,"cashback_balance":cashback,"purchases":1},
         "$set":{"updated_at":time.time()},
         "$setOnInsert":{"created_at":time.time()}},
        upsert=True, return_document=ReturnDocument.AFTER)
    loyalty_col.update_one({"user_id":int(uid)},{"$set":{"level":_loyalty_level(row.get("total_spend",0))}})
    if cashback>0:
        try: raw_bot.send_message(int(uid),f"🎖 LOYALTY REWARD\n\nYou earned ${cashback:.2f} cashback from this purchase.\nLevel: {_loyalty_level(row.get('total_spend',0))}\nCashback balance: ${float(row.get('cashback_balance',0)):.2f}")
        except Exception: pass

@bot.message_handler(func=lambda m: m.text == "🎖 Loyalty")
def loyalty_button(m):
    row=loyalty_col.find_one({"user_id":int(m.from_user.id)}) or {}
    raw_bot.send_message(m.chat.id,
        f"🎖 GLOBEXOMART LOYALTY\n\nLevel: {row.get('level','🥉 Bronze')}\n"
        f"Lifetime purchases: {int(row.get('purchases',0) or 0)}\n"
        f"Lifetime spend: ${float(row.get('total_spend',0) or 0):.2f}\n"
        f"Cashback wallet: ${float(row.get('cashback_balance',0) or 0):.2f}\n\n"
        "Your level grows automatically from completed purchases.")

# ---------- Cart ----------
def _cart_rows(uid):
    return list(cart_col.find({"user_id":int(uid)}).sort("created_at",1))

def _render_cart(uid):
    rows=_cart_rows(uid); kb=InlineKeyboardMarkup(row_width=1)
    if not rows:
        return "🛒 YOUR CART\n\nYour cart is empty.", kb
    lines=["🛒 YOUR CART\n"] ; total=0.0; valid=0
    for r in rows:
        p=_shop_product(r.get("product_id")); qty=max(1,int(r.get("quantity",1) or 1))
        if not p: continue
        price=float(p.get("price_usdt",0) or 0); subtotal=round(price*qty,2); total+=subtotal;valid+=1
        lines.append(f"• {p.get('name')} ×{qty} — ${subtotal:.2f}")
        kb.add(InlineKeyboardButton(f"🗑 Remove {p.get('name','Product')[:24]}",callback_data=f"cartdel|{p['_id']}"))
    lines.append(f"\nEstimated total: ${total:.2f} USDT")
    if valid: kb.add(InlineKeyboardButton("✅ Checkout Cart",callback_data="cartcheckout"))
    kb.add(InlineKeyboardButton("🛍 Continue Shopping",callback_data="shopcat|paid"))
    return "\n".join(lines),kb

@bot.message_handler(func=lambda m: m.text == "🛒 Cart")
def cart_button(m):
    text,kb=_render_cart(m.from_user.id);raw_bot.send_message(m.chat.id,text,reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith("cartadd|"))
def cart_add(c):
    p=_shop_product(c.data.split("|",1)[1])
    if not p or p.get("kind")!="paid":return bot.answer_callback_query(c.id,"Paid product not available",True)
    cart_col.update_one({"user_id":c.from_user.id,"product_id":str(p["_id"])},{"$inc":{"quantity":1},"$setOnInsert":{"created_at":time.time()}},upsert=True)
    bot.answer_callback_query(c.id,"Added to cart ✅")

@bot.callback_query_handler(func=lambda c:c.data.startswith("cartdel|"))
def cart_del(c):
    cart_col.delete_one({"user_id":c.from_user.id,"product_id":c.data.split("|",1)[1]})
    text,kb=_render_cart(c.from_user.id)
    try: raw_bot.edit_message_text(text,c.message.chat.id,c.message.message_id,reply_markup=kb)
    except Exception: raw_bot.send_message(c.from_user.id,text,reply_markup=kb)
    bot.answer_callback_query(c.id,"Removed")

@bot.callback_query_handler(func=lambda c:c.data=="cartcheckout")
def cart_checkout(c):
    uid=c.from_user.id; rows=_cart_rows(uid)
    if not rows:return bot.answer_callback_query(c.id,"Cart is empty",True)
    items=[]; total=0.0
    for r in rows:
        p=_shop_product(r.get("product_id")); qty=max(1,int(r.get("quantity",1) or 1))
        if not p or p.get("kind")!="paid" or not _shop_is_available(p) or _shop_stock_count(p)<qty:
            return bot.answer_callback_query(c.id,"One cart item is unavailable or has insufficient stock",True)
        each=float(p.get("price_usdt",0) or 0); subtotal=round(each*qty,2);total+=subtotal;items.append((p,qty,each,subtotal))
    total=round(total,2)
    debit=users_col.update_one({"_id":str(uid),"usdt_balance":{"$gte":total}},{"$inc":{"usdt_balance":-total}})
    if debit.modified_count!=1:return bot.answer_callback_query(c.id,f"Need ${total:.2f} balance",True)
    completed=[]
    try:
        for p,qty,each,subtotal in items:
            current=list(p.get("stock",[]) or []); delivered=current[:qty]; remaining=current[qty:]
            upd=shop_products_col.update_one({"_id":p["_id"],"stock":current},{"$set":{"stock":remaining,"updated_at":time.time()},"$inc":{"sales":qty,"revenue":subtotal}})
            if upd.modified_count!=1: raise RuntimeError("Stock changed during checkout")
            order={"user_id":uid,"chat_id":c.message.chat.id,"username":c.from_user.username,"product_id":str(p["_id"]),"product_name":p.get("name"),"kind":"paid","quantity":qty,"price_each":each,"subtotal":subtotal,"discount_percent":0,"total":subtotal,"paid_with":"USDT Balance","delivered":delivered,"status":"completed","duration":p.get("duration") or "Not specified","warranty":p.get("warranty") or "Not specified","created_at":time.time(),"cart_order":True}
            oid=shop_orders_col.insert_one(order).inserted_id;completed.append((p,qty,subtotal,delivered,oid))
            _shop_deliver(uid,p,delivered,oid)
        payments_col.insert_one({"user_id":uid,"type":"cart","amount":total,"currency":"USDT","mode":"balance","status":"paid","created_at":time.time(),"items":len(completed)})
        cart_col.delete_many({"user_id":uid})
        _loyalty_after_purchase(uid,total,"cart",str(int(time.time())))
        try:
            inv_id=f"CART{int(time.time())}"[-10:]
            invoice_rows=[("Order", "Combined Cart Purchase"),("Items", len(completed)),("Total Paid", f"${total:.2f} USDT"),("Payment", "USDT Balance"),("Bot", "@globexomartbot")]
            _send_globexomart_invoice(uid,"CART PURCHASE",inv_id,invoice_rows,status="Paid / Delivered Successfully")
        except Exception:
            pass
        bot.answer_callback_query(c.id,"Cart purchased ✅",True)
        raw_bot.send_message(uid,f"✅ CART PURCHASE COMPLETE\n\nItems: {len(completed)}\nTotal paid: ${total:.2f} USDT\nAll purchased items were delivered above.")
    except Exception as exc:
        users_col.update_one({"_id":str(uid)},{"$inc":{"usdt_balance":total}})
        log_event("cart_checkout_error",uid,details={"error":str(exc)},level="error")
        raw_bot.send_message(uid,"❌ Cart checkout could not complete. Your balance was restored. Please review stock and try again.")

# ---------- Unified order history + one-tap rebuy ----------
@bot.message_handler(func=lambda m:m.text=="🧾 My Orders")
def unified_orders(m):
    uid=m.from_user.id; kb=InlineKeyboardMarkup(row_width=1); lines=["🧾 MY ORDERS & ACCESS\n"]
    sub=list(subscriptions_col.find({"user_id":uid}).sort("created_at",-1).limit(5))
    if sub:
        lines.append("👑 VIP HISTORY")
        for x in sub:
            lines.append(f"• {x.get('plan')} — {x.get('status')} — ${float(x.get('price_usd',0) or 0):.2f}")
            if x.get("plan") in get_subscription_plans():kb.add(InlineKeyboardButton(f"🔄 Buy {x.get('plan')} Again",callback_data=f"rebuyvip|{x.get('plan')}"))
    orders=list(shop_orders_col.find({"user_id":uid}).sort("created_at",-1).limit(10))
    if orders:
        lines.append("\n🛍 PRODUCT ORDERS")
        for o in orders:
            lines.append(f"• {o.get('product_name')} ×{o.get('quantity',1)} — ${float(o.get('total',0) or 0):.2f} — #{str(o.get('_id'))[-8:]}")
            kb.add(InlineKeyboardButton(f"🛒 Rebuy {str(o.get('product_name') or 'Product')[:22]}",callback_data=f"shopview|{o.get('product_id')}"))
    if not sub and not orders: lines.append("No completed orders yet.")
    raw_bot.send_message(uid,"\n".join(lines),reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith("rebuyvip|"))
def rebuy_vip(c):
    code=c.data.split("|",1)[1]; plans=get_subscription_plans(); row=plans.get(code)
    if not row:return bot.answer_callback_query(c.id,"Plan unavailable",True)
    raw_bot.send_message(c.from_user.id,f"🔄 REBUY / RENEW\n\n{row.get('name',code)} — ${float(row.get('price',0) or 0):g}\n\nContinue from the VIP plans screen.")
    _vip_plan_selection(c.from_user.id);bot.answer_callback_query(c.id)

# ---------- VIP renew + gifting ----------
@bot.callback_query_handler(func=lambda c:c.data=="viprenew|current")
def vip_renew_current(c):
    sub=_active_subscription(c.from_user.id)
    if not sub:return bot.answer_callback_query(c.id,"No active VIP to renew",True)
    code=str(sub.get("plan") or "").upper(); row=get_subscription_plans().get(code)
    if not row:return bot.answer_callback_query(c.id,"Current plan is no longer available",True)
    raw_bot.send_message(c.from_user.id,f"🔄 RENEW VIP\n\nPlan: {row.get('name',code)}\nRenewal price: ${_effective_plan_price(c.from_user.id,code):g}\n\nSelect the same plan below to extend from your current expiry.")
    # Existing purchase handler can still be reached directly.
    kb=InlineKeyboardMarkup();kb.add(InlineKeyboardButton(f"💎 Renew {row.get('name',code)}",callback_data=f"subplan|{code}"));raw_bot.send_message(c.from_user.id,"Continue:",reply_markup=kb);bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c:c.data=="vipgift|start")
def vip_gift_start(c):
    plans=_sorted_active_plans(highest_first=False);kb=InlineKeyboardMarkup(row_width=1)
    for code,row in plans:kb.add(InlineKeyboardButton(f"🎁 {row.get('name',code)} • ${float(row.get('price',0) or 0):g}",callback_data=f"vipgiftplan|{code}"))
    raw_bot.send_message(c.from_user.id,"🎁 GIFT VIP\n\nChoose the plan you want to gift. Payment uses your approved USDT bot balance.",reply_markup=kb);bot.answer_callback_query(c.id)

@bot.callback_query_handler(func=lambda c:c.data.startswith("vipgiftplan|"))
def vip_gift_plan(c):
    code=c.data.split("|",1)[1];_V24_FLOW[c.from_user.id]={"type":"gift_vip","plan":code};msg=raw_bot.send_message(c.from_user.id,"Send the recipient's Telegram user ID or @username. They must have started the bot before.");bot.register_next_step_handler(msg,_gift_vip_recipient_step);bot.answer_callback_query(c.id,"Send recipient")

def _resolve_bot_user(raw):
    raw=(raw or "").strip()
    if raw.lstrip("-").isdigit(): return users_col.find_one({"_id":str(int(raw))})
    return users_col.find_one({"username":{"$regex":"^"+re.escape(raw.lstrip("@"))+"$","$options":"i"}})

def _gift_vip_recipient_step(m):
    st=_V24_FLOW.pop(m.from_user.id,None) or {}; u=_resolve_bot_user(m.text)
    if not u:return raw_bot.send_message(m.chat.id,"❌ Recipient not found in the bot database.",reply_markup=main_menu(m.from_user.id))
    code=st.get("plan");price=float(_effective_plan_price(m.from_user.id,code));recipient=int(u["_id"])
    debit=users_col.update_one({"_id":str(m.from_user.id),"usdt_balance":{"$gte":price}},{"$inc":{"usdt_balance":-price}})
    if debit.modified_count!=1:return raw_bot.send_message(m.chat.id,f"❌ Insufficient USDT balance. Need ${price:.2f}.")
    try:
        sub,links=activate_subscription(recipient,code,payment_mode="gift_balance",amount=price,payment_ref=f"GIFT-{m.from_user.id}-{int(time.time())}",added_by=m.from_user.id)
        _loyalty_after_purchase(m.from_user.id,price,"gift_vip",code)
        raw_bot.send_message(m.chat.id,f"✅ VIP GIFT SENT\n\nRecipient: @{u.get('username') or recipient}\nPlan: {code}\nPaid: ${price:.2f}")
        raw_bot.send_message(recipient,f"🎁 YOU RECEIVED GLOBEXOMART VIP\n\nGifted by a GLOBEXOMART member.\nPlan: {code}\nYour access is active now.")
    except Exception as exc:
        users_col.update_one({"_id":str(m.from_user.id)},{"$inc":{"usdt_balance":price}});raw_bot.send_message(m.chat.id,f"❌ Gift failed. Balance restored. {exc}")

# Product gifting
@bot.callback_query_handler(func=lambda c:c.data.startswith("giftprod|"))
def gift_product_start(c):
    pid=c.data.split("|",1)[1];_V24_FLOW[c.from_user.id]={"type":"gift_product","product_id":pid};msg=raw_bot.send_message(c.from_user.id,"🎁 GIFT PRODUCT\n\nSend recipient Telegram user ID or @username. They must have started the bot.");bot.register_next_step_handler(msg,_gift_product_recipient_step);bot.answer_callback_query(c.id,"Send recipient")

def _gift_product_recipient_step(m):
    st=_V24_FLOW.pop(m.from_user.id,None) or {}; recipient=_resolve_bot_user(m.text);p=_shop_product(st.get("product_id"))
    if not recipient or not p or p.get("kind")!="paid" or not _shop_is_available(p):return raw_bot.send_message(m.chat.id,"❌ Recipient/product unavailable.")
    price=float(p.get("price_usdt",0) or 0);debit=users_col.update_one({"_id":str(m.from_user.id),"usdt_balance":{"$gte":price}},{"$inc":{"usdt_balance":-price}})
    if debit.modified_count!=1:return raw_bot.send_message(m.chat.id,f"❌ Insufficient balance. Need ${price:.2f}.")
    stock=list(p.get("stock",[]) or []);delivered=stock[:1]
    upd=shop_products_col.update_one({"_id":p["_id"],"stock":stock},{"$set":{"stock":stock[1:],"updated_at":time.time()},"$inc":{"sales":1,"revenue":price}})
    if upd.modified_count!=1:users_col.update_one({"_id":str(m.from_user.id)},{"$inc":{"usdt_balance":price}});return raw_bot.send_message(m.chat.id,"❌ Stock changed. Balance restored.")
    rid=int(recipient["_id"]);order={"user_id":rid,"gifted_by":m.from_user.id,"product_id":str(p["_id"]),"product_name":p.get("name"),"kind":"paid","quantity":1,"price_each":price,"subtotal":price,"total":price,"paid_with":"Gift","delivered":delivered,"status":"completed","created_at":time.time()};oid=shop_orders_col.insert_one(order).inserted_id
    _shop_deliver(rid,p,delivered,oid);_loyalty_after_purchase(m.from_user.id,price,"gift_product",str(p["_id"]))
    raw_bot.send_message(m.chat.id,f"✅ Product gifted to @{recipient.get('username') or rid}.");raw_bot.send_message(rid,f"🎁 You received a GLOBEXOMART product gift: {p.get('name')}")

# ---------- Watchlist / stock alerts / trending ----------
@bot.callback_query_handler(func=lambda c:c.data.startswith("watchprod|"))
def watch_product(c):
    pid=c.data.split("|",1)[1];watchlist_col.update_one({"user_id":c.from_user.id,"target_type":"product","target_id":pid},{"$setOnInsert":{"created_at":time.time()}},upsert=True);bot.answer_callback_query(c.id,"Added to watchlist 🔔")

@bot.callback_query_handler(func=lambda c:c.data.startswith("stockwatch|"))
def stock_watch(c):
    pid=c.data.split("|",1)[1];stock_watch_col.update_one({"user_id":c.from_user.id,"product_id":pid},{"$setOnInsert":{"created_at":time.time()}},upsert=True);bot.answer_callback_query(c.id,"We'll notify you after restock ✅",True)

def _notify_product_restock(pid,name):
    rows=list(stock_watch_col.find({"product_id":str(pid)}));sent=0
    for r in rows:
        try:
            kb=InlineKeyboardMarkup();kb.add(InlineKeyboardButton("🛍 View Product",callback_data=f"shopview|{pid}"));raw_bot.send_message(int(r["user_id"]),f"📦 RESTOCK ALERT\n\n{name} is back in stock.",reply_markup=kb);sent+=1
        except Exception:pass
    stock_watch_col.delete_many({"product_id":str(pid)})
    return sent

@bot.message_handler(func=lambda m:m.text=="🔔 Watchlist")
def watchlist_button(m):
    rows=list(watchlist_col.find({"user_id":m.from_user.id}).sort("created_at",-1).limit(30));kb=InlineKeyboardMarkup(row_width=1);lines=["🔔 YOUR WATCHLIST\n"]
    for r in rows:
        if r.get("target_type")=="product":
            p=_shop_product(r.get("target_id"));
            if p: lines.append(f"• {p.get('name')} — {_shop_status(p)}");kb.add(InlineKeyboardButton(f"🛍 {p.get('name')[:28]}",callback_data=f"shopview|{p['_id']}"))
    if len(lines)==1:lines.append("No saved watch items yet.")
    raw_bot.send_message(m.chat.id,"\n".join(lines),reply_markup=kb)

# Trending calculated from sales/views rather than fabricated popularity.
@bot.callback_query_handler(func=lambda c:c.data=="v24trending")
def trending_products(c):
    rows=list(shop_products_col.find({"active":True}).sort([("sales",-1),("revenue",-1)]).limit(10));kb=InlineKeyboardMarkup(row_width=1);lines=["🔥 TRENDING PRODUCTS\n\nRanked from real completed sales."]
    for i,p in enumerate(rows,1):lines.append(f"{i}. {p.get('name')} — {int(p.get('sales',0) or 0)} sales");kb.add(InlineKeyboardButton(f"#{i} {p.get('name')[:28]}",callback_data=f"shopview|{p['_id']}"))
    raw_bot.send_message(c.from_user.id,"\n".join(lines),reply_markup=kb);bot.answer_callback_query(c.id)

# ---------- FAQ + language preference ----------
_DEFAULT_FAQ=[
("How do I get VIP?","Open ⭐ Buy VIP, choose a plan, follow the payment instructions and submit the required proof. Access activates after approval."),
("How do methods work?","Free methods may use points. Active VIP members can access Free and VIP methods according to the bot's VIP access rules."),
("How do referrals work?","Share your referral link. A referral is credited only after the invited user passes all configured Force Join checks."),
("How do I contact support?","Use 💬 Chat Admin for a conversation or 🎫 Support for an organized support ticket."),
("Where are my purchases?","Open 🧾 My Orders to view product history and VIP access history."),
]

def _faq_rows():
    rows=list(faq_col.find({"active":{"$ne":False}}).sort("position",1));return rows or [{"question":q,"answer":a} for q,a in _DEFAULT_FAQ]

@bot.message_handler(func=lambda m:m.text=="❓ FAQ")
def faq_button(m):
    rows=_faq_rows();kb=InlineKeyboardMarkup(row_width=1)
    for i,r in enumerate(rows[:20]):kb.add(InlineKeyboardButton(f"❓ {r.get('question')[:45]}",callback_data=f"faqshow|{i}"))
    raw_bot.send_message(m.chat.id,"❓ GLOBEXOMART FAQ\n\nChoose a question:",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith("faqshow|"))
def faq_show(c):
    rows=_faq_rows();i=int(c.data.split("|",1)[1]);r=rows[i] if 0<=i<len(rows) else None;bot.answer_callback_query(c.id)
    if r:raw_bot.send_message(c.from_user.id,f"❓ {r.get('question')}\n\n{r.get('answer')}")

@bot.message_handler(func=lambda m:m.text=="🌐 Language")
def language_button(m):
    kb=InlineKeyboardMarkup();kb.add(InlineKeyboardButton("🇬🇧 English",callback_data="lang|en"),InlineKeyboardButton("🇵🇰 اردو",callback_data="lang|ur"));raw_bot.send_message(m.chat.id,"🌐 LANGUAGE\n\nChoose your preferred language for supported helper messages:",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith("lang|"))
def language_set(c):
    lang=c.data.split("|",1)[1];users_col.update_one({"_id":str(c.from_user.id)},{"$set":{"language_preference":lang}});bot.answer_callback_query(c.id,"Language saved ✅",True)
    raw_bot.send_message(c.from_user.id,"✅ Language preference saved." if lang=="en" else "✅ زبان کی ترجیح محفوظ ہوگئی۔")

# ---------- Maintenance mode ----------
_ORIGINAL_FORCE_BLOCK_V24 = force_block

def force_block(uid):
    if not is_admin(uid):
        cfg=get_cached_config()
        if cfg.get("maintenance_mode",False):
            try: raw_bot.send_message(int(uid),cfg.get("maintenance_message") or "🔧 GLOBEXOMART is under maintenance. Please try again shortly.")
            except Exception: pass
            return True
    return _ORIGINAL_FORCE_BLOCK_V24(uid)

@bot.message_handler(func=lambda m:m.text=="🔧 Maintenance" and is_admin(m.from_user.id))
def maintenance_admin(m):
    on=bool(get_cached_config().get("maintenance_mode",False));kb=InlineKeyboardMarkup(row_width=1);kb.add(InlineKeyboardButton("🟢 Disable Maintenance" if on else "🔴 Enable Maintenance",callback_data="maint|toggle"),InlineKeyboardButton("✏️ Edit Message",callback_data="maint|msg"));raw_bot.send_message(m.chat.id,f"🔧 MAINTENANCE MODE\n\nStatus: {'ON' if on else 'OFF'}\n\nAdmins remain able to use the bot.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith("maint|") and is_admin(c.from_user.id))
def maintenance_cb(c):
    action=c.data.split("|",1)[1]
    if action=="toggle":set_config("maintenance_mode",not bool(get_config().get("maintenance_mode",False)));bot.answer_callback_query(c.id,"Updated",True);return maintenance_admin(type('M',(),{'from_user':c.from_user,'chat':c.message.chat})())
    msg=raw_bot.send_message(c.from_user.id,"Send the maintenance message users should see:");bot.register_next_step_handler(msg,_maintenance_msg_save);bot.answer_callback_query(c.id,"Send message")

def _maintenance_msg_save(m):
    if not is_admin(m.from_user.id):return
    set_config("maintenance_message",(m.text or "")[:1000]);raw_bot.send_message(m.chat.id,"✅ Maintenance message saved.",reply_markup=admin_menu())

# ---------- Admin activity + error monitor ----------
_ORIGINAL_LOG_EVENT_V24 = log_event

def log_event(event, actor=None, target=None, details=None, level="info", **kwargs):
    result=_ORIGINAL_LOG_EVENT_V24(event,actor,target,details,level,**kwargs)
    try:
        if actor is not None and is_admin(actor):
            admin_activity_col.insert_one({"admin_id":int(actor),"event":str(event),"target":target,"details":details or {},"level":level,"created_at":time.time()})
        if str(level).lower()=="error" and get_cached_config().get("owner_error_alerts",True):
            text=f"🚨 BOT ERROR\n\nEvent: {event}\nActor: {actor}\nTarget: {target}\nError: {str((details or {}).get('error') or details or '')[:900]}"
            try: raw_bot.send_message(int(ADMIN_ID),text)
            except Exception: pass
    except Exception: pass
    return result

@bot.message_handler(func=lambda m:m.text=="📝 Admin Activity" and is_admin(m.from_user.id))
def admin_activity(m):
    rows=list(admin_activity_col.find({}).sort("created_at",-1).limit(40));lines=["📝 ADMIN ACTIVITY\n"]
    for r in rows:
        when=datetime.fromtimestamp(float(r.get('created_at',time.time()))).strftime('%m-%d %H:%M');lines.append(f"• {when} • {r.get('admin_id')} • {r.get('event')} • {r.get('target') or '-'}")
    raw_bot.send_message(m.chat.id,"\n".join(lines)[:3900])

# ---------- Backup restore ----------
@bot.message_handler(func=lambda m:m.text=="💾 Backup/Restore" and is_admin(m.from_user.id))
def backup_restore_menu(m):
    kb=InlineKeyboardMarkup(row_width=1);kb.add(InlineKeyboardButton("📤 Create Backup",callback_data="v24backup|create"),InlineKeyboardButton("📥 Restore Backup",callback_data="v24backup|restore"));raw_bot.send_message(m.chat.id,"💾 BACKUP & RESTORE\n\nRestore accepts a GLOBEXOMART JSON backup. Existing matching _id records are replaced/upserted; other records remain.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith("v24backup|") and is_admin(c.from_user.id))
def backup_restore_cb(c):
    act=c.data.split("|",1)[1]
    if act=="create":
        from bson import json_util
        payload={n:list(db[n].find({})) for n in db.list_collection_names()};raw=json_util.dumps(payload,ensure_ascii=False).encode();z=io.BytesIO();
        with zipfile.ZipFile(z,'w',zipfile.ZIP_DEFLATED) as zz:zz.writestr('globexomart_backup.json',raw)
        z.seek(0);z.name=f"globexomart_backup_{datetime.now().strftime('%Y%m%d_%H%M')}.zip";bot.send_document(c.from_user.id,z);bot.answer_callback_query(c.id,"Backup created")
    else:
        _V24_FLOW[c.from_user.id]={"type":"restore"};msg=raw_bot.send_message(c.from_user.id,"📥 Send the backup JSON file. For safety, ZIP restore is not automatic; extract globexomart_backup.json first.");bot.register_next_step_handler(msg,_restore_file_step);bot.answer_callback_query(c.id,"Send JSON")

def _restore_file_step(m):
    if not is_admin(m.from_user.id):return
    if m.content_type!="document":return raw_bot.send_message(m.chat.id,"❌ Send a JSON document.")
    try:
        from bson import json_util
        info=bot.get_file(m.document.file_id);data=bot.download_file(info.file_path);payload=json_util.loads(data.decode('utf-8'))
        if not isinstance(payload,dict):raise ValueError("Backup root must be an object")
        restored=0
        for name,rows in payload.items():
            if not isinstance(rows,list):continue
            col=db[str(name)]
            for row in rows:
                if not isinstance(row,dict) or "_id" not in row:continue
                col.replace_one({"_id":row["_id"]},row,upsert=True);restored+=1
        restore_jobs_col.insert_one({"admin_id":m.from_user.id,"records":restored,"created_at":time.time()});raw_bot.send_message(m.chat.id,f"✅ Restore complete. {restored} records upserted.",reply_markup=admin_menu())
    except Exception as exc:raw_bot.send_message(m.chat.id,f"❌ Restore failed: {exc}",reply_markup=admin_menu())

# ---------- Staff roles (separate limited-access staff; full admins remain unchanged) ----------
_STAFF_PERMS={"support":{"tickets","chats"},"payments":{"pending"},"content":{"products","methods"},"analyst":{"analytics"}}

def staff_role(uid):
    r=staff_roles_col.find_one({"user_id":int(uid),"active":{"$ne":False}});return (r or {}).get("role")

@bot.message_handler(func=lambda m:m.text=="👥 Staff Roles" and is_admin(m.from_user.id))
def staff_roles_admin(m):
    rows=list(staff_roles_col.find({}).sort("created_at",-1).limit(30));kb=InlineKeyboardMarkup(row_width=1);kb.add(InlineKeyboardButton("➕ Add Limited Staff",callback_data="staff|new"))
    for r in rows:kb.add(InlineKeyboardButton(f"{r.get('role','staff').title()} • {r.get('user_id')} • {'ON' if r.get('active',True) else 'OFF'}",callback_data=f"staff|toggle|{r.get('user_id')}"))
    raw_bot.send_message(m.chat.id,"👥 LIMITED STAFF ROLES\n\nThese are separate from full Admins. Roles: support, payments, content, analyst. Limited staff do not receive unrestricted admin access.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith("staff|") and is_admin(c.from_user.id))
def staff_cb(c):
    p=c.data.split('|');
    if p[1]=='new':
        msg=raw_bot.send_message(c.from_user.id,"Send: USER_ID | ROLE\nRoles: support, payments, content, analyst");bot.register_next_step_handler(msg,_staff_add_step);return bot.answer_callback_query(c.id,"Send details")
    uid=int(p[2]);r=staff_roles_col.find_one({"user_id":uid}) or {};staff_roles_col.update_one({"user_id":uid},{"$set":{"active":not bool(r.get('active',True)),"updated_at":time.time()}});bot.answer_callback_query(c.id,"Updated",True)

def _staff_add_step(m):
    try:
        uid_s,role=[x.strip() for x in (m.text or '').split('|',1)];uid=int(uid_s);role=role.lower()
        if role not in _STAFF_PERMS:raise ValueError("Invalid role")
        staff_roles_col.update_one({"user_id":uid},{"$set":{"role":role,"active":True,"added_by":m.from_user.id,"updated_at":time.time()},"$setOnInsert":{"created_at":time.time()}},upsert=True);raw_bot.send_message(m.chat.id,f"✅ Limited staff added: {uid} • {role}",reply_markup=admin_menu())
        try:raw_bot.send_message(uid,f"🧑‍💼 You were added as GLOBEXOMART {role.title()} Staff. Use /staff to open your limited staff panel.")
        except Exception:pass
    except Exception as exc:raw_bot.send_message(m.chat.id,f"❌ {exc}",reply_markup=admin_menu())

@bot.message_handler(commands=['staff'])
def limited_staff_panel(m):
    role=staff_role(m.from_user.id)
    if not role:return
    if role=='support':text="🧑‍💼 SUPPORT STAFF\n\nUse /stafftickets to view open tickets."
    elif role=='payments':text="🧑‍💼 PAYMENT STAFF\n\nUse /staffpending to view counts of pending payment requests."
    elif role=='content':text="🧑‍💼 CONTENT STAFF\n\nUse the owner/admin workflow for publishing after approval. This limited role does not grant deletion or finance permissions."
    else:text="🧑‍💼 ANALYST STAFF\n\nUse /staffstats to view read-only business totals."
    raw_bot.send_message(m.chat.id,text)

@bot.message_handler(commands=['staffpending'])
def staff_pending(m):
    if staff_role(m.from_user.id)!='payments':return
    dep=wallet_tx_col.count_documents({'type':'deposit','status':'pending'});wd=wallet_tx_col.count_documents({'type':'withdraw','status':'pending'});vip=payments_col.count_documents({'status':'pending'});raw_bot.send_message(m.chat.id,f"⏳ PENDING\nDeposits: {dep}\nWithdrawals: {wd}\nPayments/VIP: {vip}\n\nApproval remains restricted to full admins for safety.")

@bot.message_handler(commands=['staffstats'])
def staff_stats(m):
    if staff_role(m.from_user.id)!='analyst':return
    raw_bot.send_message(m.chat.id,f"📊 READ-ONLY STATS\nUsers: {users_col.count_documents({})}\nVIP: {users_col.count_documents({'vip':True})}\nOrders: {shop_orders_col.count_documents({})}")

# ---------- Retention analytics ----------
@bot.message_handler(func=lambda m:m.text=="📊 Retention" and is_admin(m.from_user.id))
def retention_admin(m):
    now=time.time(); total=users_col.count_documents({})
    d1=users_col.count_documents({'last_active':{'$gte':now-86400}});d7=users_col.count_documents({'last_active':{'$gte':now-7*86400}});d30=users_col.count_documents({'last_active':{'$gte':now-30*86400}})
    new7=users_col.count_documents({'created_at':{'$gte':now-7*86400}});new30=users_col.count_documents({'created_at':{'$gte':now-30*86400}})
    raw_bot.send_message(m.chat.id,f"📊 RETENTION / ACTIVITY\n\nTotal users: {total}\nActive last 24h: {d1} ({(d1/total*100 if total else 0):.1f}%)\nActive last 7d: {d7} ({(d7/total*100 if total else 0):.1f}%)\nActive last 30d: {d30} ({(d30/total*100 if total else 0):.1f}%)\n\nNew users last 7d: {new7}\nNew users last 30d: {new30}\n\nThese figures use users.last_active and created_at.")

# ---------- Operations center / confirmations / FAQ / cashback settings ----------
@bot.message_handler(func=lambda m:m.text=="🧰 Operations Center" and is_admin(m.from_user.id))
def operations_center(m):
    kb=InlineKeyboardMarkup(row_width=1);kb.add(InlineKeyboardButton("🔥 Trending Products",callback_data="v24trending"),InlineKeyboardButton("💵 Cashback %",callback_data="opset|cashback"),InlineKeyboardButton("🚨 Toggle Owner Error Alerts",callback_data="opset|errors"));raw_bot.send_message(m.chat.id,"🧰 OPERATIONS CENTER\n\nCommerce, reliability and retention controls.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith("opset|") and is_admin(c.from_user.id))
def operations_cb(c):
    what=c.data.split('|',1)[1]
    if what=='errors':set_config('owner_error_alerts',not bool(get_config().get('owner_error_alerts',True)));bot.answer_callback_query(c.id,'Updated',True);return
    msg=raw_bot.send_message(c.from_user.id,"Send cashback percentage from 0 to 20:");bot.register_next_step_handler(msg,_cashback_save);bot.answer_callback_query(c.id,"Send percentage")

def _cashback_save(m):
    try:pct=float((m.text or '').strip());
    except Exception:return raw_bot.send_message(m.chat.id,"❌ Send a number.")
    pct=max(0,min(20,pct));set_config('cashback_percent',pct);raw_bot.send_message(m.chat.id,f"✅ Cashback set to {pct:g}%.",reply_markup=admin_menu())

# Keep menu fallback aware of limited staff command label if future UI adds it.



# =========================
# 🚀 V26 ADVANCED GROWTH + OPERATIONS SUITE
# =========================
# Adds the requested business-control features while preserving all existing systems.
# Drip VIP content is intentionally NOT included.

advanced_config_col = db["advanced_config"]
promo_pins_col = db["promo_pins"]
notification_prefs_col = db["notification_prefs"]
payment_profiles_col = db["payment_profiles"]
country_offers_col = db["country_offers"]
terms_acceptance_col = db["terms_acceptance"]
broadcast_templates_col = db["broadcast_templates"]
scheduled_drops_col = db["scheduled_drops"]
bundles_col = db["bundles"]
profit_entries_col = db["profit_entries"]
customer_notes_col = db["customer_notes"]
user_tags_col = db["user_tags"]
saved_replies_col = db["saved_replies"]
test_users_col = db["test_users"]
admin_confirm_col = db["admin_confirmations"]

for _col, _keys in [
    (notification_prefs_col, [("user_id", 1)]),
    (terms_acceptance_col, [("user_id", 1), ("accepted_at", -1)]),
    (scheduled_drops_col, [("status", 1), ("run_at", 1)]),
    (customer_notes_col, [("user_id", 1), ("created_at", -1)]),
    (user_tags_col, [("user_id", 1), ("tag", 1)]),
    (test_users_col, [("user_id", 1)]),
]:
    try:
        _col.create_index(_keys)
    except Exception:
        pass

# Add user-facing buttons without disturbing the saved ordering system.
for _row in [("🔥 Trending", "🔔 Preferences"), ("🧩 Bundles",)]:
    if not any(btn in MAIN_MENU_BUTTONS for btn in _row):
        MAIN_MENU_ROWS.append(_row)
MAIN_MENU_BUTTONS = [button for row in MAIN_MENU_ROWS for button in row]


def _adv_cfg():
    d = advanced_config_col.find_one({"_id":"settings"}) or {}
    return d


def _adv_set(key, value):
    advanced_config_col.update_one({"_id":"settings"},{"$set":{key:value}},upsert=True)


def _brand_name():
    return str(_adv_cfg().get("brand_name") or "GLOBEXOMART")


def _brand_tagline():
    return str(_adv_cfg().get("brand_tagline") or "Premium tools. Premium results.")


def _user_pref(uid):
    row = notification_prefs_col.find_one({"user_id":int(uid)}) or {}
    return {
        "methods": row.get("methods", True),
        "products": row.get("products", True),
        "deals": row.get("deals", True),
        "vip": row.get("vip", True),
        "referrals": row.get("referrals", True),
        "scanners": row.get("scanners", True),
        "quiet_enabled": row.get("quiet_enabled", False),
        "quiet_start": row.get("quiet_start", 22),
        "quiet_end": row.get("quiet_end", 8),
    }


def _in_quiet_hours(uid):
    p=_user_pref(uid)
    if not p.get("quiet_enabled"):
        return False
    h=datetime.now().hour
    start=int(p.get("quiet_start",22)); end=int(p.get("quiet_end",8))
    return (h>=start or h<end) if start>end else (start<=h<end)


def _terms_text():
    cfg=_adv_cfg()
    return str(cfg.get("terms_text") or (
        "📑 TERMS & PURCHASE POLICY\n\n"
        "• Digital purchases and VIP access are delivered according to the plan/item description.\n"
        "• Never submit fake payment proofs or reused transaction IDs.\n"
        "• VIP content must not be leaked, resold or redistributed.\n"
        "• Results from methods are not guaranteed and can change over time.\n"
        "• Contact support before purchasing if you are unsure about a product or plan.\n\n"
        "By continuing, you confirm that you understand and accept these terms."
    ))


def _has_terms(uid):
    current=int(_adv_cfg().get("terms_version",1) or 1)
    row=terms_acceptance_col.find_one({"user_id":int(uid),"version":current})
    return bool(row)


def _require_terms(uid, after="vip"):
    if _has_terms(uid):
        return False
    kb=InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("✅ I Accept",callback_data=f"v26terms|accept|{after}"))
    kb.add(InlineKeyboardButton("❌ Cancel",callback_data="globalcancel"))
    raw_bot.send_message(int(uid),_terms_text(),reply_markup=kb)
    return True


@bot.callback_query_handler(func=lambda c: c.data.startswith("v26terms|"))
def v26_terms_cb(c):
    parts=c.data.split("|",2)
    if len(parts)<3:return bot.answer_callback_query(c.id,"Invalid",True)
    action,after=parts[1],parts[2]
    if action=="accept":
        version=int(_adv_cfg().get("terms_version",1) or 1)
        terms_acceptance_col.update_one({"user_id":int(c.from_user.id),"version":version},{"$set":{"accepted_at":time.time(),"username":c.from_user.username}},upsert=True)
        bot.answer_callback_query(c.id,"Terms accepted")
        if after=="vip":
            return _vip_plan_selection(c.from_user.id)
        if after=="products":
            fake=type('M',(object,),{'from_user':c.from_user,'text':'🛍 Products'})()
            return open_catalog_menu(fake)
    bot.answer_callback_query(c.id,"Cancelled")

# Wrap the VIP button handlers with terms acceptance without changing their existing purchase logic.
_v26_buy_vip_button_original = buy_vip_button
def buy_vip_button_v26(m):
    if _require_terms(m.from_user.id,"vip"):
        return
    return _v26_buy_vip_button_original(m)
# Existing decorator points to the old function, so register a higher-priority explicit alias for a new text button is not possible.
# Instead, terms are enforced again at payment submission starts below via wrappers.

_v26_request_manual_subscription_payment_cb_original = request_manual_subscription_payment_cb
def request_manual_subscription_payment_cb(c):
    if _require_terms(c.from_user.id,"vip"):
        try: bot.answer_callback_query(c.id,"Accept purchase terms first",True)
        except Exception: pass
        return
    return _v26_request_manual_subscription_payment_cb_original(c)

# -------------------------
# 🎨 THEME MANAGER
# -------------------------
@bot.message_handler(func=lambda m:m.text=="🎨 Theme Manager" and is_admin(m.from_user.id))
def v26_theme_menu(m):
    cfg=_adv_cfg();kb=InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("🏷 Brand Name",callback_data="v26theme|name"),InlineKeyboardButton("✨ Tagline",callback_data="v26theme|tag"))
    kb.add(InlineKeyboardButton("👋 Welcome Text",callback_data="v26theme|welcome"),InlineKeyboardButton("🧾 Proof Footer",callback_data="v26theme|proof"))
    raw_bot.send_message(m.chat.id,f"🎨 THEME MANAGER\n\nBrand: {_brand_name()}\nTagline: {_brand_tagline()}\n\nChange branding text without touching code.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith("v26theme|") and is_admin(c.from_user.id))
def v26_theme_cb(c):
    key=c.data.split('|',1)[1];prompts={'name':'Send new brand name:','tag':'Send new tagline:','welcome':'Send custom welcome paragraph:','proof':'Send proof-channel footer text:'}
    msg=raw_bot.send_message(c.from_user.id,prompts.get(key,'Send value:'));bot.register_next_step_handler(msg,v26_theme_save,key);bot.answer_callback_query(c.id)

def v26_theme_save(m,key):
    val=(m.text or '').strip()[:1500]
    mapping={'name':'brand_name','tag':'brand_tagline','welcome':'welcome_custom','proof':'proof_footer'}
    _adv_set(mapping.get(key,key),val);raw_bot.send_message(m.chat.id,"✅ Theme setting updated.",reply_markup=admin_menu())

# -------------------------
# 📌 PINNED PROMOTIONS
# -------------------------
@bot.message_handler(func=lambda m:m.text=="📌 Promotions" and is_admin(m.from_user.id))
def v26_promotions(m):
    rows=list(promo_pins_col.find({}).sort("created_at",-1).limit(10));kb=InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("➕ Add Promotion",callback_data="v26promo|add"))
    for r in rows: kb.add(InlineKeyboardButton(("✅ " if r.get('active',True) else "🙈 ")+str(r.get('title','Promotion'))[:45],callback_data=f"v26promo|view|{r['_id']}"))
    raw_bot.send_message(m.chat.id,"📌 PINNED PROMOTIONS\n\nCreate a featured offer shown inside For You/Trending.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith("v26promo|") and is_admin(c.from_user.id))
def v26promo_cb(c):
    parts=c.data.split('|');action=parts[1]
    if action=='add':
        msg=raw_bot.send_message(c.from_user.id,"Send promotion as:\nTITLE | MESSAGE | BUTTON TEXT | https://link");bot.register_next_step_handler(msg,v26promo_add);return bot.answer_callback_query(c.id)
    if action=='view':
        try:r=promo_pins_col.find_one({'_id':ObjectId(parts[2])})
        except:r=None
        if not r:return bot.answer_callback_query(c.id,"Not found",True)
        kb=InlineKeyboardMarkup(row_width=2);kb.add(InlineKeyboardButton("👁 Toggle",callback_data=f"v26promo|toggle|{r['_id']}"),InlineKeyboardButton("🗑 Delete",callback_data=f"v26promo|del|{r['_id']}"))
        raw_bot.send_message(c.from_user.id,f"📌 {r.get('title')}\n\n{r.get('message')}\n\nStatus: {'Active' if r.get('active',True) else 'Hidden'}",reply_markup=kb);return bot.answer_callback_query(c.id)
    if action in ('toggle','del'):
        oid=ObjectId(parts[2]);r=promo_pins_col.find_one({'_id':oid})
        if action=='del':promo_pins_col.delete_one({'_id':oid})
        elif r:promo_pins_col.update_one({'_id':oid},{'$set':{'active':not r.get('active',True)}})
        bot.answer_callback_query(c.id,"Updated")

def v26promo_add(m):
    parts=[x.strip() for x in (m.text or '').split('|')]
    if len(parts)<4:return raw_bot.send_message(m.chat.id,"❌ Use: TITLE | MESSAGE | BUTTON TEXT | https://link")
    promo_pins_col.insert_one({'title':parts[0][:100],'message':parts[1][:1500],'button_text':parts[2][:50],'url':parts[3],'active':True,'created_at':time.time(),'created_by':m.from_user.id})
    raw_bot.send_message(m.chat.id,"✅ Promotion added.",reply_markup=admin_menu())

# -------------------------
# 🧠 SMART RECOMMENDATIONS + TRENDING
# -------------------------
def _v26_recommendations(uid,limit=6):
    # Preference signals: favorites/watchlist/purchases first, then recent/popular content.
    bought=set()
    try:
        for d in item_purchases_col.find({'user_id':int(uid),'status':'paid'},{'folder_id':1}):bought.add(str(d.get('folder_id')))
    except Exception:pass
    out=[]
    try:
        recent=list(folders_col.find({'parent':None}).sort([('pinned',-1),('created_at',-1)]).limit(20))
        for f in recent:
            if str(f.get('_id')) in bought: continue
            out.append(f)
            if len(out)>=limit:break
    except Exception:pass
    return out

@bot.message_handler(func=lambda m:m.text=="🔥 Trending")
def v26_trending(m):
    if force_block(m.from_user.id):return
    promos=list(promo_pins_col.find({'active':True}).sort('created_at',-1).limit(1))
    if promos:
        pr=promos[0];kb=InlineKeyboardMarkup();kb.add(InlineKeyboardButton(pr.get('button_text','Open'),url=pr.get('url')))
        raw_bot.send_message(m.chat.id,f"🔥 FEATURED\n\n{pr.get('title')}\n{pr.get('message')}",reply_markup=kb)
    rows=_v26_recommendations(m.from_user.id)
    kb=InlineKeyboardMarkup(row_width=1)
    for f in rows:kb.add(InlineKeyboardButton(f"{f.get('name','Item')}",callback_data=f"openid|{f['_id']}"))
    raw_bot.send_message(m.chat.id,"🔥 TRENDING & RECOMMENDED\n\nBased on recent activity and available content.",reply_markup=kb if rows else None)

# -------------------------
# 🔔 NOTIFICATION PREFERENCES + QUIET HOURS
# -------------------------
@bot.message_handler(func=lambda m:m.text=="🔔 Preferences")
def v26_prefs(m):
    p=_user_pref(m.from_user.id);kb=InlineKeyboardMarkup(row_width=2)
    labels=[('methods','Methods'),('products','Products'),('deals','Deals'),('vip','VIP'),('referrals','Referrals'),('scanners','Scanners')]
    for k,label in labels:kb.add(InlineKeyboardButton(('✅ ' if p[k] else '❌ ')+label,callback_data=f"v26pref|{k}"))
    kb.add(InlineKeyboardButton(('🌙 Quiet Hours ON' if p['quiet_enabled'] else '🌙 Quiet Hours OFF'),callback_data='v26pref|quiet'))
    raw_bot.send_message(m.chat.id,f"🔔 NOTIFICATION PREFERENCES\n\nQuiet hours: {p['quiet_start']:02d}:00–{p['quiet_end']:02d}:00\nPayment/support messages are never blocked.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith("v26pref|"))
def v26pref_cb(c):
    key=c.data.split('|',1)[1];p=_user_pref(c.from_user.id)
    if key=='quiet':
        notification_prefs_col.update_one({'user_id':int(c.from_user.id)},{'$set':{'quiet_enabled':not p['quiet_enabled']}},upsert=True)
    else:
        notification_prefs_col.update_one({'user_id':int(c.from_user.id)},{'$set':{key:not p.get(key,True)}},upsert=True)
    bot.answer_callback_query(c.id,"Updated")

# -------------------------
# 💳 PAYMENT PROFILES + 🌍 COUNTRY OFFERS
# -------------------------
@bot.message_handler(func=lambda m:m.text=="💳 Payment Profiles" and is_admin(m.from_user.id))
def v26_payment_profiles(m):
    rows=list(payment_profiles_col.find({}).sort('name',1));kb=InlineKeyboardMarkup(row_width=1);kb.add(InlineKeyboardButton("➕ Add Profile",callback_data='v26payprof|add'))
    for r in rows:kb.add(InlineKeyboardButton(f"{r.get('name')} — {r.get('network')}",callback_data=f"v26payprof|view|{r['_id']}"))
    raw_bot.send_message(m.chat.id,"💳 PAYMENT PROFILES\n\nStore multiple receiving profiles/networks for admin use and localized checkout configuration.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith('v26payprof|') and is_admin(c.from_user.id))
def v26payprof_cb(c):
    p=c.data.split('|');
    if p[1]=='add':
        msg=raw_bot.send_message(c.from_user.id,"Send: NAME | COIN | NETWORK | ADDRESS | COUNTRY(or ALL)");bot.register_next_step_handler(msg,v26payprof_add);return bot.answer_callback_query(c.id)
    r=payment_profiles_col.find_one({'_id':ObjectId(p[2])}) if len(p)>2 else None
    if not r:return bot.answer_callback_query(c.id,"Not found",True)
    raw_bot.send_message(c.from_user.id,f"💳 {r.get('name')}\nCoin: {r.get('coin')}\nNetwork: {r.get('network')}\nAddress: {r.get('address')}\nCountry: {r.get('country')}");bot.answer_callback_query(c.id)

def v26payprof_add(m):
    q=[x.strip() for x in (m.text or '').split('|')]
    if len(q)<5:return raw_bot.send_message(m.chat.id,"❌ Use 5 fields separated by |")
    payment_profiles_col.insert_one({'name':q[0][:80],'coin':q[1][:30],'network':q[2][:50],'address':q[3][:300],'country':q[4][:50].upper(),'active':True,'created_at':time.time()})
    raw_bot.send_message(m.chat.id,"✅ Payment profile saved.",reply_markup=admin_menu())

@bot.message_handler(func=lambda m:m.text=="🌍 Country Offers" and is_admin(m.from_user.id))
def v26_country_offers(m):
    rows=list(country_offers_col.find({}).sort('country',1));kb=InlineKeyboardMarkup(row_width=1);kb.add(InlineKeyboardButton("➕ Add Country Offer",callback_data='v26country|add'))
    for r in rows:kb.add(InlineKeyboardButton(f"{r.get('country')} — {r.get('discount',0)}%",callback_data='v26noop'))
    raw_bot.send_message(m.chat.id,"🌍 COUNTRY OFFERS\n\nOptional localized discount/payment notes. Country is user-selected; never inferred from private location.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data=='v26country|add' and is_admin(c.from_user.id))
def v26country_add_start(c):
    msg=raw_bot.send_message(c.from_user.id,"Send: COUNTRY CODE | DISCOUNT % | NOTE");bot.register_next_step_handler(msg,v26country_add);bot.answer_callback_query(c.id)

def v26country_add(m):
    p=[x.strip() for x in (m.text or '').split('|')]
    if len(p)<3:return raw_bot.send_message(m.chat.id,"❌ Use COUNTRY | DISCOUNT | NOTE")
    try:d=max(0,min(90,float(p[1])))
    except:return raw_bot.send_message(m.chat.id,"❌ Invalid discount")
    country_offers_col.update_one({'country':p[0].upper()},{'$set':{'discount':d,'note':p[2][:500],'active':True,'updated_at':time.time()}},upsert=True)
    raw_bot.send_message(m.chat.id,"✅ Country offer saved.",reply_markup=admin_menu())

# -------------------------
# 🧾 TAX / BUSINESS FIELDS + TERMS
# -------------------------
@bot.message_handler(func=lambda m:m.text=="📑 Terms & Business" and is_admin(m.from_user.id))
def v26_terms_business(m):
    cfg=_adv_cfg();kb=InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("📑 Edit Terms",callback_data='v26legal|terms'))
    kb.add(InlineKeyboardButton("🏢 Business/Tax Fields",callback_data='v26legal|business'))
    kb.add(InlineKeyboardButton("🔁 Increase Terms Version",callback_data='v26legal|version'))
    raw_bot.send_message(m.chat.id,f"📑 TERMS & BUSINESS\n\nTerms version: {int(cfg.get('terms_version',1) or 1)}\nBusiness: {cfg.get('business_name','Not set')}\nTax/VAT: {cfg.get('tax_id','Not set')}",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith('v26legal|') and is_admin(c.from_user.id))
def v26legal_cb(c):
    what=c.data.split('|')[1]
    if what=='version':
        _adv_set('terms_version',int(_adv_cfg().get('terms_version',1) or 1)+1);bot.answer_callback_query(c.id,"Version increased");return
    prompt='Send complete purchase terms:' if what=='terms' else 'Send: BUSINESS NAME | EMAIL | COUNTRY | TAX/VAT ID (or N/A)'
    msg=raw_bot.send_message(c.from_user.id,prompt);bot.register_next_step_handler(msg,v26legal_save,what);bot.answer_callback_query(c.id)

def v26legal_save(m,what):
    if what=='terms':_adv_set('terms_text',(m.text or '')[:4000])
    else:
        p=[x.strip() for x in (m.text or '').split('|')]
        if len(p)<4:return raw_bot.send_message(m.chat.id,"❌ Send 4 fields separated by |")
        for k,v in zip(('business_name','business_email','business_country','tax_id'),p):_adv_set(k,v[:200])
    raw_bot.send_message(m.chat.id,"✅ Saved.",reply_markup=admin_menu())

# -------------------------
# 🔐 TWO-STEP ADMIN ACTIONS
# -------------------------
def _v26_create_confirmation(uid, action, payload=None, ttl=300):
    token=''.join(random.choices(string.ascii_uppercase+string.digits,k=6))
    admin_confirm_col.insert_one({'token':token,'user_id':int(uid),'action':action,'payload':payload or {},'expires_at':time.time()+ttl,'used':False,'created_at':time.time()})
    return token

def _v26_confirm_token(uid,token,action=None):
    q={'token':token.upper(),'user_id':int(uid),'used':False,'expires_at':{'$gt':time.time()}}
    if action:q['action']=action
    r=admin_confirm_col.find_one(q)
    if r:admin_confirm_col.update_one({'_id':r['_id']},{'$set':{'used':True,'used_at':time.time()}})
    return r

@bot.message_handler(func=lambda m:m.text=="🔐 Admin Safety" and is_admin(m.from_user.id))
def v26_admin_safety(m):
    raw_bot.send_message(m.chat.id,"🔐 ADMIN SAFETY\n\nSensitive V26 actions use confirmation codes that expire after 5 minutes. Existing destructive controls keep their original confirmations.\n\nUse this section as the audit point for high-risk operations.")

# -------------------------
# 📣 BROADCAST TEMPLATES
# -------------------------
@bot.message_handler(func=lambda m:m.text=="📣 Broadcast Templates" and is_admin(m.from_user.id))
def v26_templates(m):
    rows=list(broadcast_templates_col.find({}).sort('created_at',-1).limit(20));kb=InlineKeyboardMarkup(row_width=1);kb.add(InlineKeyboardButton("➕ Save Template",callback_data='v26tpl|add'))
    for r in rows:kb.add(InlineKeyboardButton(r.get('name','Template')[:50],callback_data=f"v26tpl|view|{r['_id']}"))
    raw_bot.send_message(m.chat.id,"📣 BROADCAST TEMPLATES\n\nReusable announcements for methods, VIP offers, restocks and updates.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith('v26tpl|') and is_admin(c.from_user.id))
def v26tpl_cb(c):
    p=c.data.split('|')
    if p[1]=='add':
        msg=raw_bot.send_message(c.from_user.id,"Send: TEMPLATE NAME | MESSAGE");bot.register_next_step_handler(msg,v26tpl_add);return bot.answer_callback_query(c.id)
    r=broadcast_templates_col.find_one({'_id':ObjectId(p[2])}) if len(p)>2 else None
    if r:
        kb=InlineKeyboardMarkup();kb.add(InlineKeyboardButton("📋 Copy",copy_text=telebot.types.CopyTextButton(text=str(r.get('message',''))[:256]) if hasattr(telebot.types,'CopyTextButton') else None)) if False else None
        raw_bot.send_message(c.from_user.id,f"📣 {r.get('name')}\n\n{r.get('message')}")
    bot.answer_callback_query(c.id)

def v26tpl_add(m):
    p=[x.strip() for x in (m.text or '').split('|',1)]
    if len(p)<2:return raw_bot.send_message(m.chat.id,"❌ Send NAME | MESSAGE")
    broadcast_templates_col.insert_one({'name':p[0][:80],'message':p[1][:3500],'created_at':time.time(),'created_by':m.from_user.id})
    raw_bot.send_message(m.chat.id,"✅ Template saved.",reply_markup=admin_menu())

# -------------------------
# ⏱ SCHEDULED CONTENT DROPS
# -------------------------
@bot.message_handler(func=lambda m:m.text=="⏱ Scheduled Drops" and is_admin(m.from_user.id))
def v26_scheduled_drops(m):
    rows=list(scheduled_drops_col.find({'status':'pending'}).sort('run_at',1).limit(20));kb=InlineKeyboardMarkup(row_width=1);kb.add(InlineKeyboardButton("➕ Schedule Drop",callback_data='v26drop|add'))
    for r in rows:
        when=datetime.fromtimestamp(r.get('run_at',0)).strftime('%Y-%m-%d %H:%M');kb.add(InlineKeyboardButton(f"{when} — {r.get('title','Drop')[:25]}",callback_data=f"v26drop|view|{r['_id']}"))
    raw_bot.send_message(m.chat.id,"⏱ SCHEDULED CONTENT DROPS\n\nSchedule a bot announcement to all users. Use YYYY-MM-DD HH:MM in server/local bot time.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith('v26drop|') and is_admin(c.from_user.id))
def v26drop_cb(c):
    p=c.data.split('|')
    if p[1]=='add':
        msg=raw_bot.send_message(c.from_user.id,"Send: YYYY-MM-DD HH:MM | TITLE | MESSAGE");bot.register_next_step_handler(msg,v26drop_add);return bot.answer_callback_query(c.id)
    r=scheduled_drops_col.find_one({'_id':ObjectId(p[2])}) if len(p)>2 else None
    if r:raw_bot.send_message(c.from_user.id,f"⏱ {r.get('title')}\n{datetime.fromtimestamp(r.get('run_at',0))}\nStatus: {r.get('status')}\n\n{r.get('message')}")
    bot.answer_callback_query(c.id)

def v26drop_add(m):
    p=[x.strip() for x in (m.text or '').split('|',2)]
    if len(p)<3:return raw_bot.send_message(m.chat.id,"❌ Send DATE TIME | TITLE | MESSAGE")
    try:run_at=datetime.strptime(p[0],'%Y-%m-%d %H:%M').timestamp()
    except:return raw_bot.send_message(m.chat.id,"❌ Date format must be YYYY-MM-DD HH:MM")
    scheduled_drops_col.insert_one({'run_at':run_at,'title':p[1][:120],'message':p[2][:3500],'status':'pending','created_at':time.time(),'created_by':m.from_user.id})
    raw_bot.send_message(m.chat.id,"✅ Drop scheduled.",reply_markup=admin_menu())

def _v26_drop_worker():
    while True:
        try:
            now=time.time();rows=list(scheduled_drops_col.find({'status':'pending','run_at':{'$lte':now}}).limit(5))
            for r in rows:
                claimed=scheduled_drops_col.update_one({'_id':r['_id'],'status':'pending'},{'$set':{'status':'sending','started_at':time.time()}})
                if claimed.modified_count!=1:continue
                sent=0;failed=0
                for u in users_col.find({'banned':{'$ne':True}},{'_id':1}):
                    try:
                        if _in_quiet_hours(int(u['_id'])):continue
                        raw_bot.send_message(int(u['_id']),f"📢 {r.get('title')}\n\n{r.get('message')}");sent+=1
                    except Exception:failed+=1
                    time.sleep(0.03)
                scheduled_drops_col.update_one({'_id':r['_id']},{'$set':{'status':'sent','sent_at':time.time(),'sent_count':sent,'failed_count':failed}})
        except Exception as exc:
            log_event('scheduled_drop_worker_error',details={'error':str(exc)},level='error')
        time.sleep(30)
threading.Thread(target=_v26_drop_worker,daemon=True).start()

# -------------------------
# 🧩 BUNDLES + DYNAMIC DISCOUNTS
# -------------------------
@bot.message_handler(func=lambda m:m.text=="🧩 Bundles")
def v26_bundles_user(m):
    rows=list(bundles_col.find({'active':True}).sort('created_at',-1));kb=InlineKeyboardMarkup(row_width=1)
    for r in rows:kb.add(InlineKeyboardButton(f"{r.get('name')} — ${float(r.get('price',0)):g}",callback_data=f"v26bundle|show|{r['_id']}"))
    raw_bot.send_message(m.chat.id,"🧩 BUNDLES\n\nSave more by purchasing grouped offers.",reply_markup=kb if rows else None)

@bot.message_handler(func=lambda m:m.text=="🧩 Bundle Manager" and is_admin(m.from_user.id))
def v26_bundle_admin(m):
    rows=list(bundles_col.find({}).sort('created_at',-1));kb=InlineKeyboardMarkup(row_width=1);kb.add(InlineKeyboardButton("➕ Add Bundle",callback_data='v26bundle|add'))
    for r in rows:kb.add(InlineKeyboardButton(("✅ " if r.get('active',True) else "🙈 ")+r.get('name','Bundle')[:40],callback_data=f"v26bundle|show|{r['_id']}"))
    raw_bot.send_message(m.chat.id,"🧩 BUNDLE MANAGER\n\nCreate combined offers. Dynamic discount is calculated against the regular total you enter.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith('v26bundle|'))
def v26bundle_cb(c):
    p=c.data.split('|');action=p[1]
    if action=='add' and is_admin(c.from_user.id):
        msg=raw_bot.send_message(c.from_user.id,"Send: NAME | CONTENTS/DESCRIPTION | REGULAR TOTAL | BUNDLE PRICE");bot.register_next_step_handler(msg,v26bundle_add);return bot.answer_callback_query(c.id)
    if action=='show':
        r=bundles_col.find_one({'_id':ObjectId(p[2])})
        if not r:return bot.answer_callback_query(c.id,"Not found",True)
        regular=float(r.get('regular_total',0));price=float(r.get('price',0));save=max(0,regular-price);pct=(save/regular*100) if regular>0 else 0
        text=f"🧩 {r.get('name')}\n\n{r.get('description')}\n\nRegular: ${regular:g}\nBundle: ${price:g}\nYou save: ${save:g} ({pct:.0f}%)"
        kb=InlineKeyboardMarkup(row_width=1)
        if is_admin(c.from_user.id):kb.add(InlineKeyboardButton("👁 Toggle Active",callback_data=f"v26bundle|toggle|{r['_id']}"))
        else:kb.add(InlineKeyboardButton("💬 Buy / Ask Admin",callback_data='support_start'))
        raw_bot.send_message(c.from_user.id,text,reply_markup=kb);return bot.answer_callback_query(c.id)
    if action=='toggle' and is_admin(c.from_user.id):
        r=bundles_col.find_one({'_id':ObjectId(p[2])});bundles_col.update_one({'_id':r['_id']},{'$set':{'active':not r.get('active',True)}});return bot.answer_callback_query(c.id,"Updated")

def v26bundle_add(m):
    p=[x.strip() for x in (m.text or '').split('|')]
    if len(p)<4:return raw_bot.send_message(m.chat.id,"❌ Use 4 fields separated by |")
    try:regular=float(p[2]);price=float(p[3])
    except:return raw_bot.send_message(m.chat.id,"❌ Invalid prices")
    bundles_col.insert_one({'name':p[0][:100],'description':p[1][:1200],'regular_total':regular,'price':price,'active':True,'created_at':time.time()})
    raw_bot.send_message(m.chat.id,"✅ Bundle created.",reply_markup=admin_menu())

# -------------------------
# 💵 PROFIT TRACKING
# -------------------------
@bot.message_handler(func=lambda m:m.text=="💵 Profit Tracking" and is_admin(m.from_user.id))
def v26_profit(m):
    gross=0
    try:gross=sum(float(x.get('amount',0) or 0) for x in payments_col.find({'status':{'$in':['paid','approved']}}))
    except Exception:pass
    expenses=sum(float(x.get('amount',0) or 0) for x in profit_entries_col.find({'type':'expense'}))
    costs=sum(float(x.get('amount',0) or 0) for x in profit_entries_col.find({'type':'cost'}))
    net=gross-expenses-costs
    kb=InlineKeyboardMarkup(row_width=2);kb.add(InlineKeyboardButton("➖ Add Expense",callback_data='v26profit|expense'),InlineKeyboardButton("📦 Add Cost",callback_data='v26profit|cost'))
    raw_bot.send_message(m.chat.id,f"💵 PROFIT TRACKING\n\nGross paid revenue: ${gross:,.2f}\nExpenses: ${expenses:,.2f}\nProduct/other costs: ${costs:,.2f}\nEstimated net: ${net:,.2f}",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith('v26profit|') and is_admin(c.from_user.id))
def v26profit_cb(c):
    typ=c.data.split('|')[1];msg=raw_bot.send_message(c.from_user.id,"Send: AMOUNT | NOTE");bot.register_next_step_handler(msg,v26profit_add,typ);bot.answer_callback_query(c.id)

def v26profit_add(m,typ):
    p=[x.strip() for x in (m.text or '').split('|',1)]
    try:amt=float(p[0])
    except:return raw_bot.send_message(m.chat.id,"❌ Invalid amount")
    profit_entries_col.insert_one({'type':typ,'amount':max(0,amt),'note':p[1][:500] if len(p)>1 else '','created_at':time.time(),'created_by':m.from_user.id})
    raw_bot.send_message(m.chat.id,"✅ Entry saved.",reply_markup=admin_menu())

# -------------------------
# 👥 CUSTOMER NOTES + TAGS + ADVANCED SEARCH
# -------------------------
def _v26_find_user(q):
    q=(q or '').strip()
    if q.startswith('@'):q=q[1:]
    if q.isdigit():return users_col.find_one({'_id':q})
    return users_col.find_one({'username':{'$regex':f'^{re.escape(q)}$','$options':'i'}})

@bot.message_handler(func=lambda m:m.text=="🔎 Advanced User Search" and is_admin(m.from_user.id))
def v26_user_search(m):
    msg=raw_bot.send_message(m.chat.id,"Send user ID or @username:");bot.register_next_step_handler(msg,v26_user_search_do)

def v26_user_search_do(m):
    u=_v26_find_user(m.text)
    if not u:return raw_bot.send_message(m.chat.id,"❌ User not found.",reply_markup=admin_menu())
    uid=int(u['_id']);notes=customer_notes_col.count_documents({'user_id':uid});tags=[x.get('tag') for x in user_tags_col.find({'user_id':uid})]
    paid=payments_col.count_documents({'user_id':uid,'status':{'$in':['paid','approved']}})
    kb=InlineKeyboardMarkup(row_width=2);kb.add(InlineKeyboardButton("📝 Add Note",callback_data=f"v26user|note|{uid}"),InlineKeyboardButton("🏷 Add Tag",callback_data=f"v26user|tag|{uid}"));kb.add(InlineKeyboardButton("🧪 Toggle Test User",callback_data=f"v26user|test|{uid}"))
    raw_bot.send_message(m.chat.id,f"👤 USER PROFILE\n\nID: {uid}\nUsername: @{u.get('username') or 'none'}\nVIP: {'Yes' if User(uid).is_vip() else 'No'}\nPoints: {u.get('points',0)}\nPaid records: {paid}\nNotes: {notes}\nTags: {', '.join(tags) if tags else 'None'}",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith('v26user|') and is_admin(c.from_user.id))
def v26user_cb(c):
    p=c.data.split('|');what=p[1];uid=int(p[2])
    if what=='test':
        r=test_users_col.find_one({'user_id':uid})
        if r:test_users_col.delete_one({'_id':r['_id']});msg='Removed from test users'
        else:test_users_col.insert_one({'user_id':uid,'created_at':time.time(),'added_by':c.from_user.id});msg='Marked as test user'
        return bot.answer_callback_query(c.id,msg,True)
    prompt='Send private customer note:' if what=='note' else 'Send tag name:'
    msg=raw_bot.send_message(c.from_user.id,prompt);bot.register_next_step_handler(msg,v26user_save,what,uid);bot.answer_callback_query(c.id)

def v26user_save(m,what,uid):
    val=(m.text or '').strip()[:500]
    if what=='note':customer_notes_col.insert_one({'user_id':uid,'note':val,'created_at':time.time(),'admin_id':m.from_user.id})
    else:user_tags_col.update_one({'user_id':uid,'tag':val},{'$set':{'created_at':time.time(),'admin_id':m.from_user.id}},upsert=True)
    raw_bot.send_message(m.chat.id,"✅ Saved.",reply_markup=admin_menu())

# -------------------------
# 📤 EXPORT CENTER
# -------------------------
@bot.message_handler(func=lambda m:m.text=="📤 Export Center" and is_admin(m.from_user.id))
def v26_export_center(m):
    kb=InlineKeyboardMarkup(row_width=2)
    for key,label in [('users','Users'),('vip','VIP'),('payments','Payments'),('orders','Orders'),('referrals','Referrals'),('notes','Notes'),('tags','Tags')]:kb.add(InlineKeyboardButton(label,callback_data=f'v26export|{key}'))
    raw_bot.send_message(m.chat.id,"📤 EXPORT CENTER\n\nExport operational data to CSV.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith('v26export|') and is_admin(c.from_user.id))
def v26export_cb(c):
    key=c.data.split('|')[1];mapping={
        'users':(users_col,{}),'vip':(users_col,{'vip':True}),'payments':(payments_col,{}),'orders':(shop_orders_col,{}),'referrals':(users_col,{'ref':{'$ne':None}}),'notes':(customer_notes_col,{}),'tags':(user_tags_col,{})}
    col,q=mapping[key];rows=list(col.find(q).limit(50000))
    if not rows:return bot.answer_callback_query(c.id,"No data",True)
    fields=sorted(set().union(*(r.keys() for r in rows)));buf=io.StringIO();w=csv.DictWriter(buf,fieldnames=fields);w.writeheader()
    for r in rows:w.writerow({k:(json.dumps(v,default=str,ensure_ascii=False) if isinstance(v,(dict,list)) else str(v)) for k,v in r.items()})
    data=io.BytesIO(buf.getvalue().encode('utf-8'));data.name=f'globexomart_{key}_{datetime.now().strftime("%Y%m%d")}.csv';raw_bot.send_document(c.from_user.id,data,caption=f"📤 {key.title()} export");bot.answer_callback_query(c.id,"Exported")

# -------------------------
# 🧪 SANDBOX / TEST MODE
# -------------------------
@bot.message_handler(func=lambda m:m.text=="🧪 Sandbox Mode" and is_admin(m.from_user.id))
def v26_sandbox(m):
    rows=list(test_users_col.find({}).limit(50));kb=InlineKeyboardMarkup(row_width=1);kb.add(InlineKeyboardButton("➕ Add Test User",callback_data='v26test|add'))
    for r in rows:kb.add(InlineKeyboardButton(f"🧪 {r.get('user_id')}",callback_data=f"v26test|del|{r['_id']}"))
    raw_bot.send_message(m.chat.id,"🧪 SANDBOX / TEST MODE\n\nTest users are tagged so admins can exclude their activity from manual reporting/cleanup. Existing payment flows remain intact.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith('v26test|') and is_admin(c.from_user.id))
def v26test_cb(c):
    p=c.data.split('|')
    if p[1]=='add':msg=raw_bot.send_message(c.from_user.id,"Send user ID or @username:");bot.register_next_step_handler(msg,v26test_add);return bot.answer_callback_query(c.id)
    test_users_col.delete_one({'_id':ObjectId(p[2])});bot.answer_callback_query(c.id,"Removed")

def v26test_add(m):
    u=_v26_find_user(m.text)
    if not u:return raw_bot.send_message(m.chat.id,"❌ User not found")
    test_users_col.update_one({'user_id':int(u['_id'])},{'$set':{'created_at':time.time(),'added_by':m.from_user.id}},upsert=True);raw_bot.send_message(m.chat.id,"✅ Test user added.",reply_markup=admin_menu())

# -------------------------
# 🔄 VERSION / UPDATE MANAGER
# -------------------------
BOT_BUILD_VERSION="26.0"
@bot.message_handler(func=lambda m:m.text=="🔄 Version Manager" and is_admin(m.from_user.id))
def v26_version(m):
    cfg=_adv_cfg();raw_bot.send_message(m.chat.id,f"🔄 VERSION / UPDATE MANAGER\n\nBot: {_brand_name()}\nBuild: v{BOT_BUILD_VERSION}\nDatabase: {db.name}\nMaintenance: {'ON' if get_config().get('maintenance_mode') else 'OFF'}\nLast changelog note: {cfg.get('changelog','V26 advanced growth and operations suite')}\n\nUse /setchangelog <text> to update the admin note.")

@bot.message_handler(commands=['setchangelog'])
def v26_set_changelog(m):
    if not is_admin(m.from_user.id):return
    txt=(m.text or '').partition(' ')[2].strip();_adv_set('changelog',txt[:1000]);raw_bot.send_message(m.chat.id,"✅ Changelog note updated.")

# -------------------------
# ❤️ HEALTH CHECK + 🚦 QUEUE MONITOR
# -------------------------
@bot.message_handler(func=lambda m:m.text=="❤️ Health Check" and is_admin(m.from_user.id))
def v26_health(m):
    checks=[]
    try:client.admin.command('ping');checks.append('✅ MongoDB')
    except Exception as e:checks.append(f'❌ MongoDB: {e}')
    try:me=bot.get_me();checks.append(f'✅ Telegram API (@{me.username})')
    except Exception as e:checks.append(f'❌ Telegram API: {e}')
    cfg=get_cached_config();checks.append('✅ Payment address configured' if (cfg.get('usdt_address') or cfg.get('binance_address')) else '⚠️ No main payment address')
    checks.append(f"✅ Force-join targets: {len(cfg.get('force_channels',[]))+len(cfg.get('force_groups',[]))}")
    checks.append('✅ Proof channel configured' if cfg.get('proof_channel') else '⚠️ Proof channel not configured')
    pending=payments_col.count_documents({'status':'pending'});checks.append(f'ℹ️ Pending payments: {pending}')
    raw_bot.send_message(m.chat.id,"❤️ SYSTEM HEALTH\n\n"+'\n'.join(checks))

@bot.message_handler(func=lambda m:m.text=="🚦 Queue Monitor" and is_admin(m.from_user.id))
def v26_queue(m):
    counts={
        'Pending payments':payments_col.count_documents({'status':'pending'}),
        'Pending withdrawals':wallet_tx_col.count_documents({'type':'withdraw','status':'pending'}),
        'Pending methods':pending_methods_col.count_documents({'status':'pending'}),
        'Open chats':support_chats_col.count_documents({'status':{'$ne':'closed'}}),
        'Open tickets':db['support_tickets'].count_documents({'status':{'$in':['open','answered']}}) if 'support_tickets' in db.list_collection_names() else 0,
        'Scheduled drops':scheduled_drops_col.count_documents({'status':'pending'}),
    }
    raw_bot.send_message(m.chat.id,"🚦 QUEUE MONITOR\n\n"+'\n'.join(f"• {k}: {v}" for k,v in counts.items()))

# -------------------------
# 💬 SAVED ADMIN REPLIES
# -------------------------
@bot.message_handler(func=lambda m:m.text=="💬 Saved Replies" and is_admin(m.from_user.id))
def v26_saved_replies(m):
    rows=list(saved_replies_col.find({}).sort('title',1));kb=InlineKeyboardMarkup(row_width=1);kb.add(InlineKeyboardButton("➕ Add Reply",callback_data='v26reply|add'))
    for r in rows:kb.add(InlineKeyboardButton(r.get('title','Reply')[:50],callback_data=f"v26reply|show|{r['_id']}"))
    raw_bot.send_message(m.chat.id,"💬 SAVED ADMIN REPLIES\n\nQuick templates for payment issues, VIP questions, refunds, scanners and support.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data.startswith('v26reply|') and is_admin(c.from_user.id))
def v26reply_cb(c):
    p=c.data.split('|')
    if p[1]=='add':msg=raw_bot.send_message(c.from_user.id,"Send: TITLE | REPLY TEXT");bot.register_next_step_handler(msg,v26reply_add);return bot.answer_callback_query(c.id)
    r=saved_replies_col.find_one({'_id':ObjectId(p[2])}) if len(p)>2 else None
    if r:raw_bot.send_message(c.from_user.id,f"💬 {r.get('title')}\n\n{r.get('text')}")
    bot.answer_callback_query(c.id)

def v26reply_add(m):
    p=[x.strip() for x in (m.text or '').split('|',1)]
    if len(p)<2:return raw_bot.send_message(m.chat.id,"❌ Send TITLE | REPLY TEXT")
    saved_replies_col.insert_one({'title':p[0][:100],'text':p[1][:3500],'created_at':time.time(),'created_by':m.from_user.id});raw_bot.send_message(m.chat.id,"✅ Saved reply added.",reply_markup=admin_menu())

# Apply admin-configured payment profiles to checkout instructions.
_v26_usdt_instructions_original = _usdt_instructions
def _usdt_instructions(amount):
    rows=list(payment_profiles_col.find({'active':{'$ne':False}}).sort('created_at',1))
    if not rows:
        return _v26_usdt_instructions_original(amount)
    lines=[f"💳 PAYMENT OPTIONS\nAmount: ${float(amount):g} USDT"]
    for r in rows[:8]:
        country=str(r.get('country') or 'ALL').upper()
        lines.append(f"\n• {r.get('name')} ({r.get('coin')} / {r.get('network')})\n  {r.get('address')}\n  Region: {country}")
    lines.append("\nSend the exact amount using one active option, then submit the transaction ID and screenshot where requested.")
    return '\n'.join(lines)

# -------------------------
# 🚀 ADVANCED CONTROLS HUB
# -------------------------
@bot.message_handler(func=lambda m:m.text=="🚀 Advanced Controls" and is_admin(m.from_user.id))
def v26_advanced_hub(m):
    kb=ReplyKeyboardMarkup(resize_keyboard=True,row_width=2)
    rows=[
        ("🎨 Theme Manager","📌 Promotions"),("💳 Payment Profiles","🌍 Country Offers"),("📑 Terms & Business","🔐 Admin Safety"),
        ("📣 Broadcast Templates","⏱ Scheduled Drops"),("🧩 Bundle Manager","💵 Profit Tracking"),("🔎 Advanced User Search","📤 Export Center"),
        ("🧪 Sandbox Mode","🔄 Version Manager"),("❤️ Health Check","🚦 Queue Monitor"),("💬 Saved Replies","⚙️ ADMIN PANEL")]
    for r in rows:kb.row(*r)
    raw_bot.send_message(m.chat.id,"🚀 ADVANCED CONTROLS\n\nGrowth, branding, operations, safety and analytics tools.",reply_markup=kb)

# Ensure admin menu exposes the hub without removing any existing rows.
_v26_admin_menu_original=admin_menu
def admin_menu():
    kb=_v26_admin_menu_original()
    try:
        # Insert near the bottom while preserving every old button.
        kb.row("🚀 Advanced Controls")
    except Exception:pass
    return kb


# =========================
# 🧭 V27 PROFESSIONAL OPERATIONS + CUSTOMER INTELLIGENCE
# =========================
# Adds customer intelligence, command center, finance/ledger, safety, search,
# VIP tools, reports and menu layout controls without removing older systems.

customer_activity_col = db["customer_activity"]
search_analytics_col = db["search_analytics"]
personal_offers_col = db["personal_offers"]
financial_ledger_col = db["financial_ledger"]
refunds_col = db["refunds"]
admin_audit_col = db["admin_audit"]
rate_limit_col = db["rate_limits"]
method_meta_col = db["method_meta"]
admin_sessions_col = db["admin_sessions"]
priority_alerts_col = db["priority_alerts"]
report_runs_col = db["report_runs"]

for _c, _idx in [
    (customer_activity_col, [("user_id",1),("created_at",-1)]),
    (search_analytics_col, [("created_at",-1)]),
    (personal_offers_col, [("user_id",1),("active",1),("expires_at",1)]),
    (financial_ledger_col, [("created_at",-1),("user_id",1)]),
    (refunds_col, [("created_at",-1),("status",1)]),
    (admin_audit_col, [("created_at",-1),("admin_id",1)]),
    (method_meta_col, [("folder_id",1)]),
]:
    try: _c.create_index(_idx)
    except Exception: pass

# Add two compact user hubs; admin can reorder/hide them using existing controls.
for _b in ("🧭 My Hub", "🔎 Filter Search"):
    if _b not in MAIN_MENU_BUTTONS:
        MAIN_MENU_ROWS.append((_b,))
        MAIN_MENU_BUTTONS.append(_b)


def _v27_audit(admin_id, action, target=None, details=None):
    try:
        admin_audit_col.insert_one({"admin_id":int(admin_id),"action":str(action),"target":target,"details":details or {},"created_at":time.time()})
    except Exception: pass


def _v27_activity(uid, event, **details):
    try:
        customer_activity_col.insert_one({"user_id":int(uid),"event":str(event),"details":details,"created_at":time.time()})
    except Exception: pass


def _v27_user_doc(uid):
    return users_col.find_one({"_id":str(uid)}) or users_col.find_one({"_id":int(uid)}) or {}


def _v27_username(u):
    name=u.get("username") or u.get("first_name") or str(u.get("_id","Unknown"))
    return f"@{name}" if u.get("username") else str(name)


def _v27_lifetime_spend(uid):
    uid=int(uid); total=0.0
    for col in (payments_col, shop_orders_col, item_purchases_col):
        try:
            for r in col.find({"user_id":uid,"status":{"$in":["approved","paid","delivered","completed"]}}):
                total += float(r.get("amount", r.get("price", r.get("total",0))) or 0)
        except Exception: pass
    return total


def _v27_value_tier(uid):
    u=_v27_user_doc(uid); spend=_v27_lifetime_spend(uid)
    orders=shop_orders_col.count_documents({"user_id":int(uid),"status":{"$in":["paid","delivered","completed"]}})
    refs=int(u.get("refs",0) or 0)
    if u.get("vip") and spend>=100: return "💎 High-Value VIP"
    if spend>=100 or orders>=5: return "🏆 High Value"
    if orders>=2 or spend>=40: return "🔁 Repeat Buyer"
    if refs>=10: return "🤝 Affiliate / Referrer"
    if spend>0: return "✅ Customer"
    return "🌱 New / Free User"


def _v27_rate_allowed(uid, action, limit=4, window=600):
    now=time.time(); key=f"{int(uid)}:{action}"
    row=rate_limit_col.find_one({"_id":key}) or {}
    start=float(row.get("window_start",now)); count=int(row.get("count",0) or 0)
    if now-start>window:
        rate_limit_col.update_one({"_id":key},{"$set":{"window_start":now,"count":1}},upsert=True); return True
    if count>=limit: return False
    rate_limit_col.update_one({"_id":key},{"$inc":{"count":1},"$setOnInsert":{"window_start":now}},upsert=True); return True


def _v27_priority_alert(kind, text, user_id=None, amount=0):
    cfg=_adv_cfg(); enabled=cfg.get("priority_alerts",True)
    if not enabled:return
    threshold=float(cfg.get("large_purchase_alert",50) or 50)
    if kind=="large_purchase" and float(amount or 0)<threshold:return
    try:
        raw_bot.send_message(ADMIN_ID, f"🚨 PRIORITY ALERT\n\n{text}")
    except Exception: pass


def _v27_ledger(kind, amount, user_id=None, order_id=None, note=None, admin_id=None, direction=None):
    try:
        amt=float(amount or 0)
        if direction is None: direction="credit" if amt>=0 else "debit"
        financial_ledger_col.insert_one({"kind":kind,"amount":abs(amt),"direction":direction,"user_id":int(user_id) if user_id is not None else None,"order_id":str(order_id) if order_id else None,"note":note,"admin_id":int(admin_id) if admin_id else None,"created_at":time.time()})
    except Exception: pass


def _v27_admin_pin_hash(pin):
    return hashlib.sha256((str(ADMIN_ID)+":"+str(pin)).encode()).hexdigest()


def _v27_admin_session_ok(uid):
    if int(uid)!=int(ADMIN_ID): return False
    cfg=_adv_cfg(); h=cfg.get("admin_pin_hash")
    if not h:return True
    row=admin_sessions_col.find_one({"_id":int(uid)}) or {}
    return float(row.get("expires_at",0) or 0)>time.time()


def _v27_require_owner_session(uid):
    if int(uid)!=int(ADMIN_ID): return False
    if _v27_admin_session_ok(uid): return True
    raw_bot.send_message(uid,"🔐 Owner authorization required. Use /adminunlock YOUR_PIN to unlock sensitive controls for 15 minutes.")
    return False


@bot.message_handler(commands=["setadminpin"])
def v27_set_admin_pin(m):
    if int(m.from_user.id)!=int(ADMIN_ID):return
    pin=(m.text or "").partition(" ")[2].strip()
    if len(pin)<4:return raw_bot.send_message(m.chat.id,"Use /setadminpin followed by at least 4 characters.")
    _adv_set("admin_pin_hash",_v27_admin_pin_hash(pin));admin_sessions_col.delete_many({});_v27_audit(m.from_user.id,"set_admin_pin");raw_bot.send_message(m.chat.id,"✅ Admin PIN configured. Sensitive V27 controls now require a temporary unlock.")

@bot.message_handler(commands=["adminunlock"])
def v27_admin_unlock(m):
    if int(m.from_user.id)!=int(ADMIN_ID):return
    pin=(m.text or "").partition(" ")[2].strip();cfg=_adv_cfg()
    if not cfg.get("admin_pin_hash"):return raw_bot.send_message(m.chat.id,"No PIN is configured. Use /setadminpin first.")
    if _v27_admin_pin_hash(pin)!=cfg.get("admin_pin_hash"):return raw_bot.send_message(m.chat.id,"❌ Incorrect PIN.")
    admin_sessions_col.update_one({"_id":int(m.from_user.id)},{"$set":{"expires_at":time.time()+900}},upsert=True);raw_bot.send_message(m.chat.id,"🔓 Sensitive admin controls unlocked for 15 minutes.")

# -------------------------
# 👤 FULL CUSTOMER PROFILE + VALUE TIER
# -------------------------
def _v27_customer_profile_text(uid):
    u=_v27_user_doc(uid)
    if not u:return None
    spend=_v27_lifetime_spend(uid);vip=bool(u.get("vip"));exp=u.get("vip_expiry")
    expiry="Lifetime / no expiry" if vip and not exp else (datetime.fromtimestamp(float(exp)).strftime("%Y-%m-%d %H:%M") if exp else "—")
    pending=payments_col.count_documents({"user_id":int(uid),"status":"pending"})+wallet_tx_col.count_documents({"user_id":int(uid),"status":"pending"})
    orders=shop_orders_col.count_documents({"user_id":int(uid)})
    refs=int(u.get("refs",0) or 0);last=u.get("last_active")
    lasttxt=datetime.fromtimestamp(float(last)).strftime("%Y-%m-%d %H:%M") if last else "Unknown"
    tags=[r.get("tag") for r in user_tags_col.find({"user_id":int(uid)}).limit(20)] if 'user_tags_col' in globals() else []
    notes=customer_notes_col.count_documents({"user_id":int(uid)}) if 'customer_notes_col' in globals() else 0
    risk=(u.get("submissions_blocked") or u.get("banned") or u.get("muted"))
    return (f"👤 CUSTOMER PROFILE\n\nUser: {_v27_username(u)}\nID: {uid}\nTier: {_v27_value_tier(uid)}\nVIP: {'✅ Active' if vip else '❌ No'}\nVIP expiry: {expiry}\nLifetime spend: ${spend:.2f}\nOrders: {orders}\nPending requests: {pending}\nReferrals: {refs}\nPoints: {int(u.get('points',0) or 0):,}\nUSDT balance: ${float(u.get('usdt_balance',0) or 0):.2f}\nLast active: {lasttxt}\nTags: {', '.join(tags) if tags else '—'}\nAdmin notes: {notes}\nRisk/blocked: {'⚠️ Yes' if risk else '✅ Clear'}")


def _v27_profile_markup(uid):
    kb=InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("💬 Open Chat",callback_data=f"v27prof|chat|{uid}"),InlineKeyboardButton("📝 Add Note",callback_data=f"v27prof|note|{uid}"))
    kb.add(InlineKeyboardButton("🏷 Add Tag",callback_data=f"v27prof|tag|{uid}"),InlineKeyboardButton("🎫 Personal Offer",callback_data=f"v27prof|offer|{uid}"))
    kb.add(InlineKeyboardButton("💎 VIP Controls",callback_data=f"v27prof|vip|{uid}"),InlineKeyboardButton("🧾 Activity",callback_data=f"v27prof|activity|{uid}"))
    return kb

@bot.message_handler(commands=["customer"])
def v27_customer_command(m):
    if not is_admin(m.from_user.id):return
    q=(m.text or "").partition(" ")[2].strip()
    if not q:return raw_bot.send_message(m.chat.id,"Use /customer USER_ID or /customer @username")
    u=_v26_find_user(q) if '_v26_find_user' in globals() else None
    if not u:return raw_bot.send_message(m.chat.id,"❌ User not found.")
    uid=int(u['_id']);raw_bot.send_message(m.chat.id,_v27_customer_profile_text(uid),reply_markup=_v27_profile_markup(uid))

@bot.callback_query_handler(func=lambda c:str(c.data or '').startswith('v27prof|') and is_admin(c.from_user.id))
def v27_profile_cb(c):
    p=c.data.split('|');action=p[1];uid=int(p[2]);bot.answer_callback_query(c.id)
    if action=='activity':
        rows=list(customer_activity_col.find({'user_id':uid}).sort('created_at',-1).limit(20));txt='🧾 RECENT ACTIVITY\n\n'+('\n'.join(f"• {datetime.fromtimestamp(r['created_at']).strftime('%m-%d %H:%M')} — {r.get('event')}" for r in rows) if rows else 'No tracked V27 activity yet.');return raw_bot.send_message(c.from_user.id,txt)
    if action=='note':
        msg=raw_bot.send_message(c.from_user.id,f"Send private admin note for user {uid}:");return bot.register_next_step_handler(msg,v27_profile_note_step,uid)
    if action=='tag':
        msg=raw_bot.send_message(c.from_user.id,f"Send tag for user {uid} (example: High Value, VIP Lead):");return bot.register_next_step_handler(msg,v27_profile_tag_step,uid)
    if action=='offer':
        msg=raw_bot.send_message(c.from_user.id,"Send personal offer as: TITLE | DISCOUNT% | HOURS | MESSAGE");return bot.register_next_step_handler(msg,v27_profile_offer_step,uid)
    if action=='vip':
        u=_v27_user_doc(uid);kb=InlineKeyboardMarkup(row_width=2)
        if u.get('vip'):
            kb.add(InlineKeyboardButton("⏸ Freeze VIP",callback_data=f"v27vipadm|freeze|{uid}"),InlineKeyboardButton("▶️ Resume VIP",callback_data=f"v27vipadm|resume|{uid}"))
        kb.add(InlineKeyboardButton("➕ Add 30 Days",callback_data=f"v27vipadm|add30|{uid}"));return raw_bot.send_message(c.from_user.id,f"💎 VIP CONTROLS — {uid}",reply_markup=kb)
    if action=='chat':
        return raw_bot.send_message(c.from_user.id,f"💬 Use your existing Chats section to open user {uid}. Full profile remains available with /customer {uid}.")

def v27_profile_note_step(m,uid):
    txt=(m.text or '').strip()
    if txt:customer_notes_col.insert_one({'user_id':int(uid),'text':txt[:3000],'created_at':time.time(),'created_by':m.from_user.id});_v27_audit(m.from_user.id,'customer_note',uid)
    raw_bot.send_message(m.chat.id,"✅ Note saved.",reply_markup=admin_menu())

def v27_profile_tag_step(m,uid):
    tag=(m.text or '').strip()[:80]
    if tag:user_tags_col.update_one({'user_id':int(uid),'tag':tag},{'$set':{'created_at':time.time(),'created_by':m.from_user.id}},upsert=True);_v27_audit(m.from_user.id,'user_tag',uid,{'tag':tag})
    raw_bot.send_message(m.chat.id,"✅ Tag saved.",reply_markup=admin_menu())

def v27_profile_offer_step(m,uid):
    try:
        title,disc,hours,msg=[x.strip() for x in (m.text or '').split('|',3)];disc=max(0,min(100,float(disc)));hours=max(1,min(720,float(hours)))
    except Exception:return raw_bot.send_message(m.chat.id,"❌ Use: TITLE | DISCOUNT% | HOURS | MESSAGE")
    oid=personal_offers_col.insert_one({'user_id':int(uid),'title':title[:100],'discount_percent':disc,'message':msg[:1500],'active':True,'created_at':time.time(),'expires_at':time.time()+hours*3600,'created_by':m.from_user.id}).inserted_id
    kb=InlineKeyboardMarkup();kb.add(InlineKeyboardButton("⭐ View VIP",callback_data='get_vip'))
    try:raw_bot.send_message(uid,f"🎫 PERSONAL OFFER\n\n{title}\n{msg}\n\nDiscount: {disc:g}%\nValid for: {hours:g} hour(s)",reply_markup=kb)
    except Exception:pass
    _v27_audit(m.from_user.id,'personal_offer',uid,{'offer_id':str(oid),'discount':disc});raw_bot.send_message(m.chat.id,"✅ Personal offer created and sent.",reply_markup=admin_menu())

@bot.callback_query_handler(func=lambda c:str(c.data or '').startswith('v27vipadm|') and is_admin(c.from_user.id))
def v27_vip_admin_cb(c):
    p=c.data.split('|');act=p[1];uid=int(p[2]);u=_v27_user_doc(uid);now=time.time()
    if not u:return bot.answer_callback_query(c.id,"User not found",True)
    if act=='freeze':
        exp=u.get('vip_expiry');remaining=max(0,float(exp)-now) if exp else 0;users_col.update_one({'_id':str(uid)},{'$set':{'vip_frozen':True,'vip_frozen_remaining':remaining,'vip_frozen_at':now,'vip':False}});_v27_audit(c.from_user.id,'freeze_vip',uid);return bot.answer_callback_query(c.id,"VIP frozen",True)
    if act=='resume':
        remaining=float(u.get('vip_frozen_remaining',0) or 0);upd={'vip_frozen':False,'vip':True};upd['vip_expiry']=now+remaining if remaining>0 else None;users_col.update_one({'_id':str(uid)},{'$set':upd,'$unset':{'vip_frozen_remaining':'','vip_frozen_at':''}});_v27_audit(c.from_user.id,'resume_vip',uid);return bot.answer_callback_query(c.id,"VIP resumed",True)
    if act=='add30':
        exp=float(u.get('vip_expiry',0) or 0);base=max(now,exp);users_col.update_one({'_id':str(uid)},{'$set':{'vip':True,'vip_expiry':base+30*86400}});_v27_audit(c.from_user.id,'add_vip_days',uid,{'days':30});return bot.answer_callback_query(c.id,"30 days added",True)

# -------------------------
# 🧭 USER HUB: recent, continue, VIP upgrade, personal offers
# -------------------------
@bot.message_handler(func=lambda m:m.text=='🧭 My Hub')
@force_join_handler
def v27_my_hub(m):
    uid=m.from_user.id;u=User(uid);kb=InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("🕘 Recently Viewed",callback_data='v27hub|recent'),InlineKeyboardButton("▶️ Continue",callback_data='v27hub|continue'))
    kb.add(InlineKeyboardButton("💎 Upgrade / Renew VIP",callback_data='v27hub|upgrade'),InlineKeyboardButton("🎫 My Offers",callback_data='v27hub|offers'))
    raw_bot.send_message(uid,f"🧭 MY HUB\n\nStatus: {'💎 VIP' if u.is_vip() else '🆓 Free'}\nTier: {_v27_value_tier(uid)}\n\nQuickly continue where you left off or manage your VIP options.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:str(c.data or '').startswith('v27hub|'))
def v27_hub_cb(c):
    uid=c.from_user.id;act=c.data.split('|',1)[1];bot.answer_callback_query(c.id)
    if act=='recent':
        rows=list(customer_activity_col.find({'user_id':uid,'event':{'$in':['view_method','view_product','search_result']}}).sort('created_at',-1).limit(10));txt='🕘 RECENTLY VIEWED\n\n'+('\n'.join(f"• {r.get('details',{}).get('name') or r.get('details',{}).get('query') or r.get('event')}" for r in rows) if rows else 'Nothing tracked yet.');return raw_bot.send_message(uid,txt)
    if act=='continue':
        r=customer_activity_col.find_one({'user_id':uid,'event':{'$in':['view_method','view_product','search_result']}},sort=[('created_at',-1)])
        return raw_bot.send_message(uid,("▶️ Continue from: "+str((r or {}).get('details',{}).get('name') or (r or {}).get('details',{}).get('query') or 'your last section')) if r else 'No recent activity yet.')
    if act=='upgrade':
        u=_v27_user_doc(uid);txt='💎 VIP UPGRADE / RENEW\n\nChoose a plan from Buy VIP. If you already have active VIP, the new approved duration is added according to the existing subscription flow.';kb=InlineKeyboardMarkup();kb.add(InlineKeyboardButton('⭐ View VIP Plans',callback_data='get_vip'));return raw_bot.send_message(uid,txt,reply_markup=kb)
    if act=='offers':
        rows=list(personal_offers_col.find({'user_id':uid,'active':True,'expires_at':{'$gt':time.time()}}).sort('expires_at',1));txt='🎫 MY PERSONAL OFFERS\n\n'+('\n\n'.join(f"• {r.get('title')} — {float(r.get('discount_percent',0)):g}% OFF\n{r.get('message','')}\nExpires: {datetime.fromtimestamp(r['expires_at']).strftime('%Y-%m-%d %H:%M')}" for r in rows) if rows else 'No active personal offers.');return raw_bot.send_message(uid,txt)

# -------------------------
# 🔎 FILTER SEARCH + METHOD META/TAGS + SEARCH ANALYTICS
# -------------------------
_v27_search_state={}
@bot.message_handler(func=lambda m:m.text=='🔎 Filter Search')
@force_join_handler
def v27_filter_search(m):
    kb=InlineKeyboardMarkup(row_width=2)
    for key,label in [('all','🔎 All'),('free','🆓 Free'),('vip','💎 VIP'),('new','🆕 Newest'),('rated','⭐ Rated')]:kb.add(InlineKeyboardButton(label,callback_data=f'v27search|filter|{key}'))
    raw_bot.send_message(m.chat.id,'🔎 FILTER SEARCH\n\nChoose a filter, then send your search words.',reply_markup=kb)

@bot.callback_query_handler(func=lambda c:str(c.data or '').startswith('v27search|filter|'))
def v27_search_filter_cb(c):
    filt=c.data.rsplit('|',1)[1];_v27_search_state[c.from_user.id]={'filter':filt};msg=raw_bot.send_message(c.from_user.id,f"Send search text ({filt}):");bot.register_next_step_handler(msg,v27_search_query_step);bot.answer_callback_query(c.id)

def v27_search_query_step(m):
    uid=m.from_user.id;q=(m.text or '').strip();filt=_v27_search_state.pop(uid,{}).get('filter','all')
    if not q:return raw_bot.send_message(m.chat.id,'❌ Search cannot be empty.')
    rx={'$regex':re.escape(q),'$options':'i'};query={'name':rx}
    if filt=='free':query['cat']='free'
    elif filt=='vip':query['cat']='vip'
    rows=list(folders_col.find(query).sort('created_at',-1).limit(25))
    if filt=='rated':
        # use reviews if available; retain search name match and rank by cached metadata rating
        rows.sort(key=lambda r:float((method_meta_col.find_one({'folder_id':r.get('_id')}) or {}).get('rating',0) or 0),reverse=True)
    search_analytics_col.insert_one({'user_id':uid,'query':q,'filter':filt,'results':len(rows),'created_at':time.time()});_v27_activity(uid,'search_result',query=q,name=q,results=len(rows),filter=filt)
    kb=InlineKeyboardMarkup(row_width=1)
    for r in rows[:20]:kb.add(InlineKeyboardButton(str(r.get('name','Method'))[:55],callback_data=f"openid|{r['_id']}"))
    raw_bot.send_message(uid,f"🔎 Results for: {q}\nFilter: {filt}\nFound: {len(rows)}",reply_markup=kb if rows else None)

@bot.message_handler(commands=['methodmeta'])
def v27_method_meta_cmd(m):
    if not is_admin(m.from_user.id):return
    # /methodmeta NUMBER | category | tag1,tag2
    raw=(m.text or '').partition(' ')[2].strip()
    try:left,cat,tags=[x.strip() for x in raw.split('|',2)];num=int(left);folder=fs.get_by_number(num)
    except Exception:return raw_bot.send_message(m.chat.id,'Use /methodmeta METHOD_NUMBER | CATEGORY | tag1,tag2')
    if not folder:return raw_bot.send_message(m.chat.id,'Method not found.')
    method_meta_col.update_one({'folder_id':folder['_id']},{'$set':{'category':cat[:80],'tags':[x.strip()[:40] for x in tags.split(',') if x.strip()],'updated_at':time.time()}},upsert=True);_v27_audit(m.from_user.id,'method_meta',num);raw_bot.send_message(m.chat.id,'✅ Method category/tags updated.')

# -------------------------
# 🧭 ADMIN COMMAND CENTER
# -------------------------
@bot.message_handler(func=lambda m:m.text=='🧭 Command Center' and is_admin(m.from_user.id))
def v27_command_center(m):
    kb=ReplyKeyboardMarkup(resize_keyboard=True,row_width=2)
    rows=[('👤 Customer Lookup','💵 Financial Ledger'),('📉 Lost Sales','🔎 Search Analytics'),('↩️ Refund Manager','🚨 Priority Alerts'),('🧾 Staff Audit','📡 System Status'),('🚫 Rate Limits','🆘 Emergency Mode'),('📅 Reports','📦 Archive Center'),('🔐 Admin Session','⚙️ ADMIN PANEL')]
    for r in rows:kb.row(*r)
    raw_bot.send_message(m.chat.id,'🧭 ADMIN COMMAND CENTER\n\nCustomer intelligence, finance, safety, system health and reporting in one place.',reply_markup=kb)

@bot.message_handler(func=lambda m:m.text=='👤 Customer Lookup' and is_admin(m.from_user.id))
def v27_customer_lookup(m):
    msg=raw_bot.send_message(m.chat.id,'Send user ID or @username:');bot.register_next_step_handler(msg,v27_customer_lookup_step)

def v27_customer_lookup_step(m):
    u=_v26_find_user((m.text or '').strip()) if '_v26_find_user' in globals() else None
    if not u:return raw_bot.send_message(m.chat.id,'❌ User not found.',reply_markup=admin_menu())
    uid=int(u['_id']);raw_bot.send_message(m.chat.id,_v27_customer_profile_text(uid),reply_markup=_v27_profile_markup(uid))

# -------------------------
# 💵 FINANCIAL LEDGER + REFUND MANAGER
# -------------------------
@bot.message_handler(func=lambda m:m.text=='💵 Financial Ledger' and is_admin(m.from_user.id))
def v27_ledger_menu(m):
    rows=list(financial_ledger_col.find({}).sort('created_at',-1).limit(30));credits=sum(float(r.get('amount',0) or 0) for r in financial_ledger_col.find({'direction':'credit'}));debits=sum(float(r.get('amount',0) or 0) for r in financial_ledger_col.find({'direction':'debit'}));txt=f"💵 FINANCIAL LEDGER\n\nCredits: ${credits:.2f}\nDebits: ${debits:.2f}\nNet: ${credits-debits:.2f}\n\nRecent:\n"+('\n'.join(f"• {r.get('kind')} — {'+' if r.get('direction')=='credit' else '-'}${float(r.get('amount',0)):.2f}" for r in rows) if rows else 'No V27 ledger entries yet.')
    kb=InlineKeyboardMarkup(row_width=2);kb.add(InlineKeyboardButton('➕ Add Credit',callback_data='v27ledger|credit'),InlineKeyboardButton('➖ Add Expense',callback_data='v27ledger|debit'));raw_bot.send_message(m.chat.id,txt,reply_markup=kb)

@bot.callback_query_handler(func=lambda c:str(c.data or '').startswith('v27ledger|') and is_admin(c.from_user.id))
def v27_ledger_cb(c):
    direction=c.data.split('|')[1];msg=raw_bot.send_message(c.from_user.id,'Send: AMOUNT | NOTE');bot.register_next_step_handler(msg,v27_ledger_manual_step,direction);bot.answer_callback_query(c.id)

def v27_ledger_manual_step(m,direction):
    try:a,n=[x.strip() for x in (m.text or '').split('|',1)];a=float(a);assert a>=0
    except Exception:return raw_bot.send_message(m.chat.id,'❌ Send AMOUNT | NOTE')
    _v27_ledger('manual_adjustment',a,note=n,admin_id=m.from_user.id,direction=direction);_v27_audit(m.from_user.id,'ledger_'+direction,None,{'amount':a,'note':n});raw_bot.send_message(m.chat.id,'✅ Ledger entry saved.',reply_markup=admin_menu())

@bot.message_handler(func=lambda m:m.text=='↩️ Refund Manager' and is_admin(m.from_user.id))
def v27_refund_menu(m):
    msg=raw_bot.send_message(m.chat.id,'↩️ REFUND MANAGER\n\nSend: USER_ID | AMOUNT | ORDER/REFERENCE | REASON\n\nThis records the refund and adjusts the V27 financial ledger.');bot.register_next_step_handler(msg,v27_refund_step)

def v27_refund_step(m):
    if not _v27_require_owner_session(m.from_user.id):return
    try:u,a,ref,reason=[x.strip() for x in (m.text or '').split('|',3)];u=int(u);a=float(a);assert a>0
    except Exception:return raw_bot.send_message(m.chat.id,'❌ Send USER_ID | AMOUNT | ORDER/REFERENCE | REASON')
    doc={'user_id':u,'amount':a,'reference':ref[:200],'reason':reason[:1000],'status':'recorded','created_at':time.time(),'admin_id':m.from_user.id};refunds_col.insert_one(doc);_v27_ledger('refund',a,user_id=u,order_id=ref,note=reason,admin_id=m.from_user.id,direction='debit');_v27_audit(m.from_user.id,'refund',u,{'amount':a,'reference':ref});
    try:raw_bot.send_message(u,f"↩️ REFUND RECORDED\n\nAmount: ${a:.2f}\nReference: {ref}\nReason: {reason}\n\nIf a blockchain/exchange transfer is required, it is processed according to the payment method used.")
    except Exception:pass
    raw_bot.send_message(m.chat.id,'✅ Refund recorded and ledger adjusted.',reply_markup=admin_menu())

# -------------------------
# 📉 LOST SALE + SEARCH ANALYTICS
# -------------------------
@bot.message_handler(func=lambda m:m.text=='📉 Lost Sales' and is_admin(m.from_user.id))
def v27_lost_sales(m):
    now=time.time();open2=checkout_intents_col.count_documents({'status':'open','created_at':{'$lte':now-2*3600}});opt=checkout_intents_col.count_documents({'status':'opted_out'});conv=checkout_intents_col.count_documents({'status':'converted'});total=checkout_intents_col.count_documents({});rate=(conv/total*100 if total else 0);raw_bot.send_message(m.chat.id,f"📉 LOST-SALE ANALYTICS\n\nCheckout intents: {total}\nConverted: {conv}\nStill open >2h: {open2}\nReminder opt-outs: {opt}\nIntent conversion: {rate:.1f}%\n\nUse this with the existing funnel analytics to identify checkout drop-off.")

@bot.message_handler(func=lambda m:m.text=='🔎 Search Analytics' and is_admin(m.from_user.id))
def v27_search_analytics(m):
    since=time.time()-30*86400;pipeline=[{'$match':{'created_at':{'$gte':since}}},{'$group':{'_id':'$query','count':{'$sum':1},'zero':{'$sum':{'$cond':[{'$eq':['$results',0]},1,0]}}}},{'$sort':{'count':-1}},{'$limit':20}]
    try:rows=list(search_analytics_col.aggregate(pipeline))
    except Exception:rows=[]
    txt='🔎 SEARCH ANALYTICS — 30 DAYS\n\n'+('\n'.join(f"• {r['_id']} — {r['count']} searches ({r['zero']} zero-result)" for r in rows) if rows else 'No filtered-search data yet.');raw_bot.send_message(m.chat.id,txt)

# -------------------------
# 🚨 PRIORITY ALERTS / SYSTEM STATUS / EMERGENCY MODE
# -------------------------
@bot.message_handler(func=lambda m:m.text=='🚨 Priority Alerts' and is_admin(m.from_user.id))
def v27_priority_menu(m):
    cfg=_adv_cfg();kb=InlineKeyboardMarkup(row_width=2);kb.add(InlineKeyboardButton('Toggle Alerts',callback_data='v27alert|toggle'),InlineKeyboardButton('Set Large Purchase $',callback_data='v27alert|threshold'));raw_bot.send_message(m.chat.id,f"🚨 PRIORITY ALERTS\n\nEnabled: {bool(cfg.get('priority_alerts',True))}\nLarge-purchase threshold: ${float(cfg.get('large_purchase_alert',50) or 50):g}",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:str(c.data or '').startswith('v27alert|') and is_admin(c.from_user.id))
def v27_alert_cb(c):
    a=c.data.split('|')[1]
    if a=='toggle':_adv_set('priority_alerts',not bool(_adv_cfg().get('priority_alerts',True)));bot.answer_callback_query(c.id,'Updated',True);return
    msg=raw_bot.send_message(c.from_user.id,'Send USD threshold for large-purchase alerts:');bot.register_next_step_handler(msg,v27_alert_threshold);bot.answer_callback_query(c.id)

def v27_alert_threshold(m):
    try:v=max(0,float((m.text or '').strip()))
    except Exception:return raw_bot.send_message(m.chat.id,'❌ Send a number.')
    _adv_set('large_purchase_alert',v);raw_bot.send_message(m.chat.id,f'✅ Threshold set to ${v:g}.',reply_markup=admin_menu())

@bot.message_handler(func=lambda m:m.text=='📡 System Status' and is_admin(m.from_user.id))
def v27_system_status(m):
    cfg=get_cached_config();lastbk=backups_col.find_one(sort=[('created_at',-1)]) if 'backups_col' in globals() else None;pending=payments_col.count_documents({'status':'pending'})+wallet_tx_col.count_documents({'status':'pending'});txt=f"📡 SYSTEM STATUS\n\nBot build: v27.0\nDatabase: {db.name}\nMaintenance: {'ON' if cfg.get('maintenance_mode') else 'OFF'}\nEmergency mode: {'ON' if _adv_cfg().get('emergency_mode') else 'OFF'}\nPending payment queue: {pending}\nLast backup: {datetime.fromtimestamp(lastbk.get('created_at')).strftime('%Y-%m-%d %H:%M') if lastbk and lastbk.get('created_at') else 'Unknown'}\nPolling worker: running while this process is online\nCheckout recovery: {'ON' if get_config().get('checkout_recovery_enabled',True) else 'OFF'}";raw_bot.send_message(m.chat.id,txt)

@bot.message_handler(func=lambda m:m.text=='🆘 Emergency Mode' and is_admin(m.from_user.id))
def v27_emergency_menu(m):
    state=bool(_adv_cfg().get('emergency_mode'));kb=InlineKeyboardMarkup();kb.add(InlineKeyboardButton('🔴 Turn OFF' if state else '🟢 Turn ON',callback_data='v27emergency|toggle'));raw_bot.send_message(m.chat.id,f"🆘 EMERGENCY MODE\n\nCurrent: {'ON' if state else 'OFF'}\n\nWhen enabled, V27 safety checks can be used to stop new sensitive submissions while browsing/support remains available. Existing legacy maintenance controls remain unchanged.",reply_markup=kb)

@bot.callback_query_handler(func=lambda c:c.data=='v27emergency|toggle' and is_admin(c.from_user.id))
def v27_emergency_toggle(c):
    if not _v27_require_owner_session(c.from_user.id):return bot.answer_callback_query(c.id,'Unlock owner session first',True)
    state=not bool(_adv_cfg().get('emergency_mode'));_adv_set('emergency_mode',state);_v27_audit(c.from_user.id,'emergency_mode',None,{'enabled':state});bot.answer_callback_query(c.id,'Updated',True);raw_bot.send_message(c.from_user.id,f"🆘 Emergency mode is now {'ON' if state else 'OFF'}.",reply_markup=admin_menu())

# -------------------------
# 🧾 STAFF AUDIT / RATE LIMITS / ARCHIVE CENTER
# -------------------------
@bot.message_handler(func=lambda m:m.text=='🧾 Staff Audit' and is_admin(m.from_user.id))
def v27_staff_audit(m):
    rows=list(admin_audit_col.find({}).sort('created_at',-1).limit(40));txt='🧾 STAFF / ADMIN AUDIT\n\n'+('\n'.join(f"• {datetime.fromtimestamp(r['created_at']).strftime('%m-%d %H:%M')} — {r.get('admin_id')} — {r.get('action')} — {r.get('target','')}" for r in rows) if rows else 'No V27 audit events yet.');raw_bot.send_message(m.chat.id,txt)

@bot.message_handler(func=lambda m:m.text=='🚫 Rate Limits' and is_admin(m.from_user.id))
def v27_rate_limits(m):
    cfg=_adv_cfg();raw_bot.send_message(m.chat.id,f"🚫 RATE LIMITS\n\nDefault sensitive action limit: {int(cfg.get('rate_limit_count',4) or 4)} per {int(cfg.get('rate_limit_window',600) or 600)} seconds.\n\nUse /setratelimit COUNT SECONDS to change the V27 guard. Existing submission blocking remains available separately.")

@bot.message_handler(commands=['setratelimit'])
def v27_set_rate(m):
    if not is_admin(m.from_user.id):return
    try:_,a,b=(m.text or '').split();a=max(1,min(50,int(a)));b=max(60,min(86400,int(b)))
    except Exception:return raw_bot.send_message(m.chat.id,'Use /setratelimit COUNT SECONDS')
    _adv_set('rate_limit_count',a);_adv_set('rate_limit_window',b);_v27_audit(m.from_user.id,'set_rate_limit',None,{'count':a,'window':b});raw_bot.send_message(m.chat.id,'✅ Rate limit updated.')

@bot.message_handler(func=lambda m:m.text=='📦 Archive Center' and is_admin(m.from_user.id))
def v27_archive_center(m):
    kb=InlineKeyboardMarkup(row_width=2);kb.add(InlineKeyboardButton('📚 Archive Method',callback_data='v27archive|method'),InlineKeyboardButton('🛍 Archive Product',callback_data='v27archive|product'));raw_bot.send_message(m.chat.id,'📦 ARCHIVE CENTER\n\nArchive items instead of deleting them so purchase/invoice history remains intact.',reply_markup=kb)

@bot.callback_query_handler(func=lambda c:str(c.data or '').startswith('v27archive|') and is_admin(c.from_user.id))
def v27_archive_cb(c):
    kind=c.data.split('|')[1];msg=raw_bot.send_message(c.from_user.id,'Send method NUMBER:' if kind=='method' else 'Send product ID/name:');bot.register_next_step_handler(msg,v27_archive_step,kind);bot.answer_callback_query(c.id)

def v27_archive_step(m,kind):
    if kind=='method':
        try:num=int((m.text or '').strip());f=fs.get_by_number(num)
        except Exception:f=None
        if not f:return raw_bot.send_message(m.chat.id,'❌ Method not found.')
        folders_col.update_one({'_id':f['_id']},{'$set':{'archived':True,'archived_at':time.time(),'archived_by':m.from_user.id}});_v27_audit(m.from_user.id,'archive_method',num);return raw_bot.send_message(m.chat.id,'✅ Method archived. Historical records remain.',reply_markup=admin_menu())
    q=(m.text or '').strip();r=None
    try:r=shop_products_col.find_one({'_id':ObjectId(q)})
    except Exception:r=shop_products_col.find_one({'name':{'$regex':f'^{re.escape(q)}$','$options':'i'}})
    if not r:return raw_bot.send_message(m.chat.id,'❌ Product not found.')
    shop_products_col.update_one({'_id':r['_id']},{'$set':{'archived':True,'active':False,'archived_at':time.time(),'archived_by':m.from_user.id}});_v27_audit(m.from_user.id,'archive_product',str(r['_id']));raw_bot.send_message(m.chat.id,'✅ Product archived. Historical records remain.',reply_markup=admin_menu())

# -------------------------
# 📅 DAILY/WEEKLY BUSINESS REPORTS
# -------------------------
def _v27_business_report(days=1):
    since=time.time()-days*86400
    paid=list(payments_col.find({'status':{'$in':['approved','paid']},'created_at':{'$gte':since}}));orders=list(shop_orders_col.find({'status':{'$in':['paid','delivered','completed']},'created_at':{'$gte':since}}));revenue=sum(float(r.get('amount',0) or 0) for r in paid)+sum(float(r.get('amount',r.get('total',0)) or 0) for r in orders);new=users_col.count_documents({'created_at':{'$gte':since}});vip=users_col.count_documents({'vip':True,'vip_expiry':{'$gt':time.time()}});refund=sum(float(r.get('amount',0) or 0) for r in refunds_col.find({'created_at':{'$gte':since}}));pending=payments_col.count_documents({'status':'pending'})+wallet_tx_col.count_documents({'status':'pending'});return f"📅 {'DAILY' if days==1 else 'WEEKLY'} BUSINESS REPORT\n\nRevenue recorded: ${revenue:.2f}\nRefunds recorded: ${refund:.2f}\nNet before other expenses: ${revenue-refund:.2f}\nNew users: {new}\nActive VIP users: {vip}\nPending payment items: {pending}\nPeriod: last {days} day(s)"

@bot.message_handler(func=lambda m:m.text=='📅 Reports' and is_admin(m.from_user.id))
def v27_reports_menu(m):
    kb=InlineKeyboardMarkup(row_width=2);kb.add(InlineKeyboardButton('📅 Daily Now',callback_data='v27report|1'),InlineKeyboardButton('📆 Weekly Now',callback_data='v27report|7'),InlineKeyboardButton('🔔 Toggle Auto Reports',callback_data='v27report|toggle'));raw_bot.send_message(m.chat.id,'📅 BUSINESS REPORTS\n\nGenerate closing summaries or enable automatic owner reports.',reply_markup=kb)

@bot.callback_query_handler(func=lambda c:str(c.data or '').startswith('v27report|') and is_admin(c.from_user.id))
def v27_report_cb(c):
    a=c.data.split('|')[1]
    if a=='toggle':_adv_set('auto_business_reports',not bool(_adv_cfg().get('auto_business_reports',False)));return bot.answer_callback_query(c.id,'Auto reports toggled',True)
    raw_bot.send_message(c.from_user.id,_v27_business_report(int(a)));bot.answer_callback_query(c.id)

# lightweight report scheduler; once per process, checks hourly and sends max once per period
_v27_report_worker_started=False
def _v27_report_worker():
    global _v27_report_worker_started
    if _v27_report_worker_started:return
    _v27_report_worker_started=True
    def loop():
        while True:
            try:
                if _adv_cfg().get('auto_business_reports',False):
                    now=datetime.now();today=now.strftime('%Y-%m-%d');
                    if now.hour>=20 and not report_runs_col.find_one({'_id':'daily:'+today}):
                        raw_bot.send_message(ADMIN_ID,_v27_business_report(1));report_runs_col.insert_one({'_id':'daily:'+today,'created_at':time.time()})
                    week=now.strftime('%Y-W%W')
                    if now.weekday()==6 and now.hour>=20 and not report_runs_col.find_one({'_id':'weekly:'+week}):
                        raw_bot.send_message(ADMIN_ID,_v27_business_report(7));report_runs_col.insert_one({'_id':'weekly:'+week,'created_at':time.time()})
            except Exception:pass
            time.sleep(3600)
    threading.Thread(target=loop,daemon=True,name='v27-report-worker').start()
_v27_report_worker()

# -------------------------
# 💳 REJECTION REASONS + RESUBMIT PROOF + DUPLICATE CHECK
# -------------------------
@bot.message_handler(commands=['rejectpayment'])
def v27_reject_payment_cmd(m):
    if not is_admin(m.from_user.id):return
    # /rejectpayment OBJECTID | reason
    raw=(m.text or '').partition(' ')[2]
    try:pid,reason=[x.strip() for x in raw.split('|',1)];row=payments_col.find_one({'_id':ObjectId(pid)})
    except Exception:return raw_bot.send_message(m.chat.id,'Use /rejectpayment PAYMENT_OBJECT_ID | reason')
    if not row:return raw_bot.send_message(m.chat.id,'Payment not found.')
    reasons={'wrong amount':'Wrong amount','invalid txid':'Invalid transaction ID','wrong network':'Wrong network','proof unclear':'Payment proof is unclear'};nice=reasons.get(reason.lower(),reason[:500]);payments_col.update_one({'_id':row['_id']},{'$set':{'status':'rejected','rejection_reason':nice,'reviewed_at':time.time(),'reviewed_by':m.from_user.id,'can_resubmit':True}});uid=int(row.get('user_id'));_v27_audit(m.from_user.id,'reject_payment_reason',uid,{'payment_id':pid,'reason':nice});
    kb=InlineKeyboardMarkup();kb.add(InlineKeyboardButton('🔄 Resubmit Proof',callback_data=f'v27resubmit|{pid}'))
    try:raw_bot.send_message(uid,f"❌ PAYMENT REJECTED\n\nReason: {nice}\n\nYou can correct the proof for this same request instead of creating another order.",reply_markup=kb)
    except Exception:pass
    raw_bot.send_message(m.chat.id,'✅ Payment rejected with reason.')

@bot.callback_query_handler(func=lambda c:str(c.data or '').startswith('v27resubmit|'))
def v27_resubmit_cb(c):
    pid=c.data.split('|')[1]
    try:r=payments_col.find_one({'_id':ObjectId(pid),'user_id':int(c.from_user.id),'can_resubmit':True})
    except Exception:r=None
    if not r:return bot.answer_callback_query(c.id,'This request cannot be resubmitted',True)
    if not _v27_rate_allowed(c.from_user.id,'resubmit',limit=int(_adv_cfg().get('rate_limit_count',4) or 4),window=int(_adv_cfg().get('rate_limit_window',600) or 600)):return bot.answer_callback_query(c.id,'Too many attempts. Try again later.',True)
    msg=raw_bot.send_message(c.from_user.id,'📸 Send the corrected payment screenshot as a photo.');bot.register_next_step_handler(msg,v27_resubmit_photo_step,pid);bot.answer_callback_query(c.id)

def v27_resubmit_photo_step(m,pid):
    if m.content_type!='photo':
        msg=raw_bot.send_message(m.chat.id,'❌ Send a photo screenshot.');return bot.register_next_step_handler(msg,v27_resubmit_photo_step,pid)
    file_id=m.photo[-1].file_id
    # duplicate file-id guard for repeated proof submissions
    dup=payments_col.find_one({'proof_file_id':file_id,'_id':{'$ne':ObjectId(pid)},'status':{'$in':['pending','approved','paid']}})
    if dup:return raw_bot.send_message(m.chat.id,'❌ This exact proof image is already attached to another payment request.')
    payments_col.update_one({'_id':ObjectId(pid)},{'$set':{'status':'pending','proof_file_id':file_id,'resubmitted_at':time.time()},'$unset':{'can_resubmit':'','rejection_reason':''}});_v27_activity(m.from_user.id,'payment_resubmitted',payment_id=pid);raw_bot.send_message(m.chat.id,'✅ Corrected proof submitted for review.')

# -------------------------
# 🧮 CHECKOUT TIMER INFORMATION
# -------------------------
@bot.message_handler(commands=['checkoutstatus'])
def v27_checkout_status(m):
    row=checkout_intents_col.find_one({'user_id':int(m.from_user.id),'status':'open'},sort=[('created_at',-1)])
    if not row:return raw_bot.send_message(m.chat.id,'You do not have an open tracked checkout.')
    expires=float(row.get('expires_at') or (float(row.get('created_at',time.time()))+24*3600));remaining=max(0,int(expires-time.time()));h=remaining//3600;mi=(remaining%3600)//60;raw_bot.send_message(m.chat.id,f"⏱ CHECKOUT STATUS\n\nType: {row.get('kind','purchase')}\nStatus: Open\nTracked checkout window remaining: {h}h {mi}m\n\nA deal price only changes when the deal itself has a real configured expiry.")

# -------------------------
# ADMIN SESSION INFO
# -------------------------
@bot.message_handler(func=lambda m:m.text=='🔐 Admin Session' and is_admin(m.from_user.id))
def v27_admin_session_menu(m):
    cfg=_adv_cfg();row=admin_sessions_col.find_one({'_id':int(m.from_user.id)}) or {};exp=float(row.get('expires_at',0) or 0);txt=f"🔐 ADMIN SESSION\n\nPIN configured: {'Yes' if cfg.get('admin_pin_hash') else 'No'}\nCurrent temporary unlock: {'Active until '+datetime.fromtimestamp(exp).strftime('%H:%M:%S') if exp>time.time() else 'Locked / not needed'}\n\nOwner commands:\n/setadminpin YOUR_PIN\n/adminunlock YOUR_PIN";raw_bot.send_message(m.chat.id,txt)

# -------------------------
# Expose V27 controls in admin menu / Advanced hub
# -------------------------
_v27_admin_menu_original=admin_menu
def admin_menu():
    kb=_v27_admin_menu_original()
    try:kb.row('🧭 Command Center')
    except Exception:pass
    return kb

# Add to V26 advanced hub by providing a direct button reachable from main admin menu.
BOT_BUILD_VERSION='27.0'

# Enhance user For You with pinned promotion/recommendation if its original handler is used elsewhere.

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
        "🧾 Logs", "💾 Backup/Export", "🙈 Hide Button", "👁 Show Button", "📋 METHODS LIST", "🛡 Group Management", "📢 CHANNELS", "➕ ADD CHANNEL", "📣 Channel Approvals", "🚀 Advanced Controls", "🎨 Theme Manager", "📌 Promotions", "💳 Payment Profiles", "🌍 Country Offers", "📑 Terms & Business", "🔐 Admin Safety", "📣 Broadcast Templates", "⏱ Scheduled Drops", "🧩 Bundle Manager", "💵 Profit Tracking", "🔎 Advanced User Search", "📤 Export Center", "🧪 Sandbox Mode", "🔄 Version Manager", "❤️ Health Check", "🚦 Queue Monitor", "💬 Saved Replies", "🧭 Command Center", "👤 Customer Lookup", "💵 Financial Ledger", "📉 Lost Sales", "🔎 Search Analytics", "↩️ Refund Manager", "🚨 Priority Alerts", "🧾 Staff Audit", "📡 System Status", "🚫 Rate Limits", "🆘 Emergency Mode", "📅 Reports", "📦 Archive Center", "🔐 Admin Session"
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
