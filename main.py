import os, feedparser, yaml, requests, datetime, time, html, json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from dotenv import load_dotenv
from openai import OpenAI
from urllib.parse import urlparse

# Optional: Google Sheets logging
HAS_SHEETS = False
try:
    import gspread
    from google.oauth2.service_account import Credentials
    HAS_SHEETS = True
except Exception:
    HAS_SHEETS = False

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
TO_EMAIL = os.getenv("TO_EMAIL")
FROM_EMAIL = os.getenv("FROM_EMAIL", os.getenv("SMTP_USER"))
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
MAX_ITEMS_PER_SECTION = int(os.getenv("MAX_ITEMS_PER_SECTION", "12"))
MAJOR_ONLY_NON_CASINO = (os.getenv("MAJOR_ONLY_NON_CASINO", "true").lower() == "true")

LISTENNOTES_API_KEY = os.getenv("LISTENNOTES_API_KEY", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
SHEETS_SPREADSHEET = os.getenv("SHEETS_SPREADSHEET", "Weekly iGaming Digest Log")

assert OPENAI_API_KEY, "OPENAI_API_KEY is required"
assert TO_EMAIL, "TO_EMAIL is required"
assert FROM_EMAIL, "FROM_EMAIL is required"
assert SMTP_SERVER and SMTP_PORT and SMTP_USER and SMTP_PASS, "SMTP settings are required"

client = OpenAI(api_key=OPENAI_API_KEY)

# --------------------- Sources & helpers ---------------------

def load_sources(path="sources.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def within_lookback(published_struct):
    if not published_struct:
        return True
    published = datetime.datetime(*published_struct[:6])
    now = datetime.datetime.utcnow()
    return (now - published) <= datetime.timedelta(days=LOOKBACK_DAYS)

def strip_tags(text):
    import re
    return re.sub(r'<[^>]+>', '', text or '')

def fetch_feed(url):
    try:
        return feedparser.parse(url)
    except Exception:
        return None

def collect_rss_items(section_name, urls):
    items = []
    for url in urls:
        d = fetch_feed(url)
        if not d or d.bozo:
            continue
        for e in d.entries:
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            if not title or not link:
                continue
            published = e.get("published_parsed") or e.get("updated_parsed")
            if published and not within_lookback(published):
                continue
            summary = e.get("summary") or e.get("description") or ""
            items.append({
                "title": title,
                "link": link,
                "summary": strip_tags(summary)[:2500],
                "section": section_name,
                "source": url,
            })
    return items

def is_major(b_
