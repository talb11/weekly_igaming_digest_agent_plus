import os, feedparser, yaml, requests, datetime, time, html, json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from dotenv import load_dotenv
from openai import OpenAI

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
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.office365.com")
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

def is_major(body, major_terms):
    body = body.lower()
    return any(term.lower() in body for term in major_terms)

def collect_listennotes_items(queries, major_terms):
    if not LISTENNOTES_API_KEY or not queries:
        return []
    items = []
    base = "https://listen-api.listennotes.com/api/v2/search"
    headers = {"X-ListenAPI-Key": LISTENNOTES_API_KEY}
    since = int((datetime.datetime.utcnow() - datetime.timedelta(days=LOOKBACK_DAYS)).timestamp())
    for q in queries:
        params = {
            "q": q,
            "type": "episode",
            "sort_by_date": 1,
            "published_after": since,
            "safe_mode": 0,
            "len_min": 5
        }
        try:
            r = requests.get(base, headers=headers, params=params, timeout=20)
            if r.status_code != 200:
                continue
            data = r.json()
            for ep in data.get("results", []):
                title = ep.get("title_original") or ep.get("title") or ""
                link = ep.get("listennotes_url") or ep.get("link") or ep.get("audio") or ""
                desc = strip_tags(ep.get("description_original") or ep.get("description") or "")
                if not title or not link:
                    continue
                items.append({
                    "title": title.strip(),
                    "link": link.strip(),
                    "summary": desc.strip()[:2500],
                    "section": "podcasts_listennotes",
                    "source": "ListenNotes",
                })
        except Exception:
            continue
        time.sleep(0.5)
    # Dedup
    seen = set(); dedup = []
    for it in items:
        key = (it["title"].lower(), it["link"])
        if key in seen: 
            continue
        seen.add(key); dedup.append(it)
    # Major-only for podcasts (non-casino)
    if MAJOR_ONLY_NON_CASINO:
        dedup = [it for it in dedup if is_major(f"{it['title']} {it['summary']}", major_terms)]
    return dedup[:MAX_ITEMS_PER_SECTION] if MAX_ITEMS_PER_SECTION > 0 else dedup

def summarize(items, name):
    if not items:
        return ""
    parts = []
    for it in items:
        prompt = (
            "You are a journalist for the online gambling industry. Summarize clearly and neutrally with key facts.\n"
            "Return two short paragraphs: first in English, second in Hebrew. Each 1–3 sentences.\n"
            "Then add a single line with the source link.\n\n"
            f"Title: {it['title']}\n"
            f"Source URL: {it['link']}\n"
            f"Feed Summary: {it['summary']}"
        )
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
            )
            text = resp.choices[0].message.content.strip()
        except Exception:
            text = (
                f"{it['title']}\n"
                "English: (fallback) See source.\n"
                "עברית: (fallback) ראה מקור.\n"
                f"Source: {it['link']}"
            )
        # build HTML safely (no backslashes inside f-string expressions)
        safe_title = html.escape(it['title'])
        safe_link  = html.escape(it['link'])
        safe_text  = html.escape(text).replace("\n", "<br>")
        parts.append(
            f"<h3>{safe_title}</h3>\n"
            f"<div>{safe_text}</div>\n"
            f"<p>🔗 <a href=\"{safe_link}\" target=\"_blank\">Source</a></p>"
        )
        time.sleep(0.4)
    title_map = {
        "news_rss": "Online Casino News",
        "poker_rss": "Poker — Major Only",
        "bingo_rss": "Bingo — Major Only",
        "podcasts_listennotes": "Podcasts — Major Only",
    }
    head = f"<h2>{title_map.get(name, name)}</h2>"
    return head + "\n" + "\n".join(parts)

def build_email(collected):
    sections_order = ["news_rss", "poker_rss", "bingo_rss", "podcasts_listennotes"]
    intro = (
        "<h1>Weekly iGaming Digest</h1>"
        "<p>Online Casino first; Poker & Bingo & Podcasts are filtered for major headlines."
        " Dual-language (EN/HE).</p>"
    )
    html_parts = [intro]
    for sec in sections_order:
        s = summarize(collected.get(sec, []), sec)
        if s:
            html_parts.append(s)
    html_body = "\n<hr>\n".join(html_parts)
    plain = "Weekly iGaming Digest (open HTML)."
    return plain, html_body


def send_mail(subject, plain, html_body):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = FROM_EMAIL
    msg['To'] = TO_EMAIL
    msg.attach(MIMEText(plain, 'plain', 'utf-8'))
    msg.attach(MIMEText(html_body, 'html', 'utf-8'))

    def _send(use_ssl, port):
        import smtplib, socket
        if use_ssl:
            with smtplib.SMTP_SSL(SMTP_SERVER, port, timeout=30) as server:
                server.login(SMTP_USER, SMTP_PASS)
                server.sendmail(FROM_EMAIL, [TO_EMAIL], msg.as_string())
        else:
            with smtplib.SMTP(SMTP_SERVER, port, timeout=30) as server:
                server.ehlo()
                server.starttls()
                server.login(SMTP_USER, SMTP_PASS)
                server.sendmail(FROM_EMAIL, [TO_EMAIL], msg.as_string())

    # Try STARTTLS on current port (usually 587), then fallback to SSL:465
    try:
        _send(False, int(SMTP_PORT))
    except Exception as e1:
        try:
            _send(True, 465 if int(SMTP_PORT)!=465 else 465)
        except Exception as e2:
            raise e2


def try_log_to_sheets(collected):
    if not HAS_SHEETS or not GOOGLE_SERVICE_ACCOUNT_JSON:
        return
    try:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        creds = Credentials.from_service_account_info(info, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ])
        gc = gspread.authorize(creds)
        try:
            sh = gc.open(SHEETS_SPREADSHEET)
        except Exception:
            sh = gc.create(SHEETS_SPREADSHEET)
        today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        try:
            ws = sh.worksheet(today)
        except Exception:
            ws = sh.add_worksheet(title=today, rows=1000, cols=6)
            ws.update('A1:F1', [["section","title","link","snippet","source","logged_at_utc"]])
        rows = []
        now = datetime.datetime.utcnow().isoformat()
        for section, arr in collected.items():
            for it in arr:
                snippet = (it.get("summary","")[:200]).replace("\n"," ")
                rows.append([section, it.get("title",""), it.get("link",""), snippet, it.get("source",""), now])
        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
    except Exception as e:
        print("Sheets logging skipped/error:", e)

if __name__ == "__main__":
    src = load_sources()
    collected = {}

    # RSS sections
    for section in ["news_rss", "poker_rss", "bingo_rss"]:
        urls = src.get(section, []) or []
        items = collect_rss_items(section, urls)
        # Dedup
        seen = set(); ded = []
        for it in items:
            k = (it["title"].lower(), it["link"])
            if k in seen:
                continue
            seen.add(k); ded.append(it)
        # Major-only for non-casino
        if section in ("poker_rss", "bingo_rss") and MAJOR_ONLY_NON_CASINO:
            terms = src.get("major_keywords", [])
            ded = [it for it in ded if is_major(f"{it['title']} {it['summary']}", terms)]
        if MAX_ITEMS_PER_SECTION > 0:
            ded = ded[:MAX_ITEMS_PER_SECTION]
        collected[section] = ded

    # Podcasts via ListenNotes
    ln_queries = src.get("podcasts_listennotes_queries", []) or []
    ln_items = collect_listennotes_items(ln_queries, src.get("major_keywords", []))
    collected["podcasts_listennotes"] = ln_items

    try_log_to_sheets(collected)
    plain, html_body = build_email(collected)
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    subject = f"Weekly Gambling Digest — {today}"
    send_mail(subject, plain, html_body)
    print("Digest prepared and (if SMTP is valid) sent.")
