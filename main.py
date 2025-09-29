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

# ----------- Configuration from environment ----------- #
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL", "gpt-5")  # default to gpt-5 per request
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

# ----------- Sources & basic helpers ----------- #
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

# ----------- UK Focus Filter ----------- #
def parse_focus(sources):
    """Returns a dict with focus keywords/companies/suffixes, or None (robust to non-strings)."""
    focus = sources.get("focus") if isinstance(sources, dict) else None
    if not isinstance(focus, dict):
        return None

    def norm_list(seq):
        out = []
        for x in (seq or []):
            try:
                out.append(str(x).strip().lower())
            except Exception:
                continue
        return out

    return {
        "region": str(focus.get("region", "")).strip().lower(),
        "keywords": norm_list(focus.get("keywords")),
        "companies": norm_list(focus.get("companies")),
        "suffixes": norm_list(focus.get("domain_suffixes")),
    }

def text_matches_any(text, needles):
    return any(n in text for n in needles)

def host_matches_suffix(link, suffixes):
    try:
        host = urlparse(link).netloc.lower()
        return any(host.endswith(suf) for suf in suffixes)
    except Exception:
        return False

def item_matches_focus(it, focus):
    """True if the item is UK-related per focus config."""
    if not focus:
        return True
    text = f"{it.get('title','')} {it.get('summary','')} {it.get('link','')}".lower()
    if text_matches_any(text, focus["keywords"]):
        return True
    if text_matches_any(text, focus["companies"]):
        return True
    if host_matches_suffix(it.get("link",""), focus["suffixes"]):
        return True
    return False

def apply_focus_filter(items, focus, major_terms):
    """Keep UK items; allow non-UK only if 'major'."""
    if not focus:
        return items
    kept = []
    for it in items:
        body = f"{it['title']} {it['summary']}"
        if item_matches_focus(it, focus) or is_major(body, major_terms):
            kept.append(it)
    return kept

# ----------- ListenNotes (podcasts) ----------- #
def collect_listennotes_items(queries, major_terms, focus):
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
    # Major-only for podcasts (non-casino) + UK focus
    if MAJOR_ONLY_NON_CASINO:
        dedup = [it for it in dedup if is_major(f"{it['title']} {it['summary']}", major_terms)]
    dedup = apply_focus_filter(dedup, focus, major_terms)
    return dedup[:MAX_ITEMS_PER_SECTION] if MAX_ITEMS_PER_SECTION > 0 else dedup

# ----------- Trend extraction (new) ----------- #
def build_trends_section(collected):
    """
    Build 'Trends' section (3 global trends) from the collected items (all sections).
    Returns HTML string (empty if nothing to show).
    """
    # Gather snippets
    pool = []
    for sec, arr in collected.items():
        for it in arr:
            pool.append(f"- {it.get('title','')}: {it.get('summary','')[:220]}")
    if not pool:
        return ""

    # Limit context size
    context = "\n".join(pool[:80])  # ~80 headlines/snippets
    prompt = (
        "You are an industry analyst for online gambling (iGaming).\n"
        f"Using the headlines & snippets from the last {LOOKBACK_DAYS} days below, "
        "identify the 3 most important global trends (not only UK). "
        "For each trend provide: a short title and a 1–2 sentence explanation. "
        "Return ONLY valid JSON with this schema:\n"
        '{ "trends": ['
        '{ "title_en": "...", "desc_en": "...", "title_he": "...", "desc_he": "..." },'
        '{ "title_en": "...", "desc_en": "...", "title_he": "...", "desc_he": "..." },'
        '{ "title_en": "...", "desc_en": "...", "title_he": "...", "desc_he": "..." } ] }\n\n'
        "HEADLINES & SNIPPETS:\n"
        f"{context}"
    )

    trends = []
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        data = json.loads(resp.choices[0].message.content.strip())
        for t in (data.get("trends") or [])[:3]:
            trends.append({
                "title_en": str(t.get("title_en","")).strip(),
                "desc_en": str(t.get("desc_en","")).strip(),
                "title_he": str(t.get("title_he","")).strip(),
                "desc_he": str(t.get("desc_he","")).strip(),
            })
    except Exception as e:
        # Fallback: simple heuristic trends from words frequency (very light)
        text = " ".join(pool).lower()
        guesses = []
        if "regulat" in text or "ukgc" in text or "dcm" in text or "asa" in text:
            guesses.append(("Regulation & Compliance", "Increased regulatory scrutiny and enforcement actions.", "רגולציה וציות", "הגברת פיקוח ואכיפה רגולטורית."))
        if "merger" in text or "acquisition" in text or "ipo" in text or "funding" in text:
            guesses.append(("M&A and Funding", "Active deal-making and capital moves among operators and suppliers.", "מיזוגים וגיוסים", "פעילות עסקאות וגיוסים אצל מפעילים וספקים."))
        if "live" in text or "megaways" in text or "jackpot" in text:
            guesses.append(("Content & Live Expansion", "Push into live game shows, jackpots and new slot mechanics.", "תוכן ולייב", "דחיפה ללייב גיים-שואו, ג׳קפוטים ומכניקות חדשות."))
        for g in guesses[:3]:
            trends.append({"title_en": g[0], "desc_en": g[1], "title_he": g[2], "desc_he": g[3]})

    if not trends:
        return ""

    # Render HTML
    blocks = []
    for t in trends[:3]:
        te = html.escape(t["title_en"]); de = html.escape(t["desc_en"])
        th = html.escape(t["title_he"]); dh = html.escape(t["desc_he"])
        block = (
            '<div style="border:1px dashed #d7dbe2;border-radius:12px;'
            'background:#fbfcff;padding:14px 16px;margin:10px 0;">'
              f'<div style="font-weight:700;font-size:15px;color:#0b1220;margin-bottom:4px;">{te}</div>'
              f'<div style="font-size:13px;color:#1f2937;margin-bottom:6px;">{de}</div>'
              f'<div dir="rtl" style="font-weight:700;font-size:14px;color:#0b1220;margin:6px 0 2px;">{th}</div>'
              f'<div dir="rtl" style="font-size:13px;color:#111827;">{dh}</div>'
            '</div>'
        )
        blocks.append(block)

    header = (
        '<div style="font-size:18px;font-weight:800;margin:24px 0 8px;'
        'padding-bottom:6px;border-bottom:1px solid #eceff3;color:#111827;">'
        'Trends — 3 Most Notable (Global)</div>'
    )
    return header + "".join(blocks)

# ----------- Top Games in England (new) ----------- #
GAME_KEYWORDS = [
    "slot", "new slot", "slots", "megaways", "jackpot", "jackpots",
    "launch", "launches", "released", "release", "unveils", "rolls out",
    "live casino", "game show", "crash game", "instant win",
    "roulette", "blackjack", "baccarat", "table game"
]
STUDIO_KEYWORDS = [
    "evolution", "netent", "red tiger", "big time gaming", "btg", "pragmatic play",
    "playtech", "light & wonder", "scientific games", "games global", "blueprint",
    "relax gaming", "yggdrasil", "elk studios", "nolimit city", "hacksaw", "push gaming",
    "play'n go", "spinomenal", "isoftbet", "greentube"
]

def is_game_item(it: dict) -> bool:
    text = f"{it.get('title','')} {it.get('summary','')}".lower()
    has_kw = any(k in text for k in GAME_KEYWORDS)
    has_studio = any(s in text for s in STUDIO_KEYWORDS)
    return has_kw or has_studio

def build_games_section(collected):
    """
    Build 'Top Games in England' list (max 5) from UK-focused news items.
    Uses LLM ranking; falls back to heuristic if needed.
    """
    candidates = [it for it in (collected.get("news_rss") or []) if is_game_item(it)]
    # If not enough in news_rss, try others
    if len(candidates) < 5:
        for sec in ("bingo_rss", "poker_rss"):
            for it in (collected.get(sec) or []):
                if is_game_item(it):
                    candidates.append(it)

    # Dedup by (title, link)
    seen = set(); uniq = []
    for it in candidates:
        k = (it.get("title","").lower(), it.get("link",""))
        if k in seen: continue
        seen.add(k); uniq.append(it)
    candidates = uniq[:25]  # cap list

    if not candidates:
        return ""

    # Prepare compact list for LLM
    lines = []
    for i, it in enumerate(candidates, 1):
        lines.append(f"{i}. {it.get('title','')}\nURL: {it.get('link','')}\nSnippet: {it.get('summary','')[:200]}")
    context = "\n\n".join(lines)

    prompt = (
        "You are an expert UK iGaming curator.\n"
        "From the following candidate items (mostly UK-focused), choose the 5 most interesting ONLINE CASINO games "
        "(slots/live/table/crash etc.) relevant to the England/UK market this week. Prefer UK relevance, "
        "innovation, big brands/operators, or notable mechanics.\n"
        "Return ONLY valid JSON with this schema:\n"
        '{ "games": [ { "name": "...", "en": "...", "he": "...", "link": "..." }, ... up to 5 ] }\n\n'
        "CANDIDATES:\n" + context
    )

    games = []
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        data = json.loads(resp.choices[0].message.content.strip())
        for g in (data.get("games") or [])[:5]:
            games.append({
                "name": str(g.get("name","")).strip(),
                "en": str(g.get("en","")).strip(),
                "he": str(g.get("he","")).strip(),
                "link": str(g.get("link","")).strip(),
            })
    except Exception as e:
        # Heuristic fallback: take first 5, prefer those with .co.uk or 'uk' in title
        def score(it):
            t = f"{it.get('title','')} {it.get('summary','')} {it.get('link','')}".lower()
            s = 0
            if ".co.uk" in it.get("link","").lower() or ".uk" in it.get("link","").lower(): s += 2
            if " uk " in f" {t} ": s += 1
            if any(k in t for k in ["launch", "megaways", "jackpot", "live"]): s += 1
            return -s  # sort ascending -> higher score first when negative
        best = sorted(candidates, key=score)[:5]
        for it in best:
            games.append({
                "name": it.get("title","").strip(),
                "en": (it.get("summary") or it.get("title") or "")[:140].strip(),
                "he": "",
                "link": it.get("link",""),
            })

    if not games:
        return ""

    # Render HTML
    cards = []
    for g in games:
        name = html.escape(g["name"] or "")
        en   = html.escape(g["en"] or "")
        he   = html.escape(g["he"] or "")
        link = html.escape(g["link"] or "#")
        card = (
            '<div style="border:1px solid #e6e8eb;border-radius:12px;'
            'background:#ffffff;box-shadow:0 1px 3px rgba(0,0,0,0.05);'
            'padding:14px;margin:10px 0;">'
              f'<div style="font-size:15px;font-weight:700;margin:0 0 6px;">{name}</div>'
              f'<p style="margin:0 0 6px;line-height:1.5;font-size:13.5px;color:#1f2937;">{en}</p>'
              + (f'<p dir="rtl" style="margin:0 12px 8px 0;line-height:1.6;font-size:13.5px;color:#111827;">{he}</p>' if he else "") +
              f'<a href="{link}" target="_blank" '
              'style="display:inline-block;padding:7px 10px;border-radius:8px;'
              'background:#0369a1;color:#ffffff;text-decoration:none;'
              'font-weight:600;font-size:12.5px;">Open source</a>'
            '</div>'
        )
        cards.append(card)

    header = (
        '<div style="font-size:18px;font-weight:800;margin:24px 0 8px;'
        'padding-bottom:6px;border-bottom:1px solid #eceff3;color:#111827;">'
        'Top Games in England — 5 to Watch</div>'
    )
    return header + "".join(cards)

# ----------- Render (cards) with robust summaries ----------- #
def summarize(items, name):
    """
    Render a section as 'cards'.
    Each item -> ONE short paragraph in English + ONE short paragraph in Hebrew + Link button.
    Robust: JSON mode -> plain delimiter -> local fallback (never empty).
    """
    if not items:
        return ""

    def section_title(n):
        titles = {
            "news_rss": "Online Casino — UK Focus",
            "poker_rss": "Poker — Major Only (UK)",
            "bingo_rss": "Bingo — Major Only (UK)",
            "podcasts_listennotes": "Podcasts — Major Only (UK)",
        }
        return titles.get(n, n)

    def llm_two_paras(it):
        # 1) Try strict JSON mode (preferred)
        prompt_json = (
            "You are a journalist for the online gambling industry.\n"
            "Write one concise paragraph in English (max 2 sentences) with key facts.\n"
            "Also write one concise paragraph in Hebrew (max 2 sentences).\n"
            'Return ONLY valid JSON: {"en": "...", "he": "..."}\n\n'
            f"Title: {it['title']}\n"
            f"Source URL: {it['link']}\n"
            f"Feed Summary: {it['summary']}"
        )
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                response_format={"type": "json_object"},
                messages=[{"role": "user", "content": prompt_json}],
                temperature=0.2,
            )
            raw = resp.choices[0].message.content.strip()
            data = json.loads(raw)
            en = (data.get("en") or "").strip()
            he = (data.get("he") or "").strip()
            if en or he:
                return en, he
        except Exception as e1:
            print("LLM JSON mode error:", e1)

        # 2) Plain text with delimiter
        prompt_delim = (
            "You are a journalist for the online gambling industry.\n"
            "Write two concise paragraphs: first English (max 2 sentences), second Hebrew (max 2 sentences).\n"
            "Separate them with a single line: ---\n\n"
            f"Title: {it['title']}\n"
            f"Source URL: {it['link']}\n"
            f"Feed Summary: {it['summary']}"
        )
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt_delim}],
                temperature=0.2,
            )
            text = resp.choices[0].message.content.strip()
            parts = text.split("\n---\n", 1)
            en = parts[0].strip()
            he = parts[1].strip() if len(parts) > 1 else ""
            if en or he:
                return en, he
        except Exception as e2:
            print("LLM delimiter mode error:", e2)

        # 3) Local fallback from RSS (never empty, no ugly placeholders)
        snippet = (it.get("summary") or it.get("title") or "").strip()
        snippet = " ".join(snippet.split())[:300]
        en = snippet or "See source."
        he = ""  # avoid placeholder Hebrew if LLM failed
        return en, he

    cards_html = []
    for it in items:
        en, he = llm_two_paras(it)

        safe_title = html.escape(it["title"])
        safe_link  = html.escape(it["link"])
        safe_en    = html.escape(en)
        safe_he    = html.escape(he)

        card = (
            '<div style="border:1px solid #e6e8eb;border-radius:12px;'
            'background:#ffffff;box-shadow:0 1px 3px rgba(0,0,0,0.05);'
            'padding:16px;margin:12px 0;">'
              f'<div style="font-size:16px;font-weight:700;margin:0 0 8px;">{safe_title}</div>'
              f'<p style="margin:0 0 6px;line-height:1.5;font-size:14px;color:#1f2937;">{safe_en}</p>'
        )
        if safe_he:
            card += (
              f'<p dir="rtl" style="margin:0 12px 10px 0;line-height:1.6;font-size:14px;color:#111827;">{safe_he}</p>'
            )
        card += (
              f'<a href="{safe_link}" target="_blank" '
              'style="display:inline-block;padding:8px 12px;border-radius:8px;'
              'background:#0b5fff;color:#ffffff;text-decoration:none;'
              'font-weight:600;font-size:13px;">Open source</a>'
            '</div>'
        )
        cards_html.append(card)

    header = (
        f'<div style="font-size:18px;font-weight:800;margin:24px 0 8px;'
        'padding-bottom:6px;border-bottom:1px solid #eceff3;color:#111827;">'
        f'{section_title(name)}</div>'
    )
    return header + "".join(cards_html)

# ----------- Email shell ----------- #
def build_email(collected, uk_focus_on):
    sections_order = ["news_rss", "poker_rss", "bingo_rss", "podcasts_listennotes"]

    intro = (
        "<h1 style='margin:0 0 6px;font-size:22px;font-weight:800;color:#0b1220;'>"
        "Weekly iGaming Digest</h1>"
        f"<p style='margin:0 0 18px;color:#4b5563;font-size:14px;'>"
        f"{'UK-first: non-UK items included only if major.' if uk_focus_on else 'Online Casino first; Poker/Bingo/Podcasts show only major headlines.'} "
        "Each card includes a short paragraph in English and one in Hebrew, plus a source link."
        "</p>"
    )

    # Outer container (email-friendly)
    html_parts = [
        '<div style="background:#f6f7f9;padding:24px 0;">'
        '<div style="max-width:720px;margin:0 auto;background:#ffffff;'
        'border:1px solid #e6e8eb;border-radius:14px;box-shadow:0 2px 6px rgba(0,0,0,0.04);'
        'padding:22px;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif;">',
        intro
    ]

    # New: Trends (3)
    trends_html = build_trends_section(collected)
    if trends_html:
        html_parts.append(trends_html)

    # New: Top Games in England (5)
    games_html = build_games_section(collected)
    if games_html:
        html_parts.append(games_html)

    # Regular sections
    for sec in sections_order:
        sec_html = summarize(collected.get(sec, []), sec)
        if sec_html:
            html_parts.append(sec_html)

    html_parts.append(
        "<div style='margin-top:22px;padding-top:12px;border-top:1px solid #eceff3;"
        "color:#6b7280;font-size:12px;'>"
        "This digest is auto-generated. Sources are linked on each card."
        "</div></div></div>"
    )
    html_body = "".join(html_parts)
    plain = "Weekly iGaming Digest (open HTML for rich layout)."
    return plain, html_body

# ----------- Email & logging ----------- #
def send_mail(subject, plain, html_body):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = FROM_EMAIL
    msg['To'] = TO_EMAIL
    msg.attach(MIMEText(plain, 'plain', 'utf-8'))
    msg.attach(MIMEText(html_body, 'html', 'utf-8'))
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(FROM_EMAIL, [TO_EMAIL], msg.as_string())

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

# ----------- Main ----------- #
if __name__ == "__main__":
    src = load_sources()
    focus = parse_focus(src)  # UK focus config (may be None)
    major_terms = src.get("major_keywords", [])

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
            ded = [it for it in ded if is_major(f"{it['title']} {it['summary']}", major_terms)]
        # UK focus filter
        ded = apply_focus_filter(ded, focus, major_terms)
        if MAX_ITEMS_PER_SECTION > 0:
            ded = ded[:MAX_ITEMS_PER_SECTION]
        collected[section] = ded

    # Podcasts via ListenNotes
    ln_queries = src.get("podcasts_listennotes_queries", []) or []
    ln_items = collect_listennotes_items(ln_queries, major_terms, focus)
    collected["podcasts_listennotes"] = ln_items

    try_log_to_sheets(collected)
    plain, html_body = build_email(collected, uk_focus_on=bool(focus))
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    subject = f"Weekly Gambling Digest — {today} (UK Focus)"
    send_mail(subject, plain, html_body)
    print("Digest prepared and (if SMTP is valid) sent.")
