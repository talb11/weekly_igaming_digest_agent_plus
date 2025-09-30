import os, re, html, json, time, smtplib, datetime, requests, feedparser, yaml, difflib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from urllib.parse import urlparse
from collections import Counter
from openai import OpenAI

# ---------- Optional Google Sheets logging ----------
HAS_SHEETS = False
try:
    import gspread
    from google.oauth2.service_account import Credentials
    HAS_SHEETS = True
except Exception:
    HAS_SHEETS = False

load_dotenv()

# ---------- Env / Secrets ----------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_MODEL", "gpt-5")

TO_EMAIL = os.getenv("TO_EMAIL")
FROM_EMAIL = os.getenv("FROM_EMAIL", os.getenv("SMTP_USER"))
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
MAX_ITEMS_PER_SECTION = int(os.getenv("MAX_ITEMS_PER_SECTION", "12"))
MAJOR_ONLY_NON_CASINO = (os.getenv("MAJOR_ONLY_NON_CASINO", "true").lower() == "true")
FOCUS_THRESHOLD = int(os.getenv("FOCUS_THRESHOLD", "1"))
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

# Strict email layout
TRENDS_TARGET = 3
GAMES_TARGET = int(os.getenv("GAMES_TARGET", "5"))
NEWS_MAX = int(os.getenv("NEWS_MAX", "6"))

LISTENNOTES_API_KEY = (os.getenv("LISTENNOTES_API_KEY") or "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
SHEETS_SPREADSHEET = os.getenv("SHEETS_SPREADSHEET", "Weekly iGaming Digest Log")

assert OPENAI_API_KEY, "OPENAI_API_KEY is required"
assert TO_EMAIL, "TO_EMAIL is required"
assert FROM_EMAIL, "FROM_EMAIL is required"
assert SMTP_SERVER and SMTP_PORT and SMTP_USER and SMTP_PASS, "SMTP settings are required"

client = OpenAI(api_key=OPENAI_API_KEY)

# ---------- Sources ----------
def load_sources(path="sources.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# ---------- Helpers ----------
def within_lookback(published_struct, days=None):
    if not published_struct:
        return True
    published = datetime.datetime(*published_struct[:6])
    now = datetime.datetime.utcnow()
    return (now - published) <= datetime.timedelta(days=(days or LOOKBACK_DAYS))

def strip_tags(text):
    return re.sub(r"<[^>]+>", "", text or "")

def fetch_feed(url):
    try:
        return feedparser.parse(url)
    except Exception:
        return None

def collect_rss_items(section_name, urls, *, lookback_days=None):
    items = []
    for url in urls or []:
        d = fetch_feed(url)
        if not d or d.bozo:
            continue
        for e in d.entries:
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            if not title or not link:
                continue
            published = e.get("published_parsed") or e.get("updated_parsed")
            if published and not within_lookback(published, days=lookback_days):
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

def dedup_items(items):
    seen, out = set(), []
    for it in items:
        k = (it.get("title","").lower(), it.get("link",""))
        if k in seen:
            continue
        seen.add(k); out.append(it)
    return out

def is_major(body, terms):
    body = (body or "").lower()
    return any(t.lower() in body for t in (terms or []))

# ---------- UK Focus Scoring ----------
NON_UK_HINTS = [
    " usa ", " us ", " united states", " new jersey", " nevada",
    " canada", " ontario", " australia", " new zealand",
    " india", " brazil", " africa", " philippines"
]

def parse_focus(sources):
    focus = sources.get("focus") if isinstance(sources, dict) else None
    if not isinstance(focus, dict):
        return None
    def norm_list(seq):
        out = []
        for x in (seq or []):
            try: out.append(str(x).strip().lower())
            except: pass
        return out
    return {
        "region": str(focus.get("region","")).strip().lower(),
        "keywords": norm_list(focus.get("keywords")),
        "companies": norm_list(focus.get("companies")),
        "suffixes":  norm_list(focus.get("domain_suffixes") or focus.get("domain_suffixes_prefer") or []),
        "source_domains_prefer": norm_list(focus.get("source_domains_prefer")),
        "trend_hints": norm_list(focus.get("trend_hints")),
    }

def host_matches_suffix(link, suffixes):
    try:
        host = urlparse(link).netloc.lower()
        return any(host.endswith(suf) for suf in (suffixes or []))
    except Exception:
        return False

def host_in_pref(link, domains):
    try:
        host = urlparse(link).netloc.lower()
        return any(host.endswith(d) for d in (domains or []))
    except Exception:
        return False

def score_focus(it, focus):
    if not focus:
        return 0
    txt = f" {(it.get('title') or '')} {(it.get('summary') or '')} {(it.get('link') or '')} ".lower()
    score = 0
    if host_matches_suffix(it.get("link",""), focus["suffixes"]): score += 2
    if host_in_pref(it.get("link",""), focus.get("source_domains_prefer") or []): score += 3
    score += sum(1 for k in (focus["keywords"] or []) if k in txt)
    score += 2 * sum(1 for c in (focus["companies"] or []) if c in txt)
    if any(h in txt for h in NON_UK_HINTS): score -= 2
    return score

def apply_focus_filter(items, focus, major_terms):
    if not focus:
        return items
    kept = []
    for it in items:
        s = score_focus(it, focus)
        if s >= FOCUS_THRESHOLD or is_major(f"{it['title']} {it['summary']}", major_terms):
            kept.append(it)
    return kept

# ---------- OpenAI helpers ----------
def _llm_json(prompt, tries=2, temperature=0.2, system="You are a precise iGaming reporter. Be concise. No inventions."):
    last = None
    for _ in range(tries):
        try:
            r = client.chat.completions.create(
                model=MODEL,
                response_format={"type": "json_object"},
                messages=[{"role": "system", "content": system},
                          {"role": "user", "content": prompt}],
                temperature=temperature,
            )
            return json.loads(r.choices[0].message.content.strip())
        except Exception as e:
            last = e
            time.sleep(0.8)
            try:
                r = client.chat.completions.create(
                    model=MODEL,
                    messages=[{"role": "system", "content": system},
                              {"role": "user", "content": prompt}],
                    temperature=temperature,
                )
                return json.loads(r.choices[0].message.content.strip())
            except Exception as e2:
                last = e2
                time.sleep(0.8)
    raise last

# ---------- Podcasts (ListenNotes) ----------
LISTENNOTES_BASE = "https://listen-api.listennotes.com/api/v2/search"

def collect_listennotes_items(queries, major_terms, focus):
    if not LISTENNOTES_API_KEY or not queries:
        return []
    items = []
    headers = {"X-ListenAPI-Key": LISTENNOTES_API_KEY}
    since = int((datetime.datetime.utcnow() - datetime.timedelta(days=LOOKBACK_DAYS)).timestamp())
    for q in (queries or []):
        params = {"q": q, "type": "episode", "sort_by_date": 1, "published_after": since, "safe_mode": 0, "len_min": 5}
        try:
            r = requests.get(LISTENNOTES_BASE, headers=headers, params=params, timeout=20)
            if r.status_code != 200:
                continue
            data = r.json()
            for ep in data.get("results", []):
                title = ep.get("title_original") or ep.get("title") or ""
                link  = ep.get("listennotes_url") or ep.get("link") or ep.get("audio") or ""
                desc  = strip_tags(ep.get("description_original") or ep.get("description") or "")
                if not title or not link: 
                    continue
                items.append({
                    "title": title.strip(),
                    "link": link.strip(),
                    "summary": (desc.strip()[:2500]),
                    "section": "podcasts_listennotes",
                    "source": "ListenNotes",
                })
        except Exception:
            pass
        time.sleep(0.35)
    items = dedup_items(items)
    if MAJOR_ONLY_NON_CASINO:
        items = [it for it in items if is_major(f"{it['title']} {it['summary']}", major_terms)]
    items = apply_focus_filter(items, focus, major_terms)
    return items[:MAX_ITEMS_PER_SECTION] if MAX_ITEMS_PER_SECTION > 0 else items

# ---------- Manual must-include (direct URLs, e.g., EGR tax) ----------
OG_TITLE_RE = re.compile(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', re.I)
OG_DESC_RE  = re.compile(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']', re.I)
TITLE_RE    = re.compile(r"<title[^>]*>(.*?)</title>", re.I|re.S)
META_DESC_RE= re.compile(r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']', re.I)

def fetch_url_metadata(url, timeout=20):
    title = ""; desc = ""
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code >= 400:
            return None
        html_txt = r.text or ""
        m = OG_TITLE_RE.search(html_txt) or TITLE_RE.search(html_txt)
        if m: title = strip_tags(m.group(1)).strip()
        m2 = OG_DESC_RE.search(html_txt) or META_DESC_RE.search(html_txt)
        if m2: desc = strip_tags(m2.group(1)).strip()
        if not title: title = url
        return {"title": title, "summary": desc or "", "link": url}
    except Exception:
        return None

def inject_must_include(urls):
    out = []
    for u in (urls or []):
        meta = fetch_url_metadata(u)
        if not meta: 
            # still include a minimal shell to ensure link presence
            out.append({"title": u, "summary": "", "link": u, "section": "news_rss", "source": "manual"})
        else:
            meta.update({"section":"news_rss","source":"manual"})
            out.append(meta)
        time.sleep(0.3)
    return out

# ---------- Summaries (cards) ----------
def summarize_cards(items, title_text):
    if not items:
        return ""
    def llm_two_paras(it):
        prompt_json = (
            "Write one concise paragraph in English (max 2 sentences) and one in Hebrew (max 2). "
            'Return ONLY JSON: {"en":"...","he":"..."}\n\n'
            f"Title: {it['title']}\nSource URL: {it['link']}\nFeed Summary: {it['summary']}"
        )
        try:
            data = _llm_json(prompt_json)
            en = (data.get("en") or "").strip()
            he = (data.get("he") or "").strip()
            if en or he:
                return en, he
        except Exception:
            pass
        prompt_delim = (
            "Two concise paragraphs: first English (max 2 sentences), second Hebrew (max 2). "
            "Separate with a single line: ---\n\n"
            f"Title: {it['title']}\nSource URL: {it['link']}\nFeed Summary: {it['summary']}"
        )
        try:
            r = client.chat.completions.create(
                model=MODEL, temperature=0.2,
                messages=[{"role":"system","content":"Be factual and concise."},
                          {"role":"user","content":prompt_delim}]
            )
            text = r.choices[0].message.content.strip()
            parts = text.split("\n---\n", 1)
            en = parts[0].strip()
            he = parts[1].strip() if len(parts) > 1 else ""
            if en or he:
                return en, he
        except Exception:
            pass
        snippet = " ".join(((it.get("summary") or it.get("title") or "")).split())[:300]
        return (snippet or "See source."), ""

    cards = []
    for it in items:
        en, he = llm_two_paras(it)
        safe_title = html.escape(it["title"])
        safe_link  = html.escape(it["link"])
        safe_en    = html.escape(en)
        safe_he    = html.escape(he)
        card = (
            '<div style="border:1px solid #e6e8eb;border-radius:12px;background:#ffffff;'
            'box-shadow:0 1px 3px rgba(0,0,0,0.05);padding:16px;margin:12px 0;">'
            f'<div style="font-size:16px;font-weight:700;margin:0 0 8px;">{safe_title}</div>'
            f'<p style="margin:0 0 6px;line-height:1.5;font-size:14px;color:#1f2937;">{safe_en}</p>'
        )
        if safe_he:
            card += f'<p dir="rtl" style="margin:0 12px 10px 0;line-height:1.6;font-size:14px;color:#111827;">{safe_he}</p>'
        card += (
            f'<a href="{safe_link}" target="_blank" '
            'style="display:inline-block;padding:8px 12px;border-radius:8px;'
            'background:#0b5fff;color:#ffffff;text-decoration:none;'
            'font-weight:600;font-size:13px;">Open source</a>'
            '</div>'
        )
        cards.append(card)

    header = (
        f'<div style="font-size:18px;font-weight:800;margin:24px 0 8px;'
        'padding-bottom:6px;border-bottom:1px solid #eceff3;color:#111827;">'
        f'{html.escape(title_text)}</div>'
    )
    return header + "".join(cards)

# ---------- Trends (3, macro) ----------
STOP = set("the a an and or for of to in on with by from as at is are was were be been being it this that these those not no".split())
def _tokens(s):
    s = re.sub(r"[^a-zA-Z0-9 £]", " ", s or "")
    return [w.lower() for w in s.split() if len(w) > 2 and w.lower() not in STOP]

def build_trends_section(collected, focus):
    pool = []
    for sec, arr in collected.items():
        for it in arr:
            pool.append((it.get("title","") or "") + " " + (it.get("summary","") or ""))
    if not pool:
        return ""

    toks = []
    for p in pool:
        toks.extend(_tokens(p))
    counts = Counter(toks)
    top_terms = [w for w,_ in counts.most_common(60)]

    titles = [it.get("title","") for arr in collected.values() for it in arr][:40]
    context = "\n".join(f"- {t}" for t in titles if t)

    hints = ", ".join((focus.get("trend_hints") or []))

    prompt = (
        "You are an iGaming analyst. Based ONLY on the provided keyword frequencies and recent titles, "
        "derive exactly 3 global trends (concise, factual, UK-relevant when possible). For each trend, include: "
        "title_en, desc_en (1–2 sentences), title_he, desc_he (1–2 sentences). "
        "Avoid speculation. If hints provided, consider them only if supported by the data.\n"
        'Return ONLY JSON: {"trends":[{"title_en":"...","desc_en":"...","title_he":"...","desc_he":"..."}, x3]}\n\n'
        f"HINTS: {hints}\nTOP TERMS: {', '.join(top_terms)}\n\nRECENT TITLES:\n{context}"
    )

    trends = []
    try:
        data = _llm_json(prompt, temperature=0.1, system="Be precise, non-speculative. No hallucinations.")
        trends = (data.get("trends") or [])[:TRENDS_TARGET]
    except Exception:
        trends = []

    if not trends:
        return ""

    blocks = []
    for t in trends:
        te = html.escape((t.get("title_en") or "Trend").strip())
        de = html.escape((t.get("desc_en") or "").strip())
        th = html.escape((t.get("title_he") or "מגמה").strip())
        dh = html.escape((t.get("desc_he") or "").strip())
        blocks.append(
            '<div style="border:1px dashed #d7dbe2;border-radius:12px;background:#fbfcff;padding:14px 16px;margin:10px 0;">'
            f'<div style="font-weight:700;font-size:15px;color:#0b1220;margin-bottom:4px;">📈 {te}</div>'
            f'<div style="font-size:13px;color:#1f2937;margin-bottom:6px;">{de}</div>'
            f'<div dir="rtl" style="font-weight:700;font-size:14px;color:#0b1220;margin:6px 0 2px;">{th}</div>'
            f'<div dir="rtl" style="font-size:13px;color:#111827;">{dh}</div>'
            '</div>'
        )
    header = (
        '<div style="font-size:18px;font-weight:800;margin:24px 0 8px;'
        'padding-bottom:6px;border-bottom:1px solid #eceff3;color:#111827;">'
        '📈 Trends — 3 Most Notable</div>'
    )
    return header + "".join(blocks)

# ---------- Games (5) ----------
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
    "play'n go", "spinomenal", "isoftbet", "greentube", "quickspin"
]

def is_game_item(it: dict) -> bool:
    t = f" {it.get('title','')} {it.get('summary','')} ".lower()
    return any(k in t for k in GAME_KEYWORDS) or any(s in t for s in STUDIO_KEYWORDS)

def _game_score(it, focus):
    t = f" {it.get('title','')} {it.get('summary','')} {it.get('link','')} ".lower()
    s = 0
    if any(k in t for k in GAME_KEYWORDS): s += 2
    if any(sv in t for sv in STUDIO_KEYWORDS): s += 2
    if ".co.uk" in t or t.strip().endswith(".uk"): s += 2
    if " uk " in t or "britain" in t or "united kingdom" in t or "england" in t: s += 2
    if "launch" in t or "released" in t or "unveils" in t: s += 1
    if "megaways" in t or "jackpot" in t or "game show" in t: s += 1
    s += max(0, score_focus(it, focus))
    if any(h in t for h in NON_UK_HINTS): s -= 2
    return s

def _norm_title(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _is_title_similar(a: str, b: str, thr=0.90) -> bool:
    return difflib.SequenceMatcher(None, _norm_title(a), _norm_title(b)).ratio() >= thr

def _summarize_game_card(it):
    prompt = (
        "Rewrite two concise paragraphs about the following online casino game item: "
        "first English (max 2 sentences) with key facts; second Hebrew (max 2). "
        'Return ONLY JSON: {"en":"...","he":"..."}\n\n'
        f"Title: {it['title']}\nLink: {it['link']}\nSnippet: {it['summary']}"
    )
    en = he = ""
    try:
        data = _llm_json(prompt)
        en = (data.get("en") or "").strip()
        he = (data.get("he") or "").strip()
    except Exception:
        snippet = " ".join(((it.get("summary") or it.get("title") or "")).split())[:160]
        en, he = (snippet or it.get("title","")), ""
    return en, he

def build_games_section(collected, focus):
    # candidates from multiple buckets
    candidates = []
    for sec in ("news_rss", "games_rss", "bingo_rss", "poker_rss"):
        for it in (collected.get(sec) or []):
            if is_game_item(it):
                candidates.append(it)
    candidates = dedup_items(candidates)

    # rank
    ranked = sorted(candidates, key=lambda it: _game_score(it, focus), reverse=True)

    # ensure 5 picks with fallback (expand lookback for game feeds only)
    top = ranked[:GAMES_TARGET]
    if len(top) < GAMES_TARGET:
        fallback_urls = collected.get("_games_fallback_urls") or []
        if fallback_urls:
            extra = collect_rss_items("games_rss_fallback", fallback_urls, lookback_days=max(LOOKBACK_DAYS, 21))
            extra = [it for it in extra if is_game_item(it)]
            extra = dedup_items(extra)
            # avoid dup by title similarity/link
            safe = []
            usedL = set([x.get("link") for x in top])
            usedT = [x.get("title","") for x in top]
            for it in extra:
                if it.get("link") in usedL: continue
                if any(_is_title_similar(it.get("title",""), t) for t in usedT): continue
                safe.append(it)
            ranked_extra = sorted(safe, key=lambda it: _game_score(it, focus), reverse=True)
            need = GAMES_TARGET - len(top)
            top += ranked_extra[:need]

    cards = []
    used_links = set()
    used_titles = set()

    for it in top:
        en, he = _summarize_game_card(it)
        name = html.escape(it.get("title","").strip())
        link = html.escape(it.get("link","") or "#")
        en   = html.escape(en)
        he   = html.escape(he)
        used_links.add(it.get("link",""))
        used_titles.add(it.get("title",""))

        cards.append(
            '<div style="border:1px solid #e6e8eb;border-radius:12px;background:#ffffff;'
            'box-shadow:0 1px 3px rgba(0,0,0,0.05);padding:14px;margin:10px 0;">'
            f'<div style="font-size:15px;font-weight:700;margin:0 0 6px;">{name}</div>'
            f'<p style="margin:0 0 6px;line-height:1.5;font-size:13.5px;color:#1f2937;">{en}</p>'
            + (f'<p dir="rtl" style="margin:0 12px 8px 0;line-height:1.6;font-size:13.5px;color:#111827;">{he}</p>' if he else "") +
            f'<a href="{link}" target="_blank" style="display:inline-block;padding:7px 10px;border-radius:8px;'
            'background:#0369a1;color:#ffffff;text-decoration:none;font-weight:600;font-size:12.5px;">Open source</a>'
            '</div>'
        )

    header = (
        '<div style="font-size:18px;font-weight:800;margin:24px 0 8px;'
        'padding-bottom:6px;border-bottom:1px solid #eceff3;color:#111827;">'
        '🎮 Top Trending Games in England — 5 to Watch</div>'
    )
    return header + "".join(cards), used_links, used_titles

# ---------- Back to top ----------
def _back_to_top():
    return ("<div style='text-align:right;margin:8px 0 0'>"
            "<a href='#top' style='font-size:12px;text-decoration:none;color:#2563eb;'>↑ Back to top</a>"
            "</div>")

# ---------- Email shell ----------
def build_email(collected, focus):
    # 1) Trends
    trends_html = build_trends_section(collected, focus)

    # 2) Games (collect used links & titles to avoid duplicates later)
    games_html, used_links, used_titles = build_games_section(collected, focus)

    # 3) Online Casino — UK Focus (≤ NEWS_MAX), without items already used in Games by link OR similar title
    news = collected.get("news_rss", []) or []
    filtered_news = []
    for it in news:
        if it.get("link") in used_links:
            continue
        if any(_is_title_similar(it.get("title",""), t) for t in used_titles):
            continue
        filtered_news.append(it)
    news = filtered_news[:NEWS_MAX]
    news_html = summarize_cards(news, "🎰 Online Casino — UK Focus")

    # Compose email in your requested order
    intro = (
        "<h1 style='margin:0 0 6px;font-size:22px;font-weight:800;color:#0b1220;'>Weekly iGaming Digest</h1>"
        "<p style='margin:0 0 12px;color:#4b5563;font-size:14px;'>"
        "📈 3 Trends · 🎮 5 Games to Watch · 🎰 Online Casino — UK Focus (up to 6). "
        "Each card includes EN+HE summary and a source link."
        "</p>"
    )

    html_parts = [
        '<div style="background:#f6f7f9;padding:24px 0;"><div style="max-width:720px;margin:0 auto;background:#ffffff;'
        'border:1px solid #e6e8eb;border-radius:14px;box-shadow:0 2px 6px rgba(0,0,0,0.04);'
        "padding:22px;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,sans-serif;'>"
        "<a id='top'></a>",
        intro
    ]

    # TOC
    toc_html = (
        "<div style='display:flex;gap:8px;flex-wrap:wrap;margin:8px 0 14px'>"
        "<a href='#trends' style='padding:8px 10px;border:1px solid #e5e7eb;border-radius:8px;text-decoration:none;"
        "font-size:13px;color:#111827;background:#f8fafc;'>📈 Trends</a>"
        "<a href='#games' style='padding:8px 10px;border:1px solid #e5e7eb;border-radius:8px;text-decoration:none;"
        "font-size:13px;color:#111827;background:#f8fafc;'>🎮 Games</a>"
        "<a href='#news' style='padding:8px 10px;border:1px solid #e5e7eb;border-radius:8px;text-decoration:none;"
        "font-size:13px;color:#111827;background:#f8fafc;'>🎰 Online Casino (UK)</a>"
        "</div>"
    )
    html_parts.append(toc_html)

    if trends_html:
        html_parts.append("<a id='trends'></a>")
        html_parts.append(trends_html)
        html_parts.append(_back_to_top())

    if games_html:
        html_parts.append("<a id='games'></a>")
        html_parts.append(games_html)
        html_parts.append(_back_to_top())

    if news_html:
        html_parts.append("<a id='news'></a>")
        html_parts.append(news_html)
        html_parts.append(_back_to_top())

    if DEBUG:
        counts = {sec: len(collected.get(sec, [])) for sec in ["news_rss","poker_rss","bingo_rss","podcasts_listennotes","games_rss"]}
        dbg = (
            "<div style='margin-top:16px;padding:12px;border:1px dashed #e5e7eb;border-radius:10px;"
            "background:#fafafa;color:#374151;font-size:12px;'>"
            f"<div><b>Debug</b></div>"
            f"<div>Counts: {counts}</div>"
            f"<div>FOCUS_THRESHOLD={FOCUS_THRESHOLD} | LOOKBACK_DAYS={LOOKBACK_DAYS} | NEWS_MAX={NEWS_MAX} | GAMES_TARGET={GAMES_TARGET}</div>"
            "</div>"
        )
        html_parts.append(dbg)

    html_parts.append(
        "<div style='margin-top:22px;padding-top:12px;border-top:1px solid #eceff3;color:#6b7280;font-size:12px;'>"
        "This digest is auto-generated. Sources are linked on each card.</div></div></div>"
    )
    html_body = "".join(html_parts)
    plain = "Weekly iGaming Digest (open HTML for full layout)."
    return plain, html_body

# ---------- Email ----------
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

# ---------- Sheets logging ----------
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

# ---------- Main ----------
if __name__ == "__main__":
    src = load_sources()
    focus = parse_focus(src)
    major_terms = (src.get("major_keywords", []) or src.get("focus", {}).get("major_keywords", []))

    collected = {}

    # RSS buckets
    for section in ["news_rss", "poker_rss", "bingo_rss", "games_rss"]:
        urls = src.get(section, []) or []
        items = collect_rss_items(section, urls)
        items = dedup_items(items)
        if section in ("poker_rss", "bingo_rss") and MAJOR_ONLY_NON_CASINO:
            items = [it for it in items if is_major(f"{it['title']} {it['summary']}", major_terms)]
        items = apply_focus_filter(items, focus, major_terms)
        if MAX_ITEMS_PER_SECTION > 0:
            items = items[:MAX_ITEMS_PER_SECTION]
        collected[section] = items

    # Games fallback URLs (for deeper lookback)
    collected["_games_fallback_urls"] = src.get("games_fallback_rss", []) or []

    # Podcasts (for trends context)
    ln_queries = src.get("podcasts_listennotes_queries", []) or []
    collected["podcasts_listennotes"] = collect_listennotes_items(ln_queries, major_terms, focus)

    # Manual MUST-INCLUDE URLs (e.g., EGR tax increase)
    must_urls = (src.get("must_include", {}) or {}).get("urls", []) or []
    manual_items = inject_must_include(must_urls)
    if manual_items:
        merged = (collected.get("news_rss") or []) + manual_items
        collected["news_rss"] = dedup_items(merged)
        # re-apply focus to avoid accidental drop
        collected["news_rss"] = apply_focus_filter(collected["news_rss"], focus, major_terms)

    try_log_to_sheets(collected)
    plain, html_body = build_email(collected, focus)
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    subject = f"Weekly Gambling Digest — {today} (UK Focus)"
    send_mail(subject, plain, html_body)
    print("Digest prepared and (if SMTP is valid) sent.")
