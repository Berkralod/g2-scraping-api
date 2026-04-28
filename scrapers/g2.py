"""
G2 Scraping API — Bright Data Web Unlocker + rating_schema.json (0-credit primary)

Per-endpoint call budget (BD ~5-15s, gateway kills at 30s):
  product:      rating_schema.json (~1s) + 1 BD fetch for stars/description
  reviews:      1 BD fetch — real reviews with author/date/rating/pros/cons
  features:     1 BD fetch
  pricing:      1 BD fetch
  alternatives: 1 BD fetch (competitors page)
  search:       1 BD fetch
  category:     1 BD fetch
"""
import os
import re
import json
from datetime import datetime
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

BRIGHTDATA_API_KEY = os.getenv("BRIGHTDATA_API_KEY")
BRIGHTDATA_ZONE = os.getenv("BRIGHTDATA_ZONE", "web_unlocker1")

_BD_TIMEOUT = 90       # hard cap — BD needs up to 60s on heavy pages; gateway is now 120s
_SCHEMA_TIMEOUT = 8    # rating_schema.json is tiny


# ---------------------------------------------------------------------------
# Core fetch helpers
# ---------------------------------------------------------------------------

def _fetch_page(url: str) -> BeautifulSoup:
    resp = requests.post(
        "https://api.brightdata.com/request",
        headers={
            "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
            "Content-Type": "application/json",
        },
        json={"zone": BRIGHTDATA_ZONE, "url": url, "format": "raw"},
        timeout=_BD_TIMEOUT,
    )
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def _fetch_page_raw(url: str):
    """Returns (soup, raw_html) for callers that need regex fallbacks."""
    resp = requests.post(
        "https://api.brightdata.com/request",
        headers={
            "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
            "Content-Type": "application/json",
        },
        json={"zone": BRIGHTDATA_ZONE, "url": url, "format": "raw"},
        timeout=_BD_TIMEOUT,
    )
    resp.raise_for_status()
    raw = resp.text
    return BeautifulSoup(raw, "html.parser"), raw


def _fetch_rating_schema(slug: str) -> dict:
    resp = requests.get(
        f"https://www.g2.com/products/{slug}/rating_schema.json",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=_SCHEMA_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _safe_float(value, default=0.0):
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _safe_int(value, default=0):
    try:
        return int(str(value).replace(",", "").strip())
    except Exception:
        return default


def _text(el):
    return el.get_text(separator=" ", strip=True) if el else ""


def _slug_from_url(url):
    m = re.search(r'g2\.com/products/([^/?#]+)', url)
    return m.group(1) if m else ""


def _parse_json_ld(soup):
    result = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
            if isinstance(data, list):
                result.extend(data)
            else:
                result.append(data)
        except Exception:
            pass
    return result


# ---------------------------------------------------------------------------
# Page-level data extractors
# ---------------------------------------------------------------------------

def _rating_from_page(soup):
    # itemprop meta tag
    meta = soup.find("meta", {"itemprop": "ratingValue"})
    if meta:
        v = _safe_float(meta.get("content", 0))
        if 0 < v <= 5:
            return round(v, 1)
        if 0 < v <= 10:
            return round(v / 2, 1)

    # JSON-LD aggregateRating
    for block in _parse_json_ld(soup):
        agg = block.get("aggregateRating", {})
        if agg:
            raw = _safe_float(agg.get("ratingValue", 0))
            best = _safe_float(agg.get("bestRating", 5))
            if raw and best:
                return round(raw / best * 5, 1)

    # aria-label "X out of 5" on overall rating container
    for el in soup.find_all(attrs={"aria-label": re.compile(r'[\d.]+ out of 5', re.I)}):
        m = re.search(r'([\d.]+)\s+out of\s+5', el.get("aria-label", ""), re.I)
        if m:
            v = _safe_float(m.group(1))
            if 0 < v <= 5:
                return round(v, 1)

    return 0.0


def _review_count_from_page(soup):
    for el in soup.find_all(string=re.compile(r'[\d,]+\s+reviews?', re.I)):
        m = re.search(r'([\d,]+)\s+reviews?', str(el), re.I)
        if m:
            v = _safe_int(m.group(1))
            if v > 0:
                return v
    return 0


def _stars_dist_from_page(soup):
    dist = {"5": 0, "4": 0, "3": 0, "2": 0, "1": 0}
    html = str(soup)

    # Strategy 1: aria-label="X stars: Y%" on any element
    for el in soup.find_all(attrs={"aria-label": True}):
        label = el.get("aria-label", "")
        m = re.match(r'(\d)\s*stars?\s*[:\-]\s*(\d+)\s*%', label, re.I)
        if m:
            dist[m.group(1)] = _safe_int(m.group(2))
    if any(v > 0 for v in dist.values()):
        return dist

    # Strategy 2: raw HTML scan — picks up any inline "5 star … 62%" text regardless of markup
    # Handles patterns like: "5 stars</div><div>62%", "5 star: 62%", "62% 5 stars"
    for star in range(5, 0, -1):
        patterns = [
            rf'{star}\s*stars?[^<]{{0,60}}?(\d{{1,3}})\s*%',
            rf'(\d{{1,3}})\s*%[^<]{{0,60}}?{star}\s*stars?',
            rf'"{star}\s*stars?"[^>]*?>(\d{{1,3}})',
        ]
        for pat in patterns:
            m = re.search(pat, html, re.I)
            if m:
                dist[str(star)] = _safe_int(m.group(1))
                break

    if any(v > 0 for v in dist.values()):
        return dist

    # Strategy 3: data-score attribute
    for el in soup.find_all(attrs={"data-score": True}):
        score = el.get("data-score", "")
        m = re.search(r'(\d+)\s*%', _text(el))
        if m and score.isdigit() and 1 <= int(score) <= 5:
            dist[score] = _safe_int(m.group(1))

    return dist


# ---------------------------------------------------------------------------
# Review card parser
# ---------------------------------------------------------------------------

def _split_review_text(text):
    """Split G2 review body into pros/cons using G2's fixed question labels."""
    pros, cons = "", ""
    m_like     = re.search(r'What do you like best[^?]*\?', text, re.I)
    m_dislike  = re.search(r'What do you (?:not like|dislike)[^?]*\?', text, re.I)
    m_problems = re.search(r'(?:What problems|Recommendations)[^?]*\?', text, re.I)
    if m_like:
        end  = m_dislike.start() if m_dislike else (m_problems.start() if m_problems else len(text))
        pros = text[m_like.end():end].strip()
    if m_dislike:
        end  = m_problems.start() if m_problems else len(text)
        cons = text[m_dislike.end():end].strip()
    return pros[:500], cons[:500]


def _parse_review_card(card, index):
    # Rating — meta[itemprop="ratingValue"] is on 0-5 scale for individual reviews
    rating_val = 0
    rating_meta = card.find("meta", {"itemprop": "ratingValue"})
    if rating_meta:
        rating_val = int(_safe_float(rating_meta.get("content", 0)) + 0.5)

    # Fallback: aria-label "X out of 5" on star elements within the card
    if not rating_val:
        for el in card.find_all(attrs={"aria-label": True}):
            m = re.search(r'([\d.]+)\s+out\s+of\s+5', el.get("aria-label", ""), re.I)
            if m:
                rating_val = int(_safe_float(m.group(1)) + 0.5)
                if 1 <= rating_val <= 5:
                    break
                rating_val = 0

    # Title — div[itemprop="name"], NOT the meta tag inside itemprop="author"
    title = ""
    title_el = card.find("div", attrs={"itemprop": "name"})
    if title_el:
        title = _text(title_el)[:200]

    # Author — meta[itemprop="name"] inside itemprop="author" div
    author = "Anonymous"
    author_div = card.find(attrs={"itemprop": "author"})
    if author_div:
        name_meta = author_div.find("meta", {"itemprop": "name"})
        author = name_meta.get("content", "").strip()[:100] if name_meta else _text(author_div)[:100]
        author = author or "Anonymous"

    # Author title — first elv-text-subtle sibling right after itemprop="author" div
    # G2 structure: author_div → [job title] → [sector] → [company size] all as siblings
    author_title = ""
    if author_div:
        sib = author_div.find_next_sibling()
        if sib and "elv-text-subtle" in " ".join(sib.get("class") or []):
            author_title = _text(sib)[:120]

    # Date — G2 uses meta[itemprop="datePublished"], NOT <time> tags
    date = ""
    date_meta = card.find("meta", {"itemprop": "datePublished"})
    if date_meta:
        date = date_meta.get("content", "")

    # Pros / Cons + full text — parse <section> elements inside reviewBody
    # Remove "Review collected by and hosted on G2.com." spans before extracting text
    pros = ""
    cons = ""
    text = ""
    body_el = card.find(attrs={"itemprop": "reviewBody"})
    if body_el:
        for spht in body_el.find_all("span", class_="spht"):
            spht.decompose()

        for section in body_el.find_all("section"):
            label_el = section.find(class_=re.compile(r'elv-font-bold', re.I))
            label = _text(label_el).lower() if label_el else ""
            answer = " ".join(_text(p) for p in section.find_all("p")).strip()
            if "like best" in label:
                pros = answer[:500]
            elif "dislike" in label or "not like" in label:
                cons = answer[:500]

        text = _text(body_el)[:2000]

    # Fallback: regex-split the full text if sections parsing didn't work
    if not pros and not cons and text:
        pros, cons = _split_review_text(text)

    if not text:
        text = f"{pros} {cons}".strip()[:2000]

    if not text and not title:
        return None

    # Verified — G2 shows "Verified by {Product}" badge
    verified = bool(card.find(string=re.compile(r'\bVerified\s+by\b', re.I)))

    return {
        "id": f"g2-{index}",
        "rating": rating_val,
        "title": title,
        "pros": pros,
        "cons": cons,
        "text": text,
        "date": date,
        "author": author,
        "author_title": author_title,
        "verified": verified,
        "helpful_votes": 0,
        "platform": "g2",
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_product(slug: str) -> dict:
    try:
        profile_url = f"https://www.g2.com/products/{slug}/reviews"
        name = slug
        rating = 0.0
        total_reviews = 0
        description = ""
        categories = []
        stars_dist = {"5": 0, "4": 0, "3": 0, "2": 0, "1": 0}

        # Primary: rating_schema.json — 0 credits, ~1s
        try:
            schema = _fetch_rating_schema(slug)
            name = schema.get("name") or slug
            agg = schema.get("aggregateRating", {})
            best = _safe_float(agg.get("bestRating", 10))
            raw = _safe_float(agg.get("ratingValue", 0))
            rating = round(raw / best * 5, 1) if best else 0.0
            total_reviews = _safe_int(agg.get("reviewCount", 0))
            raw_cats = schema.get("applicationCategory", "")
            if raw_cats:
                categories = [c.strip() for c in raw_cats.split(",") if c.strip()]
        except Exception:
            pass

        # Bright Data: fill stars distribution, description, and any gaps
        try:
            soup, raw_html = _fetch_page_raw(f"https://www.g2.com/products/{slug}/reviews")

            if name == slug:
                h1 = soup.find("h1")
                if h1:
                    name = re.sub(r'\s+[Rr]eviews?.*$', '', _text(h1)).strip() or name

            if rating == 0.0:
                rating = _rating_from_page(soup)

            if total_reviews == 0:
                total_reviews = _review_count_from_page(soup)

            stars_dist = _stars_dist_from_page(soup)

            if not description:
                # Primary: <p itemprop="description"> — server-rendered product description
                desc_el = soup.find(attrs={"itemprop": "description"})
                if desc_el and desc_el.name != "meta":
                    description = _text(desc_el)[:500]
            if not description:
                # Regex fallback on raw HTML (handles html.parser edge cases)
                m_rdesc = re.search(r'itemprop="description"[^>]*>([^<]{15,})<', raw_html)
                if m_rdesc:
                    description = m_rdesc.group(1).strip()[:500]
            if not description:
                # Last resort: meta description if it's not a filter/review-count page
                for meta_sel in [{"name": "description"}, {"name": "twitter:description"}]:
                    meta_el = soup.find("meta", meta_sel)
                    if meta_el and meta_el.get("content", "").strip():
                        cand = meta_el["content"].strip()
                        if not re.search(r'Filter\s+[\d,]+\s+reviews', cand, re.I):
                            description = cand[:500]
                            break

            if not categories:
                for a in soup.find_all("a", href=re.compile(r'/categories/')):
                    cat = _text(a)
                    if cat and len(cat) < 60 and cat not in categories:
                        categories.append(cat)

        except Exception:
            pass

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "name": name,
                "rating": rating,
                "total_reviews": total_reviews,
                "stars_distribution": stars_dist,
                "categories": categories,
                "description": description,
                "platform": "g2",
                "profile_url": profile_url,
                "scraped_at": datetime.utcnow().isoformat(),
            },
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error_code": "TIMEOUT", "platform": "g2", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error_code": "HTTP_ERROR", "platform": "g2", "message": str(e)}
    except Exception as e:
        return {"status": "error", "error_code": "SCRAPER_ERROR", "platform": "g2", "message": "Scraper failed", "detail": str(e)}


def get_reviews(slug: str, limit: int = 20, rating: int = None, sort: str = "most_recent") -> dict:
    try:
        reviews = []
        soup = _fetch_page(f"https://www.g2.com/products/{slug}/reviews")

        # Primary: itemprop="review" sections
        cards = soup.find_all(attrs={"itemprop": "review"})

        # Fallback 1: divs containing review meta tags
        if not cards:
            cards = [
                div for div in soup.find_all("div")
                if div.find("meta", {"itemprop": "ratingValue"}) or
                   div.find(attrs={"aria-label": re.compile(r'[\d.]+ out of 5', re.I)})
            ][:40]

        # Fallback 2: paper--box containers
        if not cards:
            cards = soup.find_all("div", class_=re.compile(r'paper.*box|review.*card', re.I))

        for i, card in enumerate(cards):
            if len(reviews) >= limit:
                break
            parsed = _parse_review_card(card, i + 1)
            if not parsed:
                continue
            if rating is not None and parsed["rating"] and parsed["rating"] != rating:
                continue
            reviews.append(parsed)

        stars_dist = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
        for r in reviews:
            rv = r.get("rating")
            if rv and 1 <= rv <= 5:
                stars_dist[str(rv)] += 1

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "returned": len(reviews),
                "reviews": reviews,
                "stars_distribution": stars_dist,
                "platform": "g2",
            },
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error_code": "TIMEOUT", "platform": "g2", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error_code": "HTTP_ERROR", "platform": "g2", "message": str(e)}
    except Exception as e:
        return {"status": "error", "error_code": "SCRAPER_ERROR", "platform": "g2", "message": "Reviews failed", "detail": str(e)}


def get_features(slug: str) -> dict:
    try:
        features = []
        seen = set()

        soup = _fetch_page(f"https://www.g2.com/products/{slug}/features")

        # Strategy 1: combobox JSON in data attribute — most reliable source
        # G2 embeds the full feature list as JSON in the filter combobox
        combobox = soup.find(attrs={"data-elv--form--combobox-controller-choices-value": True})
        if combobox:
            raw = combobox.get("data-elv--form--combobox-controller-choices-value", "")
            raw = raw.replace("&quot;", '"')
            try:
                items = json.loads(raw)
                for item in items:
                    label = item.get("label", "").strip()
                    if label and label.lower() not in seen and 2 <= len(label) <= 80:
                        seen.add(label.lower())
                        features.append(label)
            except Exception:
                pass

        # Strategy 2: grid-item elements with feature names
        if not features:
            for el in soup.find_all(class_=re.compile(r'grid-item', re.I)):
                name = _text(el)
                if (3 <= len(name) <= 70 and
                        name.lower() not in seen and
                        not re.search(r'\d+%|\$|g2|review|rating|compare|learn|sign', name, re.I)):
                    seen.add(name.lower())
                    features.append(name)

        # Strategy 3: table cells
        if not features:
            for td in soup.find_all("td"):
                name = _text(td)
                if (3 <= len(name) <= 70 and name.lower() not in seen and
                        not re.search(r'\d+%|\$|\bvs\b|g2|review|rating|compare', name, re.I)):
                    seen.add(name.lower())
                    features.append(name)

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "features": features,
                "feature_count": len(features),
                "platform": "g2",
                "scraped_at": datetime.utcnow().isoformat(),
            },
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error_code": "TIMEOUT", "platform": "g2", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error_code": "HTTP_ERROR", "platform": "g2", "message": str(e)}
    except Exception as e:
        return {"status": "error", "error_code": "SCRAPER_ERROR", "platform": "g2", "message": "Features failed", "detail": str(e)}


def get_pricing(slug: str) -> dict:
    try:
        pricing_tiers = []
        has_free_plan = False
        has_free_trial = False
        seen_prices = set()

        soup = _fetch_page(f"https://www.g2.com/products/{slug}/pricing")
        page_text = soup.get_text()

        if re.search(r'\bfree\s*plan\b|\bfreemium\b|\bfree\s*tier\b|\$0\.00', page_text, re.I):
            has_free_plan = True
        if re.search(r'free\s*trial|trial\s*available', page_text, re.I):
            has_free_trial = True

        # G2 pricing page uses elv-font-semibold for plan names and
        # elv-text-xl + elv-font-bold for prices ($X.XX format)
        html = str(soup)
        names = re.findall(r'elv-font-semibold[^>]+>\s*([A-Z][A-Za-z][^<\n]{0,40}?)\s*</div>', html)
        prices = re.findall(r'elv-text-xl[^"]*elv-font-bold[^>]+>\s*(\$[\d.]+)\s*<', html)

        # Clean plan names: exclude noise words and keep only meaningful tier names
        plan_names = []
        seen_names = set()
        for n in names:
            n = n.strip()
            if (2 <= len(n) <= 40 and n.lower() not in seen_names and
                    not re.search(r'trial|deal|offer|savings|exclusive|limited|claim|website|'
                                  r'contact|filter|compare|review|feature|integrat', n, re.I)):
                seen_names.add(n.lower())
                plan_names.append(n)

        # Zip names with prices; extras get "Contact Sales"
        for i, plan_name in enumerate(plan_names):
            price = prices[i] if i < len(prices) else "Contact Sales"
            if plan_name.lower() not in seen_prices:
                pricing_tiers.append({"name": plan_name, "price": price})
                seen_prices.add(plan_name.lower())

        # Ensure Enterprise tier appears if mentioned
        if re.search(r'contact\s+sales|custom\s+pric|enterprise\s+pric', page_text, re.I):
            if not any(re.search(r'enterprise', t["name"], re.I) for t in pricing_tiers):
                pricing_tiers.append({"name": "Enterprise", "price": "Contact Sales"})

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "pricing_tiers": pricing_tiers,
                "has_free_plan": has_free_plan,
                "has_free_trial": has_free_trial,
                "platform": "g2",
                "scraped_at": datetime.utcnow().isoformat(),
            },
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error_code": "TIMEOUT", "platform": "g2", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error_code": "HTTP_ERROR", "platform": "g2", "message": str(e)}
    except Exception as e:
        return {"status": "error", "error_code": "SCRAPER_ERROR", "platform": "g2", "message": "Pricing failed", "detail": str(e)}


def get_alternatives(slug: str, limit: int = 5) -> dict:
    try:
        alternatives = []
        seen = {slug}

        # Correct URL: /competitors/alternatives (not /competitors/highest_rated)
        soup = _fetch_page(f"https://www.g2.com/products/{slug}/competitors/alternatives")

        # G2 alternatives page: each card has TWO /reviews links —
        #   (1) image/icon link (empty text)
        #   (2) named link — contains an elv-font-bold div with the product name
        # Only process named links so we get correct name and avoid breadcrumb links.
        named_links = [
            a for a in soup.find_all("a", href=re.compile(r'/products/[^/?#]+/reviews$'))
            if a.find(class_=re.compile(r'elv-font-bold', re.I))
        ]

        for a in named_links:
            if len(alternatives) >= limit:
                break
            href = a.get("href", "")
            alt_slug = _slug_from_url(href)
            if not alt_slug or alt_slug in seen:
                continue
            seen.add(alt_slug)

            # Name is directly inside the elv-font-bold div within the link
            name_el = a.find(class_=re.compile(r'elv-font-bold', re.I))
            name = _text(name_el)[:100] if name_el else (_text(a)[:100] or alt_slug.replace("-", " ").title())

            # Rating — "X.X/5" label is a sibling in the same card container (level 0-2)
            alt_rating = 0.0
            card = a.parent
            for _ in range(4):
                if not card:
                    break
                for lbl in card.find_all(["label", "span", "div"]):
                    t = _text(lbl)
                    m = re.match(r'^([\d.]+)/5$', t.strip())
                    if m:
                        v = _safe_float(m.group(1))
                        if 0 < v <= 5:
                            alt_rating = round(v, 1)
                        break
                if alt_rating:
                    break
                card = card.parent

            alternatives.append({
                "name": name,
                "slug": alt_slug,
                "rating": alt_rating,
                "profile_url": f"https://www.g2.com/products/{alt_slug}/reviews",
                "compare_url": f"https://www.g2.com/compare/{slug}-vs-{alt_slug}",
                "platform": "g2",
            })

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "alternatives": alternatives,
                "returned": len(alternatives),
                "platform": "g2",
                "scraped_at": datetime.utcnow().isoformat(),
            },
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error_code": "TIMEOUT", "platform": "g2", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error_code": "HTTP_ERROR", "platform": "g2", "message": str(e)}
    except Exception as e:
        return {"status": "error", "error_code": "SCRAPER_ERROR", "platform": "g2", "message": "Alternatives failed", "detail": str(e)}


def search_products(query: str, category: str = None, limit: int = 10) -> dict:
    try:
        products = []
        seen = set()

        url = f"https://www.g2.com/search?query={quote(query)}"
        if category:
            url += f"&category={quote(category)}"

        soup = _fetch_page(url)

        # G2 search/category pages use "x-software-component-card" divs for product listings.
        # Each card contains the product name link, "X.X out of 5" rating, and description text.
        for card in soup.find_all("div", class_=re.compile(r'x-software-component-card')):
            if len(products) >= limit:
                break

            # Name + slug: text link to /products/.../reviews inside the card
            slug = ""
            name = ""
            for a in card.find_all("a", href=re.compile(r'/products/[^/?#]+/reviews')):
                if a.get_text(strip=True) and not a.find("img"):
                    slug = _slug_from_url(a.get("href", ""))
                    name = a.get_text(strip=True)[:100]
                    break
            if not slug or slug in seen or "url_slug" in slug:
                continue
            seen.add(slug)
            if not name:
                name = slug.replace("-", " ").title()

            # Rating: "X.X out of 5" in card text
            card_text = card.get_text(separator=" ", strip=True)
            prod_rating = 0.0
            m_r = re.search(r'([\d.]+)\s+out\s+of\s+5', card_text, re.I)
            if m_r:
                v = _safe_float(m_r.group(1))
                if 0 < v <= 5:
                    prod_rating = round(v, 1)

            # Description: text in "Product Description" section of the card
            desc = ""
            m_d = re.search(
                r'Product Description\s+(.+?)(?:\s+Overview|\s+Pros and Cons|$)',
                card_text, re.I | re.S,
            )
            if m_d:
                desc = m_d.group(1).strip()[:300]

            products.append({
                "name": name,
                "slug": slug,
                "rating": prod_rating,
                "description": desc,
                "profile_url": f"https://www.g2.com/products/{slug}/reviews",
                "platform": "g2",
            })

        return {
            "status": "success",
            "data": {
                "query": query,
                "category": category,
                "results": products,
                "total_found": len(products),
                "platform": "g2",
            },
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error_code": "TIMEOUT", "platform": "g2", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error_code": "HTTP_ERROR", "platform": "g2", "message": str(e)}
    except Exception as e:
        return {"status": "error", "error_code": "SCRAPER_ERROR", "platform": "g2", "message": "Search failed", "detail": str(e)}


def get_category(slug: str, limit: int = 10) -> dict:
    try:
        products = []
        seen = set()

        soup = _fetch_page(f"https://www.g2.com/categories/{slug}")

        # G2 category pages use "x-software-component-card" divs — same as search pages.
        for card in soup.find_all("div", class_=re.compile(r'x-software-component-card')):
            if len(products) >= limit:
                break

            # Name + slug: text link (no img) to /products/.../reviews
            prod_slug = ""
            name = ""
            for a in card.find_all("a", href=re.compile(r'/products/[^/?#]+/reviews')):
                if a.get_text(strip=True) and not a.find("img"):
                    prod_slug = _slug_from_url(a.get("href", ""))
                    name = a.get_text(strip=True)[:100]
                    break
            if not prod_slug or prod_slug in seen or "url_slug" in prod_slug:
                continue
            seen.add(prod_slug)
            if not name:
                name = prod_slug.replace("-", " ").title()

            # Rating: "X.X out of 5" in card text
            card_text = card.get_text(separator=" ", strip=True)
            prod_rating = 0.0
            m_r = re.search(r'([\d.]+)\s+out\s+of\s+5', card_text, re.I)
            if m_r:
                v = _safe_float(m_r.group(1))
                if 0 < v <= 5:
                    prod_rating = round(v, 1)

            # Description: text in "Product Description" section
            desc = ""
            m_d = re.search(
                r'Product Description\s+(.+?)(?:\s+Overview|\s+Pros and Cons|$)',
                card_text, re.I | re.S,
            )
            if m_d:
                desc = m_d.group(1).strip()[:300]

            products.append({
                "name": name,
                "slug": prod_slug,
                "rating": prod_rating,
                "description": desc,
                "profile_url": f"https://www.g2.com/products/{prod_slug}/reviews",
                "platform": "g2",
            })

        return {
            "status": "success",
            "data": {
                "category_slug": slug,
                "category_url": f"https://www.g2.com/categories/{slug}",
                "products": products,
                "total_found": len(products),
                "platform": "g2",
                "scraped_at": datetime.utcnow().isoformat(),
            },
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error_code": "TIMEOUT", "platform": "g2", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error_code": "HTTP_ERROR", "platform": "g2", "message": str(e)}
    except Exception as e:
        return {"status": "error", "error_code": "SCRAPER_ERROR", "platform": "g2", "message": "Category failed", "detail": str(e)}
