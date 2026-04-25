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
    return BeautifulSoup(resp.text, "lxml")


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

def _parse_review_card(card, index):
    # Rating — G2 uses aria-label="X out of 5" on the star container
    rating_val = 0
    stars_el = card.find(attrs={"aria-label": re.compile(r'[\d.]+ out of 5', re.I)})
    if stars_el:
        m = re.search(r'([\d.]+)\s+out of\s+5', stars_el.get("aria-label", ""), re.I)
        if m:
            rating_val = round(_safe_float(m.group(1)))
    if not rating_val:
        rating_meta = card.find("meta", {"itemprop": "ratingValue"})
        if rating_meta:
            rating_val = round(_safe_float(rating_meta.get("content", 0)))

    # Title
    title_el = card.find(attrs={"itemprop": "name"}) or card.find("h3")
    title = _text(title_el)[:200] if title_el else ""

    # Author — G2 uses fw-semibold span inside a user link
    author = "Anonymous"
    author_el = (
        card.find(attrs={"itemprop": "author"}) or
        card.find("a", href=re.compile(r'/users/'))
    )
    if author_el:
        span = author_el.find("span", class_=re.compile(r'fw-semibold', re.I)) or author_el
        author = _text(span)[:100] or "Anonymous"

    # Author title / job
    author_title = ""
    for el in card.find_all(class_=re.compile(r'reviewer-title|job-title|l-text-muted|small', re.I)):
        t = _text(el)
        if t and len(t) < 100 and t != author:
            author_title = t[:100]
            break

    # Date
    date = ""
    date_el = card.find("time")
    if date_el:
        date = date_el.get("datetime", "") or _text(date_el)

    # Pros / Cons — G2 uses <strong> labels inside <p> before the answer <p>
    pros = ""
    cons = ""
    for strong in card.find_all("strong"):
        label = _text(strong).lower()
        parent_p = strong.find_parent("p")
        if not parent_p:
            continue
        answer_p = parent_p.find_next_sibling("p")
        if not answer_p:
            continue
        answer = _text(answer_p)
        if not answer:
            continue
        if "like best" in label or ("like" in label and "what do you" in label):
            pros = answer[:500]
        elif "dislike" in label or "least" in label or ("don't like" in label):
            cons = answer[:500]

    # Full review body
    body_el = card.find(attrs={"itemprop": "reviewBody"})
    text = _text(body_el)[:2000] if body_el else (f"{pros} {cons}".strip())[:2000]

    if not text and not title:
        return None

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
        "verified": bool(card.find(string=re.compile(r'\bverified\b', re.I))),
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
            soup = _fetch_page(f"https://www.g2.com/products/{slug}/reviews")

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
                for sel in [
                    {"property": "og:description"},
                    {"name": "description"},
                    {"name": "twitter:description"},
                ]:
                    m = soup.find("meta", sel)
                    if m and m.get("content", "").strip():
                        description = m["content"].strip()[:500]
                        break
            if not description:
                desc_el = soup.find(attrs={"itemprop": "description"})
                if desc_el:
                    description = _text(desc_el)[:500]
            if not description:
                # First substantial paragraph not inside a review card
                for p in soup.find_all("p"):
                    t = _text(p)
                    if len(t) > 80 and not p.find_parent(attrs={"itemprop": "review"}):
                        description = t[:500]
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

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "returned": len(reviews),
                "reviews": reviews,
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

        # Strategy 1: elements with class containing "feature"
        for el in soup.find_all(class_=re.compile(r'\bfeature\b', re.I)):
            name = _text(el)
            if (2 <= len(name) <= 80 and
                    name.lower() not in seen and
                    not re.search(r'reviews?|g2|rating|compare|learn|see all', name, re.I)):
                seen.add(name.lower())
                features.append(name)

        # Strategy 2: table cells (G2 features page uses a scoring table)
        if not features:
            for td in soup.find_all("td"):
                name = _text(td)
                if (3 <= len(name) <= 70 and
                        name.lower() not in seen and
                        not re.search(r'\d+%|\$|\bvs\b|g2|review|rating|compare', name, re.I)):
                    seen.add(name.lower())
                    features.append(name)

        # Strategy 3: list items in main content area
        if not features:
            main = soup.find("main") or soup.find("article") or soup
            for li in main.find_all("li"):
                name = _text(li)
                if (3 <= len(name) <= 70 and
                        name.lower() not in seen and
                        not re.search(r'reviews?|pricing|login|sign', name, re.I)):
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

        if re.search(r'\bfree\s*plan\b|\bfreemium\b|\bfree\s*tier\b', page_text, re.I):
            has_free_plan = True
        if re.search(r'free\s*trial|trial\s*available', page_text, re.I):
            has_free_trial = True

        # Strategy 1: structured pricing cards
        for card in soup.find_all(class_=re.compile(r'pricing.*card|price.*plan|tier.*card|plan.*card', re.I)):
            tier_name_el = card.find(["h2", "h3", "h4", "strong"])
            price_el = card.find(class_=re.compile(r'\bprice\b', re.I))
            if tier_name_el:
                tier_name = _text(tier_name_el)[:60]
                price = _text(price_el)[:60] if price_el else "Contact Sales"
                if tier_name and tier_name.lower() not in seen_prices:
                    pricing_tiers.append({"name": tier_name, "price": price})
                    seen_prices.add(tier_name.lower())

        # Strategy 2: regex extraction from page text
        if not pricing_tiers:
            for m in re.finditer(
                r'\b([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)?)\s*(?:Plan|Edition|Tier)?\s*[:\-\u2013]\s*'
                r'(\$[\d,]+(?:\.\d{1,2})?(?:[/ ]\w+)*)',
                page_text,
            ):
                name_t, price_t = m.group(1).strip(), m.group(2).strip()
                if price_t not in seen_prices and not re.match(
                    r'^(Find|See|Read|Get|Compare|Learn|The|This|With|For|From|All)$',
                    name_t, re.I,
                ):
                    pricing_tiers.append({"name": name_t, "price": price_t})
                    seen_prices.add(price_t)

        if re.search(r'contact\s+sales|custom\s+pric|enterprise\s+pric', page_text, re.I):
            if not any(t["name"].lower() == "enterprise" for t in pricing_tiers):
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

        soup = _fetch_page(f"https://www.g2.com/products/{slug}/competitors/highest_rated")

        # Find competitor product links with names
        for a in soup.find_all("a", href=re.compile(r'/products/[^/?#]+(?:/reviews)?')):
            if len(alternatives) >= limit:
                break
            href = a.get("href", "")
            alt_slug = _slug_from_url(href)
            if not alt_slug or alt_slug in seen:
                continue
            seen.add(alt_slug)

            name = _text(a)[:100] or alt_slug.replace("-", " ").title()

            # Walk up to card container to find rating
            alt_rating = 0.0
            card = a
            for _ in range(5):
                card = card.parent
                if not card:
                    break
                stars_el = card.find(attrs={"aria-label": re.compile(r'[\d.]+ out of 5', re.I)})
                if stars_el:
                    m = re.search(r'([\d.]+)\s+out of\s+5', stars_el.get("aria-label", ""), re.I)
                    if m:
                        v = _safe_float(m.group(1))
                        if 0 < v <= 5:
                            alt_rating = round(v, 1)
                    break

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

        for a in soup.find_all("a", href=re.compile(r'/products/[^/?#]+')):
            if len(products) >= limit:
                break
            href = a.get("href", "")
            slug = _slug_from_url(href)
            if not slug or slug in seen:
                continue
            seen.add(slug)

            name = _text(a)[:100] or slug.replace("-", " ").title()

            # Walk to card for rating + description
            alt_rating = 0.0
            desc = ""
            card = a
            for _ in range(5):
                card = card.parent
                if not card:
                    break
                stars_el = card.find(attrs={"aria-label": re.compile(r'[\d.]+ out of 5', re.I)})
                if stars_el:
                    m = re.search(r'([\d.]+)\s+out of\s+5', stars_el.get("aria-label", ""), re.I)
                    if m:
                        v = _safe_float(m.group(1))
                        if 0 < v <= 5:
                            alt_rating = round(v, 1)
                p = card.find("p")
                if p:
                    desc = _text(p)[:300]
                if alt_rating or desc:
                    break

            products.append({
                "name": name,
                "slug": slug,
                "rating": alt_rating,
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

        for a in soup.find_all("a", href=re.compile(r'/products/[^/?#]+')):
            if len(products) >= limit:
                break
            href = a.get("href", "")
            prod_slug = _slug_from_url(href)
            if not prod_slug or prod_slug in seen:
                continue
            seen.add(prod_slug)

            name = _text(a)[:100] or prod_slug.replace("-", " ").title()

            alt_rating = 0.0
            desc = ""
            card = a
            for _ in range(5):
                card = card.parent
                if not card:
                    break
                stars_el = card.find(attrs={"aria-label": re.compile(r'[\d.]+ out of 5', re.I)})
                if stars_el:
                    m = re.search(r'([\d.]+)\s+out of\s+5', stars_el.get("aria-label", ""), re.I)
                    if m:
                        v = _safe_float(m.group(1))
                        if 0 < v <= 5:
                            alt_rating = round(v, 1)
                p = card.find("p")
                if p:
                    desc = _text(p)[:300]
                if alt_rating or desc:
                    break

            products.append({
                "name": name,
                "slug": prod_slug,
                "rating": alt_rating,
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
