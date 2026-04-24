"""
G2 Scraping API — 30s RapidAPI budget

Per-endpoint SERP call budget (each call ~8-12s):
  product:      rating_schema.json (0 cr, ~1s) + 1 SERP for stars/description
  reviews:      1 SERP  (source-2 "What do you like best" dropped — too slow)
  features:     1 SERP
  pricing:      1 SERP
  alternatives: 1 SERP + parallel rating_schema.json (~1-2s, no credits)
  search:       1 SERP
  category:     1 SERP
"""
import os
import re
from datetime import datetime

import requests

SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY")

_SERP_TIMEOUT = 25    # requests.get hard cap — gateway kills at 30s
_SCHEMA_TIMEOUT = 8   # rating_schema.json is tiny
_SCRAPERAPI_TIMEOUT = 20  # ScraperAPI server-side cut-off (returns partial on timeout)


# ---------------------------------------------------------------------------
# SERP helpers
# ---------------------------------------------------------------------------

def _serp(query: str) -> dict:
    resp = requests.get(
        "https://api.scraperapi.com/structured/google/search",
        params={
            "api_key": SCRAPERAPI_KEY,
            "query": query,
            "timeout": _SCRAPERAPI_TIMEOUT,  # server-side; ScraperAPI returns before our hard cap
            "render": "false",               # no JS rendering — 2-3x faster
            "country_code": "us",            # consistent datacenter routing
        },
        timeout=_SERP_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def _organic(data: dict) -> list:
    return [r for r in data.get("organic_results", []) if isinstance(r, dict)]


def _snippets(results: list) -> list:
    return [r.get("snippet", "") or r.get("title", "") for r in results]


def _safe_float(value, default=0.0) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return default


def _safe_int(value, default=0) -> int:
    try:
        return int(str(value).replace(",", "").strip())
    except Exception:
        return default


def _extract_name(title: str) -> str:
    m = re.match(r'^(.+?)\s+Reviews\b', title, re.I)
    return m.group(1).strip() if m else re.split(r'\s+[-|:]', title)[0].strip()


def _extract_rating(texts: list) -> float:
    patterns = [
        r'([\d.]+)\s+out of\s+5\s+stars?',
        r'([\d.]+)/5\s+(?:rating|stars?)',
        r'rated\s+([\d.]+)\s+stars?',
        r'([\d.]+)\s+stars?\s+by\s+[\d,]+',
        r'([\d.]+)\s+star\s+rating',
    ]
    for s in texts:
        for pat in patterns:
            m = re.search(pat, s, re.I)
            if m:
                v = _safe_float(m.group(1))
                if 0 < v <= 5:
                    return v
    return 0.0


def _extract_review_count(texts: list) -> int:
    for s in texts:
        m = re.search(r'[Ff]ilter\s+([\d,]+)\s+reviews', s)
        if m:
            return _safe_int(m.group(1))
        m = re.search(r'[Ss]ee\s+(?:all\s+)?([\d,]+)\s+(?:more\s+)?reviews', s)
        if m:
            return _safe_int(m.group(1))
        m = re.search(r'[Rr]ead\s+([\d,]+)\s+[Rr]eviews', s)
        if m:
            return _safe_int(m.group(1))
        m = re.search(r'by\s+([\d,]+)\s+verified\s+reviews', s)
        if m:
            return _safe_int(m.group(1))
        m = re.search(r'\b([\d,]{4,})\+?\s+(?:verified\s+)?reviews\b', s)
        if m:
            return _safe_int(m.group(1))
    return 0


def _extract_stars_dist(texts: list) -> dict:
    trigger = re.compile(r'5\s*stars?\W{0,3}\s*\d+\s*%', re.I)
    for s in texts:
        if not trigger.search(s):
            continue
        dist = {}
        for star in range(5, 0, -1):
            m = re.search(rf'{star}\s*stars?\W{{0,5}}\s*(\d+)\s*%', s, re.I)
            dist[str(star)] = _safe_int(m.group(1)) if m else 0
        if any(v > 0 for v in dist.values()):
            return dist
    return {"5": 0, "4": 0, "3": 0, "2": 0, "1": 0}


def _slug_from_url(url: str) -> str:
    m = re.search(r'g2\.com/products/([^/?#]+)', url)
    return m.group(1) if m else ""


def _slug_to_words(slug: str) -> str:
    return slug.replace("-", " ")


# ---------------------------------------------------------------------------
# Primary data source
# ---------------------------------------------------------------------------

def _fetch_rating_schema(slug: str) -> dict:
    resp = requests.get(
        f"https://www.g2.com/products/{slug}/rating_schema.json",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=_SCHEMA_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Public API — 1 SERP call max per endpoint
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

        # ── 0 credits, ~1s ──────────────────────────────────────────────────
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

        # ── 1 SERP call — stars distribution + description ──────────────────
        try:
            data = _serp(f"site:g2.com/products/{slug}/reviews")
            results = _organic(data)
            texts = _snippets(results)

            stars_dist = _extract_stars_dist(texts)

            if not name or name == slug:
                for r in results:
                    if re.search(rf'g2\.com/products/{re.escape(slug)}/reviews', r.get("link", ""), re.I):
                        name = _extract_name(r.get("title", slug))
                        profile_url = r.get("link", profile_url).split("?")[0]
                        break

            if rating == 0.0:
                rating = _extract_rating(texts)
            if total_reviews == 0:
                total_reviews = _extract_review_count(texts)

            for r in results:
                snippet = r.get("snippet", "")
                if snippet and len(snippet) > 30 and re.search(rf'g2\.com/products/{re.escape(slug)}', r.get("link", ""), re.I):
                    description = snippet[:500]
                    break
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
        seen = set()

        # ── 1 SERP call ──────────────────────────────────────────────────────
        data = _serp(f"site:g2.com/products/{slug}/reviews")
        for r in _organic(data):
            snippet = r.get("snippet", "").strip()
            if not snippet or len(snippet) < 20:
                continue
            if re.search(r'Review Summary|Generated using AI|Filter \d+ reviews', snippet, re.I):
                continue
            key = snippet[:80]
            if key in seen:
                continue
            seen.add(key)
            reviews.append({
                "id": f"g2-{len(reviews)}",
                "rating": 0,
                "title": r.get("title", "")[:100],
                "pros": "",
                "cons": "",
                "text": snippet[:1000],
                "date": "",
                "author": "Anonymous",
                "author_title": "",
                "verified": False,
                "helpful_votes": 0,
                "platform": "g2",
            })
            if len(reviews) >= limit:
                break

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "returned": len(reviews),
                "reviews": reviews,
                "note": "Individual ratings/dates/authors not available via SERP extraction",
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

        # ── 1 SERP call ──────────────────────────────────────────────────────
        data = _serp(f"site:g2.com/products/{slug}/features")
        for r in _organic(data):
            if not re.search(r'g2\.com/products/[^/]+/features', r.get("link", ""), re.I):
                continue
            snippet = r.get("snippet", "")
            if not snippet:
                continue

            # Parse tokens after "including / such as / supports:" keywords
            m = re.search(
                r'(?:including|such as|supports?[:\s]+)(.+?)(?:\.|$)',
                snippet, re.I | re.S,
            )
            raw = m.group(1) if m else snippet

            for part in re.split(r'[,;·•\n]+', raw):
                part = part.strip().strip(".")
                if 2 <= len(part) <= 60 and not re.match(r'^\d+$', part):
                    if not re.search(
                        r'\b(g2|reviews?|rating|verified|users?|find out|supports?|'
                        r'features?|which|learn|explore|compare|get|see|read)\b',
                        part, re.I,
                    ):
                        key = part.lower()
                        if key not in seen:
                            seen.add(key)
                            features.append(part)

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
        raw_snippet = ""
        seen_prices: set = set()

        # ── 1 SERP call ──────────────────────────────────────────────────────
        data = _serp(f"site:g2.com/products/{slug}/pricing")
        for r in _organic(data):
            if not re.search(r'g2\.com/products/[^/]+/pricing', r.get("link", ""), re.I):
                continue
            snippet = r.get("snippet", "")
            if not snippet:
                continue
            raw_snippet = snippet[:1000]

            if re.search(r'\bfree\s*plan\b|\bfreemium\b|\bfree\s*tier\b', snippet, re.I):
                has_free_plan = True
            if re.search(r'free\s*trial|trial\s*available', snippet, re.I):
                has_free_trial = True

            # Named tier + price: "Pro Plan: $7.25/mo" or "Pro - $7.25/month"
            for m in re.finditer(
                r'\b([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)?)\s*(?:Plan|Edition|Tier)?\s*[:\-–]\s*'
                r'(\$[\d,]+(?:\.\d{1,2})?(?:[/ ]\w+)*)',
                snippet, re.I,
            ):
                name_t, price_t = m.group(1).strip(), m.group(2).strip()
                if price_t not in seen_prices and not re.match(
                    r'^(Find|See|Read|Get|Compare|Learn|The|This|With|For|From|All)$', name_t, re.I
                ):
                    pricing_tiers.append({"name": name_t, "price": price_t})
                    seen_prices.add(price_t)

            # Fallback: any bare price strings
            if not pricing_tiers:
                for m in re.finditer(r'(\$[\d,]+(?:\.\d{1,2})?(?:[/ ]\w+)*)', snippet):
                    p = m.group(1)
                    if p not in seen_prices:
                        pricing_tiers.append({"name": "Plan", "price": p})
                        seen_prices.add(p)

            if re.search(r'contact\s+sales|custom\s+pric|enterprise\s+pric', snippet, re.I):
                if not any(t["name"].lower() == "enterprise" for t in pricing_tiers):
                    pricing_tiers.append({"name": "Enterprise", "price": "Contact Sales"})
            break  # first matching result is enough

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "pricing_tiers": pricing_tiers,
                "has_free_plan": has_free_plan,
                "has_free_trial": has_free_trial,
                "raw_snippet": raw_snippet,
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
        # ── 1 SERP call ──────────────────────────────────────────────────────
        data = _serp(f'site:g2.com/compare "{slug}-vs"')
        results = _organic(data)

        alternatives_raw = []
        seen = {slug}
        for r in results:
            url = r.get("link", "")
            alt_slug = _slug_from_url(url)
            if not alt_slug:
                m = re.search(r'g2\.com/compare/([^/?#]+)-vs-([^/?#]+)', url)
                if m:
                    for candidate in [m.group(1), m.group(2)]:
                        if candidate and candidate != slug and candidate not in seen:
                            alt_slug = candidate
                            break
            if not alt_slug or alt_slug in seen:
                continue
            seen.add(alt_slug)
            title = r.get("title", "")
            name = _extract_name(title) if title else alt_slug.replace("-", " ").title()
            alternatives_raw.append({
                "name": name,
                "slug": alt_slug,
                "compare_url": f"https://www.g2.com/compare/{slug}-vs-{alt_slug}",
            })
            if len(alternatives_raw) >= limit:
                break

        # ── Build final list — no extra HTTP calls ───────────────────────────
        enriched = []
        for item in alternatives_raw:
            enriched.append({
                "name": item["name"],
                "slug": item["slug"],
                "rating": 0.0,
                "profile_url": f"https://www.g2.com/products/{item['slug']}/reviews",
                "compare_url": item["compare_url"],
                "platform": "g2",
            })

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "alternatives": enriched,
                "returned": len(enriched),
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
        cat_str = f" {category}" if category else ""

        # ── 1 SERP call — site:g2.com/products targets product pages directly ─
        data = _serp(f'site:g2.com/products "{query}"{cat_str}')
        for r in _organic(data):
            if len(products) >= limit:
                break
            url = r.get("link", "")
            slug = _slug_from_url(url)
            if not slug or slug in seen:
                continue
            # Accept any g2.com/products/{slug} URL (not just /reviews suffix)
            if not re.search(r'g2\.com/products/[^/?#]+', url):
                continue
            seen.add(slug)
            snippet = r.get("snippet", "")
            products.append({
                "name": _extract_name(r.get("title", slug)),
                "slug": slug,
                "rating": _extract_rating([snippet]),
                "description": snippet[:300],
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
        category_words = _slug_to_words(slug)

        # ── 1 SERP call — site:g2.com/products scoped to category keywords ───
        data = _serp(f'site:g2.com/products {category_words}')
        for r in _organic(data):
            if len(products) >= limit:
                break
            url = r.get("link", "")
            prod_slug = _slug_from_url(url)
            if not prod_slug or prod_slug in seen:
                continue
            if not re.search(r'g2\.com/products/[^/?#]+', url):
                continue
            seen.add(prod_slug)
            snippet = r.get("snippet", "")
            products.append({
                "name": _extract_name(r.get("title", prod_slug)),
                "slug": prod_slug,
                "rating": _extract_rating([snippet]),
                "description": snippet[:300],
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
