"""
G2 Scraping API — data source map:

  rating_schema.json  (0 credits, direct HTTP — bypasses Cloudflare)
    name, ratingValue (0-10), reviewCount, bestRating, applicationCategory

  ScraperAPI structured SERP  (~12-15 credits/call)
    reviews:      site:g2.com/products/{slug}/reviews
    features:     site:g2.com/products/{slug}/features
    pricing:      site:g2.com/products/{slug}/pricing
    alternatives: site:g2.com/compare "{slug}-vs"
    search:       site:g2.com {query} software reviews
    category:     site:g2.com/categories/{slug}
    review text:  "What do you like best about {name}" site:g2.com
"""
import os
import re
from datetime import datetime
from urllib.parse import quote_plus

import requests

SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY")

_UNAVAILABLE = {"status": "unavailable", "platform": "g2"}


# ---------------------------------------------------------------------------
# SERP helpers
# ---------------------------------------------------------------------------

def _serp(query: str) -> dict:
    resp = requests.get(
        "https://api.scraperapi.com/structured/google/search",
        params={"api_key": SCRAPERAPI_KEY, "query": query},
        timeout=60
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
    for s in texts:
        if re.search(r'5 stars?\.\s*\d+%', s, re.I):
            dist = {}
            for star in range(5, 0, -1):
                m = re.search(rf'{star}\s+stars?\.\s*(\d+)%', s, re.I)
                dist[str(star)] = _safe_int(m.group(1)) if m else 0
            if any(v > 0 for v in dist.values()):
                return dist
    return {"5": 0, "4": 0, "3": 0, "2": 0, "1": 0}


def _slug_from_url(url: str) -> str:
    m = re.search(r'g2\.com/products/([^/?#]+)', url)
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# Primary data source
# ---------------------------------------------------------------------------

def _fetch_rating_schema(slug: str) -> dict:
    """schema.org endpoint — public, no proxy, 0 credits."""
    resp = requests.get(
        f"https://www.g2.com/products/{slug}/rating_schema.json",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=15
    )
    resp.raise_for_status()
    return resp.json()


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

        # Primary: schema.org JSON — 0 credits
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

        # Fallback SERP for description and stars_dist
        try:
            data = _serp(f"site:g2.com {slug} reviews")
            results = _organic(data)
            texts = _snippets(results)

            if not name or name == slug:
                for r in results:
                    url = r.get("link", "")
                    if re.search(rf'g2\.com/products/{re.escape(slug)}/reviews', url, re.I):
                        name = _extract_name(r.get("title", slug))
                        profile_url = url.split("?")[0]
                        break

            if rating == 0.0:
                rating = _extract_rating(texts)
            if total_reviews == 0:
                total_reviews = _extract_review_count(texts)

            stars_dist = _extract_stars_dist(texts)

            for r in results:
                url = r.get("link", "")
                if re.search(rf'g2\.com/products/{re.escape(slug)}', url, re.I):
                    snippet = r.get("snippet", "")
                    if snippet and len(snippet) > 30:
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
                "scraped_at": datetime.utcnow().isoformat()
            }
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error_code": "TIMEOUT", "platform": "g2", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error_code": "HTTP_ERROR", "platform": "g2", "message": str(e)}
    except Exception as e:
        return {"status": "error", "error_code": "SCRAPER_ERROR", "platform": "g2", "message": "Scraper failed", "detail": str(e)}


def get_reviews(slug: str, limit: int = 20, rating: int = None, sort: str = "most_recent") -> dict:
    """
    SERP snippet fragments as pseudo-reviews plus dedicated review text query.
    Individual author/date/rating not available via SERP.
    """
    try:
        reviews = []
        seen_texts = set()

        # Source 1: product reviews page snippets
        try:
            data = _serp(f"site:g2.com/products/{slug}/reviews")
            for r in _organic(data):
                snippet = r.get("snippet", "").strip()
                if not snippet or len(snippet) < 20:
                    continue
                if re.search(r'Review Summary|Generated using AI|Filter \d+ reviews|your single workspace', snippet, re.I):
                    continue
                key = snippet[:80]
                if key in seen_texts:
                    continue
                seen_texts.add(key)
                reviews.append({
                    "id": f"g2-serp-{len(reviews)}",
                    "rating": 0,
                    "title": "",
                    "pros": "",
                    "cons": "",
                    "text": snippet[:1000],
                    "date": "",
                    "author": "Anonymous",
                    "author_title": "",
                    "verified": False,
                    "helpful_votes": 0,
                    "platform": "g2"
                })
        except Exception:
            pass

        # Source 2: "What do you like best" review snippets
        if len(reviews) < limit:
            try:
                product_name = slug.replace("-", " ")
                data2 = _serp(f'"What do you like best about {product_name}" site:g2.com')
                for r in _organic(data2):
                    snippet = r.get("snippet", "").strip()
                    if not snippet or len(snippet) < 20:
                        continue
                    key = snippet[:80]
                    if key in seen_texts:
                        continue
                    seen_texts.add(key)
                    # Try to extract pros/cons from snippet
                    pros = ""
                    cons = ""
                    m_pro = re.search(r'(?:like best|Pros?)[:\s]+(.+?)(?:\n|What do you dislike|Cons?[:\s]|$)', snippet, re.I | re.S)
                    m_con = re.search(r'(?:dislike|Cons?)[:\s]+(.+?)(?:\n|$)', snippet, re.I | re.S)
                    if m_pro:
                        pros = m_pro.group(1).strip()[:300]
                    if m_con:
                        cons = m_con.group(1).strip()[:300]
                    reviews.append({
                        "id": f"g2-text-{len(reviews)}",
                        "rating": 0,
                        "title": r.get("title", "")[:100],
                        "pros": pros,
                        "cons": cons,
                        "text": snippet[:1000],
                        "date": "",
                        "author": "Anonymous",
                        "author_title": "",
                        "verified": False,
                        "helpful_votes": 0,
                        "platform": "g2"
                    })
            except Exception:
                pass

        reviews = reviews[:limit]

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "returned": len(reviews),
                "reviews": reviews,
                "note": "Individual ratings/dates/authors not available via SERP extraction",
                "platform": "g2"
            }
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error_code": "TIMEOUT", "platform": "g2", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error_code": "HTTP_ERROR", "platform": "g2", "message": str(e)}
    except Exception as e:
        return {"status": "error", "error_code": "SCRAPER_ERROR", "platform": "g2", "message": "Reviews failed", "detail": str(e)}


def get_features(slug: str) -> dict:
    try:
        data = _serp(f"site:g2.com/products/{slug}/features")
        results = _organic(data)

        features = []
        seen = set()
        for r in results:
            url = r.get("link", "")
            if not re.search(r'g2\.com/products/[^/]+/features', url, re.I):
                continue
            snippet = r.get("snippet", "")
            # Parse feature names from snippet — typically comma-separated or bullet-style
            raw_features = re.split(r'[,·•\n]+', snippet)
            for f in raw_features:
                f = f.strip()
                # Skip short noise or numeric-only tokens
                if len(f) < 3 or re.match(r'^\d+$', f):
                    continue
                # Skip boilerplate
                if re.search(r'G2|reviews|rating|verified|users', f, re.I):
                    continue
                key = f.lower()
                if key not in seen:
                    seen.add(key)
                    features.append(f)

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "features": features,
                "feature_count": len(features),
                "platform": "g2",
                "scraped_at": datetime.utcnow().isoformat()
            }
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error_code": "TIMEOUT", "platform": "g2", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error_code": "HTTP_ERROR", "platform": "g2", "message": str(e)}
    except Exception as e:
        return {"status": "error", "error_code": "SCRAPER_ERROR", "platform": "g2", "message": "Features failed", "detail": str(e)}


def get_pricing(slug: str) -> dict:
    try:
        data = _serp(f"site:g2.com/products/{slug}/pricing")
        results = _organic(data)

        pricing_info = []
        raw_text = ""
        for r in results:
            url = r.get("link", "")
            if not re.search(r'g2\.com/products/[^/]+/pricing', url, re.I):
                continue
            snippet = r.get("snippet", "")
            if snippet:
                raw_text = snippet[:1000]
                # Extract price tiers from snippet
                tiers = re.findall(r'(\$[\d,]+(?:\.\d{2})?(?:/\w+)?)', snippet)
                tier_names = re.findall(r'([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)?)\s+(?:Plan|Tier|Edition)', snippet)
                for i, tier in enumerate(tiers):
                    name = tier_names[i] if i < len(tier_names) else f"Tier {i+1}"
                    pricing_info.append({"name": name, "price": tier})
                break

        # Check for free/freemium
        is_free = bool(re.search(r'\bfree\b', raw_text, re.I)) if raw_text else False
        has_trial = bool(re.search(r'free trial|trial', raw_text, re.I)) if raw_text else False

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "pricing_tiers": pricing_info,
                "has_free_plan": is_free,
                "has_free_trial": has_trial,
                "raw_snippet": raw_text,
                "platform": "g2",
                "scraped_at": datetime.utcnow().isoformat()
            }
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error_code": "TIMEOUT", "platform": "g2", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error_code": "HTTP_ERROR", "platform": "g2", "message": str(e)}
    except Exception as e:
        return {"status": "error", "error_code": "SCRAPER_ERROR", "platform": "g2", "message": "Pricing failed", "detail": str(e)}


def get_alternatives(slug: str, limit: int = 5) -> dict:
    try:
        data = _serp(f'site:g2.com/compare "{slug}-vs"')
        results = _organic(data)

        alternatives = []
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
            alternatives.append({
                "name": name,
                "slug": alt_slug,
                "rating": 0.0,
                "profile_url": f"https://www.g2.com/products/{alt_slug}/reviews",
                "compare_url": f"https://www.g2.com/compare/{slug}-vs-{alt_slug}",
                "platform": "g2"
            })
            if len(alternatives) >= limit:
                break

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "alternatives": alternatives,
                "returned": len(alternatives),
                "platform": "g2",
                "scraped_at": datetime.utcnow().isoformat()
            }
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error_code": "TIMEOUT", "platform": "g2", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error_code": "HTTP_ERROR", "platform": "g2", "message": str(e)}
    except Exception as e:
        return {"status": "error", "error_code": "SCRAPER_ERROR", "platform": "g2", "message": "Alternatives failed", "detail": str(e)}


def search_products(query: str, category: str = None, limit: int = 10) -> dict:
    try:
        q = f"site:g2.com {query} software reviews"
        if category:
            q = f"site:g2.com {query} {category} reviews"

        data = _serp(q)
        results = _organic(data)

        products = []
        seen = set()
        for r in results:
            url = r.get("link", "")
            slug = _slug_from_url(url)
            if not slug or slug in seen:
                continue
            if not re.search(r'g2\.com/products/[^/]+/reviews', url):
                continue
            seen.add(slug)
            name = _extract_name(r.get("title", slug))
            snippet = r.get("snippet", "")
            rating = _extract_rating([snippet])
            products.append({
                "name": name,
                "slug": slug,
                "rating": rating,
                "description": snippet[:300] if snippet else "",
                "profile_url": f"https://www.g2.com/products/{slug}/reviews",
                "platform": "g2"
            })
            if len(products) >= limit:
                break

        return {
            "status": "success",
            "data": {
                "query": query,
                "category": category,
                "results": products,
                "total_found": len(products),
                "platform": "g2"
            }
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error_code": "TIMEOUT", "platform": "g2", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error_code": "HTTP_ERROR", "platform": "g2", "message": str(e)}
    except Exception as e:
        return {"status": "error", "error_code": "SCRAPER_ERROR", "platform": "g2", "message": "Search failed", "detail": str(e)}


def get_category(slug: str, limit: int = 10) -> dict:
    try:
        data = _serp(f"site:g2.com/categories/{slug}")
        results = _organic(data)

        products = []
        seen = set()
        for r in results:
            url = r.get("link", "")
            prod_slug = _slug_from_url(url)
            if not prod_slug or prod_slug in seen:
                continue
            seen.add(prod_slug)
            name = _extract_name(r.get("title", prod_slug))
            snippet = r.get("snippet", "")
            rating = _extract_rating([snippet])
            products.append({
                "name": name,
                "slug": prod_slug,
                "rating": rating,
                "description": snippet[:300] if snippet else "",
                "profile_url": f"https://www.g2.com/products/{prod_slug}/reviews",
                "platform": "g2"
            })
            if len(products) >= limit:
                break

        return {
            "status": "success",
            "data": {
                "category_slug": slug,
                "category_url": f"https://www.g2.com/categories/{slug}",
                "products": products,
                "total_found": len(products),
                "platform": "g2",
                "scraped_at": datetime.utcnow().isoformat()
            }
        }
    except requests.exceptions.Timeout:
        return {"status": "error", "error_code": "TIMEOUT", "platform": "g2", "message": "Request timed out"}
    except requests.exceptions.HTTPError as e:
        return {"status": "error", "error_code": "HTTP_ERROR", "platform": "g2", "message": str(e)}
    except Exception as e:
        return {"status": "error", "error_code": "SCRAPER_ERROR", "platform": "g2", "message": "Category failed", "detail": str(e)}
