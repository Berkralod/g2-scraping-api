"""
G2 Scraping API — data source map:

  rating_schema.json  (0 credits, direct HTTP — bypasses Cloudflare)
    name, ratingValue (0-10), reviewCount, bestRating, applicationCategory

  ScraperAPI structured SERP  (~12-15 credits/call)
    product:      site:g2.com/products/{slug}/reviews "star"  → stars dist
    reviews:      site:g2.com/products/{slug}/reviews
    review text:  "What do you like best about {name}" site:g2.com
    features:     site:g2.com/products/{slug}/features
    pricing:      g2.com {slug} pricing
    alternatives: site:g2.com/compare "{slug}-vs"
    search:       site:g2.com/products {query}
    category:     g2.com best {category words} software
"""
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests

SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY")


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
    """
    Matches G2 snippet patterns:
      "5 star. 75% · 4 star. 20% · 3 star. 3% · 2 star. 1% · 1 star. 1%"
      "5 stars. 75%"  /  "5-star: 75%"
    """
    # Widen the trigger check — accept both "5 star." and "5 stars."
    trigger = re.compile(r'5\s*stars?\W{0,3}\s*\d+\s*%', re.I)
    for s in texts:
        if not trigger.search(s):
            continue
        dist = {}
        for star in range(5, 0, -1):
            # Allow dot, colon, dash, space between "N star" and the percentage
            m = re.search(
                rf'{star}\s*stars?\W{{0,5}}\s*(\d+)\s*%',
                s, re.I
            )
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

        # Dedicated SERP for stars_dist — query targets the reviews page with
        # "star" keyword so Google returns the aggregate snippet
        try:
            dist_data = _serp(f'site:g2.com/products/{slug}/reviews "star" "%"')
            dist_texts = _snippets(_organic(dist_data))
            stars_dist = _extract_stars_dist(dist_texts)
        except Exception:
            pass

        # Fallback general SERP for description, name, rating/count if needed
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

            # stars_dist fallback from general SERP if dedicated query failed
            if not any(v > 0 for v in stars_dist.values()):
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
    """
    G2 SERP snippets for features pages look like:
      "Find out which X features {Name} supports, including Chat, Tags, ..."
    We parse the token list after "including" and also try a broader query.
    """
    try:
        features = []
        seen = set()

        def _parse_features_from_snippet(snippet: str) -> list:
            result = []
            # Extract list after "including", "such as", "supports:", etc.
            m = re.search(
                r'(?:including|such as|supports?[:\s]+|features?[:\s]+)(.+?)(?:\.|$)',
                snippet, re.I | re.S
            )
            if m:
                raw = m.group(1)
            else:
                raw = snippet
            # Split on commas, semicolons, bullets, newlines
            parts = re.split(r'[,;·•\n]+', raw)
            for p in parts:
                p = p.strip().strip('.')
                # Keep tokens that look like feature names:
                # 2–60 chars, not pure numbers, not boilerplate
                if 2 <= len(p) <= 60 and not re.match(r'^\d+$', p):
                    if not re.search(
                        r'\b(g2|reviews?|rating|verified|users?|find out|supports?|features?|'
                        r'which|learn|explore|compare|get|see|read)\b',
                        p, re.I
                    ):
                        result.append(p)
            return result

        def _add_features(snippets_list: list):
            for s in snippets_list:
                for f in _parse_features_from_snippet(s):
                    key = f.lower()
                    if key not in seen:
                        seen.add(key)
                        features.append(f)

        # Query 1: direct features page
        try:
            data1 = _serp(f"site:g2.com/products/{slug}/features")
            _add_features(_snippets(_organic(data1)))
        except Exception:
            pass

        # Query 2: broader — product name + features + g2
        if len(features) < 5:
            try:
                product_name = _slug_to_words(slug)
                data2 = _serp(f'"{product_name}" features site:g2.com')
                _add_features(_snippets(_organic(data2)))
            except Exception:
                pass

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
    """
    Tries multiple SERP queries to find pricing info for a G2 product.
    Parses free/paid indicators and price tier strings from snippets.
    """
    try:
        pricing_tiers = []
        has_free_plan = False
        has_free_trial = False
        raw_snippet = ""

        def _parse_pricing(snippet: str):
            nonlocal has_free_plan, has_free_trial, raw_snippet
            if len(snippet) > len(raw_snippet):
                raw_snippet = snippet[:1000]

            if re.search(r'\bfree\s*plan\b|\bfreemium\b|\bfree\s*tier\b', snippet, re.I):
                has_free_plan = True
            if re.search(r'free\s*trial|trial\s*available', snippet, re.I):
                has_free_trial = True

            # Price patterns: $7.25/mo, $12/user/month, $99/year, etc.
            price_pattern = re.compile(
                r'(\$[\d,]+(?:\.\d{1,2})?(?:[/ ]\w+)*)',
                re.I
            )
            # Tier name candidates preceding a price or "Plan"/"Edition"
            tier_name_pattern = re.compile(
                r'\b([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)?)\s*(?:Plan|Edition|Tier)?'
                r'\s*[:\-–]?\s*(\$[\d,]+(?:\.\d{1,2})?(?:[/ ]\w+)*)',
                re.I
            )

            seen_prices = {t["price"] for t in pricing_tiers}

            for m in tier_name_pattern.finditer(snippet):
                name = m.group(1).strip()
                price = m.group(2).strip()
                if price not in seen_prices and not re.search(
                    r'^(Find|See|Read|Get|Compare|Learn|The|This|That|With|For|From|All|Any)$',
                    name, re.I
                ):
                    pricing_tiers.append({"name": name, "price": price})
                    seen_prices.add(price)

            # Fallback: standalone prices without tier names
            if not pricing_tiers:
                for m in price_pattern.finditer(snippet):
                    price = m.group(1)
                    if price not in seen_prices:
                        pricing_tiers.append({"name": "Plan", "price": price})
                        seen_prices.add(price)

            # "Contact Sales" / "Custom pricing" → enterprise tier
            if re.search(r'contact\s+sales|custom\s+pric|enterprise\s+pric', snippet, re.I):
                if not any(t["name"].lower() == "enterprise" for t in pricing_tiers):
                    pricing_tiers.append({"name": "Enterprise", "price": "Contact Sales"})

        # Query 1: G2 product pricing page
        try:
            data1 = _serp(f"site:g2.com/products/{slug}/pricing")
            for r in _organic(data1):
                if re.search(r'g2\.com/products/[^/]+/pricing', r.get("link", ""), re.I):
                    _parse_pricing(r.get("snippet", ""))
        except Exception:
            pass

        # Query 2: broader — product name + pricing + g2
        if not pricing_tiers and not raw_snippet:
            try:
                product_name = _slug_to_words(slug)
                data2 = _serp(f'g2.com "{product_name}" pricing')
                for r in _organic(data2):
                    snippet = r.get("snippet", "")
                    if snippet and re.search(r'g2\.com', r.get("link", ""), re.I):
                        _parse_pricing(snippet)
            except Exception:
                pass

        # Query 3: site:g2.com slug pricing
        if not pricing_tiers and not raw_snippet:
            try:
                data3 = _serp(f'site:g2.com "{_slug_to_words(slug)}" pricing')
                for r in _organic(data3):
                    snippet = r.get("snippet", "")
                    if snippet:
                        _parse_pricing(snippet)
            except Exception:
                pass

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "pricing_tiers": pricing_tiers,
                "has_free_plan": has_free_plan,
                "has_free_trial": has_free_trial,
                "raw_snippet": raw_snippet,
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
    """
    Fetches competitor slugs from G2 compare pages, then enriches each with
    rating from rating_schema.json (0 credits, parallel HTTP).
    """
    try:
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

        # Enrich with ratings from rating_schema.json (parallel, 0 credits)
        def _fetch_alt_rating(item: dict) -> dict:
            try:
                schema = _fetch_rating_schema(item["slug"])
                agg = schema.get("aggregateRating", {})
                best = _safe_float(agg.get("bestRating", 10))
                raw = _safe_float(agg.get("ratingValue", 0))
                rating = round(raw / best * 5, 1) if best else 0.0
                review_count = _safe_int(agg.get("reviewCount", 0))
                item["rating"] = rating
                item["total_reviews"] = review_count
                item["name"] = schema.get("name") or item["name"]
            except Exception:
                item["rating"] = 0.0
                item["total_reviews"] = 0
            item["profile_url"] = f"https://www.g2.com/products/{item['slug']}/reviews"
            item["platform"] = "g2"
            return item

        enriched = []
        with ThreadPoolExecutor(max_workers=min(len(alternatives_raw), 5)) as pool:
            futures = {pool.submit(_fetch_alt_rating, item): item for item in alternatives_raw}
            for future in as_completed(futures):
                try:
                    enriched.append(future.result())
                except Exception:
                    enriched.append(futures[future])

        # Re-sort to original order
        slug_order = {item["slug"]: i for i, item in enumerate(alternatives_raw)}
        enriched.sort(key=lambda x: slug_order.get(x["slug"], 99))

        return {
            "status": "success",
            "data": {
                "slug": slug,
                "alternatives": enriched,
                "returned": len(enriched),
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
    """
    Multiple SERP strategies to maximize product result count.
    Primary: site:g2.com/products {query}
    Fallback: g2.com {query} software reviews (broader, no strict site path)
    """
    try:
        products = []
        seen = set()

        def _collect(results: list):
            for r in results:
                if len(products) >= limit:
                    break
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

        cat_str = f" {category}" if category else ""

        # Query 1: target /products/ path directly
        try:
            data1 = _serp(f'site:g2.com/products "{query}"{cat_str}')
            _collect(_organic(data1))
        except Exception:
            pass

        # Query 2: broader g2 search without strict path
        if len(products) < limit:
            try:
                data2 = _serp(f'site:g2.com {query}{cat_str} software reviews')
                _collect(_organic(data2))
            except Exception:
                pass

        # Query 3: even broader — no site: filter, but require g2.com in results
        if len(products) < limit:
            try:
                data3 = _serp(f'g2.com best {query}{cat_str} software')
                _collect(_organic(data3))
            except Exception:
                pass

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
    """
    G2 category pages don't appear well in SERPs via site:g2.com/categories/.
    Instead use broader queries that surface product review pages belonging to
    the category.
    """
    try:
        products = []
        seen = set()
        category_words = _slug_to_words(slug)

        def _collect(results: list):
            for r in results:
                if len(products) >= limit:
                    break
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

        # Query 1: g2 best X software (most reliable for category browsing)
        try:
            data1 = _serp(f'g2.com best {category_words} software')
            _collect(_organic(data1))
        except Exception:
            pass

        # Query 2: site:g2.com/products with category words
        if len(products) < limit:
            try:
                data2 = _serp(f'site:g2.com/products {category_words}')
                _collect(_organic(data2))
            except Exception:
                pass

        # Query 3: direct category page
        if len(products) < limit:
            try:
                data3 = _serp(f'site:g2.com/categories/{slug}')
                _collect(_organic(data3))
            except Exception:
                pass

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
