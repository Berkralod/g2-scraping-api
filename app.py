import os
from datetime import datetime

from dotenv import load_dotenv
from flask import Flask, jsonify, request

load_dotenv()

from scrapers import g2
from utils.cache import flush_all, get_cached, list_keys, make_cache_key, set_cached
from utils.plan_guard import require_plan

app = Flask(__name__)

_RAPIDAPI_PROXY_SECRET = os.environ.get("RAPIDAPI_PROXY_SECRET")

@app.before_request
def _verify_proxy_secret():
    if request.path in ["/health", "/", "/favicon.ico", "/g2/reviews-verify"]:
        return None
    if not _RAPIDAPI_PROXY_SECRET:
        return None
    secret = request.headers.get("X-RapidAPI-Proxy-Secret")
    if secret != _RAPIDAPI_PROXY_SECRET:
        return jsonify({"error": "Unauthorized", "message": "Access denied"}), 401


TTL_PRODUCT = 86400    # 24h
TTL_REVIEWS = 21600    # 6h
TTL_SEARCH = 7200      # 2h
TTL_FEATURES = 86400   # 24h


def _respond(result: dict, cache_status: str = None, ttl: int = None, cache_key: str = None):
    if result.get("status") == "success":
        if cache_key and ttl:
            set_cached(cache_key, result, ttl)
        if cache_status:
            result["cache"] = cache_status
        return jsonify(result), 200
    return jsonify(result), 503


# ===========================================================================
# G2 ENDPOINTS
# ===========================================================================

@app.route("/g2/product", methods=["GET"])
def g2_product():
    slug = request.args.get("slug", "").strip().lower()
    if not slug:
        return jsonify({"error": "missing_parameter", "message": "slug is required"}), 400

    ck = make_cache_key("product", {"slug": slug})
    cached = get_cached(ck)
    if cached:
        cached["cache"] = "HIT"
        return jsonify(cached), 200

    result = g2.get_product(slug)
    return _respond(result, "MISS", TTL_PRODUCT, ck)


@app.route("/g2/reviews", methods=["GET"])
def g2_reviews():
    slug = request.args.get("slug", "").strip().lower()
    if not slug:
        return jsonify({"error": "missing_parameter", "message": "slug is required"}), 400
    limit = min(int(request.args.get("limit", 20)), 40)
    rating = request.args.get("rating")
    rating = int(rating) if rating and rating.isdigit() else None
    sort = request.args.get("sort", "most_recent")

    ck = make_cache_key("reviews", {"slug": slug, "limit": limit, "rating": rating, "sort": sort})
    cached = get_cached(ck)
    if cached:
        cached["cache"] = "HIT"
        return jsonify(cached), 200

    result = g2.get_reviews(slug, limit=limit, rating=rating, sort=sort)
    return _respond(result, "MISS", TTL_REVIEWS, ck)


@app.route("/g2/features", methods=["GET"])
def g2_features():
    slug = request.args.get("slug", "").strip().lower()
    if not slug:
        return jsonify({"error": "missing_parameter", "message": "slug is required"}), 400

    ck = make_cache_key("features", {"slug": slug})
    cached = get_cached(ck)
    if cached:
        cached["cache"] = "HIT"
        return jsonify(cached), 200

    result = g2.get_features(slug)
    return _respond(result, "MISS", TTL_FEATURES, ck)


@app.route("/g2/pricing", methods=["GET"])
def g2_pricing():
    slug = request.args.get("slug", "").strip().lower()
    if not slug:
        return jsonify({"error": "missing_parameter", "message": "slug is required"}), 400

    ck = make_cache_key("pricing", {"slug": slug})
    cached = get_cached(ck)
    if cached:
        cached["cache"] = "HIT"
        return jsonify(cached), 200

    result = g2.get_pricing(slug)
    return _respond(result, "MISS", TTL_PRODUCT, ck)


@app.route("/g2/alternatives", methods=["GET"])
def g2_alternatives():
    slug = request.args.get("slug", "").strip().lower()
    if not slug:
        return jsonify({"error": "missing_parameter", "message": "slug is required"}), 400
    limit = min(int(request.args.get("limit", 5)), 20)

    ck = make_cache_key("alternatives", {"slug": slug, "limit": limit})
    cached = get_cached(ck)
    if cached:
        cached["cache"] = "HIT"
        return jsonify(cached), 200

    result = g2.get_alternatives(slug, limit=limit)
    return _respond(result, "MISS", TTL_PRODUCT, ck)


@app.route("/g2/search", methods=["GET"])
def g2_search():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"error": "missing_parameter", "message": "q is required"}), 400
    category = request.args.get("category")
    limit = min(int(request.args.get("limit", 10)), 30)

    ck = make_cache_key("search", {"q": query, "category": category, "limit": limit})
    cached = get_cached(ck)
    if cached:
        cached["cache"] = "HIT"
        return jsonify(cached), 200

    result = g2.search_products(query, category=category, limit=limit)
    return _respond(result, "MISS", TTL_SEARCH, ck)


@app.route("/g2/category", methods=["GET"])
def g2_category():
    slug = request.args.get("slug", "").strip().lower()
    if not slug:
        return jsonify({"error": "missing_parameter", "message": "slug is required"}), 400
    limit = min(int(request.args.get("limit", 10)), 30)

    ck = make_cache_key("category", {"slug": slug, "limit": limit})
    cached = get_cached(ck)
    if cached:
        cached["cache"] = "HIT"
        return jsonify(cached), 200

    result = g2.get_category(slug, limit=limit)
    return _respond(result, "MISS", TTL_SEARCH, ck)


@app.route("/g2/reviews-verify", methods=["GET"])
def g2_reviews_verify():
    """Temp verification endpoint — no auth, fixed slug, limited results."""
    from scrapers.g2 import _fetch_page_raw, _stars_dist_from_page
    slug = request.args.get("slug", "slack")
    try:
        soup, raw_html = _fetch_page_raw(f"https://www.g2.com/products/{slug}/reviews")
        cards = soup.find_all(attrs={"itemprop": "review"})
        stars = _stars_dist_from_page(soup)
        sample = raw_html[:500]
        title = soup.find("title")
        return jsonify({
            "title": title.get_text() if title else None,
            "html_len": len(raw_html),
            "review_cards_found": len(cards),
            "stars_dist": stars,
            "html_sample": sample,
        })
    except Exception as e:
        return jsonify({"error": str(e)})


# ===========================================================================
# CACHE MANAGEMENT
# ===========================================================================

_PROXY_SECRET = os.getenv("RAPIDAPI_PROXY_SECRET", "")


def _check_admin_auth() -> bool:
    return request.headers.get("RAPIDAPI-PROXY-SECRET", "") == _PROXY_SECRET and _PROXY_SECRET != ""


@app.route("/cache/flush", methods=["DELETE"])
def cache_flush():
    if not _check_admin_auth():
        return jsonify({"error": "unauthorized"}), 401
    try:
        deleted = flush_all()
        return jsonify({"status": "ok", "deleted_keys": deleted}), 200
    except Exception as e:
        return jsonify({"error": "flush_failed", "detail": str(e)}), 500


@app.route("/cache/keys", methods=["GET"])
def cache_keys():
    if not _check_admin_auth():
        return jsonify({"error": "unauthorized"}), 401
    try:
        keys = list_keys()
        return jsonify({"status": "ok", "count": len(keys), "keys": sorted(keys)}), 200
    except Exception as e:
        return jsonify({"error": "list_failed", "detail": str(e)}), 500


# ===========================================================================
# HEALTH
# ===========================================================================

@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "api": "G2 Scraping API",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat()
    }), 200


# ===========================================================================
# ENTRY POINT
# ===========================================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
