from functools import wraps

from flask import jsonify, request

PLAN_HIERARCHY = {
    "BASIC": 0,
    "PRO": 1,
    "ULTRA": 2,
    "MEGA": 3
}


def require_plan(minimum_plan: str):
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            plan = request.headers.get("X-RapidAPI-Subscription", "BASIC").upper()
            if PLAN_HIERARCHY.get(plan, 0) < PLAN_HIERARCHY.get(minimum_plan, 0):
                return jsonify({
                    "error": "upgrade_required",
                    "message": f"This endpoint requires {minimum_plan} plan or higher.",
                    "upgrade_url": "https://rapidapi.com/berkdivaroren/api/g2-scraping-api"
                }), 403
            return f(*args, **kwargs)
        return decorated
    return decorator


def get_current_plan() -> str:
    return request.headers.get("X-RapidAPI-Subscription", "BASIC").upper()
