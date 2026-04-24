# G2 Scraping API

![RapidAPI](https://img.shields.io/badge/RapidAPI-Available-blue?style=flat-square)
![Status](https://img.shields.io/badge/Status-Active-green?style=flat-square)

Real-time G2 product data — ratings, reviews, pricing, features, alternatives, and category browsing. 8 endpoints with Redis caching.

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /g2/product` | Name, rating, review count, star distribution, categories |
| `GET /g2/reviews` | Review text snippets |
| `GET /g2/features` | Feature list |
| `GET /g2/pricing` | Pricing tiers, free plan/trial detection |
| `GET /g2/alternatives` | Competitor products |
| `GET /g2/search` | Search products by keyword |
| `GET /g2/category` | Browse by G2 category slug |
| `GET /health` | Health check |

## Quick Start

```bash
curl -X GET "https://g2-scraping-api.p.rapidapi.com/g2/product?slug=slack"   -H "X-RapidAPI-Key: YOUR_API_KEY"   -H "X-RapidAPI-Host: g2-scraping-api.p.rapidapi.com"
```

## Data Sources

- `rating_schema.json` public endpoint (0 credits) — name, rating, review count, categories
- Google SERP via ScraperAPI — star distribution, review snippets, features, pricing, competitors

## Cache TTLs

- Product / Features / Pricing / Alternatives: 24h
- Reviews: 6h
- Search / Category: 2h

## Stack

Flask · Python · Upstash Redis · ScraperAPI · Railway · RapidAPI
