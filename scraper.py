#!/usr/bin/env python3
"""
AI News Hub Scraper v2
多源异步采集：HN / Reddit / GitHub / RSS feeds
支持摘要抓取、自动分类、热度排序
"""

import asyncio
import json
import re
import html
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin

import aiohttp

# ── 配置 ──────────────────────────────────────────────────
OUTPUT = "data.json"
TOP_N = 50
TIMEOUT = aiohttp.ClientTimeout(total=25)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/125.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8",
}

# RSS 源列表：(名称, URL, 权重, 默认分类)
RSS_FEEDS = [
    ("TechCrunch", "https://techcrunch.com/category/artificial-intelligence/feed/", 1.2, "product"),
    ("TheVerge", "https://www.theverge.com/ai-artificial-intelligence/rss/index.xml", 1.1, "product"),
    ("Wired", "https://www.wired.com/feed/tag/ai/rss", 1.1, "product"),
    ("MIT TechReview", "https://www.technologyreview.com/feed/", 1.3, "research"),
    ("arXiv AI", "http://export.arxiv.org/rss/cs.AI", 1.0, "research"),
    ("机器之心", "https://www.jiqizhixin.com/rss", 0.9, "model"),
    ("量子位", "https://www.qbitai.com/feed", 0.9, "model"),
]

# AI 关键词分类映射
CATEGORY_KEYWORDS = {
    "model": ["gpt", "llm", "大模型", "模型", "claude", "gemini", "llama", "mistral", "mixtral",
              "transformer", "diffusion", "stable diffusion", "多模态", "multimodal",
              "foundation model", "参数", "billion", "trillion", "通义", "文心", "盘古", "智谱"],
    "product": ["发布", "launch", "推出", "上线", "open", "available", "新功能",
                "app", "应用", "平台", "platform", "工具", "tool", "api", "插件", "plugin",
                "sora", "midjourney", "copilot", "cursor", "ide", "浏览器", "搜索", "助手"],
    "research": ["论文", "paper", "研究", "research", "arxiv", "突破", "breakthrough",
                 "新算法", "algorithm", "训练", "training", "fine-tune", "rlhf",
                 "蒸馏", "distillation", "量化", "quantization", "推理", "inference"],
    "invest": ["融资", "投资", "funding", "收购", "acquisition", "并购", "估值",
               "valuation", "ipo", "上市", "独角兽", "unicorn", "亿美元", "million", "billion"],
    "policy": ["监管", "regulation", "法规", "法案", "act", "政策", "policy",
               "禁令", "ban", "出口管制", "限制", "ai safety", "安全", "伦理", "ethics"],
}


def strip_html(text: str) -> str:
    """去除 HTML 标签并解码实体"""
    if not text:
        return ""
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.S)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.S)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    return text.strip()


def truncate(text: str, length: int = 180) -> str:
    """截断文本，保留完整单词"""
    text = text.replace("\n", " ").replace("\r", "").strip()
    if len(text) <= length:
        return text
    return text[:length].rsplit(" ", 1)[0] + "…"


def parse_date(text: str) -> datetime:
    """解析多种日期格式"""
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%d %b %Y %H:%M:%S %z",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text.strip(), fmt)
        except ValueError:
            continue
    # 兜底：尝试提取年月日
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


def classify(title: str, summary: str = "") -> str:
    """根据关键词分类"""
    text = (title + " " + summary).lower()
    scores = {}
    for cat, keywords in CATEGORY_KEYWORDS.items():
        scores[cat] = sum(1 for kw in keywords if kw.lower() in text)
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "product"


def calc_score(item: dict) -> float:
    """计算新闻热度分数"""
    base = item.get("hot", 0) * 0.5 + item.get("comments", 0) * 2
    # RSS 源没有 hot/comments，用权重和时间衰减
    weight = item.get("weight", 1.0)
    age_hours = (datetime.now(timezone.utc) - item["time"]).total_seconds() / 3600
    time_decay = max(0.3, 1 - age_hours / 168)  # 7 天衰减到 0.3
    return (base + 50) * weight * time_decay


# ── 采集器 ────────────────────────────────────────────────

async def fetch(session: aiohttp.ClientSession, url: str) -> str:
    try:
        async with session.get(url, timeout=TIMEOUT) as resp:
            if resp.status == 200:
                return await resp.text()
    except Exception as e:
        print(f"[错误] 获取失败 {url}: {e}")
    return ""


async def fetch_json(session: aiohttp.ClientSession, url: str) -> dict | list:
    text = await fetch(session, url)
    try:
        return json.loads(text)
    except Exception:
        return {}


async def scrape_hackernews(session: aiohttp.ClientSession) -> list[dict]:
    """HackerNews 热门 AI 相关帖子"""
    print("[采集] HackerNews...")
    data = await fetch_json(session, "https://hacker-news.firebaseio.com/v0/topstories.json")
    if not isinstance(data, list):
        return []

    results = []
    ai_keywords = ["ai", "artificial intelligence", "llm", "gpt", "machine learning",
                   "deep learning", "neural", "openai", "anthropic", "gemini", "claude",
                   "model", "diffusion", "sora", "multimodal"]

    async def get_item(iid: int):
        item = await fetch_json(session, f"https://hacker-news.firebaseio.com/v0/item/{iid}.json")
        if not isinstance(item, dict):
            return None
        title = item.get("title", "")
        text = (title + " " + item.get("text", "")).lower()
        if not any(kw in text for kw in ai_keywords):
            return None
        t = item.get("time", 0)
        return {
            "title": title,
            "url": item.get("url") or f"https://news.ycombinator.com/item?id={iid}",
            "source": "HackerNews",
            "summary": strip_html(item.get("text", ""))[:300] if item.get("text") else "",
            "category": classify(title),
            "hot": item.get("score", 0),
            "comments": item.get("descendants", 0),
            "time": datetime.fromtimestamp(t, tz=timezone.utc) if t else datetime.now(timezone.utc),
            "weight": 1.2,
        }

    tasks = [get_item(iid) for iid in data[:60]]
    for res in await asyncio.gather(*tasks):
        if res:
            results.append(res)
    print(f"[完成] HackerNews: {len(results)} 条")
    return results


async def scrape_reddit(session: aiohttp.ClientSession) -> list[dict]:
    """Reddit r/artificial 热门"""
    print("[采集] Reddit r/artificial...")
    url = "https://www.reddit.com/r/artificial/hot.json?limit=30"
    data = await fetch_json(session, url)
    posts = data.get("data", {}).get("children", []) if isinstance(data, dict) else []

    results = []
    for post in posts:
        p = post.get("data", {})
        title = p.get("title", "")
        # 跳过纯讨论帖（selftext 为空且是 self 帖子）
        url_link = p.get("url_overridden_by_dest") or f"https://reddit.com{p.get('permalink', '')}"
        t = p.get("created_utc", 0)
        summary = strip_html(p.get("selftext", ""))[:400]
        results.append({
            "title": title,
            "url": url_link,
            "source": "Reddit",
            "summary": summary,
            "category": classify(title, summary),
            "hot": int(p.get("score", 0)),
            "comments": p.get("num_comments", 0),
            "time": datetime.fromtimestamp(t, tz=timezone.utc) if t else datetime.now(timezone.utc),
            "weight": 1.0,
        })
    print(f"[完成] Reddit: {len(results)} 条")
    return results


async def scrape_github(session: aiohttp.ClientSession) -> list[dict]:
    """GitHub Trending AI 项目"""
    print("[采集] GitHub Trending...")
    # GitHub 没有官方 Trending API，用搜索 API 代替
    query = "language:python+topic:artificial-intelligence+stars:>1000"
    url = f"https://api.github.com/search/repositories?q={query}&sort=stars&order=desc&per_page=20"
    data = await fetch_json(session, url)
    items = data.get("items", []) if isinstance(data, dict) else []

    results = []
    for item in items:
        t = item.get("created_at", "") or item.get("pushed_at", "")
        desc = item.get("description") or ""
        results.append({
            "title": f"GitHub: {item.get('full_name', '')} — {desc[:80]}",
            "url": item.get("html_url", ""),
            "source": "GitHub",
            "summary": desc,
            "category": classify(desc, desc) if desc else "product",
            "hot": item.get("stargazers_count", 0),
            "comments": item.get("open_issues_count", 0),
            "time": parse_date(t) if t else datetime.now(timezone.utc),
            "weight": 0.9,
        })
    print(f"[完成] GitHub: {len(results)} 条")
    return results


async def scrape_rss(session: aiohttp.ClientSession, name: str, url: str,
                     weight: float, default_cat: str) -> list[dict]:
    """通用 RSS 采集"""
    print(f"[采集] {name}...")
    text = await fetch(session, url)
    if not text:
        print(f"[跳过] {name}: 获取失败")
        return []

    results = []
    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(text)
    except Exception as e:
        print(f"[跳过] {name}: XML 解析失败 {e}")
        return []

    # 统一处理 RSS 2.0 和 Atom
    ns = {"content": "http://purl.org/rss/1.0/modules/content/",
          "atom": "http://www.w3.org/2005/Atom"}

    items = []
    if root.tag == "rss" or root.tag.endswith("rss"):
        channel = root.find("channel")
        if channel is not None:
            items = channel.findall("item")
    elif root.tag.endswith("feed"):
        items = root.findall("atom:entry", ns)
        if not items:
            items = root.findall("entry")

    for elem in items[:20]:
        # 提取标题
        title = ""
        for tag in ["title", "atom:title"]:
            node = elem.find(tag, ns) if ":" in tag else elem.find(tag)
            if node is not None and node.text:
                title = strip_html(node.text)
                break

        # 提取链接
        link = ""
        for tag in ["link", "atom:link"]:
            node = elem.find(tag, ns) if ":" in tag else elem.find(tag)
            if node is not None:
                if node.get("href"):
                    link = node.get("href")
                elif node.text:
                    link = node.text
                if link:
                    break

        # 提取摘要（优先 content:encoded，其次 description/summary）
        summary = ""
        for tag in ["content:encoded", "description", "summary", "atom:summary"]:
            node = elem.find(tag, ns) if ":" in tag else elem.find(tag)
            if node is not None and node.text:
                summary = strip_html(node.text)
                break

        # 提取日期
        pub_date = ""
        for tag in ["pubDate", "published", "updated", "atom:published"]:
            node = elem.find(tag, ns) if ":" in tag else elem.find(tag)
            if node is not None and node.text:
                pub_date = node.text
                break

        if not title or not link:
            continue

        dt = parse_date(pub_date) if pub_date else datetime.now(timezone.utc)
        # 过滤过旧的新闻（> 14 天）
        if datetime.now(timezone.utc) - dt > timedelta(days=14):
            continue

        results.append({
            "title": truncate(title, 200),
            "url": link.strip(),
            "source": name,
            "summary": truncate(summary, 350) if summary else "",
            "category": classify(title, summary) if summary else default_cat,
            "hot": 0,
            "comments": 0,
            "time": dt,
            "weight": weight,
        })

    print(f"[完成] {name}: {len(results)} 条")
    return results


# ── 主流程 ────────────────────────────────────────────────

async def main():
    print("=" * 50)
    print("  AI News Hub Scraper v2")
    print("=" * 50)

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = [
            scrape_hackernews(session),
            scrape_reddit(session),
            scrape_github(session),
        ]
        # RSS 源
        for name, url, weight, cat in RSS_FEEDS:
            tasks.append(scrape_rss(session, name, url, weight, cat))

        all_results = await asyncio.gather(*tasks)

    # 合并、去重、排序
    seen = set()
    merged = []
    for batch in all_results:
        for item in batch:
            key = item["url"].split("?")[0].rstrip("/")
            if key in seen:
                continue
            seen.add(key)
            item["score"] = round(calc_score(item), 1)
            merged.append(item)

    # 按分数排序
    merged.sort(key=lambda x: x["score"], reverse=True)

    # 取 Top N
    top = merged[:TOP_N]
    for i, item in enumerate(top, 1):
        item["rank"] = i
        item["trend"] = "up"
        # 确保时间格式为 ISO 字符串
        if isinstance(item["time"], datetime):
            item["time"] = item["time"].isoformat()

    # 保存
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(top, f, ensure_ascii=False, indent=2)

    sources = {}
    for item in top:
        sources[item["source"]] = sources.get(item["source"], 0) + 1

    print("\n" + "=" * 50)
    print(f"  总计采集: {len(merged)} 条 | 输出 Top {len(top)} 条")
    print(f"  来源分布: {sources}")
    print(f"  已保存: {OUTPUT}")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
