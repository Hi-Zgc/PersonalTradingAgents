import asyncio
import logging
import re
import urllib.request
from typing import Optional

from src.data.sources.base import DataSource
from src.utils.ticker import normalize_ticker

logger = logging.getLogger(__name__)
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


class TencentSource(DataSource):
    """Real-time A-share quotes via Tencent Finance HTTP API (qt.gtimg.cn)."""

    name = "tencent"

    @staticmethod
    def _get_prefix(code: str) -> str:
        if len(code) == 5 and code.isdigit():
            return "hk"
        if code.startswith(("6", "9")):
            return "sh"
        elif code.startswith("8"):
            return "bj"
        return "sz"

    def _fetch_quote(self, codes: list[str]) -> dict[str, dict]:
        prefixed = [f"{self._get_prefix(c)}{c}" for c in codes]
        return self._fetch_prefixed_quote(prefixed)

    def _fetch_prefixed_quote(self, prefixed: list[str]) -> dict[str, dict]:
        url = "https://qt.gtimg.cn/q=" + ",".join(prefixed)
        req = urllib.request.Request(url)
        req.add_header("User-Agent", _UA)
        resp = urllib.request.urlopen(req, timeout=10)
        raw = resp.read().decode("gbk")

        result = {}
        for line in raw.strip().split(";"):
            if not line.strip() or "=" not in line or '"' not in line:
                continue
            key = line.split("=")[0].split("_")[-1]
            vals = line.split('"')[1].split("~")
            if len(vals) < 53:
                continue
            code = key[2:]
            result[code] = {
                "name": vals[1],
                "price": float(vals[3]) if vals[3] else 0.0,
                "last_close": float(vals[4]) if vals[4] else 0.0,
                "open": float(vals[5]) if vals[5] else 0.0,
                "change_pct": float(vals[32]) if vals[32] else 0.0,
                "high": float(vals[33]) if vals[33] else 0.0,
                "low": float(vals[34]) if vals[34] else 0.0,
                "volume": int(float(vals[36])) if vals[36] else 0,
                "turnover": float(vals[37]) * 10000 if vals[37] else 0.0,
                "turnover_pct": float(vals[38]) if vals[38] else 0.0,
                "pe_ttm": float(vals[39]) if vals[39] else 0.0,
                "mcap_yi": float(vals[44]) if vals[44] else 0.0,
                "float_mcap_yi": float(vals[45]) if vals[45] else 0.0,
                "pb": float(vals[46]) if vals[46] else 0.0,
                "limit_up": float(vals[47]) if vals[47] else 0.0,
                "limit_down": float(vals[48]) if vals[48] else 0.0,
                "pe_static": float(vals[52]) if vals[52] else 0.0,
            }
        return result

    async def get_quote(self, symbol: str) -> Optional[dict]:
        code = str(normalize_ticker(symbol))
        # HK stocks are 5-digit; A-shares are 6-digit
        if not (len(code) == 5 and code.isdigit()):
            code = code.zfill(6)
        try:
            data = await asyncio.to_thread(self._fetch_quote, [code])
            if code not in data:
                return None
            q = data[code]
            return {
                "symbol": code,
                "name": q.get("name", ""),
                "price": q["price"],
                "open": q["open"],
                "high": q["high"],
                "low": q["low"],
                "prev_close": q["last_close"],
                "volume": q["volume"],
                "turnover": q["turnover"],
                "change_pct": q["change_pct"],
                "pe_ttm": q["pe_ttm"],
                "pb": q["pb"],
                "mcap": q["mcap_yi"],
                "source": self.name,
            }
        except Exception as e:
            logger.warning("Tencent quote failed for %s: %s", code, e)
            return None

    async def get_market_indices(self) -> Optional[list[dict]]:
        indices_map = {
            "sh000001": "上证指数",
            "sz399001": "深证成指",
            "sz399006": "创业板指",
            "sh000688": "科创50",
            "sh000016": "上证50",
            "sh000300": "沪深300",
        }
        try:
            data = await asyncio.to_thread(self._fetch_prefixed_quote, list(indices_map.keys()))
            results = []
            for prefixed, name in indices_map.items():
                code = prefixed[2:]
                q = data.get(code)
                if not q:
                    continue
                current = float(q.get("price") or 0.0)
                prev_close = float(q.get("last_close") or 0.0)
                change = current - prev_close if current and prev_close else 0.0
                change_pct = float(q.get("change_pct") or 0.0)
                results.append({
                    "code": code,
                    "name": name,
                    "current": current,
                    "change": round(change, 2),
                    "change_pct": round(change_pct, 2),
                    "open": float(q.get("open") or 0.0),
                    "high": float(q.get("high") or 0.0),
                    "low": float(q.get("low") or 0.0),
                    "volume": int(q.get("volume") or 0),
                    "amount": float(q.get("turnover") or 0.0),
                    "amplitude": 0.0,
                    "source": self.name,
                })
            return results or None
        except Exception as e:
            logger.warning("Tencent market indices failed: %s", e)
            return None

    async def get_fundamentals(self, symbol: str) -> Optional[dict]:
        """Tencent provides PE/PB in quote response."""
        quote = await self.get_quote(symbol)
        if not quote:
            return None
        return {
            "symbol": quote["symbol"],
            "pe_ttm": quote.get("pe_ttm", 0.0),
            "pb": quote.get("pb", 0.0),
            "source": self.name,
        }

    async def get_kline(self, symbol: str, period: str = "1d", limit: int = 60) -> Optional[list[dict]]:
        return None

    async def health_check(self) -> bool:
        try:
            await asyncio.wait_for(
                asyncio.to_thread(self._fetch_quote, ["000001"]),
                timeout=10,
            )
            return True
        except Exception:
            return False
