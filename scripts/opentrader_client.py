"""
Thin tRPC HTTP client for OpenTrader.

tRPC v11 wire format:
  Mutation:  POST /api/trpc/<procedure>  body={"json": <input>}
  Query:     GET  /api/trpc/<procedure>  ?input={"json":<input>} (url-encoded)
  Response:  {"result": {"data": {"json": <value>}}}
  Error:     {"error": {"json": {"message": ..., "code": ...}}}
"""

import json
import urllib.parse
import urllib.request
import urllib.error

OPENTRADER_URL = "http://localhost:4000/api/trpc"
ADMIN_PASSWORD = "opentrader"


class OpenTraderError(Exception):
    def __init__(self, procedure, message, code=None):
        self.procedure = procedure
        self.code = code
        super().__init__(f"[{procedure}] {code}: {message}")


class OpenTraderClient:
    def __init__(self, url=OPENTRADER_URL, password=ADMIN_PASSWORD):
        self.url = url.rstrip("/")
        self.password = password

    def _headers(self):
        return {
            "Authorization": self.password,
            "Content-Type": "application/json",
        }

    def _mutation(self, procedure: str, input_data: dict) -> dict:
        """POST mutation to tRPC endpoint."""
        url = f"{self.url}/{procedure}"
        body = json.dumps({"json": input_data}).encode()
        req = urllib.request.Request(url, data=body, headers=self._headers(), method="POST")
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            raw = json.loads(e.read())
            err = raw.get("error", {}).get("json", {})
            raise OpenTraderError(procedure, err.get("message", str(e)), err.get("data", {}).get("code"))

        if "error" in data:
            err = data["error"].get("json", {})
            raise OpenTraderError(procedure, err.get("message", "unknown"), err.get("data", {}).get("code"))

        return data["result"]["data"]["json"]

    def _query(self, procedure: str, input_data: dict) -> dict:
        """GET query to tRPC endpoint."""
        encoded = urllib.parse.quote(json.dumps({"json": input_data}))
        url = f"{self.url}/{procedure}?input={encoded}"
        req = urllib.request.Request(url, headers=self._headers(), method="GET")
        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            raw = json.loads(e.read())
            err = raw.get("error", {}).get("json", {})
            raise OpenTraderError(procedure, err.get("message", str(e)), err.get("data", {}).get("code"))

        if "error" in data:
            err = data["error"].get("json", {})
            raise OpenTraderError(procedure, err.get("message", "unknown"), err.get("data", {}).get("code"))

        return data["result"]["data"]["json"]

    # ------------------------------------------------------------------
    # Exchange account
    # ------------------------------------------------------------------

    def create_paper_exchange_account(self, name: str, exchange_code: str = "KRAKEN") -> dict:
        """
        exchange_code must be uppercase (e.g. "KRAKEN", "BYBIT").
        label and password are nullable; isDemoAccount is required.
        """
        return self._mutation("exchangeAccount.create", {
            "exchangeCode": exchange_code,
            "name": name,
            "label": None,
            "apiKey": "paper",
            "secretKey": "paper",
            "password": None,
            "isDemoAccount": False,
            "isPaperAccount": True,
        })

    def list_exchange_accounts(self) -> list:
        return self._query("exchangeAccount.list", {})

    # ------------------------------------------------------------------
    # Grid bot
    # ------------------------------------------------------------------

    def create_grid_bot(self, exchange_account_id: int, name: str, symbol: str, grid_lines: list) -> dict:
        """
        grid_lines: list of {"price": float, "quantity": float}
        """
        return self._mutation("gridBot.create", {
            "exchangeAccountId": exchange_account_id,
            "data": {
                "name": name,
                "symbol": symbol,
                "settings": {"gridLines": grid_lines},
            },
        })

    def get_bot(self, bot_id: int) -> dict:
        # bot.getOne input schema is z.number() — send the id directly, not wrapped.
        return self._query("bot.getOne", bot_id)

    def start_bot(self, bot_id: int) -> dict:
        return self._mutation("bot.start", {"botId": bot_id})

    def stop_bot(self, bot_id: int) -> dict:
        return self._mutation("bot.stop", {"botId": bot_id})
