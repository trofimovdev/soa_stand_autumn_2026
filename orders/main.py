"""Сервис заказов.

Оформление заказа: резерв товара в Inventory, списание денег в Payment,
запись заказа в базу. Заказ создаётся в статусе pending и обрабатывается
фоновыми воркерами с повторами.

Задание 2: об оплаченном заказе сервис сообщает в шину событий (order.paid),
а за подтверждённую доставку начисляет покупателю бонусы.
"""

import json
import logging
import os
import random
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

HTTP_PORT = int(os.environ.get("PORT", "8080"))
DATABASE_DSN = os.environ["DATABASE_URL"]
PAYMENT_BASE = os.environ["PAYMENT_URL"].rstrip("/")
INVENTORY_BASE = os.environ["INVENTORY_URL"].rstrip("/")
# Шина событий; пусто — события не публикуются.
EVENTS_BASE = os.environ.get("EVENTS_URL", "").rstrip("/")

DEFAULT_TIMEOUT = 0.5
PAYMENT_TIMEOUT = 3.0
WORKER_COUNT = 8

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("orders")

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    status          TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL,
    items           JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempt         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_sent      BOOLEAN NOT NULL DEFAULT false
);
CREATE TABLE IF NOT EXISTS bonuses (
    user_id TEXT PRIMARY KEY,
    points  BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS bonus_events (
    id      TEXT PRIMARY KEY,
    user_id TEXT NOT NULL
);
"""


# --------------------------------------------------------------------------
# База данных
# --------------------------------------------------------------------------


def connect_db():
    return psycopg.connect(DATABASE_DSN, autocommit=True)


def init_db():
    # База может подниматься дольше сервиса — ждём её.
    for _ in range(60):
        try:
            with connect_db() as conn:
                conn.execute(DB_SCHEMA)
            return
        except psycopg.OperationalError as exc:
            log.info("база недоступна (%s), жду", exc)
            time.sleep(1)
    raise SystemExit("не дождался базы")


# --------------------------------------------------------------------------
# HTTP-клиент, Inventory
# --------------------------------------------------------------------------


def http_call(method, url, body=None, headers=None, timeout=DEFAULT_TIMEOUT):
    """HTTP-вызов внешнего сервиса. Возвращает (код, тело)."""
    payload = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url, data=payload, method=method, headers=headers or {}
    )
    request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read() or b"null")
    except urllib.error.HTTPError as exc:
        return exc.code, None


def reserve_stock(order_id, items):
    code, _ = http_call(
        "POST",
        f"{INVENTORY_BASE}/reservations",
        {
            "order_id": order_id,
            "items": [{"sku": i["sku"], "qty": i["qty"]} for i in items],
        },
    )
    if code not in (200, 201):
        raise RuntimeError(f"inventory: {code}")


def release_stock(order_id):
    code, _ = http_call("DELETE", f"{INVENTORY_BASE}/reservations/{order_id}")
    if code != 204:
        raise RuntimeError(f"inventory release: {code}")


# --------------------------------------------------------------------------
# Предохранитель и списание в Payment
# --------------------------------------------------------------------------

breaker_lock = threading.Lock()
breaker_state = {"failures": 0, "open_until": 0.0, "probe_until": 0.0}


def payment_allowed():
    with breaker_lock:
        if breaker_state["failures"] == 0:
            return True
        now = time.monotonic()
        if now < breaker_state["open_until"] or now < breaker_state["probe_until"]:
            return False
        breaker_state["probe_until"] = now + PAYMENT_TIMEOUT + 1
        return True


def record_payment_result(ok):
    with breaker_lock:
        breaker_state["probe_until"] = 0.0
        if ok:
            breaker_state["failures"] = 0
            breaker_state["open_until"] = 0.0
        else:
            breaker_state["failures"] += 1
            pause = min(3.0, 0.5 * 2 ** (breaker_state["failures"] - 1))
            breaker_state["open_until"] = time.monotonic() + pause


def payment_wait_time():
    with breaker_lock:
        return max(0.1, breaker_state["open_until"] - time.monotonic())


def charge_payment(order_id, amount_cents):
    """Возвращает 'paid', 'declined' или None (нужен повтор)."""
    try:
        code, _ = http_call(
            "POST",
            f"{PAYMENT_BASE}/payments",
            {
                "order_id": order_id,
                "amount_cents": amount_cents,
                "currency": "RUB",
            },
            headers={"Idempotency-Key": order_id},
            timeout=PAYMENT_TIMEOUT,
        )
    except Exception as exc:
        log.warning("order %s: payment недоступен (%s)", order_id, exc)
        record_payment_result(False)
        return None
    record_payment_result(True)
    if code in (200, 201):
        return "paid"
    if code == 402:
        return "declined"
    return None


# --------------------------------------------------------------------------
# Заказы
# --------------------------------------------------------------------------


def is_valid_order(body):
    if not isinstance(body, dict):
        return False
    if not isinstance(body.get("user_id"), str) or not body["user_id"]:
        return False
    items = body.get("items")
    if not isinstance(items, list) or not items:
        return False
    for item in items:
        if not isinstance(item, dict):
            return False
        if not isinstance(item.get("sku"), str) or not item["sku"]:
            return False
        for key, minimum in (("qty", 1), ("price_cents", 0)):
            value = item.get(key)
            if not isinstance(value, int) or isinstance(value, bool):
                return False
            if value < minimum:
                return False
    return True


def create_order(body):
    order_id = str(uuid.uuid4())
    amount_cents = sum(i["qty"] * i["price_cents"] for i in body["items"])

    with connect_db() as conn:
        conn.execute(
            "INSERT INTO orders "
            "(id, user_id, status, amount_cents, items, attempt, next_attempt_at) "
            "VALUES (%s, %s, %s, %s, %s, 0, now())",
            (order_id, body["user_id"], "pending", amount_cents,
             json.dumps(body["items"])),
        )

    log.info("order %s: pending", order_id)
    return {"id": order_id, "status": "pending"}


def defer_order(order_id, delay_seconds):
    with connect_db() as conn:
        conn.execute(
            "UPDATE orders SET status = 'pending', "
            "next_attempt_at = now() + make_interval(secs => %s) WHERE id = %s",
            (delay_seconds, order_id),
        )


def schedule_retry(order_id):
    with connect_db() as conn:
        attempt = conn.execute(
            "UPDATE orders SET attempt = attempt + 1 WHERE id = %s RETURNING attempt",
            (order_id,),
        ).fetchone()[0]
    base_delay = min(3.0, 0.2 * 2 ** min(attempt, 5))
    defer_order(order_id, random.uniform(0.5, 1.0) * base_delay)


def process_order(order_id, items, amount_cents):
    try:
        try:
            reserve_stock(order_id, items)
        except (RuntimeError, OSError) as exc:
            log.warning("order %s: inventory (%s)", order_id, exc)
            return schedule_retry(order_id)

        if not payment_allowed():
            return defer_order(order_id, payment_wait_time())

        result = charge_payment(order_id, amount_cents)
        if result is None:
            return schedule_retry(order_id)

        if result == "declined":
            try:
                release_stock(order_id)
            except (RuntimeError, OSError) as exc:
                log.warning("order %s: release (%s)", order_id, exc)
                return schedule_retry(order_id)
            final_status = "rejected"
        else:
            final_status = "paid"

        with connect_db() as conn:
            conn.execute(
                "UPDATE orders SET status = %s, event_sent = (status = 'paid') "
                "WHERE id = %s",
                (final_status, order_id),
            )
        log.info("order %s: %s", order_id, final_status)
    except Exception:
        log.exception("process_order %s", order_id)
        schedule_retry(order_id)


def worker_loop():
    claim_sql = (
        "UPDATE orders SET status = 'processing', "
        "next_attempt_at = now() + interval '10 seconds' "
        "WHERE id = ("
        "  SELECT id FROM orders "
        "  WHERE status IN ('pending', 'processing') AND next_attempt_at <= now() "
        "  ORDER BY next_attempt_at ASC LIMIT 1 "
        "  FOR UPDATE SKIP LOCKED"
        ") "
        "RETURNING id, items, amount_cents"
    )
    while True:
        try:
            with connect_db() as conn:
                row = conn.execute(claim_sql).fetchone()
            if row is None:
                time.sleep(0.5)
                continue
            order_id, items, amount_cents = row[0], row[1], row[2]
            process_order(order_id, items, amount_cents)
        except Exception:
            log.exception("worker")
            time.sleep(1)


# --------------------------------------------------------------------------
# События и бонусы (задание 2)
# --------------------------------------------------------------------------


def publish_paid(order_id):
    """Сообщает в шину, что заказ оплачен."""
    if not EVENTS_BASE:
        return True
    try:
        code, _ = http_call(
            "POST",
            f"{EVENTS_BASE}/events",
            {
                "id": order_id,
                "type": "order.paid",
                "order_id": order_id,
                "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
        )
    except OSError as exc:
        log.warning("order %s: событие не опубликовано (%s)", order_id, exc)
        return False
    if code == 202:
        return True
    log.warning("order %s: событие не опубликовано (код %s)", order_id, code)
    return False


def publisher_loop():
    while True:
        try:
            with connect_db() as conn:
                rows = conn.execute(
                    "SELECT id FROM orders "
                    "WHERE status = 'paid' AND event_sent = false LIMIT 20"
                ).fetchall()
            if not rows:
                time.sleep(1)
                continue
            for (order_id,) in rows:
                if publish_paid(order_id):
                    with connect_db() as conn:
                        conn.execute(
                            "UPDATE orders SET event_sent = true WHERE id = %s",
                            (order_id,),
                        )
        except Exception:
            log.exception("publisher")
            time.sleep(1)


def apply_delivery_bonus(event):
    """Начисляет бонусы за подтверждённую доставку (один раз на событие)."""
    with psycopg.connect(DATABASE_DSN, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO bonus_events (id, user_id) VALUES (%s, %s) "
                "ON CONFLICT (id) DO NOTHING",
                (event["id"], event["user_id"]),
            )
            if cur.rowcount == 0:
                conn.rollback()
                return
            cur.execute(
                "INSERT INTO bonuses (user_id, points) VALUES (%s, %s) "
                "ON CONFLICT (user_id) DO UPDATE "
                "SET points = bonuses.points + EXCLUDED.points",
                (event["user_id"], event["bonus_points"]),
            )
        conn.commit()


def fetch_bonus(user_id):
    with connect_db() as conn:
        row = conn.execute(
            "SELECT points FROM bonuses WHERE user_id = %s", (user_id,)
        ).fetchone()
    return {"points": row[0] if row else 0}


def fetch_order(order_id):
    with connect_db() as conn:
        row = conn.execute(
            "SELECT id, status, amount_cents FROM orders WHERE id = %s",
            (order_id,),
        ).fetchone()
    if row is None:
        return None
    status = "pending" if row[1] == "processing" else row[1]
    return {"id": row[0], "status": status, "amount_cents": row[2]}


# --------------------------------------------------------------------------
# HTTP-сервер
# --------------------------------------------------------------------------

ORDER_PATH = re.compile(r"^/orders/([^/]+)$")
BONUS_PATH = re.compile(r"^/users/([^/]+)/bonus$")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def send_json(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/healthz":
            return self.send_json(200, {"status": "ok"})
        match = ORDER_PATH.match(self.path)
        if match:
            order = fetch_order(match.group(1))
            if order is None:
                return self.send_json(404, {"error": "not_found"})
            return self.send_json(200, order)
        match = BONUS_PATH.match(self.path)
        if match:
            user_id = urllib.parse.unquote(match.group(1))
            return self.send_json(200, fetch_bonus(user_id))
        self.send_json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path not in ("/orders", "/webhooks/delivery"):
            return self.send_json(404, {"error": "not_found"})
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"null")
        except ValueError:
            body = None
        if self.path == "/webhooks/delivery":
            return self.handle_delivery_webhook(body)
        if not is_valid_order(body):
            return self.send_json(400, {"error": "bad_request"})
        try:
            self.send_json(201, create_order(body))
        except Exception as exc:
            log.exception("POST /orders")
            self.send_json(500, {"error": "internal", "message": str(exc)})

    def handle_delivery_webhook(self, body):
        if (
            not isinstance(body, dict)
            or not isinstance(body.get("id"), str)
            or not isinstance(body.get("user_id"), str)
            or not isinstance(body.get("bonus_points"), int)
            or body["bonus_points"] <= 0
        ):
            return self.send_json(400, {"error": "bad_request"})
        try:
            apply_delivery_bonus(body)
        except Exception as exc:
            log.exception("POST /webhooks/delivery")
            return self.send_json(500, {"error": "internal", "message": str(exc)})
        self.send_json(200, {"status": "ok"})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    init_db()
    for _ in range(WORKER_COUNT):
        threading.Thread(target=worker_loop, daemon=True).start()
    threading.Thread(target=publisher_loop, daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), Handler)
    server.daemon_threads = True
    log.info("orders: слушаю :%d", HTTP_PORT)
    server.serve_forever()
