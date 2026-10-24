"""Orders — оформление заказов, устойчивое к отказам соседей.

Задание 1 (деньги не списываются дважды):
- POST /orders только сохраняет заказ в базе со статусом pending и сразу
  отвечает 201. Резервирование и оплату выполняют фоновые обработчики.
- Обработчики забирают заказы из базы блокировкой строк (FOR UPDATE SKIP
  LOCKED) и арендой на LEASE секунд: если обработчик упал посреди работы,
  после окончания аренды заказ заберёт другой.
- В Payment уходит Idempotency-Key = order_id: повтор запроса после ошибки,
  таймаута или потерянного ответа не создаёт второго списания, а возвращает
  результат первой операции.
- 402: сначала снимаем резерв, потом ставим rejected. Если снять резерв не
  удалось, заказ остаётся pending и обрабатывается снова.
- 500: заказ повторяется с растущей паузой (не больше MAX_BACKOFF), circuit
  breaker при этом не срабатывает.
- Нет ответа или таймаут: circuit breaker прекращает обычные запросы к Payment,
  раз в BREAKER_COOLDOWN уходит один пробный; успех возвращает обычный режим.

Задание 2 (заказ не теряется):
- У заказа есть признак event_sent. Отдельный обработчик ищет paid-заказы с
  event_sent = false, публикует order.paid и ставит признак после 202. id
  события — order_id, поэтому у всех копий один id.
- Входящие delivery.confirmed: id события записывается в bonus_events в той же
  транзакции, что и начисление бонусов (ON CONFLICT DO NOTHING); копия ничего
  не начисляет и получает 200.
"""

import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

PORT = int(os.environ.get("PORT", "8080"))
DATABASE_URL = os.environ["DATABASE_URL"]
PAYMENT_URL = os.environ["PAYMENT_URL"].rstrip("/")
INVENTORY_URL = os.environ["INVENTORY_URL"].rstrip("/")
EVENTS_URL = os.environ.get("EVENTS_URL", "").rstrip("/")

WORKERS = 8                # фоновых обработчиков заказов
LEASE = 10                 # аренда заказа обработчиком, с
PAYMENT_TIMEOUT = 5.0      # таймаут запроса в Payment, с
INVENTORY_TIMEOUT = 2.0
EVENTS_TIMEOUT = 2.0
BASE_BACKOFF = 0.5         # первая пауза перед повтором заказа, с
MAX_BACKOFF = 3.0          # предел паузы, с
BREAKER_THRESHOLD = 5      # подряд запросов без ответа до размыкания
BREAKER_COOLDOWN = 2.0     # пауза перед пробным запросом, с
IDLE_SLEEP = 0.2           # пауза обработчика, когда заказов нет, с
PUBLISH_TICK = 0.5         # интервал обработчика событий, с

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("orders")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id              TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    status          TEXT NOT NULL,
    amount_cents    BIGINT NOT NULL,
    items           JSONB NOT NULL,
    attempts        INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until    TIMESTAMPTZ,
    event_sent      BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS orders_pending ON orders (next_attempt_at) WHERE status = 'pending';
CREATE TABLE IF NOT EXISTS bonuses (
    user_id TEXT PRIMARY KEY,
    points  BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS bonus_events (
    id         TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL,
    points     BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


def db():
    return psycopg.connect(DATABASE_URL, autocommit=True, connect_timeout=5)


def init_db():
    # База может подниматься дольше сервиса — ждём её.
    for _ in range(120):
        try:
            with db() as conn:
                conn.execute(SCHEMA)
            return
        except psycopg.OperationalError as e:
            log.info("база недоступна (%s), жду", e)
            time.sleep(1)
    raise SystemExit("не дождался базы")


class NoResponse(Exception):
    """Сосед не ответил: таймаут, обрыв или отказ в соединении."""


class RetryLater(Exception):
    """Исход неизвестен или сосед ответил ошибкой — заказ надо повторить."""


def call(method, url, body=None, timeout=5.0, headers=None):
    """HTTP-вызов соседа. Возвращает (код, тело); без ответа — NoResponse."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            try:
                return resp.status, json.loads(raw or b"null")
            except ValueError:
                return resp.status, None
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:
        raise NoResponse(f"{method} {url}: {e!r}")


class Breaker:
    """Circuit breaker на Payment: closed → open → half-open (один пробный)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.failures = 0
        self.open_until = 0.0
        self.opened = False
        self.probing = False

    def allow(self):
        with self.lock:
            if not self.opened:
                return True
            if self.probing or time.monotonic() < self.open_until:
                return False
            self.probing = True
            return True

    def allow_peek(self):
        """Без побочных эффектов: имеет ли смысл сейчас пробовать Payment."""
        with self.lock:
            if not self.opened:
                return True
            return not self.probing and time.monotonic() >= self.open_until

    def success(self):
        with self.lock:
            if self.opened:
                log.info("payment: снова отвечает, breaker замкнут")
            self.failures = 0
            self.opened = False
            self.probing = False

    def failure(self):
        with self.lock:
            self.failures += 1
            if self.probing or self.failures >= BREAKER_THRESHOLD:
                if not self.opened:
                    log.warning("payment: %d запросов без ответа подряд, breaker разомкнут",
                                self.failures)
                self.opened = True
                self.probing = False
                self.open_until = time.monotonic() + BREAKER_COOLDOWN


breaker = Breaker()


class BreakerOpen(Exception):
    """Payment сейчас не опрашиваем — заказ вернётся в очередь без штрафа."""


def reserve(order_id, items):
    try:
        code, _ = call("POST", f"{INVENTORY_URL}/reservations", {
            "order_id": order_id,
            "items": [{"sku": i["sku"], "qty": i["qty"]} for i in items],
        }, timeout=INVENTORY_TIMEOUT)
    except NoResponse as e:
        raise RetryLater(str(e))
    if code not in (200, 201):
        raise RetryLater(f"inventory reserve: {code}")


def release(order_id):
    try:
        code, _ = call("DELETE", f"{INVENTORY_URL}/reservations/{urllib.parse.quote(order_id)}",
                       timeout=INVENTORY_TIMEOUT)
    except NoResponse as e:
        raise RetryLater(str(e))
    if code not in (200, 204):
        raise RetryLater(f"inventory release: {code}")


def charge(order_id, amount):
    """Списание с ключом идемпотентности: 'paid' | 'declined' | исключение."""
    if not breaker.allow():
        raise BreakerOpen()
    try:
        code, _ = call("POST", f"{PAYMENT_URL}/payments", {
            "order_id": order_id,
            "amount_cents": amount,
            "currency": "RUB",
        }, timeout=PAYMENT_TIMEOUT, headers={"Idempotency-Key": order_id})
    except NoResponse as e:
        breaker.failure()
        raise RetryLater(str(e))
    # Payment ответил — значит, он доступен, даже если ответ — ошибка.
    breaker.success()
    if code in (200, 201):
        return "paid"
    if code == 402:
        return "declined"
    raise RetryLater(f"payment: {code}")


# --- обработка заказов --------------------------------------------------------

CLAIM = f"""
UPDATE orders SET locked_until = now() + interval '{LEASE} seconds'
WHERE id = (
    SELECT id FROM orders
    WHERE status = 'pending' AND next_attempt_at <= now()
      AND (locked_until IS NULL OR locked_until < now())
    ORDER BY next_attempt_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, amount_cents, items, attempts
"""


def claim():
    """Забрать один готовый к обработке заказ в аренду или None."""
    with db() as conn:
        return conn.execute(CLAIM).fetchone()


def finish(order_id, status):
    with db() as conn:
        conn.execute(
            "UPDATE orders SET status = %s, locked_until = NULL "
            "WHERE id = %s AND status = 'pending'",
            (status, order_id))


def postpone(order_id, delay, count_attempt=True):
    with db() as conn:
        conn.execute(
            "UPDATE orders SET locked_until = NULL, "
            "next_attempt_at = now() + make_interval(secs => %s), "
            "attempts = attempts + %s WHERE id = %s",
            (delay, 1 if count_attempt else 0, order_id))


def backoff(attempts):
    return min(BASE_BACKOFF * (2 ** attempts), MAX_BACKOFF)


def process(order_id, amount, items, attempts):
    """Одна попытка довести заказ до paid/rejected. Повторять безопасно."""
    try:
        reserve(order_id, items)
        result = charge(order_id, amount)
        if result == "paid":
            finish(order_id, "paid")
            log.info("order %s: paid", order_id)
        else:
            release(order_id)
            finish(order_id, "rejected")
            log.info("order %s: rejected", order_id)
    except BreakerOpen:
        postpone(order_id, BREAKER_COOLDOWN, count_attempt=False)
    except RetryLater as e:
        delay = backoff(attempts)
        log.info("order %s: повторю через %.1f с (%s)", order_id, delay, e)
        postpone(order_id, delay)
    except Exception:
        log.exception("order %s: ошибка обработки", order_id)
        postpone(order_id, backoff(attempts))


def worker():
    while True:
        try:
            if not breaker.allow_peek():
                time.sleep(IDLE_SLEEP)
                continue
            row = claim()
            if row is None:
                time.sleep(IDLE_SLEEP)
                continue
            order_id, amount, items, attempts = row
            process(order_id, amount, items, attempts)
        except Exception:
            log.exception("worker")
            time.sleep(1)


def create_order(body):
    order_id = str(uuid.uuid4())
    amount = sum(i["qty"] * i["price_cents"] for i in body["items"])
    items = [{"sku": i["sku"], "qty": i["qty"], "price_cents": i["price_cents"]} for i in body["items"]]
    with db() as conn:
        conn.execute(
            "INSERT INTO orders (id, user_id, status, amount_cents, items) "
            "VALUES (%s, %s, 'pending', %s, %s)",
            (order_id, body["user_id"], amount, json.dumps(items)))
    return {"id": order_id, "status": "pending"}


# --- задание 2: публикация событий -------------------------------------------

publish_pool = ThreadPoolExecutor(max_workers=16, thread_name_prefix="publish")
publishing = set()
publishing_lock = threading.Lock()


def publish_one(order_id):
    try:
        code, _ = call("POST", f"{EVENTS_URL}/events", {
            "id": order_id,
            "type": "order.paid",
            "order_id": order_id,
            "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }, timeout=EVENTS_TIMEOUT)
        if code == 202:
            with db() as conn:
                conn.execute("UPDATE orders SET event_sent = true WHERE id = %s", (order_id,))
        else:
            log.info("order %s: шина ответила %s, повторю", order_id, code)
    except NoResponse as e:
        log.info("order %s: шина не ответила (%s), повторю", order_id, e)
    except Exception:
        log.exception("order %s: публикация", order_id)
    finally:
        with publishing_lock:
            publishing.discard(order_id)


def publisher():
    while True:
        time.sleep(PUBLISH_TICK)
        if not EVENTS_URL:
            continue
        try:
            with db() as conn:
                rows = conn.execute(
                    "SELECT id FROM orders WHERE status = 'paid' AND NOT event_sent "
                    "ORDER BY created_at LIMIT 200").fetchall()
            for (order_id,) in rows:
                with publishing_lock:
                    if order_id in publishing or len(publishing) >= 16:
                        continue
                    publishing.add(order_id)
                publish_pool.submit(publish_one, order_id)
        except Exception:
            log.exception("publisher")


def delivery_confirmed(event):
    """Начисляет бонусы ровно один раз на id события."""
    with db() as conn:
        with conn.transaction():
            cur = conn.execute(
                "INSERT INTO bonus_events (id, user_id, points) VALUES (%s, %s, %s) "
                "ON CONFLICT (id) DO NOTHING",
                (event["id"], event["user_id"], event["bonus_points"]))
            if cur.rowcount == 1:
                conn.execute(
                    "INSERT INTO bonuses (user_id, points) VALUES (%s, %s) "
                    "ON CONFLICT (user_id) DO UPDATE SET points = bonuses.points + EXCLUDED.points",
                    (event["user_id"], event["bonus_points"]))


def get_bonus(user_id):
    with db() as conn:
        row = conn.execute("SELECT points FROM bonuses WHERE user_id = %s", (user_id,)).fetchone()
    return {"points": row[0] if row else 0}


def get_order(order_id):
    with db() as conn:
        row = conn.execute(
            "SELECT id, status, amount_cents FROM orders WHERE id = %s", (order_id,)
        ).fetchone()
    if row is None:
        return None
    return {"id": row[0], "status": row[1], "amount_cents": row[2]}


def valid(body):
    if not isinstance(body, dict):
        return False
    if not isinstance(body.get("user_id"), str) or not body["user_id"]:
        return False
    items = body.get("items")
    if not isinstance(items, list) or not items:
        return False
    for i in items:
        if not isinstance(i, dict):
            return False
        if not isinstance(i.get("sku"), str) or not i["sku"]:
            return False
        for k, minimum in (("qty", 1), ("price_cents", 0)):
            v = i.get(k)
            if not isinstance(v, int) or isinstance(v, bool) or v < minimum:
                return False
    if sum(i["qty"] * i["price_cents"] for i in items) < 1:
        return False  # Payment не принимает нулевую сумму
    return True


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
        path = self.path.split("?", 1)[0]
        if path == "/healthz":
            return self.send_json(200, {"status": "ok"})
        try:
            m = ORDER_PATH.match(path)
            if m:
                order = get_order(urllib.parse.unquote(m.group(1)))
                if order is None:
                    return self.send_json(404, {"error": "not_found"})
                return self.send_json(200, order)
            m = BONUS_PATH.match(path)
            if m:
                return self.send_json(200, get_bonus(urllib.parse.unquote(m.group(1))))
        except Exception as e:
            log.exception("GET %s", path)
            return self.send_json(500, {"error": "internal", "message": str(e)})
        self.send_json(404, {"error": "not_found"})

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path not in ("/orders", "/webhooks/delivery"):
            return self.send_json(404, {"error": "not_found"})
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"null")
        except ValueError:
            body = None
        if path == "/webhooks/delivery":
            return self.webhook(body)
        if not valid(body):
            return self.send_json(400, {"error": "bad_request"})
        try:
            self.send_json(201, create_order(body))
        except Exception as e:
            log.exception("POST /orders")
            self.send_json(500, {"error": "internal", "message": str(e)})

    def webhook(self, body):
        if not isinstance(body, dict) or not isinstance(body.get("id"), str) or not body["id"] \
                or not isinstance(body.get("user_id"), str) \
                or not isinstance(body.get("bonus_points"), int) \
                or isinstance(body.get("bonus_points"), bool) or body["bonus_points"] <= 0:
            return self.send_json(400, {"error": "bad_request"})
        try:
            delivery_confirmed(body)
        except Exception as e:
            log.exception("POST /webhooks/delivery")
            return self.send_json(500, {"error": "internal", "message": str(e)})
        self.send_json(200, {"status": "ok"})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    init_db()
    for n in range(WORKERS):
        threading.Thread(target=worker, name=f"worker-{n}", daemon=True).start()
    threading.Thread(target=publisher, name="publisher", daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    server.request_queue_size = 128
    log.info("orders: слушаю :%d", PORT)
    server.serve_forever()
