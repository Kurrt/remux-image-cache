#!/usr/bin/env python3
"""Keeps a Remux image cache warm, so the first view of an image doesn't wait for its source.

Remux proxies images from a remote CDN. An nginx cache in front of it makes repeat views fast, but each
image's first view is still slow. This worker makes those first views happen in the background, through
the same nginx cache and with the same URLs clients use, so the cache keys match.

Three things feed it:
  - a plan built from Remux's database: home screen rows first, then the rest of the catalogue
  - new rows, found by polling, which go to the front of the plan
  - a one-way signal from nginx saying which item a viewer just looked at, whose other art is fetched next

See README.md. Only the Python standard library is used.
"""

import argparse
import collections
import contextlib
import http.client
import itertools
import json
import os
import queue
import re
import shutil
import signal
import socket
import sqlite3
import threading
import time
import traceback
import urllib.parse
import uuid
from dataclasses import dataclass, fields
from typing import NamedTuple

VERSION = "1.1"

# Tiers. These numbers are stored on disk, so don't renumber them. STAGE_ORDER says which goes first.
TIER_NEW = 1
TIER_HOME = 2
TIER_TITLES = 3
TIER_TITLE_ART = 4
TIER_SEASONS = 5
TIER_PEOPLE = 6
TIER_EPISODES = 7
TIER_CHANNELS = 8
TIER_GUIDE = 9
TIER_FULL_STILLS = 10
TIER_NAMES = {
    TIER_NEW: "new",
    TIER_HOME: "home",
    TIER_TITLES: "titles",
    TIER_TITLE_ART: "title-art",
    TIER_CHANNELS: "channels",
    TIER_GUIDE: "guide",
    TIER_SEASONS: "seasons",
    TIER_PEOPLE: "people",
    TIER_EPISODES: "episodes",
    TIER_FULL_STILLS: "full-stills",
}
# Most useful first.
STAGE_ORDER = tuple(TIER_NAMES)
LONG_TAIL = {TIER_CHANNELS, TIER_PEOPLE, TIER_EPISODES, TIER_FULL_STILLS}
# Art kept warm for good, which is all of it but guide art: a programme is over soon, so that's left to lapse.
REFRESHED = set(STAGE_ORDER) - {TIER_GUIDE}
# Art from hosts this worker can't see. What those hosts do says nothing about the catalogue's source.
THIRD_PARTY = {TIER_GUIDE, TIER_CHANNELS}

# What nginx's answer means for this worker.
CACHED = "cached"
MISSING = "missing"
UNCACHEABLE = "uncacheable"
REFUSED = "refused"
RETRY = "retry"
SOFT = "soft"
BACKOFF = "backoff"
SUSPECT = "suspect"
INVALID = "invalid"

# Job states kept on disk. A job that hasn't been tried yet is 0.
STATUS_CACHED = 1
STATUS_MISSING = 2
STATUS_UNCACHEABLE = 3
STATUS_LAPSED = 4

HOUR = 3600
DAY = 86400
MISSING_RETRY = (HOUR, DAY, 7 * DAY, 30 * DAY)
FAILURE_RETRY = (60, 5 * 60, 30 * 60, 6 * HOUR, DAY)
REFUSED_RETRY = 6 * HOUR
SUSPECT_RETRY = 10 * 60
# How often a row can be doubted before it's believed. A long run of dead rows looks just like a failing source.
SUSPECT_LIMIT = 3
# Guide art is warmed for programmes airing now, through to this long ahead.
GUIDE_WINDOW = 24 * HOUR
# Long enough for a signal's work to survive a short outage, not so long that it's stale.
SIGNAL_RETRY_FOR = HOUR
# nginx counts an entry as expired only once its life has fully passed, so asking a moment later refreshes it.
EXPIRY_MARGIN = 60
NEVER = 2**40
MIN_SPEED = 0.1
# A hit is about ten times as quick as a real fetch, so this recovers at the same pace over time.
HIT_RECOVERY = 0.002
# Of the last BREAKER_WINDOW images really fetched, this many failing means the source is in trouble. It counts
# images, not requests: each is asked for in several forms, and one title's dead art shouldn't look like an outage.
BREAKER_WINDOW = 10
BREAKER_TRIPS = 5
PLAN_CHUNK = 2000
# Raise this when the rules for what goes in a plan change, so that a restart on the new rules plans again at once.
PLAN_RULES = 2
# How much has to go well in a row before a problem stops being listed.
CLEAN_FETCHES = 20
CLEAN_READS = 3

ID_RE = re.compile(rb"([0-9a-f]{32}) (primary|logo|backdrop|thumb)\s*$")
RECENT_LIMIT = 100


def timestamp(at=None):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(at))


def log(message):
    print(f"{timestamp()} {message}", flush=True)


@dataclass
class Config:
    """Every setting, with its default. Each one is read from WARMER_<NAME>."""

    db_path: str = "/remux/db.sqlite"
    cache_url: str = "http://remux-images:8080"
    state_dir: str = "/state"
    signal_socket: str = "/run/warmer/signal.sock"
    rate: float = 1.0
    long_tail_rate: float = 0.5
    new_rate: float = 2.0
    signal_rate: float = 5.0
    signal_workers: int = 3
    signal_quiet: float = 0.25
    signal_max_hold: float = 2.0
    signal_burst_gap: float = 1.5
    season_episodes: int = 40
    cast_limit: int = 20
    dedupe_hours: float = 12.0
    poll_seconds: float = 30.0
    plan_hours: float = 6.0
    yield_seconds: float = 20.0
    home_row_items: int = 30
    people: bool = False
    episodes: bool = False
    channels: bool = False
    guide: bool = False
    budget_gb: float = 50.0
    min_free_gb: float = 20.0
    touch_home_days: float = 7.0
    touch_days: float = 50.0
    touch_untagged_days: float = 25.0
    life_days: float = 120.0
    untagged_life_days: float = 30.0
    request_timeout: float = 60.0
    slow_miss_ms: float = 4000.0
    slow_hit_ms: float = 500.0
    backoff_seconds: float = 5.0
    backoff_max_seconds: float = 300.0
    log_seconds: float = 300.0
    status_detail_seconds: float = 60.0
    max_fetches: int = 0

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env
        values = {}
        for field in fields(cls):
            raw = env.get("WARMER_" + field.name.upper())
            # An empty socket means no signals. Any other empty setting means the default.
            if raw is None or (raw == "" and field.name != "signal_socket"):
                continue
            try:
                if field.type in (bool, "bool"):
                    values[field.name] = raw.strip().lower() in ("1", "true", "yes", "on")
                elif field.type in (int, "int"):
                    values[field.name] = int(raw)
                elif field.type in (float, "float"):
                    values[field.name] = float(raw)
                else:
                    values[field.name] = raw
            except ValueError:
                raise SystemExit(f"WARMER_{field.name.upper()} must be a {field.type}, not {raw!r}") from None
        config = cls(**values)
        for name in ("rate", "long_tail_rate", "new_rate", "signal_rate"):
            if getattr(config, name) <= 0:
                raise SystemExit(f"WARMER_{name.upper()} must be above zero")
        return config


class Form(NamedTuple):
    """One way clients ask for an image. nginx keeps a separate copy of each distinct form."""

    code: int
    image: str
    step: int
    query: str

    @property
    def tagged(self):
        return "{tag}" in self.query

    def path(self, item, tag):
        tail = self.query.format(tag=tag_text(tag)) if tag else self.query
        return f"/Items/{item.hex()}/Images/{tail}"


# The codes are stored on disk, so don't renumber them. The queries are copied from real client requests.
# nginx's width steps are 342 and 500, and anything wider is kept at its original size.
POSTER_342 = Form(1, "Primary", 342, "Primary?fillHeight=513&fillWidth=342&quality=96&tag={tag}")
POSTER_500 = Form(2, "Primary", 500, "Primary?fillHeight=750&fillWidth=500&quality=96&tag={tag}")
POSTER_FULL = Form(3, "Primary", 0, "Primary?maxHeight=900&tag={tag}&quality=90")
THUMB_500 = Form(4, "Thumb", 500, "Thumb?fillHeight=281&fillWidth=500&quality=96&tag={tag}")
THUMB_FULL = Form(5, "Thumb", 0, "Thumb?fillHeight=386&fillWidth=686&quality=96&tag={tag}")
# Never requested, because nginx keeps it and BACKDROP_0 as one entry. It stays here so its code stays taken.
BACKDROP = Form(6, "Backdrop", 0, "Backdrop?maxWidth=1920&tag={tag}&quality=80")
BACKDROP_0 = Form(7, "Backdrop/0", 0, "Backdrop/0?tag={tag}&maxWidth=1920&quality=80")
LOGO_TAGGED = Form(8, "Logo", 0, "Logo?maxWidth=700&tag={tag}&quality=90")
LOGO_PLAIN = Form(9, "Logo", 0, "Logo?maxWidth=600&quality=90")
THUMB_342 = Form(10, "Thumb", 342, "Thumb?fillHeight=179&fillWidth=318&quality=96&tag={tag}")
FORMS = {
    form.code: form
    for form in (POSTER_342, POSTER_500, POSTER_FULL, THUMB_500, THUMB_FULL, BACKDROP, BACKDROP_0, LOGO_TAGGED, LOGO_PLAIN, THUMB_342)
}

# Which forms to warm for each kind of item. Chosen from what clients really ask for. Only real rows are
# requested: asking for an image a row doesn't exist for makes Remux generate one or answer 404.
TITLE_FORMS = {
    "primary": (POSTER_342, POSTER_500, POSTER_FULL),
    "logo": (LOGO_TAGGED, LOGO_PLAIN),
    "backdrop": (BACKDROP_0,),
    "thumb": (THUMB_342, THUMB_500, THUMB_FULL),
}
KIND_FORMS = {
    "movie": TITLE_FORMS,
    "series": TITLE_FORMS,
    "season": {"primary": (POSTER_342, POSTER_500)},
    "person": {"primary": (POSTER_342, POSTER_500)},
    "episode": {"primary": (POSTER_500,)},
    "collection": {"primary": (POSTER_500, POSTER_FULL)},
    "tv_channel": {"primary": (POSTER_342, POSTER_FULL)},
    # Not the full size, because these are large and arrive every day: see the README.
    "tv_program": {"primary": (POSTER_342, POSTER_500)},
}
TITLE_KINDS = ("movie", "series")

# Rough sizes on disk, for the --plan estimate only.
EST_BYTES = {
    POSTER_342: 36_000,
    POSTER_500: 66_000,
    POSTER_FULL: 190_000,
    THUMB_342: 14_000,
    THUMB_500: 27_000,
    THUMB_FULL: 75_000,
    BACKDROP_0: 180_000,
    LOGO_TAGGED: 80_000,
    LOGO_PLAIN: 80_000,
}
EST_TIER_BYTES = {
    (TIER_PEOPLE, POSTER_342): 26_000,
    (TIER_PEOPLE, POSTER_500): 47_000,
    (TIER_SEASONS, POSTER_342): 33_000,
    (TIER_SEASONS, POSTER_500): 60_000,
    (TIER_EPISODES, POSTER_500): 25_000,
    (TIER_FULL_STILLS, POSTER_FULL): 57_000,
    (TIER_CHANNELS, POSTER_342): 7_000,
    (TIER_CHANNELS, POSTER_FULL): 28_000,
    (TIER_GUIDE, POSTER_342): 18_000,
    (TIER_GUIDE, POSTER_500): 32_000,
}


def tag_text(tag):
    return str(uuid.UUID(bytes=tag)) if tag else ""


class Job(NamedTuple):
    item: bytes
    form: Form
    tag: bytes
    tier: int = TIER_NEW
    rank: int = 0
    seen_at: float = 0.0
    # True when the database doesn't have the item, so a 404 says nothing about the source.
    guess: bool = False

    @property
    def key(self):
        return (self.item, self.form.code, self.tag)

    def path(self):
        return self.form.path(self.item, self.tag)


class Result(NamedTuple):
    status: int
    cache: str = ""
    content_type: str = ""
    size: int = 0
    elapsed: float = 0.0
    retry_after: float = 0.0


def classify(result):
    """Says what an answer from the cache means, without any side effects."""
    if result.status == 0:
        return BACKOFF
    if result.status in (200, 304):
        if result.cache.upper() != "DIRECT":
            return CACHED
        # nginx couldn't store it. An SVG never can be, anything else may mean the fetch side is too busy.
        return UNCACHEABLE if "svg" in result.content_type.lower() else SOFT
    if result.status in (404, 410):
        return MISSING
    if result.status in (400, 401, 403):
        return REFUSED
    if result.status == 429 or result.status >= 500:
        return BACKOFF
    return RETRY


def retry_after_seconds(value):
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return 0.0


class Fetcher:
    """One keep-alive connection to the cache. The body is read and thrown away."""

    HEADERS = {"X-Image-Warmer": "1", "User-Agent": f"remux-warmer/{VERSION}", "Accept": "image/*"}

    def __init__(self, base_url, timeout):
        parts = urllib.parse.urlsplit(base_url)
        self.host = parts.hostname
        self.port = parts.port or 80
        self.timeout = timeout
        self.conn = None

    def close(self):
        if self.conn is not None:
            with contextlib.suppress(OSError):
                self.conn.close()
            self.conn = None

    def _once(self, path, start):
        if self.conn is None:
            self.conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        self.conn.request("GET", path, headers=self.HEADERS)
        response = self.conn.getresponse()
        size = 0
        while True:
            chunk = response.read(65536)
            if not chunk:
                break
            size += len(chunk)
        if response.will_close:
            self.close()
        return Result(
            response.status,
            response.getheader("X-Img-Cache", ""),
            response.getheader("Content-Type", ""),
            size,
            time.monotonic() - start,
            retry_after_seconds(response.getheader("Retry-After")),
        )

    def get(self, path):
        start = time.monotonic()
        reused = self.conn is not None
        try:
            return self._once(path, start)
        except (OSError, http.client.HTTPException):
            self.close()
        # The server may simply have closed an idle connection, so a reused one gets one more try.
        if reused:
            try:
                return self._once(path, start)
            except (OSError, http.client.HTTPException):
                self.close()
        return Result(0, elapsed=time.monotonic() - start)


class Bucket:
    """A steady rate with a small burst. The owner holds the lock."""

    def __init__(self, rate, burst=1.0):
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.at = time.monotonic()

    def wait(self, now, speed=1.0):
        rate = self.rate * speed
        self.tokens = min(self.burst, self.tokens + max(0.0, now - self.at) * rate)
        self.at = now
        return 0.0 if self.tokens >= 1.0 else (1.0 - self.tokens) / rate

    def take(self):
        self.tokens -= 1.0

    def refund(self, amount):
        self.tokens = min(self.burst, self.tokens + amount)


class Pace:
    """Slows everything down when the chain struggles, and recovers slowly. The owner holds the lock."""

    def __init__(self, config):
        self.config = config
        self.speed = 1.0
        self.pause_until = 0.0
        self.failures = 0
        self.quick_hits = 0
        self.recent = collections.OrderedDict()

    @property
    def source_suspect(self):
        return sum(self.recent.values()) >= BREAKER_TRIPS

    def _note(self, job, failed):
        key = (job.item, job.form.image)
        self.recent[key] = failed
        self.recent.move_to_end(key)
        while len(self.recent) > BREAKER_WINDOW:
            self.recent.popitem(last=False)

    def judge(self, job, outcome, result):
        """Second-guesses an answer that may really mean the source is failing.

        Remux answers 404 for any failure fetching from its source, a 429 included, so a failing source looks
        like a lot of missing images. One row that exists answering 404 is believable. Many at once aren't, and
        neither is a sudden run of refusals, as from an update that starts asking for a sign-in.
        """
        if job.guess:
            return outcome
        if job.tier in THIRD_PARTY:
            # nginx couldn't store it, and the likeliest reason is what that host sent.
            return UNCACHEABLE if outcome == SOFT else outcome
        if outcome in (MISSING, REFUSED):
            self._note(job, True)
            return SUSPECT if self.source_suspect else outcome
        # A hit never reached the source, so it says nothing about it.
        if outcome in (CACHED, SOFT, UNCACHEABLE) and result.cache.upper() != "HIT":
            self._note(job, False)
        return outcome

    def record(self, outcome, result, now, third_party=False):
        if outcome in (BACKOFF, SUSPECT):
            self.failures += 1
            self.quick_hits = 0
            delay = self.config.backoff_seconds * 2 ** min(self.failures - 1, 16)
            delay = min(self.config.backoff_max_seconds, delay)
            if result.retry_after:
                delay = max(delay, min(result.retry_after, HOUR))
            self.pause_until = max(self.pause_until, now + delay)
            self.speed = max(MIN_SPEED, self.speed / 2)
            return
        if outcome == INVALID:
            return
        hit = result.cache.upper() == "HIT"
        good = outcome in (CACHED, SOFT, UNCACHEABLE)
        # A programme image arriving says nothing about a catalogue source that's in doubt.
        if good and not hit and not (third_party and self.source_suspect):
            self.failures = 0
        limit = self.config.slow_hit_ms if hit else self.config.slow_miss_ms
        if not third_party and (outcome == SOFT or result.elapsed * 1000 > limit):
            self.speed = max(MIN_SPEED, self.speed / 2)
            self.quick_hits = 0
        elif not hit:
            self.speed = min(1.0, self.speed + 0.02)
        elif good:
            # A hit doesn't reach the source, so one means little, but a run of them ends a lone failure.
            self.speed = min(1.0, self.speed + HIT_RECOVERY)
            self.quick_hits += 1
            if self.quick_hits >= CLEAN_FETCHES and not self.source_suspect:
                self.failures = 0


class BurstQueue:
    """Signal work: the newest burst first, and within a burst the order the viewer's requests arrived in.

    Top results load first, so they're the first ones warmed, and whatever the viewer turned to last comes
    before what they've moved on from.
    """

    def __init__(self, capacity=4000):
        self.capacity = capacity
        self.bursts = collections.deque()
        self.newest = None
        self.last_burst = None
        self.size = 0

    def __len__(self):
        return self.size

    def add(self, jobs, burst):
        if not jobs:
            return
        # Compared with this queue's own last burst, as a burst's first batch may have added nothing to it. If
        # that batch was fetched already, its place has gone, and what's left at the front belongs to an older burst.
        if burst != self.last_burst or not self.bursts or self.bursts[0] is not self.newest:
            self.newest = collections.deque()
            self.bursts.appendleft(self.newest)
            self.last_burst = burst
        self.newest.extend(jobs)
        self.size += len(jobs)
        while self.size > self.capacity:
            oldest = self.bursts[-1]
            if oldest:
                oldest.pop()
                self.size -= 1
            else:
                self.bursts.pop()

    def pop(self):
        while self.bursts:
            head = self.bursts[0]
            if head:
                self.size -= 1
                return head.popleft()
            self.bursts.popleft()
        return None


class Recent:
    """A small memory of what was already handled, so a page of posters doesn't queue the same work twice."""

    def __init__(self, capacity, ttl):
        self.capacity = capacity
        self.ttl = ttl
        self.seen = collections.OrderedDict()

    def first_time(self, key, now):
        at = self.seen.get(key)
        if at is not None and now - at < self.ttl:
            return False
        self.seen[key] = now
        self.seen.move_to_end(key)
        while len(self.seen) > self.capacity:
            self.seen.popitem(last=False)
        return True

    def forget(self, key):
        self.seen.pop(key, None)


class SchemaError(Exception):
    pass


class Catalogue:
    """Read-only access to Remux's database.

    Every read opens its own connection, runs one short query and closes it. A long read would stop the WAL
    being checkpointed and let it grow, and short reads keep it small. The folder must be mounted whole,
    because SQLite replaces the -wal and -shm files when Remux restarts, and a bind of those single files
    would go on showing the old ones without any error.

    The columns allowed are listed here and enforced by SQLite, so nothing else can be read by mistake.
    """

    ALLOWED = {
        "media": {
            "id", "kind", "parent_id", "grandparent_id", "idx", "parent_idx", "enabled", "promoted", "created_at",
            "digital_released_at", "rating_audience", "original_language", "sort_order", "title", "live_start", "live_end",
            "collection_media_kind", "collection_smart_filter", "collection_default_sort",
        },
        "media_images": {"id", "media_id", "image_type", "image_index"},
        "media_metrics": {
            "media_id", "popularity_all_time", "popularity_daily", "popularity_weekly", "popularity_monthly",
            "trending_weekly", "trending_monthly",
        },
        "media_tags": {"media_id", "tag"},
        "media_relations": {"left_media_id", "right_media_id", "role", "weight"},
        "user_media_state": {"media_id", "last_played_at"},
    }
    REQUIRED = {
        "media": {"id", "kind", "parent_id", "grandparent_id", "idx", "parent_idx", "enabled"},
        "media_images": {"id", "media_id", "image_type", "image_index"},
    }

    def __init__(self, path, page_rows=5000, page_pause=0.01, busy_ms=2000):
        self.uri = "file:" + urllib.parse.quote(path) + "?mode=ro"
        self.page_rows = page_rows
        self.page_pause = page_pause
        self.busy_ms = busy_ms
        self.columns = {}

    @staticmethod
    def _authorize(action, arg1, arg2, _db, _trigger):
        if action == sqlite3.SQLITE_SELECT or action == sqlite3.SQLITE_FUNCTION:
            return sqlite3.SQLITE_OK
        if action == sqlite3.SQLITE_PRAGMA and arg1 in ("query_only", "table_info"):
            return sqlite3.SQLITE_OK
        if action == sqlite3.SQLITE_READ:
            allowed = Catalogue.ALLOWED.get(arg1)
            if allowed is not None and (arg2 in allowed or arg2 == "" or arg2.lower() == "rowid"):
                return sqlite3.SQLITE_OK
        return sqlite3.SQLITE_DENY

    @contextlib.contextmanager
    def connect(self):
        con = sqlite3.connect(self.uri, uri=True, timeout=self.busy_ms / 1000, isolation_level=None)
        try:
            con.execute("PRAGMA query_only=ON")
            con.set_authorizer(self._authorize)
            yield con
        finally:
            con.close()

    def check_schema(self):
        columns = {}
        with self.connect() as con:
            for table in self.ALLOWED:
                found = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
                columns[table] = found & self.ALLOWED[table]
        for table, needed in self.REQUIRED.items():
            missing = needed - columns.get(table, set())
            if missing:
                raise SchemaError(f"{table} has no column {sorted(missing)[0]}")
        self.columns = columns

    def has(self, table, *names):
        return set(names) <= self.columns.get(table, set())

    def _pages(self, sql, params=()):
        """Yields the rows of a query whose first column is a rowid and whose last parameters are that rowid and a
        limit. Each page is one short read, and only one page is in memory, however large the catalogue."""
        after = -1
        while True:
            with self.connect() as con:
                page = con.execute(sql, (*params, after, self.page_rows)).fetchall()
            yield from page
            if len(page) < self.page_rows:
                return
            after = page[-1][0]
            time.sleep(self.page_pause)

    def _image_sql(self, kinds):
        marks = ",".join("?" * len(kinds))
        return (
            "SELECT i.rowid, i.id, i.media_id, i.image_type, m.kind, m.parent_id, m.grandparent_id, m.idx, m.parent_idx "
            # CROSS JOIN makes SQLite walk images in rowid order, so a page reads a page and not the whole table.
            "FROM media_images i CROSS JOIN media m ON m.id = i.media_id "
            f"WHERE m.kind IN ({marks}) AND i.image_index = 0 AND COALESCE(m.enabled, 1) = 1 AND i.rowid > ? "
            "ORDER BY i.rowid LIMIT ?"
        )

    def all_images(self, kinds):
        return self._pages(self._image_sql(kinds), tuple(kinds))

    def images_of(self, ids):
        """The same rows as all_images, for particular items."""
        for chunk in _chunks(ids, 200):
            marks = ",".join("?" * len(chunk))
            with self.connect() as con:
                rows = con.execute(
                    "SELECT i.rowid, i.id, i.media_id, i.image_type, m.kind, m.parent_id, m.grandparent_id, m.idx, m.parent_idx "
                    "FROM media m CROSS JOIN media_images i ON i.media_id = m.id "
                    f"WHERE m.id IN ({marks}) AND i.image_index = 0 AND COALESCE(m.enabled, 1) = 1",
                    chunk,
                ).fetchall()
            yield from rows

    def max_rowid(self):
        with self.connect() as con:
            return con.execute("SELECT max(rowid) FROM media_images").fetchone()[0] or 0

    def images_after(self, kinds, after, limit=5000):
        with self.connect() as con:
            return con.execute(self._image_sql(kinds), (*kinds, after, limit)).fetchall()

    def guide_rows(self, now, after, until):
        """Image rows of programmes that haven't ended and start after one time and by another, with the start in seconds."""
        if not self.has("media", "live_start", "live_end"):
            return []
        with self.connect() as con:
            return con.execute(
                "SELECT i.rowid, i.id, i.media_id, i.image_type, CAST(strftime('%s', m.live_start) AS INTEGER) "
                "FROM media m CROSS JOIN media_images i ON i.media_id = m.id "
                "WHERE m.kind = 'tv_program' AND i.image_index = 0 AND COALESCE(m.enabled, 1) = 1 "
                "AND m.live_end > ? AND m.live_start > ? AND m.live_start <= ?",
                (timestamp(now), timestamp(after), timestamp(until)),
            ).fetchall()

    def titles(self):
        extra = [
            f"m.{name}" if self.has("media", name) else "NULL"
            for name in ("created_at", "digital_released_at", "rating_audience", "original_language")
        ]
        metrics = ["popularity_daily", "popularity_weekly", "popularity_monthly", "popularity_all_time",
                   "trending_weekly", "trending_monthly"]
        have_metrics = self.has("media_metrics", "media_id", *metrics)
        columns = extra + [f"x.{name}" for name in metrics] if have_metrics else extra + ["NULL"] * len(metrics)
        join = "LEFT JOIN media_metrics x ON x.media_id = m.id " if have_metrics else ""
        rows = self._pages(
            f"SELECT m.rowid, m.id, m.kind, {', '.join(columns)} FROM media m {join}"
            "WHERE m.kind IN ('movie', 'series') AND COALESCE(m.enabled, 1) = 1 AND m.rowid > ? "
            "ORDER BY m.rowid LIMIT ?"
        )
        titles = {}
        for row in rows:
            item = _blob(row[1])
            if item is None:
                continue
            titles[item] = Title(
                id=item, kind=row[2], created=str(row[3] or ""), digital=str(row[4] or ""), rating=_number(row[5]),
                language=str(row[6] or "").lower(), daily=_number(row[7]), weekly=_number(row[8]), monthly=_number(row[9]),
                all_time=_number(row[10]), trending_week=_number(row[11]), trending_month=_number(row[12]),
            )
        return titles

    def add_title_labels(self, titles):
        if self.has("media_tags", "media_id", "tag"):
            for _, item, tag in self._pages("SELECT rowid, media_id, tag FROM media_tags WHERE rowid > ? ORDER BY rowid LIMIT ?"):
                title = titles.get(_blob(item))
                if title and tag:
                    title.tags.add(str(tag).lower())
        if self.has("media_relations", "left_media_id", "right_media_id") and self.has("media", "title"):
            for _, item, kind, name in self._pages(
                "SELECT r.rowid, r.left_media_id, g.kind, g.title FROM media_relations r "
                "CROSS JOIN media g ON g.id = r.right_media_id "
                "WHERE g.kind IN ('genre', 'country') AND r.rowid > ? ORDER BY r.rowid LIMIT ?"
            ):
                title = titles.get(_blob(item))
                if title and name:
                    (title.genres if kind == "genre" else title.countries).add(str(name).lower())

    def home_rows(self):
        if not self.has("media", "promoted", "collection_media_kind", "collection_smart_filter", "collection_default_sort", "sort_order"):
            return []
        with self.connect() as con:
            rows = con.execute(
                "SELECT collection_media_kind, collection_smart_filter, collection_default_sort FROM media "
                "WHERE kind = 'collection' AND promoted = 1 AND COALESCE(enabled, 1) = 1 "
                "AND collection_media_kind IN ('movie', 'series') ORDER BY sort_order"
            ).fetchall()
        return [HomeRow(kind, _json_or_none(rules), _first(_json_or_none(sort))) for kind, rules, sort in rows]

    def recently_played(self, limit):
        """Items anyone played lately, newest first. Who played them is never read."""
        if not self.has("user_media_state", "media_id", "last_played_at"):
            return []
        with self.connect() as con:
            rows = con.execute(
                "SELECT s.media_id, m.kind, m.parent_id, m.grandparent_id, m.idx FROM user_media_state s "
                "JOIN media m ON m.id = s.media_id WHERE s.last_played_at IS NOT NULL "
                "ORDER BY s.last_played_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [
            (_blob(item), kind, _blob(parent), _blob(grandparent), idx)
            for item, kind, parent, grandparent, idx in rows
            if _blob(item) is not None
        ]

    def later_episodes(self, season, after, limit):
        """The episodes that come after this one in its season, for Next Up."""
        with self.connect() as con:
            rows = con.execute(
                "SELECT id FROM media WHERE parent_id = ? AND kind = 'episode' AND idx > ? ORDER BY idx LIMIT ?",
                (season, after, limit),
            ).fetchall()
        return [row[0] for row in rows if _blob(row[0]) is not None]

    def credits(self):
        if not self.has("media_relations", "left_media_id", "right_media_id", "role"):
            return []
        return self._pages(
            "SELECT rowid, left_media_id, right_media_id FROM media_relations "
            "WHERE role IN ('actor', 'director', 'creator', 'writer', 'producer') AND rowid > ? ORDER BY rowid LIMIT ?"
        )

    def items(self, ids):
        """Kind and parents for each id, read from the primary key."""
        found = {}
        for chunk in _chunks(ids, 200):
            marks = ",".join("?" * len(chunk))
            with self.connect() as con:
                for item, kind, parent, grandparent in con.execute(
                    f"SELECT id, kind, parent_id, grandparent_id FROM media WHERE id IN ({marks})", chunk
                ):
                    if _blob(item) is not None:
                        found[item] = (kind, _blob(parent), _blob(grandparent))
        return found

    def art(self, ids):
        """Each item's art as {image_type: tag}."""
        found = collections.defaultdict(dict)
        for chunk in _chunks(ids, 200):
            marks = ",".join("?" * len(chunk))
            with self.connect() as con:
                for item, tag, image_type in con.execute(
                    f"SELECT media_id, id, image_type FROM media_images WHERE media_id IN ({marks}) AND image_index = 0",
                    chunk,
                ):
                    if _blob(item) is not None and _blob(tag) is not None:
                        found[item][image_type] = tag
        return found

    def children(self, parent, kind, limit):
        with self.connect() as con:
            rows = con.execute(
                "SELECT id FROM media WHERE parent_id = ? AND kind = ? ORDER BY idx LIMIT ?", (parent, kind, limit)
            ).fetchall()
        return [row[0] for row in rows if _blob(row[0]) is not None]

    def cast(self, item, limit):
        if not self.has("media_relations", "left_media_id", "right_media_id", "role", "weight"):
            return []
        with self.connect() as con:
            rows = con.execute(
                "SELECT right_media_id FROM media_relations WHERE left_media_id = ? "
                "AND role IN ('actor', 'director', 'creator') ORDER BY weight LIMIT ?",
                (item, limit),
            ).fetchall()
        return [row[0] for row in rows if _blob(row[0]) is not None]


def _chunks(items, size):
    items = list(items)
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _blob(value):
    """An id as stored by Remux, or None for anything else, so one odd row can't stop the rest."""
    return value if isinstance(value, bytes) and len(value) == 16 else None


def _number(value):
    return value if isinstance(value, (int, float)) else None


def _json_or_none(raw):
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def _first(value):
    return value[0] if isinstance(value, list) and value else None


@dataclass
class Title:
    id: bytes
    kind: str
    created: str = ""
    digital: str = ""
    rating: float | None = None
    language: str = ""
    daily: float | None = None
    weekly: float | None = None
    monthly: float | None = None
    all_time: float | None = None
    trending_week: float | None = None
    trending_month: float | None = None

    def __post_init__(self):
        self.tags = set()
        self.genres = set()
        self.countries = set()

    @property
    def score(self):
        return self.daily or self.weekly or self.monthly or self.all_time or 0.0


class HomeRow(NamedTuple):
    kind: str
    rules: dict | None
    sort: str | None


SORT_FIELDS = {
    "PopularityDay": "daily",
    "PopularityWeek": "weekly",
    "PopularityMonth": "monthly",
    "PopularityAllTime": "all_time",
    "TrendingWeek": "trending_week",
    "TrendingMonth": "trending_month",
    "DigitalReleaseDate": "digital",
    "DateCreated": "created",
}
TRENDING_SORTS = ("TrendingWeek", "TrendingMonth")


def _rule_matches(rule, title):
    """True or False, or None for a rule this worker doesn't know, which then doesn't narrow the row."""
    field = rule.get("field")
    op = rule.get("op")
    values = {str(value).lower() for value in rule.get("values") or []}
    have = {"tag": title.tags, "genre": title.genres, "country": title.countries}.get(field)
    if field == "original_language":
        have = {title.language}
    if have is not None:
        if op in ("in", "is"):
            return bool(values & have)
        if op in ("not_in", "is_not"):
            return not values & have
        return None
    if field == "rating_audience":
        number = rule.get("value")
        if number is None or title.rating is None:
            return False
        return {"gt": title.rating > number, "lt": title.rating < number, "eq": title.rating == number,
                "not_eq": title.rating != number}.get(op)
    return None


def _group_matches(results, mode):
    known = [result for result in results if result is not None]
    if not known:
        return True
    return any(known) if mode == "any" else all(known)


def row_matches(rules, title):
    if not isinstance(rules, dict):
        return True
    groups = [
        _group_matches(
            [_rule_matches(rule, title) for rule in group.get("rules", []) if isinstance(rule, dict)],
            group.get("match_mode"),
        )
        for group in rules.get("groups", [])
        if isinstance(group, dict)
    ]
    return _group_matches(groups, rules.get("match_mode"))


def row_items(row, titles, limit):
    """The first items of a home row, the way Remux orders its smart collections."""
    of_kind = [title for title in titles.values() if title.kind == row.kind]
    members = [title for title in of_kind if row_matches(row.rules, title)] or of_kind
    field = SORT_FIELDS.get(row.sort if isinstance(row.sort, str) else None, "created")
    members.sort(key=lambda title: (getattr(title, field) or (0.0 if field not in ("digital", "created") else "")), reverse=True)
    return [title.id for title in members[:limit]]


class Snapshot(NamedTuple):
    images: object  # one pass over image rows, read page by page
    titles: dict
    rows: list
    recent: list
    credits: object  # one pass over credits
    next_up: dict  # episode to the episodes after it
    series_art: dict  # art of the series of recently played episodes
    guide: object = ()  # programmes in the window
    guide_until: int = 0  # where the window ended when it was read


def read_snapshot(catalogue, config):
    titles = catalogue.titles()
    catalogue.add_title_labels(titles)
    recent = catalogue.recently_played(RECENT_LIMIT)
    next_up = {}
    series = set()
    for item, kind, parent, grandparent, idx in recent:
        if kind == "episode":
            next_up[item] = catalogue.later_episodes(parent, _number(idx) or 0, 2) if parent else []
            if grandparent:
                series.add(grandparent)
    images = catalogue.all_images(plan_kinds(config))
    if not config.episodes:
        # Episodes on the home screen need their stills whatever the setting.
        wanted = {item for item, kind, *_ in recent if kind == "episode"}
        wanted.update(itertools.chain.from_iterable(next_up.values()))
        images = itertools.chain(images, catalogue.images_of(wanted))
    credits = catalogue.credits() if config.people else ()
    guide = ()
    guide_until = 0
    if config.guide:
        now = int(time.time())
        guide_until = now + GUIDE_WINDOW
        guide = catalogue.guide_rows(now, 0, guide_until)
    return Snapshot(images, titles, catalogue.home_rows(), recent, credits, next_up, catalogue.art(series), guide, guide_until)


def plan_kinds(config):
    kinds = ["movie", "series", "collection", "season"]
    if config.episodes:
        kinds.append("episode")
    if config.people:
        kinds.append("person")
    if config.channels:
        kinds.append("tv_channel")
    return kinds


def plan_settings(config):
    """The settings a plan depends on. When they change the old plan is no use, so a new one is made at once."""
    return json.dumps(
        {
            "rules": PLAN_RULES, "people": config.people, "episodes": config.episodes, "channels": config.channels,
            "guide": config.guide, "home_row_items": config.home_row_items,
        },
        sort_keys=True,
    )


def tier_of(kind, image_type):
    if kind in TITLE_KINDS:
        return TIER_TITLES if image_type in ("primary", "logo") else TIER_TITLE_ART
    return {
        "season": TIER_SEASONS,
        "person": TIER_PEOPLE,
        "episode": TIER_EPISODES,
        "collection": TIER_HOME,
        "tv_channel": TIER_CHANNELS,
        "tv_program": TIER_GUIDE,
    }.get(kind, TIER_EPISODES)


def tier_enabled(tier, config):
    return {
        TIER_PEOPLE: config.people,
        TIER_EPISODES: config.episodes,
        TIER_FULL_STILLS: config.episodes,
        TIER_CHANNELS: config.channels,
        TIER_GUIDE: config.guide,
    }.get(tier, True)


def _home_ranks(snapshot, config, counts):
    home = {}

    def on_home(item, rank):
        home[item] = min(home.get(item, rank), rank)

    for row in snapshot.rows:
        trending = row.sort in TRENDING_SORTS if isinstance(row.sort, str) else False
        try:
            items = row_items(row, snapshot.titles, config.home_row_items)
        except (TypeError, ValueError, AttributeError, KeyError):
            counts["skipped_rows"] += 1
            continue
        for position, item in enumerate(items):
            # Hero slides come from the trending rows, so those go first.
            on_home(item, position - 1000 if trending else position)

    # Continue Watching and Next Up. Episode cards use their series' art.
    for position, (item, kind, _parent, grandparent, _idx) in enumerate(snapshot.recent):
        rank = position - 2000
        if kind in TITLE_KINDS:
            on_home(item, rank)
        elif kind == "episode":
            on_home(item, rank)
            if grandparent:
                on_home(grandparent, rank)
            for next_episode in snapshot.next_up.get(item, []):
                on_home(next_episode, rank)
    return home


def _episode_rank(series_rank, parent_idx, idx):
    return series_rank * 1_000_000 + min(parent_idx or 0, 99) * 1000 + min(idx or 0, 999)


def _row_jobs(row, home, popularity, person_rank, config):
    """The jobs for one image row. An error here means the row isn't what this worker expects, and costs only that row."""
    _, tag, item, image_type, kind, parent, grandparent, idx, parent_idx = row
    forms = KIND_FORMS.get(kind, {}).get(image_type)
    if not forms:
        return []
    item = _blob(item)
    tag = _blob(tag)
    if item is None or tag is None:
        raise ValueError("not a 16-byte id")
    unranked = len(popularity)
    # The full size is the least useful of all, so it's left to the last stage, wherever the episode's card is.
    last = []
    if kind == "episode" and config.episodes:
        rank = _episode_rank(popularity.get(_blob(grandparent), unranked), parent_idx, idx)
        last = [Job(item, POSTER_FULL, tag, TIER_FULL_STILLS, rank)]
    if item in home:
        tier, rank = TIER_HOME, home[item]
    else:
        tier = tier_of(kind, image_type)
        if not tier_enabled(tier, config):
            return []
        if kind in TITLE_KINDS:
            rank = popularity.get(item, unranked)
        elif kind == "season":
            rank = popularity.get(_blob(parent), unranked) * 100 + min(idx or 0, 99)
        elif kind == "episode":
            rank = _episode_rank(popularity.get(_blob(grandparent), unranked), parent_idx, idx)
        elif kind == "person":
            rank = person_rank.get(item, unranked)
        else:
            rank = 5000 + (idx or 0)
    return [Job(item, form, b"" if not form.tagged else tag, tier, rank) for form in forms] + last


def guide_jobs(rows, counts):
    """Jobs for programme image rows, ranked by when they start, so that what airs soonest goes first."""
    for _, tag, item, image_type, starts in rows:
        forms = KIND_FORMS["tv_program"].get(image_type)
        if not forms:
            continue
        item = _blob(item)
        tag = _blob(tag)
        if item is None or tag is None or not isinstance(starts, int):
            counts["skipped_rows"] += 1
            continue
        counts["image_rows"] += 1
        for form in forms:
            yield Job(item, form, tag, TIER_GUIDE, starts)


def plan_jobs(snapshot, config, counts=None):
    """Yields a snapshot's jobs, each with a tier and a rank, holding one image row at a time, so that its memory
    use doesn't depend on the size of the catalogue. Pure, so it can be tested on its own. Rows that aren't what
    it expects are left out and counted."""
    counts = collections.Counter() if counts is None else counts
    by_popularity = sorted(snapshot.titles.values(), key=lambda title: (title.score, title.created), reverse=True)
    popularity = {title.id: rank for rank, title in enumerate(by_popularity)}
    home = _home_ranks(snapshot, config, counts)

    person_rank = {}
    for _, title, person in snapshot.credits:
        rank = popularity.get(_blob(title))
        person = _blob(person)
        if rank is not None and person is not None:
            person_rank[person] = min(person_rank.get(person, rank), rank)

    for row in snapshot.images:
        try:
            jobs = _row_jobs(row, home, popularity, person_rank, config)
        except (TypeError, ValueError, AttributeError, KeyError):
            counts["skipped_rows"] += 1
            continue
        counts["image_rows"] += 1
        yield from jobs

    # An episode's card asks its series for a thumb, using the backdrop's tag when the series has no thumb.
    # Remux makes that one up from the backdrop and logo, so it's the slowest first view of all.
    for series, art in snapshot.series_art.items():
        if "thumb" not in art and "backdrop" in art:
            for form in (THUMB_342, THUMB_500, THUMB_FULL):
                yield Job(series, form, art["backdrop"], TIER_HOME, home.get(series, 0))

    yield from guide_jobs(snapshot.guide, counts)


def build_plan(snapshot, config):
    return list(plan_jobs(snapshot, config))


def new_row_jobs(rows):
    """Jobs for rows that appeared since the last look. Titles and seasons go first, others to their tier's front."""
    jobs = []
    for rowid, tag, item, image_type, kind, _parent, _grandparent, _idx, _parent_idx in rows:
        forms = KIND_FORMS.get(kind, {}).get(image_type)
        if not forms or _blob(item) is None or _blob(tag) is None:
            continue
        fast = kind in TITLE_KINDS or kind == "season"
        tier = TIER_NEW if fast else tier_of(kind, image_type)
        rank = rowid if fast else -1
        for form in forms:
            jobs.append(Job(item, form, b"" if not form.tagged else tag, tier, rank))
    return jobs


def _art_forms(item, art, forms, seen_at):
    return [
        Job(item, form, b"" if not form.tagged else art[image_type], TIER_NEW, 0, seen_at)
        for form, image_type in forms
        if art.get(image_type)
    ]


def art_jobs(item, art, seen_at):
    """What opening an item would ask for first, then the rest of its art. A page of posters gets every
    item's first part before any item's second part, so whichever one the viewer opens is ready."""
    # The tagged logo first: clients ask for it several times as often as the plain one.
    first = _art_forms(item, art, ((LOGO_TAGGED, "logo"), (BACKDROP_0, "backdrop"), (LOGO_PLAIN, "logo")), seen_at)
    rest = _art_forms(
        item, art, ((THUMB_500, "thumb"), (THUMB_FULL, "thumb"), (POSTER_FULL, "primary"), (THUMB_342, "thumb")), seen_at
    )
    return first, rest


def series_jobs(series, art, seen_at):
    """Art for a series reached through one of its seasons or episodes, whose cards use the series' art."""
    first, rest = art_jobs(series, art, seen_at)
    if art.get("primary"):
        rest.append(Job(series, POSTER_500, art["primary"], TIER_NEW, 0, seen_at))
    if "thumb" not in art and art.get("backdrop"):
        for form in (THUMB_500, THUMB_FULL, THUMB_342):
            rest.append(Job(series, form, art["backdrop"], TIER_NEW, 0, seen_at))
    return first, rest


class State:
    """What's been done, in the worker's own small SQLite file, so a restart resumes where it stopped."""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS jobs (
        item BLOB NOT NULL,
        form INTEGER NOT NULL,
        tag BLOB NOT NULL,
        tier INTEGER NOT NULL,
        rank INTEGER NOT NULL,
        pending INTEGER NOT NULL DEFAULT 1,
        next_at INTEGER NOT NULL DEFAULT 0,
        status INTEGER NOT NULL DEFAULT 0,
        tries INTEGER NOT NULL DEFAULT 0,
        strikes INTEGER NOT NULL DEFAULT 0,
        bytes INTEGER NOT NULL DEFAULT 0,
        fetched_at INTEGER NOT NULL DEFAULT 0,
        gen INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (item, form, tag)
    ) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS jobs_todo ON jobs(tier, rank) WHERE pending = 1;
    CREATE INDEX IF NOT EXISTS jobs_due ON jobs(next_at) WHERE pending = 0;
    CREATE INDEX IF NOT EXISTS jobs_upkeep ON jobs(tier, rank) WHERE pending = 1 AND status = 1;
    CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value) WITHOUT ROWID;
    """

    def __init__(self, path):
        # The worker's main thread does all the writing, but it needn't be the thread that opened the file.
        self.con = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute("PRAGMA synchronous=NORMAL")
        self.con.executescript(self.SCHEMA)

    def close(self):
        self.con.close()

    @contextlib.contextmanager
    def _transaction(self):
        self.con.execute("BEGIN")
        try:
            yield
            self.con.execute("COMMIT")
        except BaseException:
            # A full disk fails partway through, and the next attempt needs a clean start.
            with contextlib.suppress(sqlite3.Error):
                self.con.execute("ROLLBACK")
            raise

    def get(self, name, default=None):
        row = self.con.execute("SELECT value FROM meta WHERE name = ?", (name,)).fetchone()
        return row[0] if row else default

    def put(self, name, value):
        self.con.execute("INSERT OR REPLACE INTO meta (name, value) VALUES (?, ?)", (name, value))

    def next_generation(self):
        return int(self.get("generation", 0)) + 1

    def write_plan(self, jobs, generation):
        """Stores part of a plan. A job's progress stays. Lapsed art that has moved to a kept stage is revived."""
        kept = ",".join(str(tier) for tier in sorted(REFRESHED))
        with self._transaction():
            self.con.executemany(
                "INSERT INTO jobs (item, form, tag, tier, rank, gen) VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (item, form, tag) DO UPDATE SET tier = excluded.tier, rank = excluded.rank, gen = excluded.gen, "
                f"pending = CASE WHEN jobs.status = {STATUS_LAPSED} AND excluded.tier IN ({kept}) THEN 1 ELSE jobs.pending END",
                ((job.item, job.form.code, job.tag, job.tier, job.rank, generation) for job in jobs),
            )

    def finish_plan(self, generation):
        """Anything the whole plan didn't mention has gone away or has new art under a new tag, so it's dropped."""
        with self._transaction():
            removed = self.con.execute("DELETE FROM jobs WHERE gen < ?", (generation,)).rowcount
            self.put("generation", generation)
        return removed

    def apply_plan(self, jobs):
        generation = self.next_generation()
        self.write_plan(jobs, generation)
        return self.finish_plan(generation)

    def add_new(self, jobs):
        generation = int(self.get("generation", 0))
        with self._transaction():
            self.con.executemany(
                "INSERT INTO jobs (item, form, tag, tier, rank, gen) VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (item, form, tag) DO UPDATE SET tier = MIN(jobs.tier, excluded.tier), gen = excluded.gen",
                ((job.item, job.form.code, job.tag, job.tier, job.rank, generation) for job in jobs),
            )

    def planned(self):
        return self.con.execute("SELECT count(*) FROM jobs").fetchone()[0]

    def next_jobs(self, limit, tiers, upkeep=()):
        """What's waiting, by stage and then by rank. Each stage is one quick read of an index. A stage in upkeep gives
        only what's already stored, because fetching that again doesn't make the cache any bigger."""
        jobs = []
        for tier in STAGE_ORDER:
            if tier in tiers:
                only = ""
            elif tier in upkeep:
                only = f"AND status = {STATUS_CACHED} "
            else:
                continue
            rows = self.con.execute(
                f"SELECT item, form, tag, tier, rank FROM jobs WHERE pending = 1 {only}AND tier = ? ORDER BY rank LIMIT ?",
                (tier, limit - len(jobs)),
            ).fetchall()
            jobs.extend(Job(item, FORMS[form], tag, tier, rank) for item, form, tag, tier, rank in rows if form in FORMS)
            if len(jobs) >= limit:
                break
        return jobs

    def revive(self, now, config):
        """Puts what's due back in the queue, except art that isn't kept for good and has reached the end of its life."""
        kept = ",".join(str(tier) for tier in sorted(REFRESHED))
        self.con.execute(
            f"UPDATE jobs SET status = ?, next_at = ? WHERE pending = 0 AND next_at <= ? AND status = ? AND tier NOT IN ({kept}) "
            "AND fetched_at + ? <= ?",
            (STATUS_LAPSED, NEVER, now, STATUS_CACHED, int(config.life_days * DAY), now),
        )
        return self.con.execute("UPDATE jobs SET pending = 1 WHERE pending = 0 AND next_at <= ?", (now,)).rowcount

    def is_uncacheable(self, job, now):
        """A signal asks again for anything else, because a hit costs next to nothing and the state can be wrong:
        the cache may have been cleared, or the entry may have expired."""
        row = self.con.execute(
            "SELECT status, next_at FROM jobs WHERE item = ? AND form = ? AND tag = ?", (job.item, job.form.code, job.tag)
        ).fetchone()
        return bool(row) and row[0] == STATUS_UNCACHEABLE and row[1] > now

    def record(self, finished, config, now):
        """Applies a batch of finished jobs in one transaction."""
        with self._transaction():
            for job, outcome, result in finished:
                self._record_one(job, outcome, result, config, now)

    def _record_one(self, job, outcome, result, config, now):
        where = "WHERE item = ? AND form = ? AND tag = ?"
        key = (job.item, job.form.code, job.tag)
        now = int(now)
        if outcome == CACHED:
            row = self.con.execute(f"SELECT fetched_at FROM jobs {where}", key).fetchone()
            # These answers came from what was already stored, which keeps its age. A hit on an entry this worker
            # didn't fetch has an age it can't know, so it's taken as new, and a later look finds out.
            stored = result.cache.upper() in ("HIT", "STALE", "UPDATING")
            fetched_at = row[0] if stored and row and row[0] else now
            self.con.execute(
                f"UPDATE jobs SET pending = 0, status = ?, tries = 0, strikes = 0, bytes = ?, fetched_at = ?, next_at = ? {where}",
                (STATUS_CACHED, result.size, fetched_at, next_look(job, fetched_at, now, config), *key),
            )
            return
        if outcome == SUSPECT:
            row = self.con.execute(f"SELECT strikes FROM jobs {where}", key).fetchone()
            if not row or row[0] + 1 < SUSPECT_LIMIT:
                self.con.execute(f"UPDATE jobs SET pending = 0, strikes = strikes + 1, next_at = ? {where}", (now + SUSPECT_RETRY, *key))
                return
            outcome = MISSING
        if outcome == MISSING:
            row = self.con.execute(f"SELECT tries FROM jobs {where}", key).fetchone()
            tries = row[0] if row else 0
            delay = MISSING_RETRY[min(tries, len(MISSING_RETRY) - 1)]
            self.con.execute(
                f"UPDATE jobs SET pending = 0, status = ?, tries = tries + 1, strikes = 0, next_at = ? {where}",
                (STATUS_MISSING, now + delay, *key),
            )
        elif outcome in (UNCACHEABLE, INVALID):
            self.con.execute(
                f"UPDATE jobs SET pending = 0, status = ?, tries = tries + 1, next_at = ? {where}",
                (STATUS_UNCACHEABLE, now + 30 * DAY, *key),
            )
        elif outcome == REFUSED:
            self.con.execute(
                f"UPDATE jobs SET pending = 0, status = 0, tries = tries + 1, next_at = ? {where}", (now + REFUSED_RETRY, *key)
            )
        else:
            row = self.con.execute(f"SELECT tries FROM jobs {where}", key).fetchone()
            tries = row[0] if row else 0
            delay = FAILURE_RETRY[min(tries, len(FAILURE_RETRY) - 1)]
            self.con.execute(
                f"UPDATE jobs SET pending = 0, tries = tries + 1, next_at = ? {where}", (now + delay, *key)
            )

    def tiers(self):
        out = {}
        for tier, status, pending, count, size in self.con.execute(
            "SELECT tier, status, pending, count(*), sum(bytes) FROM jobs GROUP BY tier, status, pending"
        ):
            entry = out.setdefault(TIER_NAMES.get(tier, str(tier)), collections.Counter())
            entry["total"] += count
            if status == STATUS_CACHED:
                entry["bytes"] += size or 0
            if pending:
                entry["waiting"] += count
            elif status == STATUS_CACHED:
                entry["cached"] += count
            elif status == STATUS_MISSING:
                entry["missing"] += count
            elif status == STATUS_UNCACHEABLE:
                entry["uncacheable"] += count
            elif status == STATUS_LAPSED:
                entry["lapsed"] += count
            else:
                entry["retrying"] += count
        return {name: dict(entry) for name, entry in out.items()}

    def cached_bytes(self):
        return self.con.execute("SELECT COALESCE(sum(bytes), 0) FROM jobs WHERE status = ?", (STATUS_CACHED,)).fetchone()[0]


def touch_after(job, config):
    """How long before asking again, which keeps the entry from aging out and moves it up nginx's eviction order."""
    if not job.form.tagged:
        return config.touch_untagged_days * DAY
    if job.tier in (TIER_NEW, TIER_HOME):
        return config.touch_home_days * DAY
    return config.touch_days * DAY


def entry_life(job, config):
    """How long nginx keeps this form, which is set in its config and repeated here."""
    return (config.life_days if job.form.tagged else config.untagged_life_days) * DAY


def next_look(job, fetched_at, now, config):
    """When to ask again. A hit keeps an entry from being evicted but doesn't extend its life, so the last look is
    timed for just after it expires. That one fetches it again, or for guide art, lets it lapse."""
    expires = fetched_at + entry_life(job, config) + EXPIRY_MARGIN
    if expires <= now:
        # An answer from an entry already past its life: the source was failing, or a refresh was under way.
        return now + HOUR
    return int(min(now + touch_after(job, config), expires))


class SignalListener(threading.Thread):
    """Receives one-way notes from nginx on a datagram socket. nginx doesn't wait for this and drops what it can't send."""

    def __init__(self, path, on_signal, stop):
        super().__init__(daemon=True, name="signals")
        self.path = path
        self.on_signal = on_signal
        self.stop_event = stop
        self.sock = None

    def _bind(self):
        with contextlib.suppress(FileNotFoundError):
            os.unlink(self.path)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            sock.bind(self.path)
            os.chmod(self.path, 0o666)
        except OSError:
            sock.close()
            raise
        sock.settimeout(1.0)
        self.sock = sock

    def run(self):
        complained = False
        while not self.stop_event.is_set():
            if self.sock is None:
                try:
                    self._bind()
                    log("signals: listening")
                except OSError as error:
                    if not complained:
                        log(f"signals: can't open the socket, carrying on without it: {error.strerror}")
                        complained = True
                    self.stop_event.wait(60)
                    continue
            try:
                data = self.sock.recv(512)
            except TimeoutError:
                continue
            except OSError:
                self.sock.close()
                self.sock = None
                continue
            match = ID_RE.search(data)
            if match:
                self.on_signal(bytes.fromhex(match[1].decode()), match[2].decode())
        if self.sock is not None:
            self.sock.close()
            with contextlib.suppress(OSError):
                os.unlink(self.path)


class PlanDone(NamedTuple):
    newest: int = 0
    counts: dict = {}
    titles: int = 0
    rows: int = 0
    error: Exception | None = None
    guide_until: int = 0


class Warmer:
    def __init__(self, config, stop=None):
        self.config = config
        self.stop = stop or threading.Event()
        self.wake = threading.Event()
        self.cond = threading.Condition()
        self.catalogue = Catalogue(config.db_path)
        self.state = State(os.path.join(config.state_dir, "warmer.sqlite"))
        self.urgent = BurstQueue()
        self.soon = BurstQueue()
        self.later = BurstQueue()
        self.background = collections.deque()
        self.in_flight = set()
        self.fetching = 0
        self.finished = collections.deque()
        self.inbox = collections.deque(maxlen=4000)
        self.pace = Pace(config)
        self.main_bucket = Bucket(config.rate)
        self.tail_bucket = Bucket(config.long_tail_rate)
        self.new_bucket = Bucket(config.new_rate, burst=2.0)
        self.fast_bucket = Bucket(config.signal_rate, burst=3.0)
        self.handled = Recent(20000, config.dedupe_hours * HOUR)
        self.last_signal = -1e9
        self.burst_started = -1e9
        self.burst = 0
        self.counts = collections.Counter()
        self.latencies = collections.deque(maxlen=200)
        self.problem = ""
        self.problem_at = 0.0
        self.problem_until_plan = False
        self.clean_fetches = 0
        self.clean_reads = 0
        self.schema_ok = False
        self.last_rowid = int(self.state.get("last_rowid", 0))
        self.guide_until = int(self.state.get("guide_until", 0))
        self.over_budget = False
        self.disk_low = False
        self.store_broken = False
        self.source_was_suspect = False
        self.threads = []
        self.listener = None
        self.planner = None
        self.plan_queue = queue.Queue(maxsize=4)
        self.plan_item = None
        self.plan_generation = 0
        self.plan_before = 0
        self.plan_written = 0
        self.plan_started = 0.0
        self.detail = ({}, 0)
        self.detail_at = -1e9
        self.next_plan = self.next_poll = self.next_status = self.next_log = self.next_revive = self.next_feed = 0.0
        self.refill = True

    # Signals. This runs on the listener's thread, so it only notes the signal.
    def on_signal(self, item, image_type):
        now = time.monotonic()
        with self.cond:
            gap = now - self.last_signal
            if gap > self.config.signal_quiet:
                self.burst_started = now
            if gap > self.config.signal_burst_gap:
                self.burst += 1
            self.last_signal = now
            self.inbox.append((item, image_type, now, self.burst))
            self.counts["signals"] += 1
        self.wake.set()

    def viewer_active(self, now):
        return now - self.last_signal < self.config.yield_seconds

    # Choosing the next job. These run with the lock held.
    def _signal_hold(self, now):
        """Waits for a viewer's burst of requests to pass, so that burst never queues behind this worker."""
        quiet = self.last_signal + self.config.signal_quiet - now
        limit = self.burst_started + self.config.signal_max_hold - now
        return max(0.0, min(quiet, limit))

    def _not_fetching(self):
        fetches_used_up = self.config.max_fetches and self.counts["misses"] >= self.config.max_fetches
        return self.disk_low or self.store_broken or fetches_used_up

    def _next_fast(self, now):
        if not self.urgent and not self.soon and not self.later:
            return None, 0.5
        if self._not_fetching():
            return None, 1.0
        wait = max(self._signal_hold(now), self.pace.pause_until - now)
        if wait > 0:
            return None, wait
        # After a failure, one request at a time finds out whether the chain has recovered.
        if self.pace.failures and self.fetching:
            return None, 0.2
        wait = self.fast_bucket.wait(now, self.pace.speed)
        if wait > 0:
            return None, wait
        self.fast_bucket.take()
        return self.urgent.pop() or self.soon.pop() or self.later.pop(), 0.0

    def _next_background(self, now):
        if not self.background or self._not_fetching():
            return None, 1.0
        job = self.background[0]
        wait = self.pace.pause_until - now
        if wait > 0:
            return None, wait
        if self.pace.failures and self.fetching:
            return None, 0.2
        if job.tier == TIER_NEW:
            bucket, speed = self.new_bucket, 1.0
        else:
            if self.viewer_active(now):
                return None, self.last_signal + self.config.yield_seconds - now
            bucket = self.tail_bucket if job.tier in LONG_TAIL else self.main_bucket
            speed = self.pace.speed
        wait = bucket.wait(now, speed)
        if wait > 0:
            return None, wait
        bucket.take()
        self.background.popleft()
        self.in_flight.add(job.key)
        return job, 0.0

    def _refund(self, job, is_background):
        """A cache hit costs almost nothing and doesn't reach Remux, so it hardly counts against the rate."""
        if not is_background:
            self.fast_bucket.refund(0.9)
        elif job.tier == TIER_NEW:
            self.new_bucket.refund(0.9)
        elif job.tier in LONG_TAIL:
            self.tail_bucket.refund(0.9)
        else:
            self.main_bucket.refund(0.9)

    def _fetch_loop(self, is_background):
        fetcher = Fetcher(self.config.cache_url, self.config.request_timeout)
        choose = self._next_background if is_background else self._next_fast
        while not self.stop.is_set():
            try:
                with self.cond:
                    job, wait = choose(time.monotonic())
                    if job is None:
                        self.cond.wait(min(wait, 1.0))
                        continue
                    self.fetching += 1
                try:
                    self._fetch(fetcher, job, is_background)
                finally:
                    with self.cond:
                        self.fetching -= 1
                        self.cond.notify_all()
            except Exception as error:
                self._note_crash("fetching", error)
                self.stop.wait(1.0)
        fetcher.close()

    def _fetch(self, fetcher, job, is_background):
        try:
            path = job.path()
        except ValueError:
            # A row whose id can't be made into a URL. There's nothing to ask, and no reason to stop for it.
            with self.cond:
                self.counts[INVALID] += 1
                self._note_problem("left out a job with an unexpected id")
                self.finished.append((job, INVALID, Result(0), is_background))
            self.wake.set()
            return
        result = fetcher.get(path)
        outcome = classify(result)
        now = time.monotonic()
        with self.cond:
            outcome = self.pace.judge(job, outcome, result)
            self.pace.record(outcome, result, now, job.tier in THIRD_PARTY)
            if result.cache.upper() == "HIT":
                self.counts["hits"] += 1
                self._refund(job, is_background)
            elif outcome not in (BACKOFF, SUSPECT):
                self.counts["misses"] += 1
            self.counts[outcome] += 1
            if outcome == SUSPECT:
                bad = sum(self.pace.recent.values())
                self._note_problem(f"source failing: {bad} of the last {len(self.pace.recent)} images answered HTTP {result.status}")
            elif outcome in (BACKOFF, SOFT, REFUSED):
                self._note_problem(f"HTTP {result.status or 'connection failed'}")
            elif outcome in (CACHED, MISSING, UNCACHEABLE):
                self.clean_fetches += 1
            if job.seen_at and job.form in (LOGO_PLAIN, LOGO_TAGGED) and outcome == CACHED:
                self.latencies.append(now - job.seen_at)
            # What a viewer was about to see is still worth having once the chain recovers.
            if outcome in (BACKOFF, SUSPECT) and not is_background and now - job.seen_at < SIGNAL_RETRY_FOR:
                self.later.add([job], self.later.last_burst)
            self.finished.append((job, outcome, result, is_background))
        self.wake.set()

    # The rest runs on the main thread, which alone touches the state file.
    def _apply_finished(self):
        with self.cond:
            batch = list(self.finished)
        if not batch:
            return
        # They stay in the list until they're on disk, so a failed write loses nothing.
        self.state.record([entry[:3] for entry in batch], self.config, time.time())
        # A background job stays "in flight" until it's on disk, or the next feed would hand it out again.
        with self.cond:
            for _ in batch:
                self.finished.popleft()
            self.in_flight.difference_update(job.key for job, _, _, is_background in batch if is_background)

    def _handle_signals(self):
        with self.cond:
            entries = list(self.inbox)
            self.inbox.clear()
        if not entries or self.disk_low or self.store_broken or not self._ensure_schema():
            return
        for burst, group in itertools.groupby(entries, key=lambda entry: entry[3]):
            self._queue_burst(burst, group)

    def _queue_burst(self, burst, entries):
        batch = {}
        for item, image_type, seen_at, _ in entries:
            batch.setdefault((item, image_type), seen_at)
        now = time.monotonic()
        marked = []

        def first_time(key):
            fresh = self.handled.first_time(key, now)
            if fresh:
                marked.append(key)
            return fresh

        wanted = {}
        for (item, image_type), seen_at in batch.items():
            if first_time((item, "art")):
                wanted.setdefault(item, {"seen_at": seen_at, "art": True})
            if image_type in ("backdrop", "logo") and first_time((item, "detail")):
                wanted.setdefault(item, {"seen_at": seen_at})["detail"] = True
            if image_type == "primary" and first_time((item, "episodes")):
                wanted.setdefault(item, {"seen_at": seen_at})["episodes"] = True
        if not wanted:
            return
        try:
            urgent, soon, later = self._expand(wanted, first_time)
        except sqlite3.Error as error:
            # Nothing was queued, so these have to count again the next time they're seen.
            for key in marked:
                self.handled.forget(key)
            self._note_db_problem(error)
            return
        clock = time.time()
        urgent = [job for job in urgent if not self.state.is_uncacheable(job, clock)]
        soon = [job for job in soon if not self.state.is_uncacheable(job, clock)]
        later = [job for job in later if not self.state.is_uncacheable(job, clock)]
        with self.cond:
            self.urgent.add(urgent, burst)
            self.soon.add(soon, burst)
            self.later.add(later, burst)
            self.counts["signal_jobs"] += len(urgent) + len(soon) + len(later)
            self.cond.notify_all()

    def _expand(self, wanted, first_time):
        """Looks up what art exists for the items that were seen, and makes it into jobs."""
        items = self.catalogue.items(wanted)
        family = set(wanted)
        for kind, parent, grandparent in items.values():
            if kind == "episode" and grandparent:
                family.add(grandparent)
            elif kind == "season" and parent:
                family.add(parent)
        art = self.catalogue.art(family)
        urgent = []
        soon = []
        later = []
        for item, need in wanted.items():
            seen_at = need["seen_at"]
            found = items.get(item)
            if found is None:
                # A search result that only exists in Remux's memory for now. Opening it asks for this logo first,
                # but it's seldom opened, so it waits behind art that's certain. Its tags change once it's stored,
                # so nothing else about it is worth fetching yet, and a later signal has to count again in case
                # the row exists by then.
                for level in ("art", "detail", "episodes"):
                    self.handled.forget((item, level))
                if first_time((item, "plain logo")):
                    later.append(Job(item, LOGO_PLAIN, b"", TIER_NEW, 0, seen_at, True))
                continue
            kind, parent, grandparent = found
            if need.get("art"):
                first, rest = [], []
                if kind in TITLE_KINDS:
                    first, rest = art_jobs(item, art.get(item, {}), seen_at)
                elif kind in ("episode", "season"):
                    # A page of these shares one series. Its own key, as this asks for more than the series' poster does.
                    series = grandparent if kind == "episode" else parent
                    if series and first_time((series, "series art")):
                        first, rest = series_jobs(series, art.get(series, {}), seen_at)
                urgent.extend(first)
                soon.extend(rest)
            if need.get("detail") and kind in TITLE_KINDS:
                soon.extend(self._detail_jobs(item, kind, seen_at))
            if need.get("episodes") and kind == "season":
                stills, full_size = self._season_jobs(item, seen_at)
                soon.extend(stills)
                later.extend(full_size)
        return urgent, soon, later

    def _detail_jobs(self, item, kind, seen_at):
        """A backdrop or logo means a title's page is open, so its seasons and cast come next."""
        ids = self.catalogue.cast(item, self.config.cast_limit)
        if kind == "series":
            ids = self.catalogue.children(item, "season", 60) + ids
        art = self.catalogue.art(ids)
        jobs = []
        for other in ids:
            tag = art.get(other, {}).get("primary")
            if tag:
                jobs.append(Job(other, POSTER_342, tag, TIER_NEW, 0, seen_at))
                jobs.append(Job(other, POSTER_500, tag, TIER_NEW, 0, seen_at))
        return jobs

    def _season_jobs(self, season, seen_at):
        """A season poster means the next click is its episode list, so its stills come next. That list uses the
        smaller form, so the full size waits behind anything else a viewer might see sooner."""
        ids = self.catalogue.children(season, "episode", self.config.season_episodes)
        art = self.catalogue.art(ids)
        stills = []
        full_size = []
        for other in ids:
            tag = art.get(other, {}).get("primary")
            if tag:
                stills.append(Job(other, POSTER_500, tag, TIER_NEW, 0, seen_at))
                full_size.append(Job(other, POSTER_FULL, tag, TIER_NEW, 0, seen_at))
        return stills, full_size

    def _note_problem(self, text, until_plan=False):
        """Remembers a problem and says whether it's a new one. The time is kept apart from the text, so a repeat
        is still recognised. Each one starts the clean stretch again. Once a problem needs a new plan to settle it,
        a later one that doesn't can't take that away."""
        with self.cond:
            new = text != self.problem
            self.problem = text
            self.problem_at = time.time()
            self.problem_until_plan = self.problem_until_plan or until_plan
            self.clean_fetches = self.clean_reads = 0
        return new

    def _clear_problem_if_clean(self):
        """Stops listing a problem once fetches and database reads have gone well for a while. With nothing to fetch,
        the reads have to do. One that only a new plan can settle waits for that plan."""
        with self.cond:
            if not self.problem or self.problem_until_plan or self.store_broken:
                return
            if self.pace.failures or self.pace.source_suspect or self.clean_reads < CLEAN_READS:
                return
            nothing_to_fetch = self._not_fetching() or not (self.background or self.urgent or self.soon or self.later)
            if self.clean_fetches >= CLEAN_FETCHES or nothing_to_fetch:
                self.problem = ""

    def _note_db_problem(self, error):
        message = str(error)
        if "no such column" in message or "no such table" in message:
            # The message names only what this worker's own query asked for.
            text = f"database layout not recognised: {message}"
        else:
            text = f"database unavailable ({getattr(error, 'sqlite_errorname', type(error).__name__)})"
        if self._note_problem(text):
            log(f"{text}, will try again")
        self.schema_ok = False

    def _note_store_problem(self, error):
        name = getattr(error, "sqlite_errorname", None) or type(error).__name__
        text = f"can't write the state ({name}), fetching is paused until it can"
        if self._note_problem(text):
            log(text)
        with self.cond:
            self.store_broken = True

    def _note_crash(self, where, error, until_plan=False):
        """One line, with the place but not the message, which could hold a row's values. Repeats aren't logged."""
        frames = traceback.extract_tb(error.__traceback__)
        place = f"{frames[-1].name}:{frames[-1].lineno}" if frames else "unknown"
        text = f"{where}: {type(error).__name__} at {place}"
        if self._note_problem(text, until_plan):
            log(f"{text}, carrying on")

    def _ensure_schema(self):
        if self.schema_ok:
            return True
        try:
            self.catalogue.check_schema()
        except sqlite3.Error as error:
            self._note_db_problem(error)
            return False
        except SchemaError as error:
            text = f"database layout not recognised: {error}"
            if self._note_problem(text):
                log(f"{text}. Idling until it changes")
            return False
        self.schema_ok = True
        return True

    # Planning. A thread reads the database and works out the jobs, so the main thread can go on handling signals.
    # It hands them over in small chunks, so a plan is never all in memory, and only the main thread writes.
    def _start_plan(self):
        if not self._ensure_schema():
            return False
        self.plan_generation = self.state.next_generation()
        self.plan_before = self.state.planned()
        self.plan_written = 0
        self.plan_started = time.monotonic()
        self.planner = threading.Thread(target=self._build_plan, daemon=True, name="planner")
        self.planner.start()
        return True

    def _hand_over(self, item):
        while not self.stop.is_set():
            try:
                self.plan_queue.put(item, timeout=1.0)
            except queue.Full:
                continue
            self.wake.set()
            return

    def _build_plan(self):
        try:
            newest = self.catalogue.max_rowid()
            snapshot = read_snapshot(self.catalogue, self.config)
            counts = collections.Counter()
            jobs = plan_jobs(snapshot, self.config, counts)
            while chunk := list(itertools.islice(jobs, PLAN_CHUNK)):
                self._hand_over(chunk)
            self._hand_over(PlanDone(newest, counts, len(snapshot.titles), len(snapshot.rows), guide_until=snapshot.guide_until))
        except Exception as error:
            self._hand_over(PlanDone(error=error))

    def _write_plan(self):
        """Writes a little of a plan in progress on each pass, so a signal never waits long."""
        if self.planner is None:
            return
        deadline = time.monotonic() + 0.1
        while time.monotonic() < deadline:
            # An item stays here until it's written, so a failed write loses nothing.
            if self.plan_item is None:
                try:
                    self.plan_item = self.plan_queue.get_nowait()
                except queue.Empty:
                    return
            if isinstance(self.plan_item, PlanDone):
                self._finish_plan(self.plan_item)
                self.plan_item = None
                self.planner = None
                self.refill = True
                return
            self.state.write_plan(self.plan_item, self.plan_generation)
            self.plan_written += len(self.plan_item)
            self.plan_item = None
        self.wake.set()

    def _finish_plan(self, done):
        now = time.monotonic()
        if done.error is not None:
            if isinstance(done.error, sqlite3.Error):
                self._note_db_problem(done.error)
                self.next_plan = now + self.config.poll_seconds
            else:
                self._note_crash("planning", done.error, until_plan=True)
                self.next_plan = now + HOUR
            return
        jobs = self.plan_written
        skipped = done.counts.get("skipped_rows", 0)
        if skipped:
            log(f"plan: left out {skipped} rows that weren't as expected")
        if skipped > done.counts.get("image_rows", 0):
            text = f"database layout not recognised: {skipped} rows weren't as expected, keeping the old plan"
            if self._note_problem(text, until_plan=True):
                log(text)
            return
        settings = plan_settings(self.config)
        settings_changed = self.state.get("plan_settings") != settings
        # A half-finished purge or restore shouldn't throw away months of state in one go, but a change of settings means it.
        if self.plan_before and jobs < self.plan_before / 2 and not settings_changed and not self.state.get("plan_shrink_ok"):
            log(f"plan: {jobs} jobs against {self.plan_before} before, keeping the old plan until the next pass")
            self.state.put("plan_shrink_ok", 1)
            return
        self.state.put("plan_shrink_ok", 0)
        removed = self.state.finish_plan(self.plan_generation)
        self.last_rowid = max(self.last_rowid, done.newest)
        self.state.put("last_rowid", self.last_rowid)
        if done.guide_until:
            self.guide_until = done.guide_until
            self.state.put("guide_until", self.guide_until)
        self.state.put("planned_at", int(time.time()))
        self.state.put("plan_settings", settings)
        with self.cond:
            if self.problem_until_plan:
                self.problem = ""
                self.problem_until_plan = False
        log(f"plan: {jobs} jobs, {removed} dropped, {done.titles} titles, {done.rows} home rows, {round(now - self.plan_started, 1)} s")

    def _poll_new_rows(self):
        # The plan being made will cover whatever has appeared, and before the first one every row looks new.
        if self.planner is not None or not self.state.get("planned_at") or not self._ensure_schema():
            return False
        try:
            newest = self.catalogue.max_rowid()
            self.clean_reads += 1
            if newest < self.last_rowid:
                # Remux also deletes its newest rows when it imports a guide. Remember the new top, or this
                # would be true on every poll and rebuild the plan each time.
                log("newest image rows have gone, planning again")
                self.last_rowid = newest
                self.state.put("last_rowid", newest)
                self.next_plan = 0.0
                return False
            if newest == self.last_rowid:
                return False
            rows = self.catalogue.images_after(plan_kinds(self.config), self.last_rowid)
        except sqlite3.Error as error:
            self._note_db_problem(error)
            return False
        self.last_rowid = rows[-1][0] if len(rows) == 5000 else newest
        self.state.put("last_rowid", self.last_rowid)
        jobs = new_row_jobs(rows)
        if jobs:
            self.state.add_new(jobs)
            with self.cond:
                self.counts["new_rows"] += len(rows)
            log(f"new rows: {len(rows)}, {len(jobs)} jobs")
        return bool(jobs)

    def _poll_guide(self):
        """Moves the window on, so that programmes join it as they come within its reach."""
        if not self.config.guide or self.planner is not None or not self.state.get("planned_at") or not self._ensure_schema():
            return False
        now = int(time.time())
        until = now + GUIDE_WINDOW
        try:
            rows = self.catalogue.guide_rows(now, self.guide_until, until)
        except sqlite3.Error as error:
            self._note_db_problem(error)
            return False
        jobs = list(guide_jobs(rows, collections.Counter()))
        if jobs:
            self.state.add_new(jobs)
            self.state.put("guide_until", until)
        self.guide_until = until
        return bool(jobs)

    def _check_limits(self):
        spent = self.state.cached_bytes()
        over = spent >= self.config.budget_gb * 1024**3
        if over != self.over_budget:
            log("budget reached, the long tail only keeps up what it has" if over else "back under budget")
        self.over_budget = over
        try:
            free = shutil.disk_usage(self.config.state_dir).free
        except OSError:
            return
        low = free < self.config.min_free_gb * 1024**3
        if low != self.disk_low:
            log("disk nearly full, pausing all fetches" if low else "disk has room again")
        with self.cond:
            self.disk_low = low

    def _check_source(self):
        with self.cond:
            suspect = self.pace.source_suspect
        if suspect != self.source_was_suspect:
            log("source looks like it's failing, backing off" if suspect else "source is answering again")
        self.source_was_suspect = suspect

    def _tiers_allowed(self):
        tiers = {tier for tier in STAGE_ORDER if tier_enabled(tier, self.config)}
        return tiers - LONG_TAIL if self.over_budget else tiers

    def _feed(self, refill=False):
        with self.cond:
            if len(self.background) >= 16 and not refill:
                return
            in_flight = set(self.in_flight)
        # Keeping up what's stored doesn't make the cache any bigger, so a budget has no reason to stop it.
        upkeep = {tier for tier in LONG_TAIL if tier_enabled(tier, self.config)} if self.over_budget else ()
        jobs = self.state.next_jobs(128, self._tiers_allowed(), upkeep)
        with self.cond:
            self.background.clear()
            self.background.extend(job for job in jobs if job.key not in in_flight)
            self.cond.notify_all()

    def _threads_ok(self):
        threads = list(self.threads)
        if self.listener is not None:
            threads.append(self.listener)
        return all(thread.is_alive() for thread in threads)

    def status(self, detail_age=0.0):
        now = time.monotonic()
        # These scan every job, so they're not run often, and not with the lock the fetch threads and the listener need.
        if now - self.detail_at >= detail_age:
            self.detail = (self.state.tiers(), self.state.cached_bytes())
            self.detail_at = now
        tiers, cached_bytes = self.detail
        with self.cond:
            latencies = sorted(self.latencies)
            status = {
                "version": VERSION,
                "updated_at": int(time.time()),
                "tiers": tiers,
                "counts": dict(self.counts),
                "cached_bytes": cached_bytes,
                "speed": round(self.pace.speed, 2),
                "paused_for": round(max(0.0, self.pace.pause_until - now), 1),
                "source_suspect": self.pace.source_suspect,
                "viewer_active": self.viewer_active(now),
                "queued": {"urgent": len(self.urgent), "soon": len(self.soon), "later": len(self.later)},
                "signal_to_logo_ms": {
                    "n": len(latencies),
                    "p50": round(latencies[len(latencies) // 2] * 1000) if latencies else None,
                    "max": round(latencies[-1] * 1000) if latencies else None,
                },
                "planning": self.planner is not None,
                "over_budget": self.over_budget,
                "disk_low": self.disk_low,
                "store_broken": self.store_broken,
                "threads_ok": self._threads_ok(),
                "last_problem": f"{timestamp(self.problem_at)} {self.problem}" if self.problem else "",
            }
        return status

    def _write_status(self, detail_age=0.0):
        status = self.status(detail_age)
        path = os.path.join(self.config.state_dir, "status.json")
        temp = path + ".tmp"
        with open(temp, "w") as out:
            json.dump(status, out)
        os.replace(temp, path)
        return status

    def _log_progress(self, status):
        parts = []
        for name in TIER_NAMES.values():
            entry = status["tiers"].get(name)
            if entry:
                parts.append(f"{name} {entry.get('cached', 0)}/{entry['total']}")
        counts = status["counts"]
        latency = status["signal_to_logo_ms"]
        log(
            f"{', '.join(parts) or 'no plan yet'}; {round(status['cached_bytes'] / 1024**3, 2)} GB; "
            f"{counts.get('misses', 0)} fetched, {counts.get('hits', 0)} already there; "
            f"{counts.get('signals', 0)} signals, logo in {latency['p50']} ms; speed {status['speed']}"
        )

    def _tick(self):
        """One pass of the main loop."""
        config = self.config
        now = time.monotonic()
        if self.store_broken:
            # A real write, because a pass with nothing to record proves nothing.
            self.state.put("alive_at", int(time.time()))
        self._apply_finished()
        self._handle_signals()
        self._write_plan()
        self._check_source()
        if now >= self.next_plan and self.planner is None:
            started = self._start_plan()
            self.next_plan = now + (config.plan_hours * HOUR if started else config.poll_seconds)
        elif now >= self.next_poll:
            self.next_poll = now + config.poll_seconds
            if self._poll_new_rows():
                self.refill = True
            if self._poll_guide():
                self.refill = True
        if now >= self.next_revive:
            self.next_revive = now + 60
            if self.state.revive(int(time.time()), config):
                self.refill = True
            self._check_limits()
        if self.refill or now >= self.next_feed:
            self._feed(self.refill)
            self.refill = False
            self.next_feed = now + 2
        if now >= self.next_status:
            self.next_status = now + 10
            status = self._write_status(config.status_detail_seconds)
            if now >= self.next_log:
                self.next_log = now + config.log_seconds
                self._log_progress(status)
        # Only a whole pass counts, because on a nearly full disk a small write can work when a large one doesn't.
        if self.store_broken:
            log("state can be written again, carrying on")
            with self.cond:
                if not self.problem_until_plan:
                    self.problem = ""
                self.store_broken = False
        self._clear_problem_if_clean()

    def run(self):
        config = self.config
        extras = {"people": config.people, "episodes": config.episodes, "channels": config.channels, "guide": config.guide}
        also = ", ".join(name for name, on in extras.items() if on) or "nothing else"
        log(f"starting, {config.rate} a second, budget {config.budget_gb} GB, also warming: {also}")
        if config.signal_socket:
            self.listener = SignalListener(config.signal_socket, self.on_signal, self.stop)
            self.listener.start()
        self.threads = [threading.Thread(target=self._fetch_loop, args=(True,), daemon=True, name="background")]
        for number in range(config.signal_workers):
            self.threads.append(threading.Thread(target=self._fetch_loop, args=(False,), daemon=True, name=f"fast-{number}"))
        for thread in self.threads:
            thread.start()

        now = time.monotonic()
        self.next_plan = now
        # A change of settings needs a new plan at once. Otherwise a restart keeps to the old schedule.
        if self.state.get("plan_settings") == plan_settings(config):
            planned_at = float(self.state.get("planned_at", 0) or 0)
            self.next_plan = now + max(0.0, planned_at + config.plan_hours * HOUR - time.time())
        self.next_poll = now + config.poll_seconds
        while not self.stop.is_set():
            try:
                self._tick()
            except (sqlite3.Error, OSError) as error:
                self._note_store_problem(error)
                self.stop.wait(5.0)
            except Exception as error:
                self._note_crash("main loop", error)
                self.stop.wait(5.0)
            self.wake.wait(0.25)
            self.wake.clear()
        self._shutdown()

    def _shutdown(self):
        with self.cond:
            self.cond.notify_all()
        for thread in self.threads:
            thread.join(timeout=1.0)
        with contextlib.suppress(sqlite3.Error, OSError):
            self._apply_finished()
            self._write_status()
        if self.listener:
            self.listener.join(timeout=2.0)
        with contextlib.suppress(sqlite3.Error):
            self.state.close()
        log("stopped")


def open_warmer(config, stop):
    """A full disk or a bad mount shouldn't turn into a restart loop that reads the database each time."""
    complained = False
    while not stop.is_set():
        try:
            os.makedirs(config.state_dir, exist_ok=True)
            return Warmer(config, stop)
        except (sqlite3.Error, OSError) as error:
            if not complained:
                log(f"can't open the state ({getattr(error, 'sqlite_errorname', None) or type(error).__name__}), will keep trying")
                complained = True
            stop.wait(30.0)
    return None


def health(config):
    """True if the main loop wrote its status recently and the fetch threads are alive. It says nothing about nginx
    or Remux, on purpose."""
    path = os.path.join(config.state_dir, "status.json")
    try:
        age = time.time() - os.path.getmtime(path)
        with open(path) as status:
            threads_ok = json.load(status).get("threads_ok", False)
    except (OSError, ValueError):
        return False
    return age < 180 and threads_ok


def print_plan(config):
    """Prints what would be warmed, with sizes and times at the set rates. Reads the database, fetches nothing."""
    catalogue = Catalogue(config.db_path)
    catalogue.check_schema()
    counts = collections.Counter()
    totals = collections.defaultdict(lambda: [0, 0, 0.0])
    for job in plan_jobs(read_snapshot(catalogue, config), config, counts):
        entry = totals[job.tier]
        entry[0] += 1
        entry[1] += EST_TIER_BYTES.get((job.tier, job.form), EST_BYTES[job.form]) + 2048
        # A guide job is only ever fetched once, and the window brings about this many new ones each day.
        entry[2] += 1.0 if job.tier == TIER_GUIDE else DAY / entry_life(job, config)
    hours = 0.0
    size = 0
    print(f"{'stage':11} {'requests':>9} {'est. GB':>8} {'hours':>7} {'running hours':>14} {'running GB':>11} {'fetches a day':>14}")
    for tier in STAGE_ORDER:
        if tier not in totals:
            continue
        count, bytes_, daily = totals[tier]
        rate = config.long_tail_rate if tier in LONG_TAIL else config.rate
        took = count / rate / HOUR
        hours += took
        size += bytes_
        print(f"{TIER_NAMES[tier]:11} {count:9d} {bytes_ / 1024**3:8.2f} {took:7.1f} {hours:14.1f} {size / 1024**3:11.2f} {daily:14.0f}")
    print(f"{'total':11} {sum(entry[0] for entry in totals.values()):9d} {size / 1024**3:8.2f} {hours:7.1f} {'':14} {'':11} {sum(entry[2] for entry in totals.values()):14.0f}")
    if counts["skipped_rows"]:
        print(f"{counts['skipped_rows']} rows left out because they weren't as expected")


def main():
    parser = argparse.ArgumentParser(description="Keeps a Remux image cache warm.")
    parser.add_argument("--health", action="store_true", help="exit 0 if the worker is running normally")
    parser.add_argument("--status", action="store_true", help="print the current status as JSON")
    parser.add_argument("--plan", action="store_true", help="print what would be warmed, without fetching anything")
    args = parser.parse_args()
    config = Config.from_env()
    if args.health:
        raise SystemExit(0 if health(config) else 1)
    if args.status:
        with open(os.path.join(config.state_dir, "status.json")) as status:
            print(status.read())
        return
    if args.plan:
        print_plan(config)
        return
    stop = threading.Event()
    wake = []

    def stop_soon(*_):
        stop.set()
        for event in wake:
            event.set()

    signal.signal(signal.SIGTERM, stop_soon)
    signal.signal(signal.SIGINT, stop_soon)
    warmer = open_warmer(config, stop)
    if warmer is not None:
        wake.append(warmer.wake)
        warmer.run()


if __name__ == "__main__":
    main()
