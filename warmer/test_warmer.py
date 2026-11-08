#!/usr/bin/env python3
"""Tests for the warmer. Run from this folder: python3 -m unittest -v test_warmer

They build a small database with the same layout as Remux's, in WAL mode, and a fake cache that answers like
the nginx config does. Nothing here touches a real server.
"""

import collections
import contextlib
import http.server
import io
import json
import os
import re
import socket
import socketserver
import sqlite3
import tempfile
import threading
import time
import unittest
import urllib.parse
import uuid

import warmer
from warmer import (
    BACKDROP,
    BACKDROP_0,
    LOGO_PLAIN,
    LOGO_TAGGED,
    POSTER_342,
    POSTER_500,
    POSTER_FULL,
    THUMB_342,
    THUMB_500,
    THUMB_FULL,
    TIER_EPISODES,
    TIER_HOME,
    TIER_NEW,
    TIER_PEOPLE,
    TIER_SEASONS,
    TIER_TITLE_ART,
    TIER_TITLES,
    Bucket,
    BurstQueue,
    Catalogue,
    Config,
    Job,
    Pace,
    Recent,
    Result,
    State,
    build_plan,
    classify,
    new_row_jobs,
    read_snapshot,
    row_items,
    row_matches,
)

SCHEMA = """
CREATE TABLE media (
    id TEXT, title TEXT, kind TEXT, parent_id TEXT, grandparent_id TEXT, idx INTEGER, parent_idx INTEGER,
    digital_released_at TIMESTAMP, rating_audience REAL, original_language TEXT, promoted INTEGER, enabled INTEGER,
    sort_order INTEGER, collection_media_kind TEXT, collection_smart_filter TEXT, collection_default_sort TEXT,
    created_at TIMESTAMP, live_start TIMESTAMP, live_end TIMESTAMP, notes TEXT, PRIMARY KEY (id));
CREATE TABLE media_images (
    id TEXT NOT NULL PRIMARY KEY, media_id TEXT NOT NULL, image_type TEXT NOT NULL, image_index INTEGER NOT NULL DEFAULT 0,
    path TEXT NOT NULL, width INTEGER, height INTEGER, created_at DATETIME, UNIQUE (media_id, image_type, image_index));
CREATE TABLE media_metrics (
    media_id BLOB PRIMARY KEY, popularity_all_time REAL, popularity_daily REAL, popularity_weekly REAL,
    popularity_monthly REAL, popularity_yearly REAL, trending_weekly REAL, trending_monthly REAL, synced_at TEXT);
CREATE TABLE media_tags (media_id TEXT NOT NULL, tag TEXT NOT NULL, PRIMARY KEY (media_id, tag));
CREATE TABLE media_relations (
    relation_id TEXT NOT NULL PRIMARY KEY, left_media_id TEXT NOT NULL, right_media_id TEXT NOT NULL, weight INTEGER,
    role TEXT, character TEXT);
CREATE TABLE user_media_state (
    user_id BLOB, media_id BLOB, media_raw TEXT, last_played_at DATETIME);
CREATE TABLE settings (id TEXT, value TEXT);
INSERT INTO settings VALUES ('x', 'private');
"""

ALL_TIERS = range(1, 11)


def when(seconds):
    """A time from now, the way Remux stores programme times."""
    return warmer.timestamp(time.time() + seconds)


def blob(name):
    """A stable 16-byte id from a readable name, the way Remux stores its UUIDs."""
    return uuid.uuid5(uuid.NAMESPACE_DNS, name).bytes


class FakeLibrary:
    def __init__(self, path):
        self.path = path
        self.con = sqlite3.connect(path, isolation_level=None)
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.executescript(SCHEMA)

    def close(self):
        self.con.close()

    def item(self, name, kind, parent=None, grandparent=None, idx=None, parent_idx=None, **extra):
        columns = ["id", "title", "kind", "parent_id", "grandparent_id", "idx", "parent_idx", "enabled"] + list(extra)
        values = [blob(name), name, kind, parent and blob(parent), grandparent and blob(grandparent), idx, parent_idx, 1]
        values += list(extra.values())
        self.con.execute(
            f"INSERT INTO media ({', '.join(columns)}) VALUES ({', '.join('?' * len(values))})", values
        )
        return blob(name)

    def art(self, name, *types):
        for image_type in types:
            self.con.execute(
                "INSERT INTO media_images (id, media_id, image_type, image_index, path) VALUES (?, ?, ?, 0, ?)",
                (blob(f"{name}/{image_type}"), blob(name), image_type, f"https://example.invalid/{image_type}.jpg"),
            )

    def title(self, name, kind="movie", daily=None, weekly=None, art=("primary", "logo", "backdrop", "thumb"), **extra):
        self.item(name, kind, **extra)
        self.art(name, *art)
        if daily is not None or weekly is not None:
            self.con.execute(
                "INSERT INTO media_metrics (media_id, popularity_daily, trending_weekly, synced_at) VALUES (?, ?, ?, 'now')",
                (blob(name), daily, weekly),
            )

    def programme(self, name, starts_in, ends_in, art=("primary",)):
        self.item(name, "tv_program", live_start=when(starts_in), live_end=when(ends_in))
        self.art(name, *art)

    def row(self, name, media_kind, sort, rules=None, order=0):
        self.item(
            name, "collection", promoted=1, sort_order=order, collection_media_kind=media_kind,
            collection_default_sort=json.dumps([sort]), collection_smart_filter=json.dumps(rules) if rules else None,
        )


def nginx_key(path_and_query):
    """Works out the cache key the way the nginx maps do, so a test can compare it with what's expected."""
    url = urllib.parse.urlsplit(path_and_query)
    query = {key.lower(): value for key, value in urllib.parse.parse_qsl(url.query, keep_blank_values=True)}
    match = re.fullmatch(r"(?i)/items/([0-9a-f-]+)/images/(primary|thumb|backdrop|logo)(/[0-9]+)?", url.path)
    if not match:
        raise AssertionError(f"not a shape nginx accepts: {url.path}")
    ident = match[1].replace("-", "")
    image_type = match[2].lower()
    index = "" if match[3] == "/0" else (match[3] or "")
    width = 0
    if image_type in ("primary", "thumb"):
        fill = int(query.get("fillwidth", 0) or 0)
        width = 342 if 1 <= fill <= 342 else 500 if 343 <= fill <= 500 else 0
    return f"{ident}|{image_type}{index}|{query.get('tag', '')}|{width}"


class FakeCache:
    """Answers like nginx's image location: MISS the first time, HIT after that, or whatever a test sets."""

    def __init__(self):
        self.seen = []
        self.keys = set()
        self.status_for = {}
        self.lock = threading.Lock()
        fake = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_):
                pass

            def do_GET(self):
                key = nginx_key(self.path)
                with fake.lock:
                    fake.seen.append((self.path, self.headers.get("X-Image-Warmer")))
                    answer = None
                    for prefix, value in fake.status_for.items():
                        if key.startswith(prefix):
                            answer = value
                    known = key in fake.keys
                    if answer is None:
                        fake.keys.add(key)
                status, cache, content_type, retry = answer or (200, "HIT" if known else "MISS", "image/jpeg", None)
                body = b"x" * 1000 if status == 200 else b""
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("X-Img-Cache", cache)
                if retry:
                    self.send_header("Retry-After", str(retry))
                self.end_headers()
                self.wfile.write(body)

        class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
            daemon_threads = True

        self.server = Server(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def paths(self):
        with self.lock:
            return [path for path, _ in self.seen]


def collections_counter(holder):
    holder["counter"] = collections.Counter()
    return holder["counter"]


def wait_until(check, timeout=10.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if check():
            return True
        time.sleep(0.02)
    return check()


class TempCase(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.db_path = os.path.join(self.folder.name, "db.sqlite")
        self.library = FakeLibrary(self.db_path)
        self.addCleanup(self.library.close)

    def config(self, **values):
        state_dir = os.path.join(self.folder.name, "state")
        os.makedirs(state_dir, exist_ok=True)
        settings = dict(db_path=self.db_path, state_dir=state_dir, signal_socket="", home_row_items=30)
        settings.update(values)
        return Config(**settings)

    def plan(self, **values):
        config = self.config(**values)
        catalogue = Catalogue(config.db_path)
        catalogue.check_schema()
        return build_plan(read_snapshot(catalogue, config), config)


class FormTests(unittest.TestCase):
    ITEM = blob("item")
    TAG = blob("tag")

    def test_paths_match_the_shapes_clients_use(self):
        tag = str(uuid.UUID(bytes=self.TAG))
        ident = self.ITEM.hex()
        expected = {
            POSTER_342: f"/Items/{ident}/Images/Primary?fillHeight=513&fillWidth=342&quality=96&tag={tag}",
            POSTER_500: f"/Items/{ident}/Images/Primary?fillHeight=750&fillWidth=500&quality=96&tag={tag}",
            THUMB_500: f"/Items/{ident}/Images/Thumb?fillHeight=281&fillWidth=500&quality=96&tag={tag}",
            THUMB_FULL: f"/Items/{ident}/Images/Thumb?fillHeight=386&fillWidth=686&quality=96&tag={tag}",
            BACKDROP: f"/Items/{ident}/Images/Backdrop?maxWidth=1920&tag={tag}&quality=80",
            BACKDROP_0: f"/Items/{ident}/Images/Backdrop/0?tag={tag}&maxWidth=1920&quality=80",
            LOGO_TAGGED: f"/Items/{ident}/Images/Logo?maxWidth=700&tag={tag}&quality=90",
        }
        for form, path in expected.items():
            self.assertEqual(form.path(self.ITEM, self.TAG), path)
        self.assertEqual(LOGO_PLAIN.path(self.ITEM, b""), f"/Items/{ident}/Images/Logo?maxWidth=600&quality=90")

    def test_cache_keys(self):
        tag = str(uuid.UUID(bytes=self.TAG))
        ident = self.ITEM.hex()
        self.assertEqual(nginx_key(POSTER_342.path(self.ITEM, self.TAG)), f"{ident}|primary|{tag}|342")
        self.assertEqual(nginx_key(POSTER_500.path(self.ITEM, self.TAG)), f"{ident}|primary|{tag}|500")
        self.assertEqual(nginx_key(THUMB_500.path(self.ITEM, self.TAG)), f"{ident}|thumb|{tag}|500")
        self.assertEqual(nginx_key(THUMB_FULL.path(self.ITEM, self.TAG)), f"{ident}|thumb|{tag}|0")
        self.assertEqual(nginx_key(LOGO_TAGGED.path(self.ITEM, self.TAG)), f"{ident}|logo|{tag}|0")
        self.assertEqual(nginx_key(LOGO_PLAIN.path(self.ITEM, b"")), f"{ident}|logo||0")
        self.assertEqual(nginx_key(BACKDROP.path(self.ITEM, self.TAG)), nginx_key(BACKDROP_0.path(self.ITEM, self.TAG)))

    def test_form_codes_are_stable_because_they_are_stored_on_disk(self):
        self.assertEqual(
            [form.code for form in (POSTER_342, POSTER_500, warmer.POSTER_FULL, THUMB_500, THUMB_FULL, BACKDROP, BACKDROP_0, LOGO_TAGGED, LOGO_PLAIN, THUMB_342)],
            list(range(1, 11)),
        )

    def test_the_small_thumb_has_its_own_step_and_can_be_read_back(self):
        tag = str(uuid.UUID(bytes=self.TAG))
        ident = self.ITEM.hex()
        self.assertEqual(nginx_key(THUMB_342.path(self.ITEM, self.TAG)), f"{ident}|thumb|{tag}|342")
        self.assertIs(warmer.FORMS[THUMB_342.code], THUMB_342)

    def test_the_plan_estimate_knows_every_form_a_plan_can_hold(self):
        planned = {form for kinds in warmer.KIND_FORMS.values() for forms in kinds.values() for form in forms}
        self.assertEqual(planned - set(warmer.EST_BYTES), set())


class ClassifyTests(unittest.TestCase):
    def test_outcomes(self):
        cases = [
            (Result(200, "MISS"), warmer.CACHED),
            (Result(200, "HIT"), warmer.CACHED),
            (Result(200, "EXPIRED"), warmer.CACHED),
            (Result(200, "DIRECT", "image/svg+xml"), warmer.UNCACHEABLE),
            (Result(200, "DIRECT", "image/jpeg"), warmer.SOFT),
            (Result(404, "MISS"), warmer.MISSING),
            (Result(403), warmer.REFUSED),
            (Result(429), warmer.BACKOFF),
            (Result(500), warmer.BACKOFF),
            (Result(503), warmer.BACKOFF),
            (Result(0), warmer.BACKOFF),
        ]
        for result, outcome in cases:
            self.assertEqual(classify(result), outcome, result)


class CatalogueTests(TempCase):
    def setUp(self):
        super().setUp()
        self.library.title("Film", daily=1.0)
        self.catalogue = Catalogue(self.db_path)
        self.catalogue.check_schema()

    def test_reads_only_listed_columns(self):
        for sql in (
            "SELECT value FROM settings",
            "SELECT * FROM settings",
            "SELECT width FROM media_images",
            "SELECT path FROM media_images",
            "SELECT * FROM media_images",
            "SELECT notes FROM media",
            "SELECT count(*) FROM settings",
            "SELECT name FROM sqlite_master",
        ):
            with self.catalogue.connect() as con, self.assertRaises(sqlite3.DatabaseError, msg=sql):
                con.execute(sql).fetchall()

    def test_programme_times_are_on_the_list(self):
        with self.catalogue.connect() as con:
            self.assertEqual(con.execute("SELECT live_start, live_end FROM media").fetchall(), [(None, None)])

    def test_cannot_write(self):
        with self.catalogue.connect() as con, self.assertRaises(sqlite3.DatabaseError):
            con.execute("DELETE FROM media_images")

    def test_sees_rows_still_in_the_wal(self):
        self.library.con.execute("PRAGMA wal_autocheckpoint=0")
        before = self.catalogue.max_rowid()
        self.library.art("Film", "banner")
        self.assertGreater(os.path.getsize(self.db_path + "-wal"), 0)
        self.assertEqual(self.catalogue.max_rowid(), before + 1)

    def test_reading_never_blocks_the_writer_or_a_checkpoint(self):
        stop = threading.Event()

        def keep_reading():
            while not stop.is_set():
                self.catalogue.max_rowid()

        reader = threading.Thread(target=keep_reading)
        reader.start()
        try:
            self.library.con.execute("PRAGMA busy_timeout=0")
            busy = 0
            for number in range(200):
                try:
                    self.library.con.execute("INSERT INTO media_tags VALUES (?, ?)", (blob("Film"), f"tag{number}"))
                except sqlite3.OperationalError:
                    busy += 1
            self.assertEqual(busy, 0)
            self.library.con.execute("PRAGMA busy_timeout=5000")
            result = self.library.con.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            self.assertEqual(result[0], 0)
        finally:
            stop.set()
            reader.join()

    def test_paged_reads_return_everything(self):
        for number in range(25):
            self.library.title(f"More {number}")
        catalogue = Catalogue(self.db_path, page_rows=7, page_pause=0)
        catalogue.check_schema()
        rows = list(catalogue.all_images(["movie"]))
        self.assertEqual(len(rows), 26 * 4)
        self.assertEqual(len({row[0] for row in rows}), 26 * 4)

    def test_missing_required_column_is_reported(self):
        self.library.con.execute("ALTER TABLE media_images RENAME COLUMN image_index TO position")
        with self.assertRaises(warmer.SchemaError):
            Catalogue(self.db_path).check_schema()

    def test_optional_tables_can_be_absent(self):
        for table in ("media_metrics", "media_tags", "media_relations", "user_media_state"):
            self.library.con.execute(f"DROP TABLE {table}")
        catalogue = Catalogue(self.db_path)
        catalogue.check_schema()
        config = Config(db_path=self.db_path)
        snapshot = read_snapshot(catalogue, config)
        self.assertEqual(snapshot.recent, [])
        self.assertTrue(build_plan(snapshot, config))

    def test_optional_columns_of_media_can_be_absent(self):
        library = self.library
        library.item("Drama", "genre")
        library.con.execute(
            "INSERT INTO media_relations (relation_id, left_media_id, right_media_id, role) VALUES ('r', ?, ?, 'genre')",
            (blob("Film"), blob("Drama")),
        )
        for column in ("created_at", "digital_released_at", "rating_audience", "original_language", "title"):
            library.con.execute(f"ALTER TABLE media DROP COLUMN {column}")
        catalogue = Catalogue(self.db_path)
        catalogue.check_schema()
        config = Config(db_path=self.db_path)
        self.assertEqual({job.item for job in build_plan(read_snapshot(catalogue, config), config)}, {blob("Film")})

    def test_missing_database_raises_the_error_the_worker_handles(self):
        with self.assertRaises(sqlite3.OperationalError):
            Catalogue(os.path.join(self.folder.name, "nothing.sqlite")).check_schema()


class RuleTests(unittest.TestCase):
    def title(self, **values):
        title = warmer.Title(id=b"x", kind="movie", rating=values.pop("rating", None), language=values.pop("language", ""))
        for name, value in values.items():
            setattr(title, name, value)
        return title

    def rules(self, *rules, mode="all"):
        return {"match_mode": "all", "groups": [{"match_mode": mode, "rules": list(rules)}]}

    def test_tag_and_genre(self):
        title = self.title(tags={"netflix"}, genres={"comedy"})
        self.assertTrue(row_matches(self.rules({"field": "tag", "op": "in", "values": ["Netflix"]}), title))
        self.assertFalse(row_matches(self.rules({"field": "tag", "op": "in", "values": ["stan"]}), title))
        self.assertFalse(row_matches(self.rules({"field": "genre", "op": "not_in", "values": ["comedy"]}), title))

    def test_all_and_any(self):
        title = self.title(tags={"a"})
        both = ({"field": "tag", "op": "in", "values": ["a"]}, {"field": "tag", "op": "in", "values": ["b"]})
        self.assertFalse(row_matches(self.rules(*both), title))
        self.assertTrue(row_matches(self.rules(*both, mode="any"), title))

    def test_numbers_and_language(self):
        self.assertTrue(row_matches(self.rules({"field": "rating_audience", "op": "gt", "value": 7}), self.title(rating=8.0)))
        self.assertFalse(row_matches(self.rules({"field": "rating_audience", "op": "gt", "value": 7}), self.title()))
        self.assertTrue(row_matches(self.rules({"field": "original_language", "op": "in", "values": ["KO"]}), self.title(language="ko")))

    def test_unknown_rule_does_not_narrow_the_row(self):
        self.assertTrue(row_matches(self.rules({"field": "studio", "op": "in", "values": ["x"]}), self.title()))

    def test_row_with_no_matches_falls_back_to_its_whole_kind(self):
        titles = {b"a": warmer.Title(id=b"a", kind="movie", daily=1.0), b"b": warmer.Title(id=b"b", kind="series", daily=9.0)}
        row = warmer.HomeRow("movie", self.rules({"field": "tag", "op": "in", "values": ["none"]}), "PopularityDay")
        self.assertEqual(row_items(row, titles, 10), [b"a"])

    def test_sort_field(self):
        titles = {
            b"a": warmer.Title(id=b"a", kind="movie", daily=1.0, digital="2026-01-01"),
            b"b": warmer.Title(id=b"b", kind="movie", daily=5.0, digital="2020-01-01"),
        }
        self.assertEqual(row_items(warmer.HomeRow("movie", None, "PopularityDay"), titles, 10), [b"b", b"a"])
        self.assertEqual(row_items(warmer.HomeRow("movie", None, "DigitalReleaseDate"), titles, 10), [b"a", b"b"])


class PlanTests(TempCase):
    def test_tiers_follow_the_agreed_order(self):
        library = self.library
        library.row("Trending", "movie", "TrendingWeek")
        library.title("Hero", weekly=9.0, daily=1.0)
        library.title("Popular", daily=8.0)
        library.title("Tail", daily=0.5)
        library.title("Show", kind="series", daily=2.0)
        library.item("S1", "season", parent="Show", idx=1)
        library.art("S1", "primary")
        library.item("E1", "episode", parent="S1", grandparent="Show", idx=1, parent_idx=1)
        library.art("E1", "primary")
        library.item("Actor", "person")
        library.art("Actor", "primary")
        library.con.execute(
            "INSERT INTO media_relations (relation_id, left_media_id, right_media_id, role) VALUES ('r', ?, ?, 'actor')",
            (blob("Popular"), blob("Actor")),
        )
        jobs = self.plan(home_row_items=1, people=True, episodes=True)
        tiers = {(job.item, job.form): job.tier for job in jobs}
        self.assertEqual(tiers[(blob("Hero"), POSTER_342)], TIER_HOME)
        self.assertEqual(tiers[(blob("Hero"), BACKDROP_0)], TIER_HOME)
        self.assertEqual(tiers[(blob("Popular"), POSTER_342)], TIER_TITLES)
        self.assertEqual(tiers[(blob("Popular"), LOGO_PLAIN)], TIER_TITLES)
        self.assertEqual(tiers[(blob("Popular"), BACKDROP_0)], TIER_TITLE_ART)
        self.assertEqual(tiers[(blob("S1"), POSTER_500)], TIER_SEASONS)
        self.assertEqual(tiers[(blob("Actor"), POSTER_342)], TIER_PEOPLE)
        self.assertEqual(tiers[(blob("E1"), POSTER_500)], TIER_EPISODES)

    def test_popularity_orders_a_tier(self):
        self.library.title("Low", daily=1.0)
        self.library.title("High", daily=9.0)
        jobs = sorted((job for job in self.plan() if job.form == POSTER_342), key=lambda job: job.rank)
        self.assertEqual([job.item for job in jobs], [blob("High"), blob("Low")])

    def test_logo_jobs_only_for_rows_that_exist(self):
        self.library.title("With", art=("primary", "logo"))
        self.library.title("Without", art=("primary",))
        self.library.item("S", "season", parent="With", idx=1)
        self.library.art("S", "primary")
        self.library.item("E", "episode", parent="S", grandparent="With", idx=1, parent_idx=1)
        self.library.art("E", "primary")
        logos = {job.item for job in self.plan() if job.form in (LOGO_PLAIN, LOGO_TAGGED)}
        self.assertEqual(logos, {blob("With")})

    def test_both_logo_forms_and_only_the_plain_one_is_untagged(self):
        self.library.title("Film", art=("logo",))
        jobs = {job.form: job for job in self.plan()}
        self.assertEqual(jobs[LOGO_PLAIN].tag, b"")
        self.assertEqual(jobs[LOGO_TAGGED].tag, blob("Film/logo"))

    def test_kinds_not_worth_warming_are_left_out(self):
        self.library.item("Programme", "tv_program")
        self.library.art("Programme", "primary")
        self.library.item("Channel", "tv_channel")
        self.library.art("Channel", "primary")
        self.library.item("Blank collection", "collection", promoted=1)
        self.assertEqual(self.plan(), [])
        self.assertEqual({job.item for job in self.plan(channels=True)}, {blob("Channel")})

    def test_collection_image_is_only_asked_for_when_a_row_exists(self):
        self.library.item("With image", "collection")
        self.library.art("With image", "primary")
        self.library.item("No image", "collection")
        self.assertEqual({job.item for job in self.plan()}, {blob("With image")})

    def test_people_and_episodes_are_off_unless_asked_for(self):
        self.library.title("Show", kind="series")
        self.library.item("S", "season", parent="Show", idx=1)
        self.library.art("S", "primary")
        self.library.item("E", "episode", parent="S", grandparent="Show", idx=1, parent_idx=1)
        self.library.art("E", "primary")
        self.library.item("Actor", "person")
        self.library.art("Actor", "primary")
        base = {blob("Show"), blob("S")}
        self.assertEqual(base, {job.item for job in self.plan()})
        self.assertEqual(base | {blob("Actor")}, {job.item for job in self.plan(people=True)})
        self.assertEqual(base | {blob("E")}, {job.item for job in self.plan(episodes=True)})

    def test_titles_get_every_form_clients_ask_for_and_one_backdrop_form(self):
        self.library.row("Trending", "movie", "TrendingWeek")
        self.library.title("Film", weekly=9.0)
        self.library.title("Other", weekly=1.0)
        jobs = self.plan(home_row_items=1)
        forms = {job.form for job in jobs if job.item == blob("Film")}
        self.assertEqual(forms, {POSTER_342, POSTER_500, POSTER_FULL, THUMB_342, THUMB_500, THUMB_FULL, BACKDROP_0, LOGO_TAGGED, LOGO_PLAIN})
        self.assertNotIn(BACKDROP, forms)

    def test_thumbs_outside_home_rows_go_in_the_title_art_stage(self):
        self.library.row("Trending", "movie", "TrendingWeek")
        self.library.title("Film", weekly=9.0)
        self.library.title("Other", weekly=1.0)
        jobs = self.plan(home_row_items=1)
        thumbs = {job.form: job.tier for job in jobs if job.item == blob("Other") and job.form.image == "Thumb"}
        self.assertEqual(thumbs, {THUMB_342: TIER_TITLE_ART, THUMB_500: TIER_TITLE_ART, THUMB_FULL: TIER_TITLE_ART})
        self.assertEqual(len([job for job in jobs if job.item == blob("Other")]), 9)

    def test_new_rows_bring_their_thumbs_too(self):
        self.library.title("Film")
        catalogue = Catalogue(self.db_path)
        catalogue.check_schema()
        jobs = new_row_jobs(catalogue.images_after(["movie"], 0))
        self.assertEqual(len(jobs), 9)
        self.assertEqual({job.form for job in jobs if job.form.image == "Thumb"}, {THUMB_342, THUMB_500, THUMB_FULL})
        self.assertEqual({job.tier for job in jobs}, {TIER_NEW})

    def show_with_episodes(self):
        self.library.title("Low", kind="series", daily=1.0, art=())
        self.library.title("High", kind="series", daily=9.0, art=())
        for show in ("Low", "High"):
            self.library.item(f"{show} S1", "season", parent=show, idx=1)
            for number in (1, 2):
                self.library.item(f"{show} E{number}", "episode", parent=f"{show} S1", grandparent=show, idx=number, parent_idx=1)
                self.library.art(f"{show} E{number}", "primary")

    def test_full_size_stills_are_a_stage_of_their_own(self):
        self.show_with_episodes()
        jobs = self.plan(episodes=True)
        list_form = {(job.item, job.tier) for job in jobs if job.form == POSTER_500}
        full = sorted((job for job in jobs if job.form == POSTER_FULL), key=lambda job: job.rank)
        self.assertEqual({tier for _, tier in list_form}, {TIER_EPISODES})
        self.assertEqual({job.tier for job in full}, {warmer.TIER_FULL_STILLS})
        # Most popular show first, in episode order, as in the stage before it.
        self.assertEqual([job.item for job in full], [blob("High E1"), blob("High E2"), blob("Low E1"), blob("Low E2")])
        self.assertEqual({job.tag for job in full if job.item == blob("High E1")}, {blob("High E1/primary")})

    def test_no_full_size_stills_with_episodes_off(self):
        self.show_with_episodes()
        self.library.con.execute(
            "INSERT INTO user_media_state (user_id, media_id, last_played_at) VALUES (x'00', ?, '2026-01-01')", (blob("Low E1"),)
        )
        forms = {job.form for job in self.plan() if job.item in (blob("Low E1"), blob("Low E2"))}
        self.assertEqual(forms, {POSTER_500})

    def test_an_episode_on_the_home_screen_still_leaves_its_full_size_to_the_last_stage(self):
        self.show_with_episodes()
        self.library.con.execute(
            "INSERT INTO user_media_state (user_id, media_id, last_played_at) VALUES (x'00', ?, '2026-01-01')", (blob("Low E1"),)
        )
        tiers = {job.form: job.tier for job in self.plan(episodes=True) if job.item == blob("Low E1")}
        self.assertEqual(tiers, {POSTER_500: TIER_HOME, POSTER_FULL: warmer.TIER_FULL_STILLS})

    def test_the_printed_plan_follows_the_stages(self):
        self.show_with_episodes()
        self.library.programme("On now", -60, 600)
        self.library.item("Channel", "tv_channel")
        self.library.art("Channel", "primary")
        self.library.item("Actor", "person")
        self.library.art("Actor", "primary")
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            warmer.print_plan(self.config(episodes=True, people=True, channels=True, guide=True))
        names = [line.split()[0] for line in out.getvalue().splitlines()[1:] if line.split()[0] in warmer.TIER_NAMES.values()]
        self.assertEqual(names, ["channels", "guide", "people", "episodes", "full-stills"])

    def test_a_plan_holds_one_row_at_a_time(self):
        for number in range(5):
            self.library.title(f"Film {number}", art=("primary",))
        config = self.config()
        catalogue = Catalogue(config.db_path)
        catalogue.check_schema()
        snapshot = read_snapshot(catalogue, config)
        read = []

        def counted():
            for row in snapshot.images:
                read.append(row)
                yield row

        jobs = warmer.plan_jobs(snapshot._replace(images=counted()), config)
        next(jobs)
        self.assertEqual(len(read), 1)

    def test_odd_rows_cost_only_themselves(self):
        self.library.title("Fine", daily=2.0)
        self.library.title("Odd numbers", daily="lots")
        library = self.library
        # A filter that isn't an object, a sort that isn't a string, and a filter whose values don't compare.
        library.item("Row 1", "collection", promoted=1, sort_order=0, collection_media_kind="movie", collection_smart_filter="[1]")
        library.item("Row 2", "collection", promoted=1, sort_order=1, collection_media_kind="movie", collection_default_sort='[{"by": "x"}]')
        library.item(
            "Row 3", "collection", promoted=1, sort_order=2, collection_media_kind="movie",
            collection_smart_filter='{"groups": [{"rules": [{"field": "rating_audience", "op": "gt", "value": "high"}]}]}',
        )
        library.item("Row 4", "collection", promoted=1, sort_order=3, collection_media_kind="movie", collection_smart_filter='{"groups": 7}')
        library.item("Text id", "movie")
        library.con.execute(
            "INSERT INTO media_images (id, media_id, image_type, image_index, path) VALUES ('11111111-2222-3333-4444-555555555555', ?, 'primary', 0, 'x')",
            (blob("Text id"),),
        )
        library.item("Short id", "movie")
        library.con.execute(
            "INSERT INTO media_images (id, media_id, image_type, image_index, path) VALUES (x'0011223344556677', ?, 'primary', 0, 'x')",
            (blob("Short id"),),
        )
        library.con.execute("UPDATE media SET parent_idx = 'x' WHERE id = ?", (blob("Fine"),))
        config = self.config()
        catalogue = Catalogue(config.db_path)
        catalogue.check_schema()
        counts = {}
        jobs = list(warmer.plan_jobs(read_snapshot(catalogue, config), config, collections_counter(counts)))
        items = {job.item for job in jobs}
        self.assertIn(blob("Fine"), items)
        self.assertNotIn(blob("Text id"), items)
        self.assertNotIn(blob("Short id"), items)
        self.assertGreaterEqual(counts["counter"]["skipped_rows"], 2)

    def test_recently_played_episode_brings_its_series_and_next_episodes_home(self):
        library = self.library
        library.title("Show", kind="series", art=("primary", "backdrop", "logo"))
        library.item("S1", "season", parent="Show", idx=1)
        for number in (1, 2, 3, 4):
            library.item(f"E{number}", "episode", parent="S1", grandparent="Show", idx=number, parent_idx=1)
            library.art(f"E{number}", "primary")
        library.con.execute(
            "INSERT INTO user_media_state (user_id, media_id, last_played_at) VALUES (x'00', ?, '2026-01-01')", (blob("E1"),)
        )
        jobs = self.plan()
        home = {job.item for job in jobs if job.tier == TIER_HOME}
        self.assertEqual(home, {blob("Show"), blob("E1"), blob("E2"), blob("E3")})
        fallback = [job for job in jobs if job.form in (THUMB_342, THUMB_500, THUMB_FULL)]
        self.assertEqual(len(fallback), 3)
        self.assertEqual({job.tag for job in fallback}, {blob("Show/backdrop")})
        self.assertEqual({job.item for job in fallback}, {blob("Show")})

    def test_series_with_a_thumb_row_uses_it_not_the_fallback(self):
        self.library.title("Show", kind="series")
        self.library.item("S1", "season", parent="Show", idx=1)
        self.library.item("E1", "episode", parent="S1", grandparent="Show", idx=1, parent_idx=1)
        self.library.con.execute(
            "INSERT INTO user_media_state (user_id, media_id, last_played_at) VALUES (x'00', ?, '2026-01-01')", (blob("E1"),)
        )
        thumbs = [job for job in self.plan() if job.form == THUMB_500]
        self.assertEqual([job.tag for job in thumbs], [blob("Show/thumb")])

    def test_trending_rows_come_before_other_rows(self):
        self.library.row("Trending", "movie", "TrendingWeek", order=9)
        self.library.row("Popular", "movie", "PopularityDay", order=0)
        self.library.title("A", weekly=9.0, daily=1.0)
        self.library.title("B", weekly=1.0, daily=9.0)
        jobs = [job for job in self.plan(home_row_items=1) if job.form == POSTER_342]
        by_item = {job.item: job.rank for job in jobs}
        self.assertLess(by_item[blob("A")], by_item[blob("B")])

    def test_new_rows_put_titles_first(self):
        self.library.title("Film")
        self.library.item("S", "season", parent="Film", idx=1)
        self.library.art("S", "primary")
        self.library.item("Actor", "person")
        self.library.art("Actor", "primary")
        catalogue = Catalogue(self.db_path)
        catalogue.check_schema()
        jobs = new_row_jobs(catalogue.images_after(["movie", "season", "person"], 0))
        tiers = {job.item: job.tier for job in jobs}
        self.assertEqual(tiers[blob("Film")], TIER_NEW)
        self.assertEqual(tiers[blob("S")], TIER_NEW)
        self.assertEqual(tiers[blob("Actor")], TIER_PEOPLE)


class StateTests(TempCase):
    def setUp(self):
        super().setUp()
        self.state = State(os.path.join(self.folder.name, "state.sqlite"))
        self.addCleanup(self.state.close)
        self.a = Job(blob("a"), POSTER_342, blob("a-tag"), TIER_TITLES, 2)
        self.b = Job(blob("b"), POSTER_342, blob("b-tag"), TIER_HOME, 1)
        self.c = Job(blob("c"), LOGO_PLAIN, b"", TIER_TITLES, 1)
        self.settings = Config()

    def done(self, job, outcome=warmer.CACHED, now=1000, result=None):
        self.state.record([(job, outcome, result or Result(200, "MISS", size=500))], self.settings, now)

    def test_order_is_tier_then_rank(self):
        self.state.apply_plan([self.a, self.b, self.c])
        self.assertEqual([job.key for job in self.state.next_jobs(10, ALL_TIERS)], [self.b.key, self.c.key, self.a.key])

    def one_job_in_each_stage(self):
        return [Job(blob(f"stage {tier}"), POSTER_342, blob("tag"), tier, 1) for tier in ALL_TIERS]

    def test_stage_numbers_are_stable_because_they_are_stored_on_disk(self):
        stages = (
            warmer.TIER_NEW, warmer.TIER_HOME, warmer.TIER_TITLES, warmer.TIER_TITLE_ART, warmer.TIER_SEASONS,
            warmer.TIER_PEOPLE, warmer.TIER_EPISODES, warmer.TIER_CHANNELS, warmer.TIER_GUIDE, warmer.TIER_FULL_STILLS,
        )
        self.assertEqual(stages, tuple(range(1, 11)))

    def test_stages_are_worked_in_the_agreed_order(self):
        self.state.apply_plan(self.one_job_in_each_stage())
        order = [warmer.TIER_NAMES[job.tier] for job in self.state.next_jobs(100, ALL_TIERS)]
        self.assertEqual(
            order,
            ["new", "home", "titles", "title-art", "channels", "guide", "seasons", "people", "episodes", "full-stills"],
        )

    def test_a_few_jobs_at_a_time_and_only_from_the_stages_asked_for(self):
        self.state.apply_plan(self.one_job_in_each_stage())
        self.assertEqual([job.tier for job in self.state.next_jobs(2, ALL_TIERS)], [warmer.TIER_NEW, warmer.TIER_HOME])
        chosen = self.state.next_jobs(100, {warmer.TIER_PEOPLE, warmer.TIER_CHANNELS})
        self.assertEqual([job.tier for job in chosen], [warmer.TIER_CHANNELS, warmer.TIER_PEOPLE])

    def test_over_budget_a_stage_only_keeps_up_what_it_has_stored(self):
        stored = Job(blob("stored"), POSTER_342, blob("t"), warmer.TIER_PEOPLE, 5)
        unfetched = Job(blob("unfetched"), POSTER_342, blob("t"), warmer.TIER_PEOPLE, 1)
        self.state.apply_plan([stored, unfetched])
        self.done(stored, now=1000)
        self.state.revive(1000 + 121 * 86400, self.settings)
        keep = self.state.next_jobs(10, {warmer.TIER_TITLES}, upkeep={warmer.TIER_PEOPLE})
        self.assertEqual([job.key for job in keep], [stored.key])
        plan = self.state.con.execute(
            f"EXPLAIN QUERY PLAN SELECT item FROM jobs WHERE pending = 1 AND status = {warmer.STATUS_CACHED} AND tier = ? ORDER BY rank LIMIT 9", (6,)
        ).fetchone()[3]
        self.assertIn("jobs_upkeep", plan)

    def test_max_tier_is_respected(self):
        self.state.apply_plan([self.a, self.b])
        self.assertEqual([job.key for job in self.state.next_jobs(10, [TIER_HOME])], [self.b.key])

    def test_finished_job_stays_finished_through_a_new_plan(self):
        self.state.apply_plan([self.a, self.b])
        self.done(self.a)
        self.state.apply_plan([self.a._replace(rank=9), self.b])
        self.assertEqual([job.key for job in self.state.next_jobs(10, ALL_TIERS)], [self.b.key])

    def test_changed_art_drops_the_old_tag_and_adds_the_new_one(self):
        self.state.apply_plan([self.a])
        self.done(self.a)
        changed = self.a._replace(tag=blob("new-tag"))
        removed = self.state.apply_plan([changed])
        self.assertEqual(removed, 1)
        self.assertEqual([job.tag for job in self.state.next_jobs(10, ALL_TIERS)], [blob("new-tag")])

    def test_row_that_went_away_is_dropped(self):
        self.state.apply_plan([self.a, self.b])
        self.state.apply_plan([self.b])
        self.assertEqual(self.state.planned(), 1)

    def test_known_404s_are_retried_ever_more_rarely(self):
        self.state.apply_plan([self.a])
        now = 1000
        delays = []
        for _ in range(5):
            self.state.record([(self.a, warmer.MISSING, Result(404))], self.settings, now)
            row = self.state.con.execute("SELECT next_at, pending, status FROM jobs").fetchone()
            self.assertEqual(row[1:], (0, warmer.STATUS_MISSING))
            delays.append(row[0] - now)
        self.assertEqual(delays, [3600, 86400, 7 * 86400, 30 * 86400, 30 * 86400])

    def test_uncacheable_waits_a_long_time(self):
        self.state.apply_plan([self.a])
        self.done(self.a, warmer.UNCACHEABLE)
        self.assertEqual(self.state.con.execute("SELECT next_at FROM jobs").fetchone()[0], 1000 + 30 * 86400)

    def test_revive_only_when_due(self):
        self.state.apply_plan([self.a])
        self.done(self.a)
        self.assertEqual(self.state.revive(1001, self.settings), 0)
        self.assertEqual(self.state.revive(1000 + int(50 * 86400), self.settings), 1)
        self.assertEqual(len(self.state.next_jobs(10, ALL_TIERS)), 1)

    def test_touch_times_by_kind_of_job(self):
        settings = self.settings
        self.assertEqual(warmer.touch_after(self.a, settings), 50 * 86400)
        self.assertEqual(warmer.touch_after(self.b, settings), 7 * 86400)
        self.assertEqual(warmer.touch_after(self.c, settings), 25 * 86400)

    def test_touch_and_lifetime_stay_inside_nginx_settings(self):
        # 60 days inactive, 120 days of life, 30 days for an untagged logo. Each touch must land inside them.
        settings = self.settings
        self.assertLess(settings.touch_days, 60)
        self.assertLess(settings.touch_home_days, 60)
        self.assertLess(settings.touch_untagged_days, 30)

    def test_restart_keeps_the_work_done(self):
        path = os.path.join(self.folder.name, "again.sqlite")
        first = State(path)
        first.apply_plan([self.a, self.b])
        first.record([(self.a, warmer.CACHED, Result(200, "MISS", size=5))], self.settings, 1000)
        first.close()
        second = State(path)
        self.addCleanup(second.close)
        self.assertEqual([job.key for job in second.next_jobs(10, ALL_TIERS)], [self.b.key])
        self.assertEqual(second.cached_bytes(), 5)

    def test_only_uncacheable_art_is_skipped_by_a_signal(self):
        self.state.apply_plan([self.a, self.b])
        self.assertFalse(self.state.is_uncacheable(self.a, 1001))
        self.done(self.a)
        self.assertFalse(self.state.is_uncacheable(self.a, 1001))
        self.done(self.b, warmer.UNCACHEABLE)
        self.assertTrue(self.state.is_uncacheable(self.b, 1001))
        self.assertFalse(self.state.is_uncacheable(self.b, 1000 + 31 * 86400))

    def row(self, job):
        names = ("pending", "status", "tries", "strikes", "next_at", "fetched_at")
        values = self.state.con.execute(
            f"SELECT {', '.join(names)} FROM jobs WHERE item = ? AND form = ? AND tag = ?", job.key
        ).fetchone()
        return dict(zip(names, values))

    def test_a_refusal_comes_back_in_hours_and_is_not_kept_as_a_fact(self):
        self.state.apply_plan([self.a])
        self.state.record([(self.a, warmer.REFUSED, Result(401))], self.settings, 1000)
        row = self.row(self.a)
        self.assertEqual(row["next_at"], 1000 + warmer.REFUSED_RETRY)
        self.assertEqual(row["status"], 0)
        self.assertFalse(self.state.is_uncacheable(self.a, 1001))

    def test_a_doubted_404_does_not_count_against_the_row(self):
        self.state.apply_plan([self.a])
        self.state.record([(self.a, warmer.SUSPECT, Result(404))], self.settings, 1000)
        row = self.row(self.a)
        self.assertEqual((row["tries"], row["status"], row["strikes"]), (0, 0, 1))
        self.assertEqual(row["next_at"], 1000 + warmer.SUSPECT_RETRY)

    def test_a_row_doubted_again_and_again_is_believed_in_the_end(self):
        self.state.apply_plan([self.a])
        for _ in range(warmer.SUSPECT_LIMIT):
            self.state.record([(self.a, warmer.SUSPECT, Result(404))], self.settings, 1000)
        row = self.row(self.a)
        self.assertEqual((row["status"], row["tries"], row["strikes"]), (warmer.STATUS_MISSING, 1, 0))
        self.assertEqual(row["next_at"], 1000 + warmer.MISSING_RETRY[0])

    def test_a_hit_keeps_the_age_and_a_fetch_resets_it(self):
        self.state.apply_plan([self.a])
        self.done(self.a, now=1000)
        self.assertEqual(self.row(self.a)["fetched_at"], 1000)
        self.done(self.a, now=2000, result=Result(200, "HIT", size=500))
        self.assertEqual(self.row(self.a)["fetched_at"], 1000)
        self.done(self.a, now=3000, result=Result(200, "EXPIRED", size=500))
        self.assertEqual(self.row(self.a)["fetched_at"], 3000)

    def test_a_first_hit_is_taken_as_new(self):
        self.state.apply_plan([self.a])
        self.done(self.a, now=1000, result=Result(200, "HIT", size=500))
        self.assertEqual(self.row(self.a)["fetched_at"], 1000)

    def test_the_last_look_lands_just_after_the_entry_expires(self):
        settings = self.settings
        life = settings.life_days * 86400
        margin = warmer.EXPIRY_MARGIN
        # Touches every 50 days until the last one, which is the one that fetches it again.
        self.assertEqual(warmer.next_look(self.a, 1000, 1000, settings), 1000 + 50 * 86400)
        self.assertEqual(warmer.next_look(self.a, 1000, 1000 + 100 * 86400, settings), 1000 + life + margin)
        # The untagged logo has a shorter life.
        self.assertEqual(warmer.next_look(self.c, 1000, 1000 + 25 * 86400, settings), 1000 + 30 * 86400 + margin)
        # An answer from an entry already past its life doesn't ask again straight away.
        self.assertEqual(warmer.next_look(self.a, 1000, 1000 + life + margin, settings), 1000 + life + margin + 3600)

    def expired_share(self, job, days=1000):
        """Replays a job's schedule against an entry with nginx's life, and says how much of the time it's expired."""
        settings = self.settings
        life = warmer.entry_life(job, settings)
        fetched = now = 0
        expired = 0
        refetches = 0
        horizon = days * 86400
        while now < horizon:
            look = warmer.next_look(job, fetched, now, settings)
            if look > fetched + life:
                expired += look - (fetched + life)
                fetched = look
                refetches += 1
            now = look
        return expired / horizon, refetches

    def test_kept_art_is_hardly_ever_expired(self):
        others = [self.a._replace(tier=tier) for tier in (TIER_SEASONS, TIER_PEOPLE, TIER_EPISODES, warmer.TIER_CHANNELS, warmer.TIER_FULL_STILLS)]
        for job in (self.a, self.b, self.c, *others):
            share, refetches = self.expired_share(job)
            self.assertLess(share, 0.001, job.form)
            self.assertGreater(refetches, 0)

    def test_only_guide_art_lapses_at_the_end_of_its_life(self):
        guide = Job(blob("g"), POSTER_342, blob("g-tag"), warmer.TIER_GUIDE, 1)
        self.state.apply_plan([guide, self.a])
        self.done(guide, now=1000)
        self.done(self.a, now=1000)
        # Still touched while it lasts.
        self.assertEqual(self.state.revive(1000 + 50 * 86400, self.settings), 2)
        self.done(guide, now=1000 + 100 * 86400, result=Result(200, "HIT", size=500))
        self.done(self.a, now=1000 + 100 * 86400, result=Result(200, "HIT", size=500))
        end = 1000 + 120 * 86400 + warmer.EXPIRY_MARGIN
        self.assertEqual(self.state.revive(end, self.settings), 1)
        self.assertEqual(self.row(guide)["status"], warmer.STATUS_LAPSED)
        self.assertEqual(self.row(guide)["pending"], 0)
        self.assertEqual(self.state.tiers()["guide"]["lapsed"], 1)
        self.assertEqual(self.state.cached_bytes(), 500)
        # A signal can still bring it back.
        self.assertFalse(self.state.is_uncacheable(guide, end))
        self.done(guide, now=end, result=Result(200, "MISS", size=500))
        self.assertEqual(self.row(guide)["status"], warmer.STATUS_CACHED)

    def test_every_other_stage_is_fetched_again_when_its_life_ends(self):
        tiers = (TIER_SEASONS, TIER_PEOPLE, TIER_EPISODES, warmer.TIER_CHANNELS, warmer.TIER_FULL_STILLS)
        jobs = [Job(blob(f"item {tier}"), POSTER_342, blob("t"), tier, 1) for tier in tiers]
        self.state.apply_plan(jobs)
        for job in jobs:
            self.done(job, now=1000)
        # Two touches, each a hit that keeps the entry's age, then the look that fetches it again.
        for days in (50, 100):
            self.assertEqual(self.state.revive(1000 + days * 86400, self.settings), len(jobs))
            for job in jobs:
                self.done(job, now=1000 + days * 86400, result=Result(200, "HIT", size=500))
                self.assertEqual(self.row(job)["fetched_at"], 1000)
        end = 1000 + 120 * 86400 + warmer.EXPIRY_MARGIN
        self.assertEqual(self.state.revive(end, self.settings), len(jobs))
        self.assertEqual({job.tier for job in self.state.next_jobs(100, ALL_TIERS)}, set(tiers))
        for job in jobs:
            self.assertEqual(self.row(job)["status"], warmer.STATUS_CACHED)
            self.done(job, now=end, result=Result(200, "EXPIRED", size=500))
            self.assertEqual(self.row(job)["fetched_at"], end)
            self.assertEqual(self.row(job)["next_at"], end + 50 * 86400)
        self.assertEqual(self.state.cached_bytes(), 500 * len(jobs))

    def test_lapsed_art_comes_back_with_the_next_plan_unless_it_is_guide_art(self):
        guide = Job(blob("g"), POSTER_342, blob("g-tag"), warmer.TIER_GUIDE, 1)
        self.state.apply_plan([guide])
        self.done(guide, now=1000)
        self.state.revive(1000 + 121 * 86400, self.settings)
        self.assertEqual(self.row(guide)["status"], warmer.STATUS_LAPSED)
        self.state.apply_plan([guide])
        self.assertEqual(self.row(guide)["pending"], 0)
        # Numbers above the guide's and below it alike, as the stored numbers don't follow the order of stages.
        for tier in (TIER_HOME, warmer.TIER_FULL_STILLS):
            self.state.apply_plan([guide])
            self.state.con.execute("UPDATE jobs SET pending = 0")
            self.state.apply_plan([guide._replace(tier=tier)])
            self.assertEqual(self.row(guide)["pending"], 1)

    def test_a_failed_write_leaves_nothing_half_done(self):
        self.state.apply_plan([self.a, self.b])
        with self.assertRaises(ValueError):
            with self.state._transaction():
                self.state.put("half", 1)
                raise ValueError
        self.assertIsNone(self.state.get("half"))
        self.done(self.a)
        self.assertEqual(self.row(self.a)["status"], warmer.STATUS_CACHED)


class PaceTests(unittest.TestCase):
    def setUp(self):
        self.pace = Pace(Config())

    def test_failures_back_off_exponentially_to_a_limit(self):
        pauses = []
        for _ in range(9):
            self.pace.record(warmer.BACKOFF, Result(503), 1000.0)
            pauses.append(self.pace.pause_until - 1000.0)
        self.assertEqual(pauses[:4], [5.0, 10.0, 20.0, 40.0])
        self.assertEqual(pauses[-1], 300.0)

    def test_another_hosts_image_does_not_reset_the_back_off_while_the_source_is_in_doubt(self):
        self.pace.record(warmer.BACKOFF, Result(503), 1000.0)
        self.pace.recent.update({(bytes([n]), "primary"): True for n in range(warmer.BREAKER_TRIPS)})
        self.assertTrue(self.pace.source_suspect)
        self.pace.record(warmer.CACHED, Result(200), 1001.0, third_party=True)
        self.assertEqual(self.pace.failures, 1)
        self.pace.record(warmer.CACHED, Result(200), 1002.0)
        self.assertEqual(self.pace.failures, 0)

    def test_retry_after_is_respected(self):
        self.pace.record(warmer.BACKOFF, Result(429, retry_after=120.0), 1000.0)
        self.assertEqual(self.pace.pause_until, 1120.0)

    def test_success_clears_the_failure_count(self):
        self.pace.record(warmer.BACKOFF, Result(503), 1000.0)
        self.pace.record(warmer.CACHED, Result(200, "MISS", elapsed=0.3), 1000.0)
        self.pace.record(warmer.BACKOFF, Result(503), 2000.0)
        self.assertEqual(self.pace.pause_until, 2005.0)

    def test_slow_answers_halve_the_speed_and_fast_ones_restore_it_slowly(self):
        self.pace.record(warmer.CACHED, Result(200, "MISS", elapsed=9.0), 1.0)
        self.assertEqual(self.pace.speed, 0.5)
        self.pace.record(warmer.CACHED, Result(200, "HIT", elapsed=2.0), 1.0)
        self.assertEqual(self.pace.speed, 0.25)
        self.pace.record(warmer.CACHED, Result(200, "MISS", elapsed=0.3), 1.0)
        self.assertAlmostEqual(self.pace.speed, 0.27)

    def test_speed_has_a_floor(self):
        for _ in range(20):
            self.pace.record(warmer.BACKOFF, Result(500), 1.0)
        self.assertEqual(self.pace.speed, warmer.MIN_SPEED)

    def test_a_hit_says_nothing_about_the_source(self):
        self.pace.record(warmer.BACKOFF, Result(503), 1000.0)
        self.pace.record(warmer.CACHED, Result(200, "HIT", elapsed=0.001), 1000.0)
        self.assertEqual(self.pace.failures, 1)
        self.assertAlmostEqual(self.pace.speed, 0.5 + warmer.HIT_RECOVERY)

    def test_quick_hits_bring_the_speed_back(self):
        for _ in range(3):
            self.pace.record(warmer.CACHED, Result(200, "HIT", elapsed=0.7), 1.0)
        self.assertEqual(self.pace.speed, 0.125)
        for _ in range(500):
            self.pace.record(warmer.CACHED, Result(200, "HIT", elapsed=0.001), 1.0)
        self.assertEqual(self.pace.speed, 1.0)

    def test_hits_recover_more_slowly_than_real_fetches(self):
        self.pace.speed = 0.5
        self.pace.record(warmer.CACHED, Result(200, "HIT", elapsed=0.001), 1.0)
        after_hit = self.pace.speed
        self.pace.speed = 0.5
        self.pace.record(warmer.CACHED, Result(200, "MISS", elapsed=0.3), 1.0)
        self.assertLess(after_hit, self.pace.speed)

    def test_a_stored_404_is_no_sign_of_recovery(self):
        self.pace.speed = 0.5
        self.pace.record(warmer.MISSING, Result(404, "HIT", elapsed=0.001), 1.0)
        self.assertEqual(self.pace.speed, 0.5)

    def test_a_run_of_quick_hits_ends_a_lone_failure(self):
        self.pace.record(warmer.BACKOFF, Result(503), 1000.0)
        for _ in range(warmer.CLEAN_FETCHES - 1):
            self.pace.record(warmer.CACHED, Result(200, "HIT", elapsed=0.001), 1000.0)
        self.assertEqual(self.pace.failures, 1)
        self.pace.record(warmer.CACHED, Result(200, "HIT", elapsed=0.001), 1000.0)
        self.assertEqual(self.pace.failures, 0)

    def test_a_run_of_hits_leaves_the_back_off_alone_while_the_source_is_in_doubt(self):
        self.pace.recent.update({number: True for number in range(warmer.BREAKER_TRIPS)})
        self.pace.record(warmer.SUSPECT, Result(404), 1000.0)
        for _ in range(warmer.CLEAN_FETCHES * 2):
            self.pace.record(warmer.CACHED, Result(200, "HIT", elapsed=0.001), 1000.0)
        self.assertEqual(self.pace.failures, 1)

    def test_a_new_failure_starts_that_run_again(self):
        self.pace.record(warmer.BACKOFF, Result(503), 1000.0)
        for _ in range(warmer.CLEAN_FETCHES - 1):
            self.pace.record(warmer.CACHED, Result(200, "HIT", elapsed=0.001), 1000.0)
        self.pace.record(warmer.BACKOFF, Result(503), 1000.0)
        self.pace.record(warmer.CACHED, Result(200, "HIT", elapsed=0.001), 1000.0)
        self.assertEqual(self.pace.failures, 2)


class BreakerTests(unittest.TestCase):
    def setUp(self):
        self.pace = Pace(Config())
        self.number = 0

    def job(self, **values):
        self.number += 1
        return Job(blob(f"item {self.number}"), POSTER_342, blob("t"), TIER_TITLES, 1)._replace(**values)

    def judge(self, outcome, cache="MISS", job=None):
        return self.pace.judge(job or self.job(), outcome, Result(404 if outcome == warmer.MISSING else 200, cache))

    def test_a_few_404s_are_believed(self):
        for _ in range(warmer.BREAKER_TRIPS - 1):
            self.assertEqual(self.judge(warmer.MISSING), warmer.MISSING)

    def test_many_404s_are_not(self):
        outcomes = [self.judge(warmer.MISSING) for _ in range(warmer.BREAKER_TRIPS)]
        self.assertEqual(outcomes[-1], warmer.SUSPECT)
        self.assertTrue(self.pace.source_suspect)

    def test_refusals_count_too(self):
        outcomes = [self.pace.judge(self.job(), warmer.REFUSED, Result(401)) for _ in range(warmer.BREAKER_TRIPS)]
        self.assertEqual(outcomes[0], warmer.REFUSED)
        self.assertEqual(outcomes[-1], warmer.SUSPECT)

    def test_one_title_with_all_its_art_dead_is_not_an_outage(self):
        item = blob("dead")
        forms = (POSTER_342, POSTER_500, POSTER_FULL, THUMB_500, THUMB_FULL, BACKDROP_0, LOGO_TAGGED, LOGO_PLAIN)
        for form in forms:
            self.assertEqual(self.judge(warmer.MISSING, job=Job(item, form, blob("t"))), warmer.MISSING)
        self.assertFalse(self.pace.source_suspect)

    def test_404s_among_plenty_of_good_fetches_are_believed(self):
        for number in range(40):
            outcome = warmer.MISSING if number % 4 == 0 else warmer.CACHED
            self.assertEqual(self.judge(outcome), outcome)

    def test_it_closes_after_enough_good_fetches_but_not_on_the_first(self):
        for _ in range(warmer.BREAKER_WINDOW):
            self.judge(warmer.MISSING)
        self.judge(warmer.CACHED)
        self.assertTrue(self.pace.source_suspect)
        for _ in range(warmer.BREAKER_TRIPS + 1):
            self.judge(warmer.CACHED)
        self.assertFalse(self.pace.source_suspect)

    def test_hits_do_not_close_it(self):
        for _ in range(warmer.BREAKER_WINDOW):
            self.judge(warmer.MISSING)
        for _ in range(50):
            self.judge(warmer.CACHED, "HIT")
        self.assertTrue(self.pace.source_suspect)

    def test_a_guess_never_counts(self):
        for _ in range(50):
            self.assertEqual(self.judge(warmer.MISSING, job=self.job(guess=True)), warmer.MISSING)
        self.assertFalse(self.pace.source_suspect)

    def test_guide_and_channel_art_comes_from_hosts_that_say_nothing_about_the_source(self):
        for tier in (warmer.TIER_GUIDE, warmer.TIER_CHANNELS):
            for _ in range(50):
                self.assertEqual(self.judge(warmer.MISSING, job=self.job(tier=tier)), warmer.MISSING)
            self.assertFalse(self.pace.source_suspect)

    def test_a_guide_image_nginx_cannot_store_is_left_alone(self):
        job = self.job(tier=warmer.TIER_GUIDE)
        self.assertEqual(self.pace.judge(job, warmer.SOFT, Result(200, "DIRECT", "image/webp")), warmer.UNCACHEABLE)
        self.assertEqual(self.pace.judge(self.job(), warmer.SOFT, Result(200, "DIRECT", "image/jpeg")), warmer.SOFT)

    def test_a_slow_third_party_host_does_not_slow_the_worker(self):
        self.pace.record(warmer.MISSING, Result(404, "MISS", elapsed=15.0), 1.0, True)
        self.pace.record(warmer.CACHED, Result(200, "MISS", elapsed=9.0), 1.0, True)
        self.assertEqual(self.pace.speed, 1.0)
        self.pace.record(warmer.CACHED, Result(200, "MISS", elapsed=9.0), 1.0)
        self.assertEqual(self.pace.speed, 0.5)

    def test_a_failure_on_a_third_party_job_still_backs_off(self):
        self.pace.record(warmer.BACKOFF, Result(502), 1000.0, True)
        self.assertEqual(self.pace.pause_until, 1005.0)

    def test_a_suspect_answer_backs_off_like_a_failure(self):
        self.pace.record(warmer.SUSPECT, Result(404), 1000.0)
        self.assertEqual(self.pace.pause_until, 1005.0)
        self.assertEqual(self.pace.speed, 0.5)


class BurstQueueTests(unittest.TestCase):
    def test_within_a_burst_arrival_order_and_the_newest_burst_first(self):
        queue = BurstQueue()
        queue.add(["a1"], 1)
        queue.add(["a2", "a3"], 1)
        queue.add(["b1"], 2)
        queue.add(["b2"], 2)
        self.assertEqual(len(queue), 5)
        self.assertEqual([queue.pop() for _ in range(5)], ["b1", "b2", "a1", "a2", "a3"])
        self.assertIsNone(queue.pop())
        self.assertEqual(len(queue), 0)

    def test_a_burst_whose_first_batch_was_already_fetched_stays_ahead_of_older_ones(self):
        queue = BurstQueue()
        queue.add(["old1", "old2", "old3"], 4)
        queue.add(["new1"], 5)
        self.assertEqual([queue.pop(), queue.pop()], ["new1", "old1"])
        queue.add(["new2"], 5)
        self.assertEqual([queue.pop() for _ in range(3)], ["new2", "old2", "old3"])

    def test_a_burst_keeps_its_own_order_while_it_has_work_left(self):
        queue = BurstQueue()
        queue.add(["old1"], 4)
        queue.add(["new1", "new2"], 5)
        self.assertEqual(queue.pop(), "new1")
        queue.add(["new3"], 5)
        self.assertEqual([queue.pop() for _ in range(3)], ["new2", "new3", "old1"])

    def test_it_stays_bounded_and_drops_the_oldest(self):
        queue = BurstQueue(capacity=3)
        queue.add(["a1", "a2"], 1)
        queue.add(["b1", "b2"], 2)
        self.assertEqual(len(queue), 3)
        self.assertEqual([queue.pop() for _ in range(3)], ["b1", "b2", "a1"])

    def test_nothing_added_opens_no_burst(self):
        queue = BurstQueue()
        queue.add(["a1"], 1)
        queue.add([], 1)
        queue.add(["a2"], 1)
        self.assertEqual([queue.pop(), queue.pop()], ["a1", "a2"])

    def test_a_burst_whose_first_batch_added_nothing_still_goes_first(self):
        queue = BurstQueue()
        queue.add(["a1"], 1)
        queue.add([], 2)
        queue.add(["b1"], 2)
        self.assertEqual([queue.pop(), queue.pop()], ["b1", "a1"])


class SignalQueueTests(TempCase):
    """The signal path with no threads running, so exactly what was queued can be looked at."""

    def setUp(self):
        super().setUp()
        self.worker = warmer.Warmer(self.config(signal_rate=10.0))
        self.addCleanup(self.worker.state.close)
        self.assertTrue(self.worker._ensure_schema())

    def signal(self, burst, *names, image_type="primary"):
        now = time.monotonic()
        self.worker._queue_burst(burst, [(blob(name), image_type, now, burst) for name in names])

    @staticmethod
    def drain(queue):
        jobs = []
        while len(queue):
            jobs.append(queue.pop())
        return jobs

    def queued(self):
        return self.drain(self.worker.urgent) + self.drain(self.worker.soon)

    def show(self, episodes=1, art=("primary", "logo", "backdrop", "thumb")):
        self.library.title("Show", kind="series", art=art)
        self.library.item("S1", "season", parent="Show", idx=1)
        names = [f"E{number}" for number in range(episodes)]
        for number, name in enumerate(names):
            self.library.item(name, "episode", parent="S1", grandparent="Show", idx=number, parent_idx=1)
            self.library.art(name, "primary")
        return names

    def broken(self, name):
        """Makes one catalogue read fail. Calling what this returns repairs it."""

        def fail(*_):
            raise sqlite3.OperationalError("database is locked")

        setattr(self.worker.catalogue, name, fail)
        return lambda: delattr(self.worker.catalogue, name)

    def capture_log(self):
        lines = []
        self.addCleanup(setattr, warmer, "log", warmer.log)
        warmer.log = lines.append
        return lines

    def test_a_new_page_goes_first_even_when_its_first_result_is_unknown(self):
        self.library.title("Old", art=("logo",))
        self.library.title("New", art=("logo",))
        self.signal(1, "Old")
        # Search results are often not stored yet, so this batch adds nothing to the urgent queue.
        self.signal(2, "Not stored yet")
        self.signal(2, "New")
        urgent = self.drain(self.worker.urgent)
        self.assertEqual(urgent[0].item, blob("New"))
        self.assertEqual(urgent[-1].item, blob("Old"))
        self.assertEqual([job.item for job in self.drain(self.worker.later)], [blob("Not stored yet")])

    def test_a_page_of_episodes_queues_its_series_art_once(self):
        names = self.show(40)
        self.signal(1, *names[:20])
        for name in names[20:]:
            self.signal(1, name)
        jobs = self.queued()
        self.assertEqual({job.item for job in jobs}, {blob("Show")})
        self.assertEqual(len(jobs), len({job.key for job in jobs}))
        self.assertEqual(len(jobs), 8)

    def test_a_page_of_seasons_queues_its_series_art_once(self):
        self.library.title("Show", kind="series", art=("logo",))
        seasons = [f"S{number}" for number in range(10)]
        for number, name in enumerate(seasons):
            self.library.item(name, "season", parent="Show", idx=number)
        self.signal(1, *seasons)
        self.assertEqual(len(self.queued()), 2)

    def test_a_series_seen_first_does_not_hold_back_what_its_episode_cards_use(self):
        self.show(art=("primary", "backdrop"))
        self.signal(1, "Show")
        self.queued()
        self.signal(1, "E0")
        jobs = self.queued()
        self.assertIn(POSTER_500, {job.form for job in jobs})
        composed = {job.form for job in jobs if job.form.image == "Thumb" and job.tag == blob("Show/backdrop")}
        self.assertEqual(composed, {THUMB_342, THUMB_500, THUMB_FULL})

    def test_a_season_poster_queues_every_list_form_before_any_full_size(self):
        self.show(2, art=("logo",))
        self.signal(1, "S1")
        stills = [(job.item, job.form) for job in self.queued() if job.item != blob("Show")]
        self.assertEqual(stills, [(blob("E0"), POSTER_500), (blob("E1"), POSTER_500)])
        self.assertEqual(
            [(job.item, job.form) for job in self.drain(self.worker.later)],
            [(blob("E0"), POSTER_FULL), (blob("E1"), POSTER_FULL)],
        )

    def test_a_full_size_still_waits_behind_a_newer_page(self):
        self.show(1, art=())
        self.library.title("Film", art=("logo",))
        self.signal(1, "S1")
        self.signal(2, "Film")
        self.worker.fast_bucket = Bucket(1000.0, burst=1000.0)
        got = []
        while len(self.worker.urgent) or len(self.worker.soon) or len(self.worker.later):
            got.append(self.worker._next_fast(time.monotonic() + 100)[0])
        self.assertEqual(
            [(job.item, job.form) for job in got],
            [(blob("Film"), LOGO_TAGGED), (blob("Film"), LOGO_PLAIN), (blob("E0"), POSTER_500), (blob("E0"), POSTER_FULL)],
        )

    def test_a_full_size_still_nginx_cannot_store_is_not_asked_for_again(self):
        self.show(1, art=())
        job = Job(blob("E0"), POSTER_FULL, blob("E0/primary"), warmer.TIER_FULL_STILLS, 1)
        self.worker.state.apply_plan([job])
        self.worker.state.record([(job, warmer.UNCACHEABLE, Result(200, "DIRECT"))], self.worker.config, time.time())
        self.signal(1, "S1")
        self.assertEqual(self.drain(self.worker.later), [])

    def test_a_signal_queues_the_small_thumb_after_the_others(self):
        self.library.title("Film", art=("thumb",))
        self.signal(1, "Film")
        self.assertEqual([job.form for job in self.queued()], [THUMB_500, THUMB_FULL, THUMB_342])

    def test_a_database_error_does_not_lose_the_art(self):
        self.library.title("Film", art=("logo",))
        repair = self.broken("art")
        self.signal(1, "Film")
        self.assertEqual(self.queued(), [])
        self.assertIn("database unavailable", self.worker.problem)
        repair()
        self.signal(1, "Film")
        self.assertEqual(len(self.queued()), 2)

    def test_a_database_error_part_way_forgets_that_batch_and_nothing_else(self):
        self.show(art=("logo",))
        self.library.title("Earlier", art=("logo",))
        self.signal(1, "Earlier")
        self.assertEqual(len(self.queued()), 2)
        # The series art is marked by then, and so is the unknown item's logo.
        repair = self.broken("children")
        self.signal(1, "Unknown", "S1")
        self.assertEqual(self.queued() + self.drain(self.worker.later), [])
        repair()
        self.signal(1, "Unknown", "S1", "Earlier")
        self.assertEqual({job.item for job in self.queued()}, {blob("Show"), blob("E0")})
        self.assertEqual([job.item for job in self.drain(self.worker.later)], [blob("Unknown"), blob("E0")])

    def test_a_missing_column_is_called_a_layout_problem(self):
        self.worker._note_db_problem(sqlite3.OperationalError("no such column: m.idx"))
        self.assertIn("database layout not recognised", self.worker.problem)
        self.assertNotIn("unavailable", self.worker.problem)
        self.assertFalse(self.worker.schema_ok)

    def test_signal_fetches_follow_the_slow_down(self):
        worker = self.worker
        worker.urgent.add(["job"], 1)
        now = time.monotonic()

        def wait_with_no_tokens():
            worker.fast_bucket.tokens = 0.0
            worker.fast_bucket.at = now
            return worker._next_fast(now)

        self.assertEqual(wait_with_no_tokens()[0], None)
        self.assertAlmostEqual(wait_with_no_tokens()[1], 0.1)
        worker.pace.speed = 0.5
        self.assertAlmostEqual(wait_with_no_tokens()[1], 0.2)

    def test_a_normal_viewer_gets_the_full_signal_rate_and_burst(self):
        worker = self.worker
        worker.urgent.add(["a", "b", "c", "d"], 1)
        now = time.monotonic()
        worker.fast_bucket.at = now
        got = [worker._next_fast(now) for _ in range(4)]
        self.assertEqual(got[:3], [("a", 0.0), ("b", 0.0), ("c", 0.0)])
        self.assertAlmostEqual(got[3][1], 0.1)

    def test_a_problem_shows_when_it_happened(self):
        self.assertEqual(self.worker.status()["last_problem"], "")
        self.worker._note_db_problem(sqlite3.OperationalError("database is locked"))
        self.assertRegex(self.worker.status()["last_problem"], r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d database unavailable")
        self.assertNotRegex(self.worker.problem, r"\d\d:\d\d")

    def test_a_repeated_problem_is_logged_once_and_its_time_moves_on(self):
        lines = self.capture_log()
        error = sqlite3.OperationalError("database is locked")
        self.worker._note_db_problem(error)
        self.worker.problem_at = 1000.0
        self.worker._note_db_problem(error)
        self.assertEqual(len(lines), 1)
        self.assertGreater(self.worker.problem_at, 1000.0)

    def stretch(self, fetches, reads):
        worker = self.worker
        worker.clean_fetches = fetches
        worker.clean_reads = reads
        worker._clear_problem_if_clean()
        return worker.problem

    def test_a_problem_clears_after_good_fetches_and_good_reads_but_not_either_alone(self):
        self.worker.background.append("job")
        self.worker._note_problem("HTTP 503")
        self.assertTrue(self.stretch(warmer.CLEAN_FETCHES - 1, warmer.CLEAN_READS))
        self.assertTrue(self.stretch(warmer.CLEAN_FETCHES, warmer.CLEAN_READS - 1))
        self.assertEqual(self.stretch(warmer.CLEAN_FETCHES, warmer.CLEAN_READS), "")

    def test_with_nothing_to_fetch_good_reads_are_enough(self):
        self.worker._note_problem("database unavailable (SQLITE_BUSY)")
        self.assertTrue(self.stretch(0, warmer.CLEAN_READS - 1))
        self.assertEqual(self.stretch(0, warmer.CLEAN_READS), "")

    def test_a_repeat_starts_the_stretch_again(self):
        self.worker._note_problem("HTTP 503")
        self.worker.clean_fetches = self.worker.clean_reads = 1000
        self.worker._note_problem("HTTP 503")
        self.assertEqual((self.worker.clean_fetches, self.worker.clean_reads), (0, 0))

    def test_a_problem_stays_while_the_source_is_still_failing(self):
        self.worker._note_problem("HTTP 503")
        self.worker.pace.failures = 1
        self.assertTrue(self.stretch(1000, 1000))
        self.worker.pace.failures = 0
        self.worker.pace.recent.update({number: True for number in range(warmer.BREAKER_TRIPS)})
        self.assertTrue(self.stretch(1000, 1000))

    def test_a_problem_only_a_plan_can_settle_stays_until_a_plan_works(self):
        self.worker._note_problem("planning: RuntimeError at somewhere", until_plan=True)
        self.assertTrue(self.stretch(1000, 1000))
        self.worker._finish_plan(warmer.PlanDone(newest=0, counts={}, titles=1, rows=0))
        self.assertEqual(self.worker.problem, "")

    def answer(self, *results):
        class Fake:
            def __init__(self):
                self.answers = iter(results)

            def get(self, _path):
                return next(self.answers)

        fetcher = Fake()
        job = Job(blob("Film"), LOGO_TAGGED, blob("Film/logo"))
        for _ in results:
            self.worker._fetch(fetcher, job, True)
        self.worker._apply_finished()

    def test_a_lone_failure_followed_only_by_hits_clears(self):
        self.answer(Result(503))
        self.assertEqual(self.worker.problem, "HTTP 503")
        self.assertEqual(self.worker.pace.failures, 1)
        self.answer(*[Result(200, "HIT", "image/jpeg", 10, 0.001)] * warmer.CLEAN_FETCHES)
        self.assertEqual(self.worker.pace.failures, 0)
        self.assertEqual(self.stretch(self.worker.clean_fetches, warmer.CLEAN_READS), "")

    def test_only_a_real_fetch_settles_doubt_about_the_source(self):
        self.answer(Result(503))
        self.worker.pace.recent.update({number: True for number in range(warmer.BREAKER_TRIPS)})
        self.answer(*[Result(200, "HIT", "image/jpeg", 10, 0.001)] * (warmer.CLEAN_FETCHES * 2))
        self.assertEqual(self.stretch(self.worker.clean_fetches, warmer.CLEAN_READS), "HTTP 503")

    def test_a_problem_only_a_plan_can_settle_outlasts_a_passing_one(self):
        self.worker._note_problem("planning: RuntimeError at somewhere", until_plan=True)
        self.answer(Result(503), Result(200, "MISS", "image/jpeg", 10, 0.5))
        self.assertTrue(self.stretch(1000, 1000))
        self.worker._finish_plan(warmer.PlanDone(newest=0, counts={}, titles=1, rows=0))
        self.assertEqual(self.worker.problem, "")
        self.assertFalse(self.worker.problem_until_plan)

    def test_a_cleared_problem_is_logged_again_if_it_comes_back(self):
        lines = self.capture_log()
        error = sqlite3.OperationalError("database is locked")
        self.worker._note_db_problem(error)
        self.assertEqual(self.stretch(0, warmer.CLEAN_READS), "")
        self.worker._note_db_problem(error)
        self.assertEqual(len(lines), 2)


class SmallPartsTests(unittest.TestCase):
    def test_bucket_paces_requests(self):
        bucket = Bucket(2.0)
        now = bucket.at
        self.assertEqual(bucket.wait(now), 0.0)
        bucket.take()
        self.assertAlmostEqual(bucket.wait(now), 0.5)
        self.assertEqual(bucket.wait(now + 0.5), 0.0)

    def test_bucket_speed_slows_it(self):
        bucket = Bucket(2.0)
        now = bucket.at
        bucket.take()
        self.assertAlmostEqual(bucket.wait(now, speed=0.5), 1.0)

    def test_recent_forgets_after_its_ttl_and_past_its_capacity(self):
        recent = Recent(2, 10)
        self.assertTrue(recent.first_time("a", 0))
        self.assertFalse(recent.first_time("a", 5))
        self.assertTrue(recent.first_time("a", 11))
        recent.first_time("b", 11)
        recent.first_time("c", 11)
        self.assertTrue(recent.first_time("a", 11))

    def test_settings_come_from_the_environment(self):
        config = Config.from_env({"WARMER_RATE": "0.25", "WARMER_PEOPLE": "on", "WARMER_HOME_ROW_ITEMS": "10"})
        self.assertEqual((config.rate, config.people, config.episodes, config.home_row_items), (0.25, True, False, 10))

    def test_the_guide_is_off_unless_asked_for(self):
        self.assertFalse(Config.from_env({}).guide)
        self.assertTrue(Config.from_env({"WARMER_GUIDE": "on"}).guide)

    def test_an_empty_signal_socket_turns_signals_off(self):
        self.assertEqual(Config.from_env({"WARMER_SIGNAL_SOCKET": ""}).signal_socket, "")
        self.assertEqual(Config.from_env({}).signal_socket, Config().signal_socket)

    def test_other_empty_settings_keep_their_defaults(self):
        config = Config.from_env({"WARMER_CACHE_URL": "", "WARMER_RATE": ""})
        self.assertEqual((config.cache_url, config.rate), (Config().cache_url, Config().rate))

    def test_bad_setting_stops_with_a_clear_message(self):
        with self.assertRaises(SystemExit):
            Config.from_env({"WARMER_RATE": "fast"})
        with self.assertRaises(SystemExit):
            Config.from_env({"WARMER_RATE": "0"})


class GuideTests(TempCase):
    """Live TV programme images: a window that moves with the clock."""

    def guide_jobs(self, **values):
        return [job for job in self.plan(guide=True, **values) if job.tier == warmer.TIER_GUIDE]

    def test_off_by_default(self):
        self.library.programme("On now", -60, 600)
        self.assertEqual(self.plan(), [])

    def test_the_window_runs_from_now_through_the_next_day(self):
        library = self.library
        library.programme("Over", -7200, -60)
        library.programme("On now", -60, 600)
        library.programme("Soon", 600, 1200)
        library.programme("Tomorrow", 23 * 3600, 24 * 3600)
        library.programme("Too far", 25 * 3600, 26 * 3600)
        library.programme("No image", 60, 120, art=())
        self.assertEqual({job.item for job in self.guide_jobs()}, {blob("On now"), blob("Soon"), blob("Tomorrow")})

    def test_the_forms_clients_ask_for_and_what_airs_soonest_goes_first(self):
        self.library.programme("Later", 3600, 7200)
        self.library.programme("On now", -60, 600)
        jobs = sorted(self.guide_jobs(), key=lambda job: job.rank)
        self.assertEqual(jobs[0].item, blob("On now"))
        self.assertEqual(jobs[-1].item, blob("Later"))
        self.assertEqual({job.form for job in jobs}, {POSTER_342, POSTER_500})
        self.assertEqual({job.tag for job in jobs if job.item == blob("Later")}, {blob("Later/primary")})
        self.assertAlmostEqual(jobs[-1].rank, time.time() + 3600, delta=5)

    def test_what_the_keys_come_to_for_each_form(self):
        self.library.programme("On now", -60, 600)
        tag = str(uuid.UUID(bytes=blob("On now/primary")))
        keys = {nginx_key(job.path()) for job in self.guide_jobs()}
        self.assertEqual(keys, {f"{blob('On now').hex()}|primary|{tag}|342", f"{blob('On now').hex()}|primary|{tag}|500"})

    def test_a_row_it_cannot_read_costs_only_itself(self):
        self.library.programme("Fine", 60, 120)
        self.library.programme("Odd", 60, 120)
        self.library.con.execute("UPDATE media SET live_start = '9 November' WHERE id = ?", (blob("Odd"),))
        self.library.programme("Short id", 60, 120, art=())
        self.library.con.execute(
            "INSERT INTO media_images (id, media_id, image_type, image_index, path) VALUES (x'0011', ?, 'primary', 0, 'x')",
            (blob("Short id"),),
        )
        counts = collections.Counter()
        config = self.config(guide=True)
        catalogue = Catalogue(config.db_path)
        catalogue.check_schema()
        jobs = list(warmer.plan_jobs(read_snapshot(catalogue, config), config, counts))
        self.assertEqual({job.item for job in jobs}, {blob("Fine")})
        self.assertEqual(counts["skipped_rows"], 1)

    def test_with_no_programme_times_there_is_simply_no_guide(self):
        self.library.title("Film", art=("primary",))
        self.library.programme("On now", -60, 600)
        for column in ("live_start", "live_end"):
            self.library.con.execute(f"ALTER TABLE media DROP COLUMN {column}")
        self.assertEqual({job.item for job in self.plan(guide=True)}, {blob("Film")})

    def test_the_setting_is_part_of_what_a_plan_depends_on(self):
        self.assertNotEqual(warmer.plan_settings(Config()), warmer.plan_settings(Config(guide=True)))

    def test_the_budget_does_not_stop_it(self):
        worker = warmer.Warmer(self.config(guide=True, people=True))
        self.addCleanup(worker.state.close)
        worker.over_budget = True
        allowed = worker._tiers_allowed()
        self.assertIn(warmer.TIER_GUIDE, allowed)
        self.assertNotIn(TIER_PEOPLE, allowed)

    def test_it_stays_out_when_the_setting_is_off(self):
        worker = warmer.Warmer(self.config())
        self.addCleanup(worker.state.close)
        self.assertNotIn(warmer.TIER_GUIDE, worker._tiers_allowed())


class Running:
    """Runs a real worker in a thread for one test."""

    def __init__(self, config):
        self.worker = warmer.Warmer(config)
        self.thread = threading.Thread(target=self.worker.run, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self.worker

    def __exit__(self, *_):
        self.worker.stop.set()
        self.worker.wake.set()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise AssertionError("worker did not stop")


class WorkerTests(TempCase):
    def setUp(self):
        super().setUp()
        self.cache = FakeCache()
        self.addCleanup(self.cache.close)
        self.socket_path = os.path.join(self.folder.name, "signal.sock")

    def settings(self, **values):
        fast = dict(
            cache_url=self.cache.url, rate=500.0, long_tail_rate=500.0, new_rate=500.0, signal_rate=500.0,
            signal_quiet=0.0, signal_max_hold=0.0, yield_seconds=0.0, poll_seconds=0.1, log_seconds=3600.0,
            backoff_seconds=0.05, min_free_gb=0.0,
        )
        fast.update(values)
        return self.config(**fast)

    def send(self, item, image_type="primary"):
        client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        client.sendto(f"<190>Nov  5 22:00:00 warm: {item.hex()} {image_type}".encode(), self.socket_path)
        client.close()

    def wait_for_socket(self):
        self.assertTrue(wait_until(lambda: os.path.exists(self.socket_path)))

    def status(self, worker):
        with worker.cond:
            return dict(worker.counts)

    def answered(self, worker):
        counts = self.status(worker)
        return counts.get("misses", 0) + counts.get("hits", 0)

    def test_warms_the_plan_in_order_and_identifies_itself(self):
        self.library.row("Trending", "movie", "TrendingWeek")
        self.library.title("Hero", weekly=9.0)
        self.library.title("Other", weekly=1.0)
        # Nine forms for each.
        expected = 18
        with Running(self.settings(home_row_items=1)) as worker:
            self.assertTrue(wait_until(lambda: self.answered(worker) >= expected))
        paths = self.cache.paths()
        self.assertEqual(len(set(paths)), expected)
        first_other = next(number for number, path in enumerate(paths) if blob("Other").hex() in path)
        last_hero = max(number for number, path in enumerate(paths) if blob("Hero").hex() in path)
        self.assertLess(last_hero, first_other)
        self.assertTrue(all(sent == "1" for _, sent in self.cache.seen))

    def test_restart_resumes_without_fetching_again(self):
        self.library.title("Film")
        with Running(self.settings()) as worker:
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 9))
        count = len(self.cache.paths())
        with Running(self.settings()):
            time.sleep(0.5)
        self.assertEqual(len(self.cache.paths()), count)

    def test_new_row_is_warmed_at_the_next_poll(self):
        self.library.title("Old")
        with Running(self.settings()) as worker:
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 9))
            self.library.title("Fresh", art=("primary", "logo", "backdrop"))
            fresh = blob("Fresh").hex()
            self.assertTrue(wait_until(lambda: sum(fresh in path for path in self.cache.paths()) >= 6))

    def test_new_row_is_not_held_back_by_a_viewer(self):
        self.library.title("Old", art=("primary",))
        with Running(self.settings(yield_seconds=30.0, poll_seconds=0.05)) as worker:
            self.assertTrue(wait_until(lambda: self.status(worker).get("misses", 0) >= 2))
            worker.on_signal(blob("Old"), "primary")
            self.library.title("Fresh", art=("primary",))
            fresh = blob("Fresh").hex()
            self.assertTrue(wait_until(lambda: any(fresh in path for path in self.cache.paths())))

    def test_deleting_the_newest_rows_rebuilds_the_plan_only_once(self):
        self.library.title("Old", art=("primary",))
        self.library.title("Newest", art=("primary",))
        lines = []
        self.addCleanup(setattr, warmer, "log", warmer.log)
        warmer.log = lines.append
        plans = lambda: sum(line.startswith("plan: ") for line in lines)
        with Running(self.settings(poll_seconds=0.02)) as worker:
            self.assertTrue(wait_until(lambda: plans() == 1))
            self.library.con.execute("DELETE FROM media_images WHERE rowid = (SELECT max(rowid) FROM media_images)")
            self.assertTrue(wait_until(lambda: plans() == 2))
            time.sleep(1.0)
            self.assertEqual(plans(), 2)
            self.assertEqual(worker.last_rowid, self.library.con.execute("SELECT max(rowid) FROM media_images").fetchone()[0])

    def test_background_work_yields_to_a_viewer(self):
        self.library.title("A", art=("primary",), daily=2.0)
        self.library.title("B", art=("primary",), daily=1.0)
        config = self.settings(yield_seconds=30.0, rate=500.0, poll_seconds=30.0)
        worker = warmer.Warmer(config)
        worker.on_signal(blob("nothing"), "thumb")
        worker.inbox.clear()
        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        time.sleep(0.5)
        self.assertEqual(self.cache.paths(), [])
        worker.stop.set()
        worker.wake.set()
        thread.join(timeout=10)

    def test_signal_warms_logo_first_then_the_rest(self):
        self.library.title("Film")
        settings = self.settings(
            signal_socket=self.socket_path, signal_workers=1, rate=0.001, long_tail_rate=0.001, new_rate=0.001, poll_seconds=30.0
        )
        with Running(settings) as worker:
            self.wait_for_socket()
            # The plan's rate is close to zero, so after its first request the worker is busy with nothing else.
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 1))
            self.cache.seen.clear()
            self.send(blob("Film"))
            self.assertTrue(wait_until(lambda: len(self.cache.paths()) >= 6))
        paths = self.cache.paths()
        self.assertIn("/Logo?maxWidth=700", paths[0])
        self.assertIn("/Backdrop/0?", paths[1])
        self.assertIn("/Logo?maxWidth=600", paths[2])
        self.assertTrue(any("/Thumb?" in path for path in paths[3:]))
        self.assertTrue(any("/Primary?maxHeight=" in path for path in paths[3:]))
        self.assertFalse(any("/Backdrop?" in path for path in paths))

    def test_season_signal_reaches_the_series_and_its_episodes(self):
        library = self.library
        library.title("Show", kind="series", art=("primary", "logo", "backdrop"))
        library.item("S1", "season", parent="Show", idx=1)
        library.art("S1", "primary")
        library.item("E1", "episode", parent="S1", grandparent="Show", idx=1, parent_idx=1)
        library.art("E1", "primary")
        library.item("E2", "episode", parent="S1", grandparent="Show", idx=2, parent_idx=1)
        library.art("E2", "primary")
        settings = self.settings(signal_socket=self.socket_path, rate=0.001, long_tail_rate=0.001, new_rate=0.001, poll_seconds=30.0)
        with Running(settings) as worker:
            self.wait_for_socket()
            self.send(blob("S1"))
            show = blob("Show").hex()
            self.assertTrue(wait_until(lambda: any(show in path and "/Logo" in path for path in self.cache.paths())))
            for episode in ("E1", "E2"):
                self.assertTrue(wait_until(lambda: any(blob(episode).hex() in path for path in self.cache.paths())))
            self.assertTrue(worker.counts["signals"] >= 1)

    def test_backdrop_signal_reaches_seasons_and_cast(self):
        library = self.library
        library.title("Show", kind="series", art=("backdrop",))
        library.item("S1", "season", parent="Show", idx=1)
        library.art("S1", "primary")
        library.item("Actor", "person")
        library.art("Actor", "primary")
        library.con.execute(
            "INSERT INTO media_relations (relation_id, left_media_id, right_media_id, role, weight) VALUES ('r', ?, ?, 'actor', 1)",
            (blob("Show"), blob("Actor")),
        )
        settings = self.settings(signal_socket=self.socket_path, rate=0.001, long_tail_rate=0.001, new_rate=0.001, poll_seconds=30.0)
        with Running(settings):
            self.wait_for_socket()
            self.send(blob("Show"), "backdrop")
            for name in ("S1", "Actor"):
                self.assertTrue(wait_until(lambda: any(blob(name).hex() in path for path in self.cache.paths())))

    def test_page_of_repeated_signals_queues_the_work_once(self):
        self.library.title("Film", art=("logo",))
        settings = self.settings(signal_socket=self.socket_path, rate=0.001, long_tail_rate=0.001, new_rate=0.001, poll_seconds=30.0)
        with Running(settings):
            self.wait_for_socket()
            time.sleep(0.2)
            self.cache.seen.clear()
            for _ in range(60):
                self.send(blob("Film"))
            time.sleep(0.5)
        logos = [path for path in self.cache.paths() if "/Logo" in path]
        self.assertLessEqual(len(logos), 2)

    def test_item_not_in_the_database_gets_only_its_plain_logo(self):
        settings = self.settings(signal_socket=self.socket_path, poll_seconds=30.0)
        with Running(settings):
            self.wait_for_socket()
            unknown = blob("search result")
            self.send(unknown)
            self.assertTrue(wait_until(lambda: len(self.cache.paths()) >= 1))
            time.sleep(0.2)
        self.assertEqual(self.cache.paths(), [f"/Items/{unknown.hex()}/Images/Logo?maxWidth=600&quality=90"])

    def test_a_guess_waits_behind_art_that_is_certain(self):
        self.library.title("Known", art=("logo",))
        settings = self.settings(
            signal_socket=self.socket_path, signal_workers=1, signal_quiet=0.3, signal_max_hold=0.3, poll_seconds=30.0,
            rate=0.001, long_tail_rate=0.001, new_rate=0.001,
        )
        with Running(settings) as worker:
            self.wait_for_socket()
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 1))
            self.cache.seen.clear()
            self.send(blob("Unknown"))
            self.send(blob("Known"))
            self.assertTrue(wait_until(lambda: len(self.cache.paths()) >= 3))
        paths = self.cache.paths()
        self.assertIn(blob("Known").hex(), paths[0])
        self.assertIn(blob("Unknown").hex(), paths[-1])

    def test_a_page_of_results_is_warmed_from_the_top(self):
        names = [f"Result {number}" for number in range(6)]
        for name in names:
            self.library.title(name, art=("logo",))
        settings = self.settings(
            signal_socket=self.socket_path, signal_workers=1, signal_quiet=0.2, signal_max_hold=0.2, poll_seconds=30.0,
            rate=0.001, long_tail_rate=0.001, new_rate=0.001,
        )
        with Running(settings) as worker:
            self.wait_for_socket()
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 1))
            self.cache.seen.clear()
            # Spaced out, so each lands in a batch of its own, as real ones do.
            for name in names:
                self.send(blob(name))
                time.sleep(0.05)
            self.assertTrue(wait_until(lambda: len(self.cache.paths()) >= 12))
        tagged = [path for path in self.cache.paths() if "maxWidth=700" in path]
        self.assertEqual([path.split("/")[2] for path in tagged], [blob(name).hex() for name in names])

    def test_the_newest_page_comes_before_the_one_before_it(self):
        old = [f"Old {number}" for number in range(6)]
        for name in old + ["New"]:
            self.library.title(name, art=("logo",))
        settings = self.settings(
            signal_socket=self.socket_path, signal_workers=1, signal_quiet=0.0, signal_max_hold=0.0, signal_burst_gap=0.05,
            signal_rate=10.0, poll_seconds=30.0, rate=0.001, long_tail_rate=0.001, new_rate=0.001,
        )
        with Running(settings) as worker:
            self.wait_for_socket()
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 1))
            self.cache.seen.clear()
            for name in old:
                self.send(blob(name))
            time.sleep(0.2)
            self.send(blob("New"))
            self.assertTrue(wait_until(lambda: len(self.cache.paths()) >= 14, timeout=15.0))
        order = [path.split("/")[2] for path in self.cache.paths()]
        self.assertLess(order.index(blob("New").hex()), order.index(blob("Old 5").hex()))

    def test_a_signal_still_works_when_the_cache_was_cleared_under_the_worker(self):
        self.library.title("Film", art=("logo",))
        settings = self.settings(signal_socket=self.socket_path, poll_seconds=30.0)
        with Running(settings) as worker:
            self.wait_for_socket()
            self.assertTrue(wait_until(lambda: self.status(worker).get("cached", 0) >= 2))
            worker.pace.recent.clear()
            self.cache.keys.clear()
            self.cache.seen.clear()
            self.send(blob("Film"))
            self.assertTrue(wait_until(lambda: len(self.cache.paths()) >= 2))

    def test_search_result_that_gets_opened_is_warmed_properly_on_its_next_signal(self):
        settings = self.settings(signal_socket=self.socket_path, poll_seconds=30.0, plan_hours=24.0)
        with Running(settings):
            self.wait_for_socket()
            self.send(blob("Opened"))
            self.assertTrue(wait_until(lambda: len(self.cache.paths()) == 1))
            self.send(blob("Opened"))
            time.sleep(0.2)
            self.assertEqual(len(self.cache.paths()), 1)
            self.library.title("Opened", art=("primary", "logo", "backdrop"))
            self.send(blob("Opened"))
            tagged = str(uuid.UUID(bytes=blob("Opened/backdrop")))
            self.assertTrue(wait_until(lambda: any(tagged in path for path in self.cache.paths())))

    def test_malformed_signals_are_ignored(self):
        settings = self.settings(signal_socket=self.socket_path, poll_seconds=30.0)
        with Running(settings) as worker:
            self.wait_for_socket()
            client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            for bad in (b"", b"garbage", b"warm: nothex primary", b"warm: " + b"a" * 32 + b" banner"):
                client.sendto(bad, self.socket_path)
            client.close()
            time.sleep(0.3)
            self.assertEqual(self.status(worker).get("signals", 0), 0)
        self.assertEqual(self.cache.paths(), [])

    def test_changed_art_and_removed_rows_are_handled_by_the_next_plan(self):
        library = self.library
        library.title("Stays", art=("primary",))
        library.title("Goes", art=("primary",))
        with Running(self.settings(poll_seconds=30.0, plan_hours=1.0 / 3600)) as worker:
            self.assertTrue(wait_until(lambda: self.status(worker).get("misses", 0) >= 4))
            library.con.execute("UPDATE media_images SET id = ? WHERE media_id = ?", (blob("new art"), blob("Stays")))
            library.con.execute("DELETE FROM media_images WHERE media_id = ?", (blob("Goes"),))
            new_tag = str(uuid.UUID(bytes=blob("new art")))
            self.assertTrue(wait_until(lambda: sum(new_tag in path for path in self.cache.paths()) >= 2))
            self.assertTrue(wait_until(lambda: worker.status()["tiers"].get("titles", {}).get("total") == 3))

    def test_a_struggling_cache_gets_backed_off(self):
        self.library.title("Film", art=("primary",))
        self.cache.status_for[blob("Film").hex()] = (503, "", "text/html", None)
        with Running(self.settings(backoff_seconds=0.3, poll_seconds=30.0)) as worker:
            self.assertTrue(wait_until(lambda: self.status(worker).get("backoff", 0) >= 1))
            time.sleep(0.2)
            self.assertLessEqual(self.status(worker).get("backoff", 0), 1)
            self.assertLess(worker.pace.speed, 1.0)

    def test_429_with_retry_after_stops_requests_for_that_long(self):
        self.library.title("Film", art=("primary",))
        self.cache.status_for[blob("Film").hex()] = (429, "", "text/html", 60)
        with Running(self.settings(poll_seconds=30.0)) as worker:
            self.assertTrue(wait_until(lambda: self.status(worker).get("backoff", 0) >= 1))
            time.sleep(0.3)
            self.assertEqual(len(self.cache.paths()), 1)
            self.assertGreater(worker.pace.pause_until - time.monotonic(), 50)

    def test_404_is_remembered(self):
        self.library.title("Gone", art=("primary",))
        self.cache.status_for[blob("Gone").hex()] = (404, "MISS", "text/html", None)
        with Running(self.settings(poll_seconds=30.0)) as worker:
            self.assertTrue(wait_until(lambda: self.status(worker).get("missing", 0) >= 3))
            time.sleep(0.3)
            self.assertEqual(len(self.cache.paths()), 3)

    def failing_library(self, status):
        for number in range(30):
            self.library.title(f"Film {number}", art=("primary",))
        self.cache.status_for[""] = (status, "MISS", "text/html", None)

    def wait_for_breaker(self, worker):
        self.assertTrue(wait_until(lambda: self.status(worker).get("suspect", 0) >= 1))

    def test_a_source_answering_404_for_everything_gets_backed_off(self):
        self.failing_library(404)
        with Running(self.settings(backoff_seconds=2.0, poll_seconds=30.0)) as worker:
            self.wait_for_breaker(worker)
            time.sleep(0.5)
            # Each title here has one image in three forms.
            self.assertLessEqual(len(self.cache.paths()), warmer.BREAKER_TRIPS * 3)
            self.assertLess(worker.pace.speed, 1.0)
            self.assertGreater(worker.pace.pause_until, time.monotonic())
            self.assertIn("source failing", worker.problem)
            self.assertTrue(worker.status()["source_suspect"])
        count = self.library.con.execute("SELECT count(*) FROM media WHERE kind = 'movie'").fetchone()[0]
        self.assertEqual(count, 30)

    def test_rows_answering_404_in_an_outage_keep_their_standing_and_are_warmed_after_it(self):
        self.failing_library(404)
        with Running(self.settings(backoff_seconds=0.1, backoff_max_seconds=0.2, poll_seconds=30.0)) as worker:
            self.wait_for_breaker(worker)
            self.cache.status_for.clear()
            # What was doubted comes back after SUSPECT_RETRY, too long to wait here. What wasn't tried yet closes the breaker.
            self.assertTrue(wait_until(lambda: self.status(worker).get("cached", 0) >= warmer.BREAKER_WINDOW))
            self.assertFalse(worker.pace.source_suspect)
            tries = worker.state.con.execute("SELECT max(tries) FROM jobs WHERE strikes > 0").fetchone()[0]
            self.assertEqual(tries, 0)

    def test_a_refusal_storm_gets_backed_off_and_is_not_locked_out_for_a_month(self):
        self.failing_library(401)
        with Running(self.settings(backoff_seconds=2.0, poll_seconds=30.0)) as worker:
            self.wait_for_breaker(worker)
            time.sleep(0.5)
            self.assertLessEqual(len(self.cache.paths()), warmer.BREAKER_TRIPS * 3)
            self.assertGreater(worker.pace.pause_until, time.monotonic())
            self.assertIn("source failing", worker.problem)
            longest = worker.state.con.execute("SELECT max(next_at) FROM jobs WHERE pending = 0").fetchone()[0]
            self.assertLessEqual(longest, time.time() + warmer.REFUSED_RETRY + 5)
            self.assertEqual(worker.state.con.execute("SELECT count(*) FROM jobs WHERE status = ?", (warmer.STATUS_UNCACHEABLE,)).fetchone()[0], 0)

    def test_a_signal_during_an_outage_is_warmed_after_it(self):
        self.library.title("Film", art=("logo",))
        settings = self.settings(
            signal_socket=self.socket_path, backoff_seconds=0.2, backoff_max_seconds=0.4, poll_seconds=30.0,
            rate=0.001, long_tail_rate=0.001, new_rate=0.001,
        )
        with Running(settings) as worker:
            self.wait_for_socket()
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 1))
            with worker.cond:
                worker.pace.recent.update({number: True for number in range(warmer.BREAKER_WINDOW)})
            self.cache.status_for[""] = (404, "MISS", "text/html", None)
            self.send(blob("Film"))
            self.assertTrue(wait_until(lambda: self.status(worker).get("suspect", 0) >= 1))
            self.cache.status_for.clear()
            self.assertTrue(wait_until(lambda: self.status(worker).get("cached", 0) >= 2))

    def test_svg_logo_is_left_alone(self):
        self.library.title("Film", art=("logo",))
        self.cache.status_for[blob("Film").hex()] = (200, "DIRECT", "image/svg+xml", None)
        with Running(self.settings(poll_seconds=30.0)) as worker:
            self.assertTrue(wait_until(lambda: self.status(worker).get("uncacheable", 0) >= 2))
            time.sleep(0.3)
            self.assertEqual(len(self.cache.paths()), 2)
            self.assertEqual(worker.pace.pause_until, 0.0)

    def test_budget_stops_the_long_tail_but_not_the_titles(self):
        self.library.title("Film", art=("primary",))
        self.library.item("Actor", "person")
        self.library.art("Actor", "primary")
        with Running(self.settings(budget_gb=0.0, people=True, poll_seconds=30.0)) as worker:
            film = blob("Film").hex()
            self.assertTrue(wait_until(lambda: sum(film in path for path in self.cache.paths()) >= 2))
            time.sleep(0.3)
            self.assertFalse(any(blob("Actor").hex() in path for path in self.cache.paths()))
            self.assertTrue(worker.over_budget)

    def test_over_budget_stored_art_in_the_long_tail_is_still_kept_up(self):
        self.library.item("Stored", "person")
        self.library.art("Stored", "primary")
        self.library.item("Unfetched", "person")
        self.library.art("Unfetched", "primary")
        stored = blob("Stored").hex()
        with Running(self.settings(people=True, poll_seconds=30.0)) as worker:
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 4))
            self.assertTrue(wait_until(lambda: worker.state.con.execute("SELECT count(*) FROM jobs WHERE pending = 1").fetchone()[0] == 0))
        self.cache.seen.clear()
        path = os.path.join(self.folder.name, "state", "warmer.sqlite")
        con = sqlite3.connect(path, isolation_level=None)
        self.addCleanup(con.close)
        # The first is due again, and the other has never been fetched.
        con.execute("UPDATE jobs SET next_at = 1 WHERE item = ?", (blob("Stored"),))
        con.execute("UPDATE jobs SET pending = 1, status = 0, bytes = 0 WHERE item = ?", (blob("Unfetched"),))
        with Running(self.settings(budget_gb=0.0, people=True, poll_seconds=30.0)) as worker:
            self.assertTrue(wait_until(lambda: len(self.cache.paths()) >= 2))
            time.sleep(0.3)
            self.assertTrue(worker.over_budget)
        self.assertEqual({path.split("/")[2] for path in self.cache.paths()}, {stored})

    def test_full_disk_pauses_everything(self):
        self.library.title("Film", art=("primary",))
        with Running(self.settings(min_free_gb=1e9, poll_seconds=30.0)) as worker:
            self.assertTrue(wait_until(lambda: worker.disk_low))
            time.sleep(0.3)
        self.assertEqual(len(self.cache.paths()), 0)

    def test_fetch_limit_is_honoured(self):
        self.library.title("Film")
        with Running(self.settings(max_fetches=3, poll_seconds=30.0)):
            time.sleep(0.5)
        self.assertLessEqual(len(self.cache.paths()), 4)

    def test_keeps_running_with_no_database(self):
        settings = self.settings(db_path=os.path.join(self.folder.name, "absent.sqlite"), poll_seconds=0.05)
        with Running(settings) as worker:
            self.assertTrue(wait_until(lambda: "database unavailable" in worker.problem))
            self.assertTrue(wait_until(lambda: warmer.health(settings)))

    def test_a_missing_optional_column_is_no_problem_at_all(self):
        self.library.title("Film", art=("primary",))
        self.library.con.execute("ALTER TABLE media DROP COLUMN rating_audience")
        with Running(self.settings(poll_seconds=0.02)) as worker:
            self.assertTrue(wait_until(lambda: self.status(worker).get("misses", 0) >= 3))
            self.assertEqual(worker.problem, "")

    def test_a_missing_required_column_says_so_clearly(self):
        self.library.title("Film", art=("primary",))
        self.library.con.execute("ALTER TABLE media RENAME COLUMN parent_idx TO position")
        with Running(self.settings(poll_seconds=0.02)) as worker:
            self.assertTrue(wait_until(lambda: "database layout not recognised" in worker.problem))
            self.assertIn("parent_idx", worker.problem)

    def test_a_column_that_goes_while_running_is_called_a_layout_problem(self):
        self.library.title("Film", art=("primary",))
        logged = []
        self.addCleanup(setattr, warmer, "log", warmer.log)
        warmer.log = logged.append
        with Running(self.settings(rate=0.001, poll_seconds=0.02)) as worker:
            self.assertTrue(wait_until(lambda: any(line.startswith("plan: ") for line in logged)))
            # What the columns check saw is out of date now, so the next query is the first to find out.
            self.library.con.execute("ALTER TABLE media RENAME COLUMN digital_released_at TO released")
            worker.next_plan = 0.0
            self.assertTrue(wait_until(lambda: any("database layout not recognised" in line for line in logged)))
            self.assertFalse(any("database unavailable" in line for line in logged))
            # The columns are looked at again, so the plan after that works without it.
            self.assertTrue(wait_until(lambda: sum(line.startswith("plan: ") for line in logged) == 2))

    def test_last_problem_clears_after_a_clean_stretch(self):
        for number in range(40):
            self.library.title(f"Film {number}", art=("primary",), daily=100.0 - number)
        self.cache.status_for[blob("Film 0").hex()] = (503, "", "text/html", None)
        with Running(self.settings(poll_seconds=0.02)) as worker:
            self.assertTrue(wait_until(lambda: self.status(worker).get("backoff", 0) >= 1))
            self.assertTrue(wait_until(lambda: self.status(worker).get("cached", 0) >= warmer.CLEAN_FETCHES))
            self.assertTrue(wait_until(lambda: worker.problem == ""))

    def test_last_problem_stays_while_the_database_is_down(self):
        self.library.title("Film", art=("primary",))
        settings = self.settings(db_path=os.path.join(self.folder.name, "absent.sqlite"), poll_seconds=0.02)
        logged = []
        self.addCleanup(setattr, warmer, "log", warmer.log)
        warmer.log = logged.append
        with Running(settings) as worker:
            self.assertTrue(wait_until(lambda: "database unavailable" in worker.problem))
            time.sleep(0.5)
            self.assertIn("database unavailable", worker.problem)
        self.assertEqual(sum("database unavailable" in line for line in logged), 1)

    def test_keeps_running_with_no_cache(self):
        self.library.title("Film", art=("primary",))
        settings = self.settings(cache_url="http://127.0.0.1:9", poll_seconds=30.0)
        with Running(settings) as worker:
            self.assertTrue(wait_until(lambda: self.status(worker).get("backoff", 0) >= 1))
            self.assertTrue(wait_until(lambda: warmer.health(settings)))

    def test_keeps_running_when_the_signal_socket_cannot_be_opened(self):
        self.library.title("Film", art=("primary",))
        settings = self.settings(signal_socket="/nonexistent/folder/signal.sock", poll_seconds=30.0)
        with Running(settings) as worker:
            self.assertTrue(wait_until(lambda: self.status(worker).get("misses", 0) >= 2))

    def test_turning_the_long_tail_off_takes_effect_at_once(self):
        self.library.title("Film", art=("primary",))
        for number in range(20):
            self.library.item(f"Actor {number}", "person")
            self.library.art(f"Actor {number}", "primary")
        config = self.settings(people=True, long_tail_rate=500.0, poll_seconds=30.0, plan_hours=24.0)
        with Running(config) as worker:
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 5))
        slow = self.settings(people=False, long_tail_rate=0.001, poll_seconds=30.0, plan_hours=24.0)
        self.cache.seen.clear()
        with Running(slow) as worker:
            # The plan is rebuilt straight away, though the last one is recent, and a much smaller one is accepted.
            self.assertTrue(wait_until(lambda: "people" not in worker.status()["tiers"]))
            self.assertTrue(wait_until(lambda: worker.state.get("plan_settings") == warmer.plan_settings(slow)))
            self.assertEqual(worker.state.get("plan_shrink_ok"), 0)
            self.assertFalse(worker._tiers_allowed() & {TIER_PEOPLE, TIER_EPISODES})

    def test_turning_the_long_tail_on_takes_effect_at_once(self):
        self.library.title("Film", art=("primary",))
        self.library.item("Actor", "person")
        self.library.art("Actor", "primary")
        with Running(self.settings(poll_seconds=30.0, plan_hours=24.0)) as worker:
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 3))
        self.assertFalse(any(blob("Actor").hex() in path for path in self.cache.paths()))
        with Running(self.settings(people=True, poll_seconds=30.0, plan_hours=24.0)):
            self.assertTrue(wait_until(lambda: any(blob("Actor").hex() in path for path in self.cache.paths())))

    def test_a_state_from_before_a_change_of_plan_rules_replans_at_once_and_keeps_the_work_done(self):
        self.library.row("Trending", "movie", "TrendingWeek")
        self.library.title("Home", weekly=9.0, art=("thumb",))
        self.library.title("Film", weekly=1.0, art=("primary", "thumb"))
        slow = dict(poll_seconds=30.0, plan_hours=24.0, home_row_items=1)
        with Running(self.settings(**slow)) as worker:
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 9))
        self.cache.seen.clear()
        path = os.path.join(self.folder.name, "state", "warmer.sqlite")
        con = sqlite3.connect(path, isolation_level=None)
        # What a state file holds from before the rules were part of this: its plan had no small thumbs.
        settings = {"people": False, "episodes": False, "channels": False, "home_row_items": 1}
        con.execute("UPDATE meta SET value = ? WHERE name = 'plan_settings'", (json.dumps(settings, sort_keys=True),))
        small = str(uuid.UUID(bytes=blob("Film/thumb")))
        con.execute("DELETE FROM jobs WHERE form = ? AND item = ?", (THUMB_342.code, blob("Film")))
        con.close()
        logged = []
        self.addCleanup(setattr, warmer, "log", warmer.log)
        warmer.log = logged.append
        with Running(self.settings(**slow)):
            self.assertTrue(wait_until(lambda: any(line.startswith("plan: ") for line in logged)))
            self.assertTrue(wait_until(lambda: len(self.cache.paths()) >= 1))
            time.sleep(0.2)
        # The new form goes out, and nothing already done goes out again.
        self.assertEqual(len(self.cache.paths()), 1)
        self.assertIn(small, self.cache.paths()[0])
        self.assertIn("/Thumb?fillHeight=179", self.cache.paths()[0])

    def test_a_shrunken_plan_is_kept_back_once_if_the_settings_did_not_change(self):
        for number in range(10):
            self.library.title(f"Film {number}", art=("primary",))
        with Running(self.settings(rate=0.001, poll_seconds=30.0, plan_hours=24.0)) as worker:
            self.assertTrue(wait_until(lambda: worker.state.get("planned_at")))
        self.library.con.execute("DELETE FROM media_images WHERE media_id != ?", (blob("Film 0"),))
        logged = []
        original = warmer.log
        warmer.log = logged.append
        self.addCleanup(setattr, warmer, "log", original)
        with Running(self.settings(rate=0.001, poll_seconds=30.0, plan_hours=1.0 / 3600)) as worker:
            self.assertTrue(wait_until(lambda: any("keeping the old plan" in line for line in logged)))
            # It's logged just before it's stored.
            self.assertTrue(wait_until(lambda: worker.state.get("plan_shrink_ok") == 1))
            self.assertTrue(wait_until(lambda: any(line.startswith("plan: 3 jobs,") for line in logged)))
            self.assertEqual(worker.state.planned(), 3)

    def test_the_guide_comes_after_home_and_titles(self):
        self.library.row("Trending", "movie", "TrendingWeek")
        self.library.title("Hero", weekly=9.0, art=("primary",))
        self.library.title("Other", weekly=1.0, art=("primary",))
        self.library.programme("On now", -60, 600)
        hero, guide, other = (blob(name).hex() for name in ("Hero", "On now", "Other"))
        with Running(self.settings(guide=True, home_row_items=1, poll_seconds=30.0)):
            self.assertTrue(wait_until(lambda: any(guide in path for path in self.cache.paths())))
        order = [path.split("/")[2] for path in self.cache.paths()]
        self.assertLess(max(number for number, item in enumerate(order) if item == hero), order.index(other))
        self.assertLess(max(number for number, item in enumerate(order) if item == other), order.index(guide))

    def test_a_programme_entering_the_window_is_warmed_at_the_next_poll(self):
        self.library.programme("On now", -60, 600)
        with Running(self.settings(guide=True, poll_seconds=0.05)) as worker:
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 2))
            self.library.programme("Enters soon", 24 * 3600 + 3, 25 * 3600)
            self.library.programme("Far ahead", 48 * 3600, 49 * 3600)
            entering = blob("Enters soon").hex()
            # The window's far edge hasn't got there yet.
            time.sleep(0.3)
            self.assertFalse(any(entering in path for path in self.cache.paths()))
            self.assertTrue(wait_until(lambda: sum(entering in path for path in self.cache.paths()) == 2, timeout=8.0))
            self.assertFalse(any(blob("Far ahead").hex() in path for path in self.cache.paths()))

    def test_a_restart_carries_on_from_the_edge_of_the_window(self):
        self.library.programme("On now", -60, 600)
        settings = self.settings(guide=True, poll_seconds=30.0, plan_hours=24.0)
        with Running(settings) as worker:
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 2))
            edge = worker.guide_until
        self.assertGreater(edge, time.time() + 23 * 3600)
        with Running(settings) as worker:
            self.assertEqual(worker.guide_until, edge)

    def test_a_programme_that_ended_lapses_with_the_next_plan(self):
        self.library.programme("Ending", -60, 600)
        self.library.programme("Still on", -60, 600)
        logged = []
        self.addCleanup(setattr, warmer, "log", warmer.log)
        warmer.log = logged.append
        plans = lambda: sum(line.startswith("plan: ") for line in logged)
        with Running(self.settings(guide=True, poll_seconds=30.0, plan_hours=0.5 / 3600)) as worker:
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 4))
            count = len(self.cache.paths())
            self.library.con.execute("UPDATE media SET live_end = ? WHERE id = ?", (when(-1), blob("Ending")))
            done = plans()
            self.assertTrue(wait_until(lambda: plans() > done + 1))
            items = {row[0] for row in worker.state.con.execute("SELECT DISTINCT item FROM jobs")}
            self.assertEqual(items, {blob("Still on")})
            self.assertEqual(worker.state.cached_bytes(), 2000)
            self.assertEqual(len(self.cache.paths()), count)

    def test_turning_the_guide_on_takes_effect_at_once(self):
        self.library.title("Film", art=("primary",))
        self.library.programme("On now", -60, 600)
        with Running(self.settings(poll_seconds=30.0, plan_hours=24.0)) as worker:
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 3))
        self.assertFalse(any(blob("On now").hex() in path for path in self.cache.paths()))
        with Running(self.settings(guide=True, poll_seconds=30.0, plan_hours=24.0)):
            self.assertTrue(wait_until(lambda: any(blob("On now").hex() in path for path in self.cache.paths())))

    def test_a_guide_host_answering_404_for_everything_does_not_look_like_a_source_outage(self):
        for number in range(30):
            self.library.programme(f"Programme {number}", 60 + number, 600)
        self.library.title("Film", art=("primary",))
        self.cache.status_for[""] = (404, "MISS", "text/html", None)
        with Running(self.settings(guide=True, poll_seconds=30.0)) as worker:
            self.assertTrue(wait_until(lambda: self.status(worker).get("missing", 0) >= 60))
            self.assertEqual(self.status(worker).get("suspect", 0), 0)
            self.assertFalse(worker.pace.source_suspect)
            self.assertEqual(worker.pace.pause_until, 0.0)
            self.assertEqual(worker.pace.speed, 1.0)

    def test_signals_are_handled_while_a_plan_is_being_made(self):
        self.library.title("Film", art=("logo",))
        original = warmer.read_snapshot
        release = threading.Event()

        def slow_snapshot(catalogue, config):
            release.wait(10)
            return original(catalogue, config)

        warmer.read_snapshot = slow_snapshot
        self.addCleanup(setattr, warmer, "read_snapshot", original)
        self.addCleanup(release.set)
        settings = self.settings(signal_socket=self.socket_path, poll_seconds=30.0)
        with Running(settings) as worker:
            self.wait_for_socket()
            self.assertTrue(wait_until(lambda: worker.status()["planning"]))
            self.send(blob("Film"))
            self.assertTrue(wait_until(lambda: any("/Logo?" in path for path in self.cache.paths()), timeout=3.0))
            self.assertTrue(worker.status()["planning"])
            release.set()
            self.assertTrue(wait_until(lambda: not worker.status()["planning"]))

    def test_new_rows_found_during_a_plan_are_not_lost(self):
        self.library.title("Old", art=("primary",))
        original = warmer.read_snapshot
        release = threading.Event()

        def held_snapshot(catalogue, config):
            snapshot = original(catalogue, config)
            release.wait(5)
            return snapshot

        warmer.read_snapshot = held_snapshot
        self.addCleanup(setattr, warmer, "read_snapshot", original)
        self.addCleanup(release.set)
        with Running(self.settings(poll_seconds=0.05)) as worker:
            self.assertTrue(wait_until(lambda: worker.planner is not None))
            time.sleep(0.2)
            self.library.title("Fresh", art=("primary",))
            release.set()
            fresh = blob("Fresh").hex()
            self.assertTrue(wait_until(lambda: any(fresh in path for path in self.cache.paths())))

    def test_a_plan_that_blows_up_leaves_the_old_plan_and_the_worker_running(self):
        self.library.title("Film", art=("primary",))
        with Running(self.settings(rate=0.001, poll_seconds=30.0)) as worker:
            self.assertTrue(wait_until(lambda: worker.state.get("planned_at")))
        original = warmer.plan_jobs

        def broken(*_):
            raise RuntimeError("no")
            yield

        warmer.plan_jobs = broken
        self.addCleanup(setattr, warmer, "plan_jobs", original)
        settings = self.settings(rate=0.001, poll_seconds=30.0, plan_hours=1.0 / 3600, signal_socket=self.socket_path)
        with Running(settings) as worker:
            self.assertTrue(wait_until(lambda: "planning: RuntimeError" in worker.problem))
            self.assertEqual(worker.state.planned(), 3)
            self.wait_for_socket()
            self.send(blob("Film"))
            self.assertTrue(wait_until(lambda: self.status(worker).get("signals", 0) >= 1))
            self.assertTrue(worker.status()["threads_ok"])

    def test_an_unwritable_state_pauses_fetching_and_recovers(self):
        for number in range(40):
            self.library.title(f"Film {number}", art=("primary",))
        with Running(self.settings(rate=50.0, poll_seconds=30.0)) as worker:
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 3))
            real = worker.state.record
            broken = threading.Event()
            broken.set()

            def flaky(*args):
                if broken.is_set():
                    raise sqlite3.OperationalError("database or disk is full")
                real(*args)

            worker.state.record = flaky
            self.assertTrue(wait_until(lambda: worker.store_broken))
            time.sleep(0.2)
            count = self.answered(worker)
            time.sleep(0.4)
            self.assertEqual(self.answered(worker), count)
            self.assertIn("can't write the state", worker.problem)
            broken.clear()
            # The retry comes after the 5 s pause.
            self.assertTrue(wait_until(lambda: not worker.store_broken, timeout=8.0))
            self.assertTrue(wait_until(lambda: self.answered(worker) > count))

    def test_a_job_that_cannot_be_made_into_a_url_costs_only_itself(self):
        self.library.title("Film", art=("primary",))
        with Running(self.settings(rate=0.001, poll_seconds=30.0)) as worker:
            self.assertTrue(wait_until(lambda: worker.state.get("planned_at")))
        path = os.path.join(self.folder.name, "state", "warmer.sqlite")
        con = sqlite3.connect(path, isolation_level=None)
        con.execute("INSERT INTO jobs (item, form, tag, tier, rank) VALUES (?, 1, x'0011', 1, 0)", (blob("bad"),))
        con.close()
        self.library.title("Next", art=("primary",))
        settings = self.settings(poll_seconds=0.1)
        with Running(settings) as worker:
            next_item = blob("Next").hex()
            self.assertTrue(wait_until(lambda: self.status(worker).get("invalid", 0) == 1))
            self.assertTrue(wait_until(lambda: any(next_item in path for path in self.cache.paths())))
            self.assertTrue(worker.status()["threads_ok"])
            self.assertTrue(wait_until(lambda: warmer.health(settings)))

    def test_health_goes_bad_when_a_fetch_thread_is_dead(self):
        settings = self.settings(poll_seconds=30.0)
        with Running(settings) as worker:
            self.assertTrue(wait_until(lambda: warmer.health(settings)))
            self.assertTrue(worker.status()["threads_ok"])
            worker.threads.append(threading.Thread(target=lambda: None))
            self.assertFalse(worker.status()["threads_ok"])
            worker._write_status()
            self.assertFalse(warmer.health(settings))
            worker.threads.pop()

    def test_the_state_folder_being_unusable_at_the_start_does_not_exit(self):
        blocked = os.path.join(self.folder.name, "blocked")
        with open(blocked, "w") as out:
            out.write("not a folder")
        settings = self.settings(state_dir=os.path.join(blocked, "state"))
        stop = threading.Event()
        found = []
        thread = threading.Thread(target=lambda: found.append(warmer.open_warmer(settings, stop)), daemon=True)
        thread.start()
        time.sleep(0.2)
        self.assertTrue(thread.is_alive())
        stop.set()
        thread.join(timeout=5)
        self.assertEqual(found, [None])

    def test_status_scans_jobs_without_holding_the_lock_and_not_every_time(self):
        self.library.title("Film", art=("primary",))
        with Running(self.settings(poll_seconds=30.0)) as worker:
            self.assertTrue(wait_until(lambda: worker.state.get("planned_at")))
            real = worker.state.tiers
            scans = []

            def scan():
                # The fetch threads and the listener need this lock, so it has to be free during a scan.
                free = []
                other = threading.Thread(target=lambda: free.append(worker.cond.acquire(timeout=2.0) and (worker.cond.release() or True)))
                other.start()
                other.join()
                scans.append(free == [True])
                return real()

            worker.state.tiers = scan
            worker.status()
            self.assertEqual(scans[-1:], [True])
            count = len(scans)
            worker.status(detail_age=3600.0)
            self.assertEqual(len(scans), count)

    def test_status_never_contains_ids_or_paths(self):
        self.library.title("Film")
        settings = self.settings(poll_seconds=30.0)
        with Running(settings) as worker:
            self.assertTrue(wait_until(lambda: self.answered(worker) >= 6))
            text = json.dumps(worker.status())
        self.assertNotIn(blob("Film").hex(), text)
        self.assertNotIn("/Items/", text)

    def test_health_goes_bad_when_the_status_is_old(self):
        settings = self.settings()
        self.assertFalse(warmer.health(settings))
        with Running(settings):
            self.assertTrue(wait_until(lambda: warmer.health(settings)))
        old = time.time() - 600
        os.utime(os.path.join(settings.state_dir, "status.json"), (old, old))
        self.assertFalse(warmer.health(settings))


if __name__ == "__main__":
    unittest.main()
