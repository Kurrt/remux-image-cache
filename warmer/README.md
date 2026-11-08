# Remux image warmer

A background worker that fills an nginx image cache in front of [Remux](https://github.com/lostb1t/remux), so
that the first view of a poster, logo or backdrop doesn't have to wait for the image's source.

It asks for images and nothing else. It never requests a stream, a stream list or anything else that could use up
an indexer or debrid quota, and it never will: that is a rule of this project, not a setting.

Remux fetches images from a remote CDN, such as TMDB's, on request. The cache makes repeat views take a few
milliseconds, but a first view can still take from 260 ms to a second or more. This worker makes the first
view happen in the background. It asks the cache for each image with the same URLs clients use, so the entries
it creates are the ones clients will hit.

It uses only the Python standard library, and runs in a stock `python:3.14-alpine` container.

## What it warms, and in what order

| Stage | What | Rate |
|---|---|---|
| Signal | The art of whatever a viewer just saw (see below) | 5 a second, once a viewer's burst has passed |
| New | Titles and seasons whose rows just appeared in the database | 2 a second |
| Home | Hero sources, every home screen row, Continue Watching, Next Up | 1 a second |
| Titles | Posters and logos of every other movie and show, most popular first | 1 a second |
| Title art | Their backdrops and thumbs | 1 a second |
| Channels | Live TV channel logos. Off unless `WARMER_CHANNELS=on` | 0.5 a second |
| Guide | Live TV programme images, airing now through the next 24 hours, soonest first. Off unless `WARMER_GUIDE=on` | 1 a second |
| Seasons | Season posters | 1 a second |
| People | Cast photos. Off unless `WARMER_PEOPLE=on` | 0.5 a second |
| Episodes | Episode stills, most popular show first. Off unless `WARMER_EPISODES=on` | 0.5 a second |
| Full stills | The same stills at full size, in the same order. Comes with `WARMER_EPISODES=on` | 0.5 a second |

A request that turns out to be a cache hit costs nothing upstream, so it hardly counts against the rate.

People and episodes are off by default because they're most of the requests and little of the benefit. On the library
this was written against they were 90% of the plan, and without the full-size stills they warmed 7 points more of the
images clients asked for over two weeks. With them off, a viewer's signal still warms cast photos and episode stills
when someone is about to see them. Channel logos and programme images come from whichever hosts your IPTV source uses,
so they're off by default too.

Here's that library with everything on: about 1,200 movies, 1,100 shows, 120,000 episodes with a still, 32,000 people
with a photo and 740 channels, from `--plan`. The guide's figures are for one day's window.

| Stage | Requests | GB | Hours for the first fill | Fetches a day once it's all warm |
|---|---|---|---|---|
| Home | 6,800 | 0.6 | 1.9 | 76 |
| Guide | 14,000 | 0.35 | 3.9 | 12,800 |
| Titles | 7,600 | 0.7 | 2.1 | 100 |
| Title art | 5,400 | 0.4 | 1.5 | 45 |
| Seasons | 12,200 | 0.6 | 3.4 | 102 |
| Channels | 1,500 | 0.03 | 0.8 | 12 |
| People | 63,500 | 2.3 | 35 | 529 |
| Episodes | 120,300 | 3.0 | 67 | 1,002 |
| Full stills | 120,300 | 6.6 | 67 | 1,003 |

That's about 183 hours in all, and more like nine days with the guide's daily share and viewers to give way to. After
that, everything but the guide comes to 2,870 fetches a day on average, but not spread evenly. The first fill happens in
one go, so each entry's life ends at the same pace 120 days later, and the fill comes round again at the same rates.

Home rows are worked out the way Remux does it: each promoted smart collection's filter rules (tag, genre,
country, language, audience rating) and default sort. A rule it doesn't recognise doesn't narrow the row. A row
that hides played items is read to the same depth as the others, because the worker can't see who has played what.
Getting a row slightly wrong costs little, as the next stages cover every title's art anyway.

Only real image rows are requested. An item without a logo row is never asked for a logo, because that's a
404, and a collection without an image row is never asked for one, because that makes Remux generate one.

### Live TV programmes

The home screen's On Now row and Live TV's Suggested page show programme images, so with `WARMER_GUIDE=on` they come
after home, titles and channels, and once those are done they keep up as they enter the window. The Guide page itself draws only channel logos. Most programme rows are days ahead and
are of no use until around when they air, so the worker warms a moving window: rows airing now through the next 24 hours,
looked at again at each plan and each poll. A row further ahead waits until it comes within reach, and whatever airs
soonest goes first.

A programme's images aren't fetched again. When it's over, the next plan drops its row, and the worker forgets it.
nginx removes its copy when it hasn't been asked for in 60 days, the `inactive` time, or sooner if it needs the room.
Those copies aren't counted against `WARMER_BUDGET_GB` once the row is dropped, because the worker has forgotten them. On
the library this was written against, about 6,400 programme images a day come to 12,800 fetches, 0.33 GB a day and so
about 20 GB and 770,000 entries on disk after 60 days. Stay above that in `max_size` and `keys_zone`, on top of the plan.
If `keys_zone` fills, nginx removes an eighth of all entries at once, longest unasked first, and that can include
catalogue art waiting for its next look. So leave room to spare.

Clients also ask for these at original size, when a card is more than 500 pixels wide, but that isn't warmed. It would
be about 520 KB each, against 18 to 32 KB for the resized forms, so about 3 GB a day. That would fill a 60 GB cache in
under three weeks and push out everything else. An image that isn't a JPEG can't be resized, so it's kept whole in each
form, and a guide with many of those will take more than these figures. The `bytes` in the status show what they really
come to.

## The forms it asks for

nginx keeps a separate copy of each distinct form of an image, so what matters is which forms clients ask for.
These come from real access logs.

| Image | Forms |
|---|---|
| Movie and show posters | fill width up to 342, 343 to 500, and original size |
| Season posters, cast photos | fill width up to 342, and 343 to 500 |
| Episode stills | fill width 343 to 500, and original size in a last stage of its own |
| Thumbs | fill width up to 342, 343 to 500, and original size |
| Backdrops | tagged. With and without the `/0` index are one entry |
| Logos | tagged, and untagged for web loaders that only know the item's id |
| Channel logos | fill width up to 342, and original size |
| Programme images | fill width up to 342, and 343 to 500 |

An episode has no logo of its own, so its card asks for its series' art. If the series has no thumb, the card
asks for a thumb with the backdrop's tag and Remux composes one, which is the slowest first view there is. That
form is warmed for recently played shows and on a signal.

## Reacting to viewers

nginx sends the worker a one-way note for each image a viewer gets: the item id and image type, with no client
address and no query string. It's a syslog datagram to a unix socket, so nginx never waits for it. The worker's
own requests carry `X-Image-Warmer: 1` and aren't reported.

A viewer's search doesn't need to be watched, or any per-user data read. A search result page makes the client
ask for posters, and that's the signal:

- A poster means the viewer might open this item, so its logo and backdrop are fetched at once, then its thumbs and
  full-size poster. A season or episode brings its series' art, once however many of them are on the page.
- A season poster means its episode list may be next, so its episode stills are fetched, and then their full size,
  once there's nothing more urgent to do.
- A backdrop or logo means a title's page is open, so its season posters and cast photos are fetched.

A small memory stops a page of 60 posters from queueing the same work twice. The worker waits for a viewer's burst
of requests to pass before it starts, so that burst never queues behind the worker. Results are warmed from the top
of the page down, and a new page goes ahead of the one before it.

This takes time on a full page. Each poster brings three urgent fetches at 5 a second, so all of a 40-poster page is
ready after about 24 seconds, with the top results first. A lone poster's logo is ready in about half a second.

### Search results that aren't in the database

A Remux search doesn't create rows. Results for titles it doesn't know yet live in its memory for an hour, with
ids and tags that exist only there, and a row is created when someone opens one. So:

- The posters on a search page for titles Remux hasn't stored come from the source at its usual speed. They can't
  be warmed ahead, because nothing knows about them until the page asks.
- For those ids the worker fetches only the untagged logo, which a web loader asks for at the moment of the click,
  and only after any art that's certain to be used.
- Once the title is opened, its new rows are found by the next poll and warmed at the New stage.

## Being a good neighbour

- **To Remux.** The database is opened read-only, with `mode=ro` and `query_only`, from a read-only mount, and
  every read is one short query on its own connection, a few hundred ms at most. This matters more than it looks:
  a long read stops the WAL being checkpointed and lets it grow, and short reads keep it small. The columns it may
  read are listed in `warmer.py` and enforced by SQLite's authorizer, so nothing else can be read by mistake.
- **To viewers.** Background stages pause for 20 seconds after any viewer's image request, except new rows. At
  most four requests are in flight, and the steady rate is a small part of what nginx allows for fetches.
- **To the source.** Remux answers 404 for any failure fetching from its source, so a TMDB 429 or outage reaches
  this worker only as a rise in 404s. When 5 of the last 10 images it really fetched answer 404, or are
  refused, it treats that as a failing source: it backs off, sends one request at a time to find out when the source
  recovers, and doesn't hold those answers against the rows. A failure nginx or Remux reports directly, a connection
  failure, 429 or 5xx, gets the same back-off, and `Retry-After` is respected. After a failure it sends one request at a
  time until a real fetch works, or 20 quick cache hits in a row. It halves its speed when answers get slow, signal work
  included, and recovers slowly on quick answers, where a hit counts for a tenth of a real fetch. A lone 404 is retried
  after an hour, then a day, a week and every 30 days. A lone refusal is retried after 6 hours. An SVG logo can't be
  stored by the cache, so it's only tried every 30 days.
- **To other hosts.** Channel logos and programme images come from hosts the worker can't see. Whatever those hosts do
  says nothing about the catalogue's source, so their 404s don't count towards that rule, their slow answers don't lower the
  speed, and an image the cache can't store isn't asked for again for 30 days. A failure nginx or Remux
  reports directly still gets the back-off. There's one background thread, so a host that hangs does hold up later stages
  for as long as its answers take. Signals have their own workers and aren't affected.

If the worker stops, is slow or was never started, nginx serves as usual. If nginx or Remux is down, the worker
waits and tries again. If its state can't be written, as on a full disk, it pauses fetching and tries again rather
than exiting. A row it can't make sense of is left out and counted. Its state is in its own small SQLite file, so a
restart resumes where it stopped.

## TMDB's terms

[TMDB's API terms](https://www.themoviedb.org/api-terms-of-use) say not to cache anything from TMDB for longer
than 6 months (section 1.C), and not to use an excessive amount of bandwidth, at their discretion. There's no
clause about bulk images or a numeric limit for images. [Their rate limit page](https://developer.themoviedb.org/docs/rate-limiting)
mentions an upper limit around 40 requests a second for the API, there to stop bulk scraping, and asks you to
respect a 429.

So the defaults are these:

- Nothing stays on disk for more than 180 days. An entry lives for 120 days in the cache, and then at most 60 days
  of `inactive`. A restart resets nginx's `inactive` timer, so a daily sweep by file age removes anything older than
  178 days whatever nginx thinks.
- The steady rate is 1 a second, then 0.5 for people and episodes if they're on, well below that limit.
- The download recurs. A cache hit keeps an entry from being evicted but doesn't extend its life, so all art from
  TMDB is fetched again when it expires: every 120 days, and every 30 for untagged logos. That's 323 fetches a day on
  average with the defaults, and 2,870 with people and episodes on, on the library above. A fetch replaces the entry,
  so its file is new again, and the sweep never reaches art that's being kept up.

This is still a bulk download of images, and a person at TMDB might see it differently from a viewer browsing. Turning
on people and episodes is the part that pushes hardest. Using TMDB data also comes with an attribution requirement, and
commercial use needs a separate agreement. Both are between you and TMDB, as they are for Remux itself.

## Setting up

The worker needs nginx in front of Remux's image route, caching with a key that treats clients' sizes as a few fixed
steps, and setting `X-Img-Cache` to `$upstream_cache_status`. It also needs these in nginx's config:

```nginx
# A larger cache. Each megabyte of keys_zone holds about 8000 entries, and when it fills, nginx evicts entries
# early whatever max_size says. Stay above the worker's plan: see --plan.
proxy_cache_path /cache levels=1:2 keys_zone=img:192m max_size=60g min_free=5g inactive=60d use_temp_path=off;

# The note to the worker: the item and type only, for a viewer's successful image requests.
log_format warm_signal '$img_id_key $img_type_key';
map "$request_method:$status:$http_x_image_warmer" $warm_signal {
  default 0;
  "GET:200:" 1;
  "GET:304:" 1;
}

# Remux answers 404 when its source fails for a moment. Don't keep one that only the worker asked for, or a
# viewer arriving after the source recovers would be given it.
map "$http_x_image_warmer:$upstream_status" $warm_no_store {
  default 0;
  "1:404" 1;
}

# In the image location:
proxy_cache_valid 200 120d;
proxy_no_cache $warm_no_store;
access_log syslog:server=unix:/run/warmer/signal.sock,tag=warm,nohostname warm_signal if=$warm_signal;
```

Give untagged logos a life of 30 days, because a warmed copy that lapses daily is no use. If you change either life,
set `WARMER_LIFE_DAYS` and `WARMER_UNTAGGED_LIFE_DAYS` to match. Make `/0` and no index share an entry, because Remux
has one image of each type and ignores the number. The worker only asks for `Backdrop/0`.

And these on the nginx container:

```yaml
    # With the worker stopped, nginx writes an error line for each image, because it can't send the note. It keeps
    # serving normally and adds no delay, but this line would otherwise fill Docker's log for as long as the worker is away.
    logging:
      driver: json-file
      options:
        max-size: 5m
        max-file: "2"
    # The sweep that keeps everything inside the 6 months.
    entrypoint:
      - sh
      - -c
      - |
        while :; do nice -n 19 find /cache -type f -mtime +178 -delete; sleep 86400; done &
        exec nginx -g 'daemon off;'
    volumes:
      # Where nginx sends its notes. nginx starts and serves normally without it.
      - ${DATA_DIR:-./data}/warmer/signal:/run/warmer:ro
```

Then create the worker's folders, owned by the user it runs as:

```bash
sudo install -d -o 65534 -g 65534 "$DATA_DIR/warmer/state" "$DATA_DIR/warmer/signal"
```

The database files must be readable by that user. Set `user:` in `compose.example.yaml` if yours aren't readable by everyone.

Recreating the nginx container is needed, as a reload can't change the size of `keys_zone` or add a mount. Entries
already in the cache stay valid.

To remove the worker, stop it, undo those changes and recreate nginx. nginx then trims the cache to the old `max_size`
by itself. If you clear the cache but keep the worker, delete its state folder too, or it goes on believing the art
is cached until each entry's next look comes round.

## Settings

Everything has a default. Put `WARMER_<NAME>=value` lines in the `.env` file beside `compose.yaml` (copied from `compose.example.yaml`), then recreate the
container. Changing people, episodes, channels, the guide or the home row depth takes effect at once.

| Name | Default | What |
|---|---|---|
| `DB_PATH` | `/remux/db.sqlite` | Remux's database. Mount its whole folder read-only |
| `CACHE_URL` | `http://remux-images:8080` | The nginx cache |
| `STATE_DIR` | `/state` | Where the worker keeps its state and status |
| `SIGNAL_SOCKET` | `/run/warmer/signal.sock` | The socket for nginx's notes. Set it empty to turn signals off. Any other setting left empty keeps its default |
| `RATE` | `1` | Fetches a second for the catalogue stages |
| `LONG_TAIL_RATE` | `0.5` | Fetches a second for channels, people, episodes and full-size stills |
| `NEW_RATE` | `2` | Fetches a second for new rows |
| `SIGNAL_RATE` | `5` | Fetches a second for signals |
| `SIGNAL_WORKERS` | `3` | Requests in flight for signals |
| `SIGNAL_QUIET` | `0.25` | Seconds of quiet that mean a viewer's burst has passed |
| `SIGNAL_MAX_HOLD` | `2` | The longest a burst can hold signal work back |
| `SIGNAL_BURST_GAP` | `1.5` | Seconds of quiet that make the next poster part of a new page |
| `SEASON_EPISODES` | `40` | Stills fetched when a season poster is seen |
| `CAST_LIMIT` | `20` | Cast photos fetched when a title's page opens |
| `DEDUPE_HOURS` | `12` | How long a signal for the same item is ignored |
| `POLL_SECONDS` | `30` | How often to look for new rows |
| `PLAN_HOURS` | `6` | How often to rebuild the plan, which is how changed art and removed rows are noticed. See below for the other time it's rebuilt |
| `YIELD_SECONDS` | `20` | How long background stages pause after a viewer's request |
| `HOME_ROW_ITEMS` | `30` | Items warmed from each home row |
| `PEOPLE` | `off` | Whether to warm all cast photos, not just on signals |
| `EPISODES` | `off` | Whether to warm all episode stills, in both forms, not just on signals |
| `CHANNELS` | `off` | Whether to warm live TV channel logos |
| `GUIDE` | `off` | Whether to warm live TV programme images for what airs in the next 24 hours |
| `BUDGET_GB` | `50` | Stop adding channels, people and episodes when this much is cached. What's stored is still kept up, as that doesn't add to it. Programme images aren't counted and carry on. Keep it below `max_size` |
| `MIN_FREE_GB` | `20` | Pause all fetches when the disk has less than this free |
| `TOUCH_HOME_DAYS` | `7` | How often to ask again for home screen art |
| `TOUCH_DAYS` | `50` | How often to ask again for everything else. Keep it below `inactive` |
| `TOUCH_UNTAGGED_DAYS` | `25` | How often to ask again for untagged logos |
| `LIFE_DAYS` | `120` | How long nginx keeps a tagged image. Must match its config |
| `UNTAGGED_LIFE_DAYS` | `30` | How long nginx keeps an untagged logo. Must match its config |
| `REQUEST_TIMEOUT` | `60` | Seconds to wait for one answer |
| `SLOW_MISS_MS`, `SLOW_HIT_MS` | `4000`, `500` | Answers slower than this halve the speed |
| `BACKOFF_SECONDS`, `BACKOFF_MAX_SECONDS` | `5`, `300` | The pause after a failure, and its limit |
| `LOG_SECONDS` | `300` | How often to log progress |
| `STATUS_DETAIL_SECONDS` | `60` | How often the per-stage counts in the status are refreshed |
| `MAX_FETCHES` | `0` | Stop fetching after this many. For trying it out |

## Memory

A plan is read and written a page at a time, so the worker's memory hardly depends on the size of the catalogue. With
about 205,000 image rows it peaks at about 60 MB, or 85 MB with everything on, and running `--plan` inside the
container adds about 45 MB. The limit in `compose.example.yaml` leaves plenty of room. If the worker ever runs out, Docker
restarts it and the log shows only `starting` lines, so raise `mem_limit`.

## Looking at it

```bash
# What would be warmed, with sizes, times at the set rates and fetches a day once it's warm. Reads the database, fetches nothing.
docker exec remux-warmer python3 -I /app/warmer.py --plan

# Progress as JSON, by stage
docker exec remux-warmer python3 -I /app/warmer.py --status

docker logs remux-warmer
```

The logs and status hold counts only, never ids, titles or paths. `source_suspect` in the status means the 404 rule
above has tripped. The health check says the main loop and fetch threads are alive, and nothing more, so a Remux
outage doesn't turn it unhealthy.

`last_problem` in the status is the latest problem, after the date and time it last happened, in UTC. It's empty again
once 20 answers and 3 looks at the database in a row have gone well, or just the looks if there's nothing to fetch, and
only if the worker isn't backing off or doubting the source. A 404 counts as an answer going well, and so does a cache
hit. 20 quick hits in a row end the back-off after a failure, but doubt about the source takes real fetches to settle,
because a hit never reaches it. Once a problem only a new plan can settle has happened, `last_problem` stays until a plan
works, even if it goes on to show a later problem that wouldn't need one. A column or table Remux no longer has shows as
`database layout not recognised`. If it was one the worker can do without, the next plan carries on without it. If it
can't, the worker idles until the layout changes.

Remux deletes its guide rows when it imports a guide. When those are the newest image rows, the next poll logs
`newest image rows have gone, planning again` and rebuilds the plan, so one rebuild per guide import is expected. It
reads the database once more and fetches nothing that's already been done.

## Tests

From this folder:

```bash
python3 -m unittest -v test_warmer
```

They need nothing but Python: a small database in Remux's layout and a fake cache that answers like the nginx config.
