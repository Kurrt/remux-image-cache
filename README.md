# remux-image-cache

A disk cache and resizer for the images [Remux](https://github.com/lostb1t/remux) serves, and a background
worker that fills it before anyone asks.

Remux ignores the size a client asks for and fetches each image from its source, such as TMDB, on every
request. On a TV that means full-size posters and a wait on every first view. This puts two things between the
client and Remux:

- **An nginx cache** in front of `GET` and `HEAD` on `/items/{id}/images/{type}`. It shrinks posters and
  thumbs to one of two fixed widths (342 or 500 pixels), keeps the result on disk, and leaves everything else
  at its original size. Repeat views come from disk in a few milliseconds.
- **A warmer** that reads Remux's database, read-only, and asks the cache for the images viewers are likely to
  want, slowly and in the background. It also reacts to what viewers are looking at: when a poster is shown,
  that title's logo and backdrop are fetched first. See [warmer/README.md](warmer/README.md) for what it warms,
  its rates and every setting.

The cache is optional. Nothing depends on the warmer, and if the cache stops, Traefik falls back to Remux's own
route: posters still load, at full size and without the cache.

## What you need

- Remux, running as a container named `remux` on port 3000. Change `http://remux:3000` in
  [nginx/nginx.conf](nginx/nginx.conf) if yours differs.
- [Traefik](https://traefik.io/) with the Docker provider, on a Docker network shared with Remux. The cache is
  routed by labels, so Remux itself isn't touched or restarted.
- Docker Compose, and room on disk. The cache is capped at 60 GB by default.

## Set up

```bash
git clone https://github.com/Kurrt/remux-image-cache.git
cd remux-image-cache
cp compose.example.yaml compose.yaml
cp .env.example .env     # then edit it
sudo install -d -o 65534 -g 65534 data/warmer/state data/warmer/signal
docker compose --profile all up -d
```

Check it from a client's side by asking for the same poster twice. The `X-Img-Cache` header goes from `MISS`
to `HIT`:

```bash
curl -sI "https://your-remux/items/<an item id>/images/primary?fillWidth=300" | grep -i x-img-cache
```

The warmer logs its progress to `docker logs remux-warmer`. With everything on, the first fill of a large library
takes days at its polite rates: read "What it warms" first and leave out the stages you don't want.

Make the `remux-images` router's `priority` higher than your Remux router's if you've changed either.

To remove it, stop both containers and delete the `remux-images` router. Nothing else was changed.

## Layout

| Path | What |
|---|---|
| `compose.example.yaml` | Both containers, and the Traefik labels for the cache |
| `nginx/nginx.conf` | The cache and resizer |
| `warmer/warmer.py` | The worker. One file, Python standard library only |
| `warmer/test_warmer.py` | Its tests |
| `warmer/README.md` | What it warms, how it treats Remux and the source, and all settings |

## Limits worth knowing

- **TMDB's terms.** They say not to cache TMDB data for longer than 6 months. Nothing stays on disk for more
  than 178 days: entries live 120 days, and a daily sweep removes anything older. Warming everything is still a
  bulk download, so read [the section in the warmer's README](warmer/README.md#tmdbs-terms) before turning on
  the large stages.
- **Images only.** The warmer never asks for a stream or a stream list, which would use up indexer or debrid
  quota.
- **Remux's database.** The warmer opens it read-only, one short query at a time, and can read only the columns
  listed in `warmer.py`.

## Tests

```bash
cd warmer
python3 -m unittest -v test_warmer
```

## Licence

MIT. See [LICENSE](LICENSE). Contributions are welcome: see [CONTRIBUTING.md](CONTRIBUTING.md).
