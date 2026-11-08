# Security

## Reporting a problem

Please report security problems privately, using GitHub's private vulnerability reporting.
On this repo's **Security** tab, open **Advisories** and choose **Report a vulnerability**.
Please don't open a public issue for one. If that button isn't there, open an issue that only
asks how to report something privately, and leave out the details.

## What access this has

- **The cache** serves images the same way Remux's image route does, so it's as open as that route is.
  It passes no client header, cookie or query string to Remux, and it drops cookies and sets its own cross-origin
  header on what it keeps.
- **The warmer** reads Remux's database read-only, from a read-only mount, and can read only the columns
  listed in `warmer.py`. It runs as an unprivileged user with no capabilities and a read-only root
  filesystem. Its logs and status hold counts only, never ids, titles or paths.
- **The signal socket.** nginx tells the warmer which images viewers get, as an item id and image type
  only, over a unix socket.
