# LMS-YouTube — `yt-dlp Download` Feature Branch

> **Status: mothballed / not submitted as PR.**  
> This document captures the design and implementation for future reference.

---

## Overview

This branch adds three things to the LMS YouTube plugin:

1. **yt-dlp download feature** — a fire-and-forget background download of any video, playlist, or channel reachable from within the plugin UI, triggered from the Material skin's ⋮ context menu or from a currently-playing track's More menu.
2. **German (DE) and Danish (DA) translations** — added for a number of existing strings that were missing those locales, plus all new download-related strings in EN/DE/DA/CS.
3. **GitHub Actions release workflow** — repackages the plugin under a standalone `YouTubeDL` namespace for installation alongside the upstream plugin.

---

## New Files

| File | Purpose |
|---|---|
| `plugin/Download.pm` | All download logic: CLI registration, process launch, log viewing |
| `plugin/HTML/EN/plugins/YouTube/html/downloadlog.html` | Terminal-style web log viewer page |
| `.github/workflows/release.yml` | Automated fork-release workflow |

---

## Modified Files

| File | What changed |
|---|---|
| `plugin/Plugin.pm` | Loads `Download`; registers three new CLI commands; adds `itemActions` to video and playlist items in `_renderList`; registers the log page web route; adds three new pref defaults |
| `plugin/Settings.pm` | Adds `File::Spec`; exposes three new prefs; adds media folder validation on save |
| `plugin/HTML/EN/plugins/YouTube/settings/basic.html` | New Download Settings section (media folder + two output templates) |
| `plugin/strings.txt` | DE/DA translations for ~15 previously untranslated existing strings; 25 new download-related strings in EN/DE/DA/CS |

---

## Architecture

### `Download.pm` — module structure

```
registerCLI()           — wires CLI commands into the LMS dispatch table
cliDownload()           — CLI handler: "youtube download url:<url>"
cliDownloadLog()        — CLI handler: "youtube download log" (log viewer menu)
makeDownloadAction()    — called from Plugin.pm to build itemActions.download hashes
downloadInfoMenu()      — TrackInfo provider (fires from More menu on a playing track)
downloadHandler()       — OPML callback: initiates download, returns status items
startDownload()         — orchestration: resolve binary, URL, output path, command; dispatch to OS launcher
_launchUnix()           — fork+exec implementation for Linux/macOS
_launchWindows()        — Win32::Process::Create implementation for Windows
webDownloadLog()        — web handler for downloadlog.html
getRecentLogContent()   — reads the log file backwards to the last === separator ===
_parseUrl()             — normalises all supported URL forms into ($type, $id)
_buildYtUrl()           — turns ($type, $id) back into a canonical https:// URL for yt-dlp
_outputTemplate()       — resolves the yt-dlp -o template from prefs or defaults
_buildCommand()         — assembles the full yt-dlp argument list
_ytdlpBinary()          — delegates to Utils::yt_dlp_bin(), same as ProtocolHandler
_mediaFolder()          — resolves pref → server audiodir → mediadirs → OS music dir
_logFile()              — returns <LMS log dir>/yt-dlp-download.log
```

### CLI commands

Two commands are registered via `registerCLI()` (called from `Plugin::initPlugin`):

```
youtube download url:<url>      # start a download (not a query; no client required)
youtube download log            # fetch recent log lines as a menu (query, has tags)
```

`cliDownload` accepts both the tagged form (`url:`) used by `itemActions`, and the
positional form (`_p2`) for direct telnet/JSON-RPC use.

Two further commands are registered directly in `Plugin::initPlugin` to back the
`itemActions.info` slot on browse-list rows:

```
youtube video info id:<id> name:<title>      # More sub-menu for a video
youtube playlist info id:<id> name:<title>   # More sub-menu for a playlist
```

### Menu integration — browse lists

`_renderList` in `Plugin.pm` is extended so that every `youtube#video` item and every
`youtube#playlist` item carries an `itemActions` hash:

```perl
# video
$item->{itemActions} = {
    info => {
        command     => ['youtube', 'video', 'info'],
        fixedParams => { id => $id, name => $title },
    },
    download => Plugins::YouTube::Download::makeDownloadAction('video', $id),
};

# playlist
$item->{itemActions} = {
    info => {
        command     => ['youtube', 'playlist', 'info'],
        fixedParams => { id => $id, name => $title },
    },
    download => Plugins::YouTube::Download::makeDownloadAction('playlist', $id),
};
```

In Material skin, when `itemActions.info` is present, tapping ⋮ fires the `info`
command and renders its result as a sub-menu. The Download option is reached through
that sub-menu:

- **`cliVideoInfo`** returns up to three items: **Download** (item 0), **Play from
  beginning** (item 1), and optionally **Play from last position** (item 2, when a
  `yt:lastpos-<id>` cache entry exists).
- **`cliPlaylistInfo`** returns a single item: **Download**.

The `download` key in `itemActions` (produced by `makeDownloadAction`) provides a
direct-action shortcut for clients that honour that specific slot natively.

Note that `youtube#channel` items are deliberately left without `itemActions` — only
video and playlist items get them.

### Menu integration — currently-playing track

A `Slim::Menu::TrackInfo` provider (`downloadInfoMenu`, registered as `youtubedownload`
after `bottom`) handles tracks already in the play queue. This is a separate OPML
path: it returns a `type: url` item pointing at `downloadHandler`, which calls
`startDownload('video', $id)` immediately and returns status text (download
started / PID / destination folder / link to log) as a flat item list.

### Process launch — Unix

`_launchUnix` uses a plain `fork`+`exec` rather than `AnyEvent::Util::run_cmd`. The
reasons are documented extensively in the source and are worth preserving here:

- `run_cmd` is designed for capturing subprocess output and driving a callback on
  exit. A long-running fire-and-forget download needs neither.
- **`SIG{CHLD}` must not be touched — not even temporarily with `local`.** LMS's
  AnyEvent event loop uses an internal SIGCHLD watcher to detect when the yt-dlp
  child launched by `ProtocolHandler::getNextTrack` (via `run_cmd`) has exited.
  Clobbering it causes that callback never to fire, leaving the playing playlist
  stuck between tracks. This was a hard-won lesson from earlier debugging of the
  `getNextTrack` retry mechanism. The branch avoids all `SIG{CHLD}` manipulation;
  instead `POSIX::setsid()` in the child makes it a new session leader, so init
  (PID 1) reaps the orphan when it exits — standard POSIX behaviour for orphaned
  processes.
- LMS ties STDIN/STDOUT/STDERR to `Slim::Utils::Log::Trapper` objects. Perl's
  `open()` on a tied glob calls `OPEN()` on the tied object, which `Trapper` does
  not implement. All fd manipulation in the child therefore uses raw POSIX calls
  (`POSIX::open`, `POSIX::dup2`, `POSIX::close`) to bypass the tied-handle layer
  entirely. The log file path is resolved in the **parent** before `fork()` so the
  child never calls any LMS modules.
- All signal handlers are reset to `DEFAULT` in the child so that yt-dlp (a
  PyInstaller binary) gets a clean signal table and can `waitpid()` on its own
  ffmpeg children.

### Process launch — Windows

`_launchWindows` mirrors what `ProtocolHandler::getNextTrack` does on Windows:
`Win32::Process::Create` with `cmd.exe /c` as the launcher, appending
`>> logfile 2>&1` to the shell string. The `inherit` flag is `0` to avoid
inheriting LMS's tied stdio handles. Unlike `getNextTrack`, there is no
temp-file/polling-timer dance because the caller does not need to read yt-dlp's
output.

### Log file

Every download appends to `<LMS log dir>/yt-dlp-download.log`
(resolved via `Slim::Utils::OSDetect::dirsFor('log')`). Each run is delimited by a
timestamp separator written by the child immediately after `dup2`:

```
=== 2025-04-01 14:23:11 ===
/usr/local/bin/yt-dlp https://www.youtube.com/watch?v=... -x -o ...
[yt-dlp output follows]
```

`getRecentLogContent` uses `File::ReadBackwards` to scan from the end of the file to
the last separator, avoiding a full read. `cliDownloadLog` does the equivalent scan
forward after locating the last separator index. Both show only the most recent run,
falling back to the last 50 lines if no separator is found. A safety cap of 1000
lines is applied in `getRecentLogContent`.

### Web log page

`downloadlog.html` is a minimal terminal-aesthetic page (dark green on black,
monospace font) registered at `plugins/YouTube/downloadlog.html`. It auto-refreshes
every 2 seconds via `<meta http-equiv='Refresh' content='2'>`. Content is rendered
newest-first using `flex-direction: column-reverse` on the `<pre>` block. A link to
this page is included in the status items returned by both `cliDownload` and
`downloadHandler`.

### yt-dlp command

`_buildCommand` produces:

```
yt-dlp <url>
  -x                             # extract audio only
  -o <output template>
  -f bestaudio
  --parse-metadata 'playlist_index:%(track_number)s'
  --parse-metadata '%(release_date,upload_date)s:(?P<meta_date>[0-9]{4})'
  --embed-metadata
  --embed-thumbnail
  --convert-thumbnails jpg
  --postprocessor-args 'ThumbnailsConvertor:-vf scale=500:500:force_original_aspect_ratio=increase,crop=500:500'
  [--no-abort-on-error]          # playlists only
```

The `release_date`/`upload_date` fallback in `--parse-metadata` is the same pattern
used in the standalone yt-dlp bash/Python scripts for correct year tags on YouTube
Music downloads.

### URL parsing

`_parseUrl` accepts:

| Input form | Resolved as |
|---|---|
| `youtube://www.youtube.com/v/<id>` | video |
| `youtube://<id>` (bare 11-char ID) | video |
| `ytplaylist://playlistId=PL...` | playlist |
| `ytplaylist://channelId=UC...` | playlist (channel) |
| `https://*.youtube.com/watch?v=<id>` | video |
| `https://youtu.be/<id>` | video |
| `https://*.youtube.com/playlist?list=PL...` | playlist |
| `https://music.youtube.com/playlist?list=PL...` | playlist |
| `https://*.youtube.com/channel/<id>` | playlist (channel) |
| `https://*.youtube.com/c/<n>` or `/user/<n>` | playlist (channel) |

`_buildYtUrl` maps `($type, $id)` back to a canonical `https://` URL that yt-dlp
understands.

### Settings

Three new preferences, initialised in `Plugin.pm` and exposed via `Settings.pm`:

| Pref key | Default | Description |
|---|---|---|
| `download_media_folder` | LMS `audiodir`, then `mediadirs` then the OS music folder.| Root folder for downloads. |
| `download_output_playlist` | `<media folder>/YouTube/%(playlist)s/%(playlist_index)03d.%(title)s.%(ext)s` | yt-dlp `-o` template for playlists. |
| `download_output_video` | `<media folder>/YouTube/Singles/%(uploader)s - %(title)s.%(ext)s` | yt-dlp `-o` template for single videos. |

If a custom template value is an absolute path it is used as-is; otherwise the
resolved media folder is prepended via `File::Spec->catfile`.

`Settings.pm` adds a folder validation step that runs on save only when a non-empty
value is provided. It checks existence (`-d`) and then probes write permission by
creating and immediately deleting a temp file (`.yt_write_test_<pid>`), working
around `-w` being unreliable on Windows. The result is surfaced in the settings page
as a green ✓ or red ✗ with a translated message.

The settings page also sanitises the folder input value before saving: strips
leading/trailing whitespace, strips surrounding shell quotes (`"..."` or `'...'`),
and removes backslash-escaped spaces (e.g. `/mnt/My\ Music` → `/mnt/My Music`).

### Translations

The branch adds DE and DA translations for approximately 15 existing strings that
were previously missing those locales: codec settings, sorting options and their
long description, cache TTL, channel/playlist ID labels, prefix/suffix labels,
yt-dlp binary label and description, and a few others. All 25 new download-related
strings are provided in EN/DE/DA/CS.

---

## GitHub Actions Release Workflow

`.github/workflows/release.yml` triggers on tags matching `dl*` and:

1. Copies `plugin/` into `release/YouTubeDL/`.
2. Renames all `YouTube` directory names to `YouTubeDL` (using `find -depth -execdir mv`
   to safely rename bottom-up).
3. Rewrites all `Plugins::YouTube` namespace strings to `Plugins::YouTubeDL` and all
   `plugins/YouTube` web paths to `plugins/YouTubeDL` throughout every file.
4. Injects a combined version (`<xml_version>-<tag>`) and the display name `YouTubeDL`
   into `install.xml`.
5. Zips the result as `YouTubeDL-<full_version>.zip` and computes a SHA1.
6. Creates a GitHub Release with the zip attached.
7. Prints the `repo.xml` snippet to the action log for manual copy-paste.

The combined version scheme (`xml_version` from `install.xml` + the git tag) ensures
release filenames are unique across multiple tags against the same plugin version.
This allows installing the fork from a personal LMS plugin repository URL without
conflicting with the upstream YouTube plugin.

---

## Why This Was Not Submitted as a PR

The download feature has a hard dependency on **ffmpeg** being installed on the host
system — yt-dlp requires it for audio extraction (`-x`), thumbnail conversion
(`--convert-thumbnails jpg`), and the postprocessor crop/scale step. Resolving this
for an upstream PR would require a decision on one of several unappealing options:
shipping large platform-specific ffmpeg binaries in the repo, adding user-facing
install instructions, implementing a platform-specific auto-download mechanism, or
silently degrading when ffmpeg is absent. Any of those felt too intrusive for a PR
against a project where the maintainer's bandwidth is limited and the chance for a
feature of this scope being accepted is low.

The UI integration (Material ⋮ menu, `itemActions`, info sub-menus) also adds
considerable surface area for a maintainer who would need to understand and maintain
it. In practice, the only functionality needed personally is the `youtube download`
and `youtube download log` CLI commands accessible via JSON-RPC or telnet — the
entire Material/UI layer is overhead for that use case. The next step is a minimal
standalone plugin exposing only those two commands, with no UI integration.

## Dependencies

No new Perl module dependencies beyond what the plugin already requires, with two caveats:

- **`File::ReadBackwards`** — used in `getRecentLogContent`. A pure-Perl CPAN module,
  commonly present in LMS Perl distributions. If absent, `->new` returns undef and
  the log viewer shows an empty-log message rather than crashing.
- **`Win32::Process`** — Windows only, loaded with `eval { require ... }` so a
  missing module produces a logged error message rather than an exception.