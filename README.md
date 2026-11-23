<div align="center">

# ◈ LibreHeatmaps

**Free, open-source GPS heatmaps.**  
*Your data stays local. Your history stays yours.*

<table>
  <tr>
    <td align="center"><strong>Python only</strong></td>
    <td align="center"><strong>Local-first</strong></td>
    <td align="center"><strong>No account required</strong></td>
    <td align="center"><strong>MIT licensed</strong></td>
  </tr>
</table>

</div>

<p align="center">
  <a href="#get-started">Get started</a> ·
  <a href="#import-activities-your-way">Import activities your way</a> ·
  <a href="#explore-your-history">Explore your history</a> ·
  <a href="#settings">Settings</a> ·
  <a href="#privacy">Privacy</a>
</p>

---

LibreHeatmaps is a free, open-source **GPS activity viewer written in Python**. Explore your routes as heatmaps, review ride and activity statistics, and preview individual GPS tracks.

Your imported data stays on **your own computer** in a local SQLite database. No subscription, account, or third-party Python packages required.

## Get started

1. Install **[Python 3.9+](https://www.python.org/downloads/)**.
2. Download **`LibreHeatmaps.py`** and run it (double-click on Windows, or run `python3 LibreHeatmaps.py`). The interface opens in your browser.
3. On first launch, select **Import my GPS data**. For later imports, use the top-left **Import GPS files or account ZIP** button or drag-and-drop files directly onto LibreHeatmaps. With no imported routes, the map starts in Nashville, Tennessee.

### Download your complete Strava data

1. Sign in to [Strava](https://www.strava.com/).
2. Go to **Settings → My Account**.
3. Under **Download My Account**, select **Get Started**.
4. Request your archive, download it when ready, then select the **entire ZIP as-is**—you do not need to extract it or choose individual files.

> [!TIP]
> Keep the full export, including `activities.csv` and bike/gear files, to populate bike names, moving time, and elevation. Already imported? Choose **Activity statistics → Add activity details** and select the ZIP again. Your existing routes are preserved.

You can also import individual **FIT, FIT.GZ, GPX, GPX.GZ, TCX, and TCX.GZ** files (and `.gps` files containing supported FIT/GPX/TCX data). **Strava is optional.** Complete Garmin account export ZIPs are also supported, including FIT files inside nested ZIP archives, without extraction.

---

## Import activities your way

<details open>
<summary><strong>Manual:</strong></summary>

Keep using the top-left **Import GPS files or account ZIP** button, or drag-and-drop activity files onto the LibreHeatmaps window. Multiple files are queued and imported sequentially.

</details>

<details>
<summary><strong>Your watched folder:</strong></summary>

In **Settings → Imports & connections**, optionally designate any existing local or network folder and save. No default folder is assigned. LibreHeatmaps checks for new FIT/GPX/TCX/`.gps` files at startup, including those added while it was closed, and then about every 15 seconds while running. Originals are not moved or deleted.

</details>

<details>
<summary><strong>Garmin Edge 1030 wireless:</strong></summary>

Connect a **Garmin Connect Personal API** key in **Settings → Imports & connections** and choose **Sync now**. The Edge first uploads completed rides over Wi-Fi/Bluetooth to Garmin Connect; LibreHeatmaps then securely retrieves their ZIP/FIT recordings through Garmin's official Personal API. It checks on startup and then once every 60 minutes while running by default, with selectable 15-, 30-, 60-, 120-, or 240-minute intervals or a custom interval (15–1440 minutes); **Sync now** remains available at any time. A manual sync resets the automatic countdown. If you also choose a watched folder, Garmin FIT originals are saved there without overwriting existing files. This optional feature needs Garmin Connect and internet access; it is not a direct local Wi-Fi receiver. The key is saved locally first; a successful activity sync is what verifies authentication. Reconnect or revoke the key in Garmin Connect if the API rejects it. The API key remains in your private local data folder and is not included in library backups.

</details>

<details>
<summary><strong>Optional Strava GPX desktop sync (experimental):</strong></summary>

Import the full Strava account archive once, then open **Settings → Imports & connections → Strava GPX sync**. Read the website-automation warning and choose **Sign in for auto-sync**. This opens a separate Edge/Chrome browser profile and requires a one-time sign-in; the standard browser's protected cookies are neither accessed nor copied. LibreHeatmaps keeps only that isolated browser login profile locally; no Strava password or developer API key is entered into LibreHeatmaps. With an active session it checks the newest 25 activity links by default (configurable from 1 to 50) at startup and then hourly by default, or at a configurable 15–1440-minute interval. It lets the activity list finish rendering and follows bounded pagination when needed; if fewer than the chosen number are available, the completed sync reports the actual count. **Sync now** checks immediately and downloads GPX files only for activity IDs missing from the local library. When you have chosen a watched folder, new GPX originals can be saved there; otherwise files are processed locally without creating an import folder. **Disconnect & delete session** closes the dedicated browser and deletes its local credentials while preserving all imported rides. The saved session lives in the local LibreHeatmaps data folder, so restarting the program or updating its ZIP does not require signing in again while Strava keeps the login valid. If Strava explicitly requests login again, scheduled checks pause (including after restart) and Settings shows **Reconnect Strava**; reconnecting reuses the dedicated browser profile and leaves rides intact. Temporary network or website failures do not delete that sign-in. The original recent-ten GPX import was confirmed working in a Windows user field test. The new customizable 1–50 activity scan, long-term scheduled sync, and future Strava website changes still require field testing. Strava prohibits automated website extraction under its current terms; this is an unofficial opt-in method, not an approved integration. Official OAuth/API syncing is not included.

</details>

<details>
<summary><strong>Garmin full-history import:</strong></summary>

For accounts with hundreds or thousands of Garmin activities, choose **Import full history** in **Settings → Imports & connections**. This is separate from the normal recent-500 hourly scan. LibreHeatmaps continuously scans all available historical pages in one operation with durable per-activity progress across restarts. On the main **Import activities** panel and **Your GPS heatmap** map caption, progress is shown while the import runs; the displayed heatmap is refreshed after every 50 newly imported recordings, without stopping the full-history download. It pauses only when manually requested or when Garmin requires error recovery or a rate-limit cooldown. **Pause** and **Resume full import** are available; retries after rate limits wait rather than repeatedly contacting Garmin. The existing database and original files are never cleared or overwritten.

</details>

<details>
<summary><strong>Garmin USB:</strong></summary>

The **Find connected Garmin** and **Copy & import FIT rides** controls remain in the same Settings section for wired local transfers.

</details>

All normal imports and offline viewing work without a Garmin account or API key.

---

## Explore your history

Open **Activity statistics** to switch between **Overview** and **Activities**. Overview groups related totals (including moving and elapsed time, and average and peak speed), personal records, trends, milestones, and bike mileage. Activities lists individual GPS routes.

Selecting a record opens its route over a dark online map when map tiles are enabled, or a local route outline otherwise. **Settings → Appearance → Display units** persists miles/°F or kilometers/°C across statistics and activity editing. The optional **custom cycling speed filter** defaults to 50 mph, can be changed in Settings, and displays its threshold in your selected units without editing your recordings. Optional **Insights → Garmin & advanced activity data** show available device-recorded cycling cadence, total cycles/strokes, power, heart-rate, and training fields, without estimating missing values. The **Cycle paths** overlay stays beneath your heatmap and has just two controls: **Show** and **Overlay color**. The transparent CyclOSM tile overlay uses your color at all supported zoom levels, drawing one level of higher-detail tiles where the on-screen request budget permits so more paths remain visible at wider scales. No second cycling source or separate path-type editor is required. CyclOSM determines which cycling features exist in tiles at each zoom; extremely wide views can still omit individual paths.

---

## Settings

Settings groups controls into **Appearance**, **Imports & connections**, **Insights**, **Library & backups**, and **Updates**. Choose a section to see its controls while keeping **Save settings** visible. The speed-threshold control displays mph or km/h beside the value, matching your unit selection.

---

## Privacy

Tracks, imported account data, and statistics stay on **your local hardware**. Opt-in Garmin wireless sync fetches your own Garmin Connect data from Garmin over HTTPS; it does not upload your local library to Garmin. The experimental Strava GPX connector opens Strava in a dedicated local Edge/Chrome profile; only website requests needed to view and export your own rides are sent to Strava, and the profile is deleted on disconnect. A portable library backup does not include that browser profile.

> [!IMPORTANT]
> **Optional online map tiles** contact their providers and disclose your IP address and the map area viewed. Disable **Online map tiles** to avoid those requests. Manual update checks contact GitHub. Settings displays your local data folder.

Never attach an unredacted GPS export or database to a public [GitHub issue](https://github.com/Gubna-Tech/LibreHeatmaps/issues).

---

<div align="center">

<sub>MIT License · Copyright (c) 2026 Gubna. Independent of Strava, Garmin, and third-party map providers.</sub>

</div>
