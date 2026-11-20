# LibreHeatmaps User Guide

A practical companion to the short [README](README.md), describing LibreHeatmaps **v1.2.0** as shipped. LibreHeatmaps is a Python application that opens a local browser interface and stores imported activities in a SQLite database on your computer. No account, subscription, or third-party Python packages are required.

## 1. Start and import

1. Install Python 3.9 or newer. Run `LibreHeatmaps.py` (double-click on Windows or run `python3 LibreHeatmaps.py`) to open the local browser interface.
2. On first launch, choose **Import my GPS data**. Later, use **Import GPS files or account ZIP** in the sidebar.
3. Select a complete Strava or Garmin account ZIP, or individual FIT, FIT.GZ, GPX, GPX.GZ, TCX, or TCX.GZ tracks. You can also import a Strava `activities.csv` for activity details. Keep account export ZIPs intact; supported GPS files can be read from them, including nested Garmin archives.
4. Choose **Activity statistics** (or **Open activity statistics** in the sidebar) to explore your library.

Your original source files are not moved or deleted. LibreHeatmaps keeps locally stored, compact/sampled route geometry and available statistics; its database is **not** a substitute for your original FIT, GPX, TCX or account archive files. Reimporting recognized activities can fill in missing details or timestamped telemetry when the source supports it, without replacing their stored route geometry. To add available Strava-reported distances, moving times, and gear information, import the complete archive with `activities.csv` and its gear metadata. You can also choose **Activity statistics → Add activity details** to reimport that archive.

**Using both Strava and Garmin exports:** LibreHeatmaps checks closely matching recording times, route endpoints and distances to recognize likely duplicate recordings across sources. Available metadata can enrich a match, with Strava account metadata preferred where available. It does not automatically merge uncertain matches, and it does not retroactively remove duplicates already stored in an older database. Check your activity totals after combining histories.

**No routes yet?** The empty map starts near Nashville, Tennessee. This is only a starting map view, **not** a recorded activity. If **Online map tiles** is on, the tile provider can receive requests for that map area.

## 2. Use the heatmap

- **Map background:** Choose **Dark**, **Light**, or **Satellite** in Map view. **Online map tiles** controls external basemap requests. Switch it off to view your local routes without contacting a map-tile provider.
- **Heatmap appearance:** Change the **Heat colors** palette in the map legend or under Settings. Adjust **Route definition**, **Glow radius**, **Intensity**, and **Transparency**. **Show all routes** and **Heatmap layer** can be switched independently.
- **Filters:** Use **Activity type**, **Activity name**, and the date-range inputs. **Clear filters** restores those sidebar selections; other active views, such as Time Machine or a focused activity, may need to be cleared separately.
- **Individual route:** Choose a ride from **Activity statistics → Activities** (recent/longest highlights) or an available ride link in the statistics. Its preview displays the route. Select **Show only this activity on map** to focus the main map on it; **Show all activities** on the main map exits that focus.

Online basemap tiles contact their providers, revealing your IP address and the map areas requested. The GPS library itself stays local.

## 3. Explore Activity statistics

| View | What it does |
|---|---|
| **Overview** | Totals, moving and elapsed time, speed, milestones, records and bike mileage |
| **Analytics** | Trends and comparisons for distance, speed, time and other statistics, with customizable views |
| **Advanced data** | Additional recorded Garmin/FIT values and available weather observations; enable **Show advanced activity insights** in Settings |
| **Compare rides** | Select two individual routes to inspect side by side |
| **Year in review** | Pick a year to view annual totals and export a recap image |
| **Activities** | Open previews from recent activities and longest-route highlights; this is not an exhaustive list of every import |
| **Data explorer** | Search and page through the complete activity table, choose columns, and export the table-filtered statistics as CSV |
| **Review anomalies** | When a speed filter is enabled, inspect excluded rides and approve known-good exceptions without changing the recordings |

**Explore your timeline** opens **Heatmap Time Machine** with year-by-year playback. **Through selected year** shows all qualifying years up to the chosen year; **Selected year only** restricts the map to that year. Moving the year slider updates the map after a short delay, and **Apply** explicitly applies the chosen view. **All years** removes the Time Machine restriction; any separate sidebar filters may still apply.

Speed anomaly filters are optional and off by default. The **Statistics & data quality** settings provide a threshold filter and an anomaly filter; enabled filters affect speed figures and related charts, **not** recorded GPS tracks or distance totals. **Review anomalies** is useful after enabling a filter, and you can approve genuine outliers. Values originally recorded remain visible in Data explorer.

## 4. What is ride telemetry?

**Telemetry** is a time-stamped series of recorded measurements tied to locations on an activity. Depending on the file, the supported graph may show **speed, heart rate, power, cadence, elevation, or temperature**. Not every device or file records these values, and a route containing only coordinates cannot supply a sensor timeline.

1. Open **Activity statistics → Activities**, then select an activity.
2. Find **Ride telemetry** beside the map on wide screens (or beneath it on smaller screens).
3. Choose a **Measurement** from the dropdown. Only values actually available in that activity are offered.
4. Move the pointer across the graph, click a graph position, or use the **Ride position** slider (keyboard arrow keys work when focused). The corresponding GPS location is marked on the larger route map.

The graph is based on stored, sampled readings, **not** a calculated fitness or performance estimate. Unsupported, missing or invalid readings are left missing; the available measurements depend on the original file. The **Units** selector in Activity statistics controls compatible display units such as mph/km/h, feet/meters and °F/°C. Older activity imports may need their original FIT, GPX or TCX file reimported to add telemetry, without replacing the stored route. The telemetry chart is read-only and does not modify the recording.

## 5. Export and sharing

**GPX:** Use **Download filtered GPX** in the sidebar. The file contains the **stored, sampled GPS route coordinates** for the selected activities. This is a route-only GPX export: it does **not** recreate the original source file, original timing, sensor/telemetry streams, or other device metadata. The selected activity focus or Time Machine view can also affect what is exported. Coordinates are **not privacy-masked**; inspect the result before sharing.

**PNG heatmap:** Choose **Create heatmap PNG** in the sidebar to open the **Export heatmap image** editor. Choose canvas shape, PNG width, dark/light/transparent background and zoom/framing. Its preview may display online basemap tiles, but the saved PNG contains the application's route/heat drawing **without third-party basemap imagery**. No location masking is applied, so the image may reveal precise areas you visited.

**CSV:** Under **Activity statistics → Data explorer**, **Export filtered CSV** downloads activity-statistics rows matching the Data explorer filters, not raw GPS points or telemetry streams. Numeric measurements in this CSV use unrounded SI units (for example, meters and seconds). Individual **Analytics** charts also offer CSV export. **Year in review → Save recap PNG** exports a statistics recap image, not a map screenshot.

### Heatmap visit counts

On the **main heatmap**, hover with a pointer over a highlighted route to see how many **distinct recorded activities** passed nearby. Counts follow the active map selection, including sport, activity-name and date filters, Time Machine, or an individual-ride focus. The lookup uses nearby map cells at the current zoom, not an exact coordinate. Loops and out-and-back passages within **one activity count once**: this is **not** an exact traversal count. A dedicated touch tap-to-inspect control is not available in this build.

**Cadence readability:** The cadence graph uses a short rolling median of positive pedaling readings to make the trend easier to follow. Zero-RPM coasting samples are marked as short ticks along the bottom. The highlighted **Raw reading** is the original saved value; smoothing only changes the drawing, not the stored data. Other measurements are plotted from their sampled readings.

## 6. Backup, restore, and data location

Under **Settings → Portable library**:

- **Download backup ZIP** saves a portable SQLite-database snapshot with your imported routes, available measurements and settings. It contains location data and should be kept private, preferably on another drive. **It does not contain your original imported FIT/GPX/TCX files or full Strava/Garmin archive ZIPs.**
- **Restore from ZIP** accepts the application's library-backup format, validates the database and replaces the active local library and settings after confirmation. **Restore overwrites the current library:** download a fresh backup first and confirm it exists before restoring another one.
- **Settings → Local data & cache** shows the exact data-folder and SQLite paths. **Open folder** opens the local data folder. **Refresh map cache** discards rendered calculations without deleting activities.
- **Delete local activity data** removes imported activities from LibreHeatmaps after typed confirmation. It does not remove the original GPS files from your computer.

Do not publish the database, full backup ZIP, private GPS files, or personal Strava archives in a GitHub issue. For technical support, provide an error message or a deliberately sanitized synthetic example instead.

## 7. Settings and software updates

Under Settings, the **Appearance**, **Statistics & data quality**, and **Garmin & advanced activity data** options are previewed and committed with **Save settings**. Closing Settings with unsaved changes prompts to discard them. Backup, restore, clearing the map cache and deleting activity data are separate actions; they are not performed by **Save settings**. The **Units** selection is in Activity statistics.

**Settings → Software update** checks GitHub release information when Settings is opened and has a manual **Check for updates** button. A compatible release can be installed after confirmation and SHA-256 verification. The updater backs up the previous application files (including this guide in v1.2.0) under `LibreHeatmaps-backups`; that is **not** the same as a portable library backup. Updates are intended to leave your SQLite activity database intact. Restart the program after installation.

Checking for updates contacts GitHub. When enabled, online basemap tiles—including tiles shown in individual ride previews—contact external map providers with the requested map area and your IP address. The app does not upload the GPS database to a LibreHeatmaps account. With privacy masking removed in v1.2.0, **every GPX/PNG export and portable library backup should be treated as potentially location-sensitive**.

## 8. Troubleshooting

**An activity has no telemetry:** Check whether the original file contains timestamped route points and sensor or elevation readings. Import the original file again if it does. Coordinates alone cannot generate a time-based sensor chart.

**No background map appears:** Verify **Online map tiles** is enabled and your network can reach the map provider. Offline route drawings and heatmaps remain usable without tiles.

**Strava details or bike names are missing:** Use **Add activity details** or reimport the full Strava ZIP containing `activities.csv` and available gear files. Individual GPS files often lack official activity metadata.

**A Garmin archive reports many skipped files:** Some Garmin FIT records contain summaries or other information without a usable GPS route. Those are not added as mapped activities. Check the successful activity count and any reported import errors separately.

**A speed looks unrealistic:** Check the recorded activity, enable optional anomaly filtering if desired, and use **Review anomalies** to approve genuine high-speed recordings.

**Exports look different from the editor:** PNG export intentionally omits third-party basemap tiles; its dark/light/transparent background is part of the export instead. GPX export contains sampled route coordinates but no original timestamps or sensor data. Both can disclose places you visited.

**Restore or update did not succeed:** Read the displayed error; keep your original activity export and previous backup ZIPs until the application is working again. Avoid copying database backups into public repositories.

---

LibreHeatmaps is independent of Strava, Garmin and third-party map providers. MIT licensed; see [LICENSE](LICENSE).
