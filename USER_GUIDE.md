# LibreHeatmaps User Guide

LibreHeatmaps v1.3.0 is a local-first GPS history and heatmap application. It runs from a Python file, opens its interface in your web browser, and stores imported activities in a local SQLite library. No LibreHeatmaps account, subscription, or third-party Python package is required. Cycling is the default activity focus, but running, walking, hiking, and other recorded GPS activities are supported.

This guide covers everyday use, available controls, and important data and privacy limits. For a short introduction, see the [README](README.md).

## 1. Start LibreHeatmaps

1. Install **Python 3.9 or newer**.
2. Run `LibreHeatmaps.py` by double-clicking it on Windows or using `python3 LibreHeatmaps.py` in a terminal. The program opens the local browser interface at `http://127.0.0.1:63120`.
3. On an empty library, choose **Import my GPS data**. Later, choose **Import GPS files or account ZIP** in the Explore sidebar, or drag files onto the application window.
4. Choose **Explore** to see your heatmap, or select a destination on the narrow left navigation rail to inspect the recorded activities behind it.

The application does not move or delete your original import files. Keep your original FIT, GPX, TCX, account archives, and other GPS files separately: the local SQLite library is a working database with stored route geometry and available summary/telemetry fields, **not a complete archive of the original source files**. An empty library's map may initially be centered near Nashville, Tennessee; this is only a starting view, not an imported route.

### How the interface is organized

The **narrow left rail** selects a destination. The **wider panel beside it** provides shortcuts *within the selected destination*, rather than a second list of unrelated pages. The main workspace displays the selected page. On narrower screens, the navigation adapts so the content remains usable.

| Left-rail destination | What you can do |
| --- | --- |
| **Explore** | View the original 2D heatmap, import, filter, customize map layers, use Time Lens and Place Lens, and open Journey Space. |
| **Overview** | See your personal metric-card dashboard and its grouped totals, personal records, trends, milestones, and Types & Gear sections. |
| **Analytics** | Inspect activity patterns and charts for distance, speed, time, and frequency. |
| **Advanced data** | See supported recorded device, sensor, and weather fields when advanced insights are enabled. |
| **Compare** | Select two recorded activities and compare routes, summaries, and available sensor traces. |
| **Year in review** | Explore one year's recorded history and export a recap image. |
| **Activities** | Find recent activities and longest-route highlights; open a ride's preview, elevation view, or flyover as available. |
| **Data explorer** | Search and filter the full activity table, select columns, change pages, and export filtered statistics. |
| **Review anomalies** | Review activities excluded by optional speed-quality filters and approve valid exceptions. |
| **Calendar** | Explore recorded days and months, annual activity patterns, and activities on a selected day. |
| **Sources** | Set library-wide priorities for fields available from overlapping Garmin, Strava, and imported-file recordings. |

**Settings** and **Exit** are located at the bottom of the rail. Use the **Close** control on an insights workspace, or select **Explore**, to return to the map. The adjacent contextual shortcuts depend on the selected page: for example, **Activities** offers **Recent activities** and **Longest routes**, while **Data explorer** offers shortcuts to filters, columns, table, and pagination. They scroll or navigate within the existing content; they do not make copies of a page.

On desktop, **Ctrl+K** (or **Cmd+K** on macOS) opens quick navigation. Type the destination or action, press Enter to activate a result, or Escape to dismiss it. All of these features remain available through visible controls as well.

## 2. Import and sync activities

### Manual imports, account archives, and supported formats

Select **Import GPS files or account ZIP** or drag supported files into the LibreHeatmaps browser window. The manual drop target accepts up to 50 files at a time and processes them in order. Keep whole Strava and Garmin account ZIP files intact; LibreHeatmaps can read supported activities inside them, including nested archives.

| Format | What LibreHeatmaps can read |
| --- | --- |
| **FIT, GPX, TCX** | GPS tracks and the available recorded timing, elevation, and device measurements. |
| **KML / KMZ** | LineString routes and supported timestamped `gx:Track` recordings; KMZ is compressed KML. |
| **GeoJSON** | Line-based routes, including LineString/MultiLineString features and supported geometry collections. |
| **Coordinate CSV** | Rows containing latitude/longitude columns, optionally with timestamps or elevations. This differs from a Strava `activities.csv` metadata export. |
| **Compressed files / ZIP** | Supported compressed track variants, mixed-format archives, and supported nested ZIPs within importer limits. |

A GPS-only course can appear on the map without recording time, speed, moving duration, pace, power, cadence, heart rate, or weather. LibreHeatmaps does **not** infer missing source measurements. Non-route geometry and malformed files are skipped rather than fabricated into activities.

**Strava archive enrichment:** Import the full account ZIP containing `activities.csv` and any available gear metadata to add activity titles, distance, time, and bicycle information missing from a standalone route. The **Add activity details** action in the Overview/statistics workflow can be used for this purpose. If a recently synced Strava GPX is already in the library, a later matching archive can enrich that activity without creating a second copy. More complete FIT/TCX recordings can also supply measurements a GPX did not contain. Original source files are not modified.

**Combining Garmin and Strava:** LibreHeatmaps uses activity identifiers and strong recording similarities to recognize matching source recordings and keep source information available. Uncertain or older duplicates may still need manual review. Check your totals when combining large archives, and use **Sources** to decide which provider's existing measurements to prefer. A source priority cannot create fields that its underlying recording lacks.

### Automatic imports from a watched folder

Under **Settings → Imports & connections**, choose **Your automatic import folder** and select an existing local or accessible network directory. No folder is chosen automatically. Save settings to enable monitoring; leave the path blank to disable it.

- The app checks the selected directory at startup and approximately every **15 seconds** while running; **Scan now** requests an immediate check.
- Only files directly inside that folder are watched, not files in subdirectories. New files are given time to finish copying before import.
- Supported files use the ordinary local importer. Existing source files are not moved or deleted.
- The watched folder is optional and separate from Garmin Connect or Strava connectivity. If another service copies recordings into the folder, LibreHeatmaps can import them without controlling that service.

### Garmin Edge: USB transfer

Connect a Garmin Edge or another compatible device that appears as a mounted USB storage drive. In **Settings → Imports & connections**, select **Find connected Garmin**, then the control for copying and importing FIT rides. The importer reads recorded FIT files in the device's `Garmin/Activity` or `Garmin/Activities` directory, makes local archival copies in `Garmin-FIT-Archive` beside the database, and imports new recordings. The original device files are not deleted. **Open local FIT archive** opens the computer-side copy directory. This path requires USB storage access, not cloud connectivity.

### Garmin Connect: wireless sync and full history

For optional wireless retrieval, configure a **Garmin Connect Personal API key** under **Settings → Imports & connections → Garmin wireless activity sync**, then select **Connect Garmin** and **Sync now**. Your Edge must first send its recordings to Garmin Connect; LibreHeatmaps reads from Garmin Connect over the internet, **not directly from the Edge over local Wi-Fi**.

- A stored key means the key was saved locally, **not** that authentication and downloading have succeeded. Use **Sync now** and read its result to confirm access.
- Normal checks occur on startup and, by default, every **60 minutes** while the application runs. Change the interval in Settings (15–1440 minutes) and save it. A manual check resets the next automatic countdown.
- **Recent Garmin activities to check (1–100)** defaults to **25**. It limits routine discovery, not the number of activities the library may contain.
- For a substantial older account history, choose **Import full history**. This separately scans older activity pages, tracks progress, and offers **Pause**, **Resume full import**, and **Rescan full history**. Its progress can appear in the import area and map caption; newly imported routes periodically refresh the local map.
- If a watched folder is configured and accessible, supported downloaded FIT files can also be retained there. Sync can still add activities to the database when the optional folder is unavailable.
- **Disconnect Garmin** removes the locally saved key. To revoke the credential at the provider as well, revoke/delete that key in Garmin Connect. The key is not included in portable library backups.

Remote access can fail due to expired permissions, authentication errors, connectivity, or Garmin rate limits. A failed check should not be interpreted as a successful import, and repeatedly retrying during a rate limit is not recommended. Manual file import remains available without Garmin connectivity.

### Strava website GPX sync — experimental and optional

This feature uses a **separate Edge/Chrome browser profile** to access the signed-in Strava website and download missing activity GPX files. It does not use an official Strava developer API, copy login cookies from your regular browser, or ask you to type your Strava password into LibreHeatmaps. **It is unofficial website automation, may conflict with Strava's terms, and can stop working if the site changes.** It is opt-in; ordinary local imports do not require it.

1. Ideally import your complete Strava account ZIP first, including `activities.csv`, so existing IDs and details can be recognized.
2. Open **Settings → Imports & connections → Strava GPX sync**, review the warning, and acknowledge it. Choose **Sign in for auto-sync** and complete sign-in in the dedicated browser window.
3. After sign-in, the application checks for recent activity links and imports missing downloadable GPX routes. Check its completed status; successful sign-in alone does not mean a ride was downloaded.
4. **Recent Strava rides to check (1–50)** defaults to **25**. Checks occur on startup and every **60 minutes** by default, with a configurable 15–1440-minute interval; **Sync now** starts an on-demand check. The actual number accessible may be lower, and a ride without GPS may not offer a GPX route.
5. If you have selected a watched folder, downloaded original GPX files can be retained there without overwriting existing files. Otherwise temporary files are imported locally and then removed.
6. To stop the integration and delete local browser-session data, use **Disconnect & delete session**. Existing imported routes remain in the library. Disconnecting locally does not revoke a remote Strava login. When Strava requires authentication again, use **Reconnect Strava**.

Only downloads from activities actually accessible through the signed-in account can succeed. Website pagination, login challenges, network failures, and access restrictions may prevent a check; the connector does not bypass these restrictions. The optional official OAuth/API integration is **not** included in this version. Activity data arriving through a connected third-party bicycle app can be imported if the corresponding Strava recording supplies an accessible GPX; absent sensor data cannot be reconstructed.

## 3. Explore the 2D heatmap

Select **Explore** from the left rail. The original 2D heatmap and its five color palettes—**Classic glow, Ember, Violet, Ocean, and Forest**—remain independent from the navy, cream, and yellow application styling.

- **Activity focus:** Choose **Cycling** (default), **Running & walking**, or **All GPS activities** from the map controls or **Settings → Appearance**. Running & walking includes recognized runs, walks, hikes, and treks and hides bicycle-specific controls. Unclassified older tracks appear in Cycling or All for compatibility until their sport is edited.
- **Filters:** Use sport/activity type, activity name, and date. Activity focus also carries into compatible statistics, calendar, history, and comparison views. Clear sidebar filters when a route appears to be missing; an active Time Lens or selected single activity may need to be cleared separately.
- **Map layers:** Choose **Dark**, **Light**, or **Satellite** when **Online map tiles** is enabled. Toggle **Show all routes** and **Heatmap layer** separately. Adjust the heatmap palette, **Route definition**, **Glow radius**, **Intensity**, and **Transparency**.
- **Cycle paths:** Toggle the optional CyclOSM path overlay and select its **Overlay color**. It sits beneath your recorded routes. The overlay is not a routing guarantee and will not show every path at every map zoom. It does not need to load for Running & walking focus.
- **Route focus:** Choose **Show only this activity on map** from a route preview. Use **Show all activities** to return to the wider library view.

**Time Lens:** Use the compact year/month control on the map. **Through date** includes activity history up to the selected period; **Selected period** limits it to that year or month. Step with the arrows, use **Play** for the year sequence (unavailable in reduced-motion mode), or select **All** to remove the time restriction. **Details** reveals a year slider and period information; **Hide** collapses the control. Time Lens filters the view, not the activity records or their timestamps.

**Place Lens:** Click or tap a map location to inspect locally stored activities passing nearby; dragging continues to pan. Choose a radius of **100, 250, 500, or 1,000 metres**. The results can show visits and a monthly **Visit rhythm**; choose an activity to preview it or **On map** to focus its route. A linked telemetry point may also be opened in Place Lens. For very dense libraries, **Partial result** means a bounded search did not inspect every eligible segment. This tool measures approximate track proximity, not visits to a specific building or address. The application does not send your full library to an external place-search service.

**Visit counts:** Hover over highlighted heatmap sections to see the number of distinct recorded activities near that part of the map, within the current filters. Going out and back along a road in the same recording counts once; this is an activity count, not an exact traversal count.

### Journey Space: a separate 3D visit-frequency view

From **Explore**, choose **Explore in 3D** in the **Journey Space** card. The raised route geometry represents **repeat visit frequency, not altitude, terrain, or road height**. The underlying 2D heatmap and all original palettes remain unchanged.

- **Depth** adjusts the height of the displayed visit-frequency structures.
- **Ground fill** independently controls how opaque the colored surfaces beneath the raised routes appear, from **0%** to **100%**. At higher values it can give the route supports a continuous, solid appearance even in the cleaner presentation mode.
- **Clean / Details** changes the visible support structure without changing any route. **Reset view** restores the initial camera and appearance controls, including a light ground fill.
- Drag to **orbit**, Shift-drag to **pan**, and scroll to **zoom**. **Top** returns to an overhead perspective; camera/compass controls help orient the view.
- Click a visible segment to inspect recorded passage frequency. Choose **Return to heatmap** to restore the normal 2D map.

Journey Space is a visual exploration of filtered local route data. It does not download terrain elevation or rewrite GPS coordinates. The source map's applicable attribution remains visible.

## 4. Calendar: find when you were active

Choose **Calendar** on the left rail. It reads dates and available measurements already imported into the local SQLite library; it does not synchronize with an outside calendar or schedule training activities.

- Switch between **Month** and **Year**, step between periods, and use **Latest activity** to return to the latest recorded date.
- Colored days identify recorded activity dates. Summary totals show **activities, distance, moving time, and active days** for the current period. Missing moving time is not estimated.
- Select a highlighted date. **Selected day** shows the activities directly beside the month grid on desktop; choose an available **View route**, **Elevation**, or **Flyover** action for one of them.
- Expand **Year at a glance** to navigate the 12-month miniature activity overview without pushing the selected-day details out of the main layout.
- Choose **Cycling**, **Running & walking**, or **All GPS activities** in **Activity focus**. The calendar uses the same saved activity-focus preference as Explore.

The calendar reflects activity dates stored in your library. It cannot display recordings you have not imported, and it does not fabricate dates for coordinate-only tracks.

## 5. Overview: build your own metric dashboard

Open **Overview** directly from the left rail. Choose **Edit entire Overview** near the top, or use **Edit cards** beside any Overview section heading. The layout editor has a list of all six sections on the left and the cards for your selected section on the right. Select any section from the list or the **Choose section** dropdown to edit it; check a card to add it to that section, uncheck it to remove it, and use the arrow controls or desktop drag-and-drop to change card order. When a section has named metric groups, you can move those groups up or down too. The section list also lets you show or hide entire sections and reorder them. These changes affect the Overview only, not the other Insights destinations or source activity data.

**At a glance** is also a board of individual metrics. Select it in the same editor to search available metrics, show or hide cards, change their order, and star favorites. **Favorites: On/Off** controls whether saved stars affect prioritization without deleting them. **Restore all defaults** returns both the section arrangement and individual cards to their original setup. Cards requiring measurements absent from your imported files are not fabricated and may not appear until relevant data is available.

These layout and favorites choices are saved in the local browser, separately from the SQLite activity library. Clearing browser-site storage may reset the dashboard layout without deleting imported GPS tracks.

Available cards can cover distance, activity count, moving time, peak recorded speed, longest ride, climbing, milestone dates, trends, equipment, and advanced sensor or weather extremes when genuine source fields exist. **Coldest weather** uses recorded weather temperature, while **device temperature** is a separate source measurement. Some weather, cadence, heart-rate, or power cards require enabling advanced insights and importing suitable original recordings. Missing measurements do not become zero or estimated records.

Overview includes **Your activity totals**, **Your activity records** (including source-recorded Weather & wind and Sensors & training when available), **Trends & patterns**, **Milestones & habits**, and **Types & gear**. Those groups can now appear in the order you choose, alongside the optional At a glance board. The adjacent contextual panel follows the visible section order. Each section keeps its original calculations and source-linked activity actions. Long bicycle names and activity types remain in padded rows with distance values separated from their labels.

Selecting a metric linked to a specific activity opens that activity. On supported linked records, hovering or keyboard-focusing a preview displays the locally stored route and, where actual recorded elevation samples exist, a small 3D elevation quick-look. The feature cannot reconstruct a course from summary statistics alone.

## 6. Other Insights destinations

The Insights pages are **individual destinations on the left rail**, not nested pages beneath Overview:

| Destination | Main functions |
| --- | --- |
| **Analytics** | Activity-distance, speed, time, and frequency patterns; supported charts and CSV exports. Density-chart blue indicates more recordings, not a change to heatmap colors. |
| **Advanced data** | Device-provided Garmin/FIT summaries, recorded sensor data, and recorded weather; turn on **Show advanced activity insights** in **Settings → Insights** when needed. |
| **Compare** | Find two activities by name/date or browse older rides, compare recorded values and routes, and overlay available time-aligned sensor graphs. |
| **Year in review** | Select a year, review that year's totals/highlights, and choose **Save recap PNG**. All report sections are on the same page without redundant contextual navigation shortcuts. |
| **Activities** | **Recent activities** and **Longest routes**, with links into the individual activity preview and 3D views; for an exhaustive listing use Data explorer. |
| **Data explorer** | Full activity table, filtering, column selection, paging, and **Export filtered CSV**. CSV numeric data uses unrounded SI measurements rather than the display formatting. |
| **Review anomalies** | Review activities excluded by active speed anomaly controls; approve a valid outlier without editing its source GPS route. |

The shared Insights filters include sport, bike where relevant, year, and month. **Units** selects miles/mph/feet/°F or kilometers/km/h/meters/°C and is shared with the saved display-unit preference in Settings and the activity editor. Filters and focus can affect the records and summary values you see; they do not edit the actual source recording.

**Speed and data quality:** Under **Settings → Insights → Statistics & data quality**, the optional **Custom cycling speed filter** is off unless enabled. Its default threshold is **50 mph (about 80.5 km/h)**, with a configurable range equivalent to **5–200 mph** and the selected display units. A separate optional anomaly filter can exclude suspicious speeds from relevant statistics and charts. Neither filter erases GPS geometry or changes non-speed distance totals. Use **Review anomalies** to assess genuine exceptions.

**Recorded versus calculated values:** Cycling average cadence and running cadence are distinct; a device-reported total cycles/strokes is not reconstructed from cadence alone. Weather readings and device-temperature measurements are not interchangeable. Advanced records such as normalized power, training stress, intensity factor, cadence, heart rate, and power are shown only when imported from a source that provides them. A comparison graph aligns sensor streams by **percentage of elapsed time**, not by identical road position; missing data appears as missing, not interpolated into fabricated sensor history.

## 7. Open an activity: routes, telemetry, and 3D views

Choose a linked activity from Overview, Activities, Data explorer, Calendar, Compare, or a nearby-place result. Its individual preview shows the stored route over the selected map style when online tiles are enabled; with tiles disabled, a locally drawn route remains available. Some screens label the activity a **Ride preview** even when the library contains other GPS sports.

**Telemetry** is a time-stamped sequence of GPS position and recorded sensor values. Depending on the source, the graph can offer **speed, heart rate, power, cadence, elevation, or temperature**. Select a **Measurement**, move over or click the graph, or change the **Ride position** slider to highlight the corresponding recorded position on the route. Missing measurements are not synthesized. For a maximum-speed or cadence record, **Show where** identifies the highest available timestamped sample; a device-wide summary maximum might not occur at exactly that sample.

### Offline 3D elevation viewer

Choose **3D elevation** when a route contains sufficient recorded elevation information. The display is an exaggerated 3D **route profile**, **not geographic terrain**. Its vertical values come from original saved altitude samples; no elevation model is downloaded to fill gaps. A route without usable elevation shows an unavailable state rather than an invented landscape.

Use the playback and position controls to inspect the course. The accompanying **Route location** map shows the corresponding GPS point, the traveled portion of the route, and the remaining path. **Follow rider** tracks the current position; **Whole route** frames the course; use **+ / −** or supported scrolling to change the view without forcing an unrelated mode. Select a recorded path position in that map to jump to the matching section of the 3D profile and telemetry. Available live instruments display recorded speed, cadence, power, heart rate, temperature, and position when present. The condensed replay is **not the original riding speed**.

Where a course revisits nearly identical GPS coordinates, the elevation presentation may visually align closely overlapping recorded passes under conservative safeguards. This is display-only and does not rewrite recorded altitudes, summaries, or stored GPS points. Bridges, different track levels, and GPS drift can still create ambiguous profiles.

### Online 3D Flyover

Choose **Flyover** in an activity preview to follow its route across optional online satellite imagery, terrain, and simplified mapped buildings. This is separate from the offline 3D elevation profile and the local Journey Space visited-roads visualization.

- Use **Play / Pause**, the progress scrubber, and supported playback rates to move along the recording. Replay is distance-adaptive and cinematic, not a reproduction of the original activity speed.
- Choose available camera views, use **Follow rider** to resume automatic tracking after manual camera movement, and change between **3D scenery + satellite** and **Flat satellite · reference**.
- Choose available terrain relief settings. Enhanced relief exaggerates terrain for visibility; it does **not** claim that the altered slope is the actual recorded terrain height.
- The Flyover overlay can display source-recorded speed, heart rate, cadence, power, temperature, elevation, and elapsed position if available.

**Flyover requires internet access, online tiles, and a browser/device with working WebGL.** Opening it can load a browser-rendering library and request map imagery, elevation, and building data from third-party providers. Those services see your IP address and requested geographic area. If terrain data is missing, the application should identify the limited satellite-only result rather than claiming working 3D terrain. Close Flyover to return to the ordinary activity preview. Normal offline viewing does not require Flyover.

## 8. Edit details, repair GPS spikes, and reconcile duplicates

In an individual activity preview, choose **Edit details** to adjust the name, sport, date, bike/gear, and supported summary measurements. The bike selector draws from previously known names but allows free-text names. The editor uses your saved display units, converts values on save, and retains the imported originals for **Restore original details**. Reimporting the activity does not intentionally discard saved manual corrections.

**Analyze speeds** is an optional, experimental review inside the editor. It compares the entered distance/moving time with supported timestamped GPS observations and may suggest a sustained peak and route-based context. The suggestion is **not** an original device measurement. It does not change saved data until you explicitly choose a suggested value and save the activity.

**Review GPS spikes** is a separate, manual, experimental route-repair tool for isolated suspicious timestamped GPS excursions. Inspect the before/after drawings and each proposed correction before choosing **Apply selected repairs**. Opening the tool alone changes nothing. The original stored sampled route can be recovered with **Restore original GPS route**; original imported source files are not changed. Corrected route geometry affects the heatmap, preview, route-only GPX exports, and GPS-derived distance; it does not fabricate sensor measurements or automatically approve a speed-anomaly exclusion. Make a portable backup before applying multiple repairs.

Under **Settings → Library & backups → Local data & cache**, **Review possible duplicate recordings → Scan library** checks for strongly matching recordings using time, distance, endpoints, and route positions. It is a manual scan, not a startup operation, and same-name/same-date activities alone do not qualify. If you choose **Combine source recordings**, confirm which record to keep. The other is retained in LibreHeatmaps' reversible duplicate archive, with **Restore** available while that archive exists. Take a backup before consolidating a large library. **Delete local activity data** also clears internal duplicate records.

### Sources: one field-priority policy for the library

Choose **Sources** on the rail, or **Settings → Library & backups → Review data sources**. For supported fields such as distance, time, elevation, speed/power/heart rate, bike/gear, GPS route, and timestamped sensor trace, select **Automatic**, **Garmin**, **Strava**, or **Imported file**. The **Garmin metrics · Strava details** preset is a convenience option. **Apply to entire library** applies your choices to existing matched recordings and future matching imports; unavailable source fields fall back to existing imported values. Manual corrections and reviewed GPS repairs remain protected. The source editor is **library-wide**, not a separate review button for each activity. To recover alternative provider fields for older, flattened imports, reimport the original recordings.

## 9. Export routes, heatmaps, statistics, and recaps

- **Download filtered GPX** in Explore exports the **stored, sampled route coordinates** for currently selected activities. It does not recreate original FIT/GPX timing, detailed telemetry, or source metadata. Map focus, activity focus, date filters, and Time Lens may limit the export.
- **Create heatmap PNG** opens the image-export editor. Select shape, dimensions, framing, and dark/light/transparent background. The saved image draws LibreHeatmaps route/heat layers **without third-party online basemap imagery**, even if basemap tiles appear in the preview.
- **Data explorer → Export filtered CSV** exports the filtered activity-statistics table, not raw GPS points. Analytics charts may also have CSV exports. **Year in review → Save recap PNG** exports the recap summary.
- Some activity previews and elevation plots provide their own export actions, such as a locally rendered profile PNG.

**None of these exports automatically masks the places you visited.** Inspect GPX files, screenshots, PNG heatmaps, CSV data, and library backups before sharing them publicly.

## 10. Backup, restore, settings, and updates

Open **Settings** from the bottom of the rail. The main categories are **Appearance**, **Imports & connections**, **Insights**, **Library & backups**, and **Updates**; **About LibreHeatmaps** describes the project. Use **Save settings** to commit applicable preference changes. Closing with unsaved edits can prompt for confirmation. Actions such as backup, restore, or deleting activities are separate from Save settings.

Under **Settings → Library & backups**:

- **Download backup ZIP** produces a portable snapshot containing the local activity database and supported settings. It includes sensitive location data. It **does not** contain your original FIT/GPX/TCX source files, complete account archives, Garmin API key, or Strava browser session.
- **Restore from ZIP** validates a compatible backup and, after confirmation, **replaces the current library and settings**. Save a backup of the current library before restoring another one.
- **Local data & cache** shows the actual data/database directory. **Open folder** displays it. **Refresh map cache** removes generated view calculations without deleting activity records.
- **Delete local activity data** is destructive and requires explicit confirmation. It removes locally stored activity data, import history, and internal duplicate history, but not your original imported files in external folders, Garmin/Strava accounts, or separately managed connected sessions. Sync settings may allow source activities to be retrieved again later; do not use Delete as a remote-account revocation control.

Under **Settings → Updates**, choose **Check for updates** to check GitHub. The application can download and verify a compatible release and back up earlier application files in `LibreHeatmaps-backups` before updating. **That updater backup is not a portable activity-library backup.** Keep a separate database backup before a major upgrade and restart the application after installing an update. Update checks require internet access.

To stop saved account access, disconnect the **Garmin key** and the **Strava website session** separately in **Imports & connections**. Disconnecting should not delete already imported rides. For provider-wide revocation, use the provider's own account controls.

## 11. Privacy and external services

- Imported GPS tracks, recorded statistics, dashboard content, and the SQLite library are stored locally; LibreHeatmaps has no cloud account for your library.
- **Online map tiles**, optional cycling layers, satellite/terrain imagery, and 3D Flyover may contact third-party providers. They can see your IP address and the geographic area requested. Keep map-provider credits, including **OpenStreetMap contributors**, visible in the applicable views.
- Turning **Online map tiles** off avoids ordinary basemap requests. A locally drawn heatmap/route remains available without an online background. Flyover itself requires online resources and is not an offline terrain viewer.
- Optional **Garmin** and **Strava** connections contact those services to retrieve activities; Strava browser automation is experimental and unofficial. Manual **update checks** contact GitHub. These connections do not mean your full SQLite library is uploaded to a LibreHeatmaps server.
- An exported GPX, screenshot, heatmap image, or database backup may disclose home locations, habitual travel patterns, or other precise places. Do not post an unredacted database, full activity export, private keys, or account archive in a public issue.

## 12. Troubleshooting

| Problem | What to check |
| --- | --- |
| **A recent ride is missing** | Check the current activity focus, date filters, Time Lens, and single-route focus. Check the relevant import/sync result and whether the original source contained a GPS route. |
| **No background map** | Verify **Online map tiles** and connectivity. Local routes remain visible without a basemap. Cycling overlay coverage depends on CyclOSM at the selected zoom. |
| **No time, pace, or sensor trace** | A coordinates-only file cannot provide them. Reimport the original FIT/GPX/TCX or complete account archive if it actually contains recorded timestamps/sensors. |
| **A weather or performance card is missing** | Enable supported advanced insights, confirm the original activity has the corresponding field, then select it in **Customize cards**. Missing source readings cannot be calculated into factual records. |
| **Strava sign-in succeeded but no rides appeared** | Check the actual completed **Strava GPX sync** status; you may have no missing GPX activities, insufficient accessible links, or a download failure. The default scan checks up to 25 IDs, not the entire history. |
| **Strava says Reconnect** | Complete the sign-in again through the dedicated browser profile. A failed website/network check does not imply that your existing library was deleted. |
| **Garmin does not import** | Confirm the connected key was authenticated by a successful sync, check Garmin Connect for available recordings, and review permissions/network/rate-limit errors. For old rides, use **Import full history**. For USB, ensure the device mounts as a drive. |
| **An activity has unrealistic speed or a GPS jump** | Review optional **Review anomalies** filtering, then inspect the individual activity's **Review GPS spikes** proposals if you want to consider a manual route repair. |
| **A 3D elevation viewer is unavailable** | Confirm that the source contains sufficient usable recorded altitude samples; it does not generate terrain or synthetic altitude from 2D GPS coordinates. |
| **Flyover is blank or flat** | Check internet access, browser WebGL, online tiles, and terrain-provider status. Use the normal 2D route preview as a fallback. |
| **A PNG export lacks street-map tiles** | This is intentional: the saved heatmap image contains locally drawn routes/heat, not third-party basemap imagery. |
| **A restore or update failed** | Read the displayed error and keep the previous library backup and original source files until the application works normally. |

LibreHeatmaps is independent of Strava, Garmin, and third-party map providers. See [LICENSE](LICENSE) for the license.
