<div align="center">

# LibreHeatmaps

**Free, open-source GPS heatmaps.**  
*Your data stays local. Your history stays yours.*

**Python only** · **Local-first** · **No account required** · **MIT licensed**

</div>

---

LibreHeatmaps is a free, open-source **GPS activity viewer written in Python**. Explore your routes as heatmaps, review ride and activity statistics, and preview individual GPS tracks.

Your imported data stays on **your own computer** in a local SQLite database. No subscription, account, or third-party Python packages required.

## Get started

1. Install **[Python 3.9+](https://www.python.org/downloads/)**.
2. Download **`LibreHeatmaps.py`** and run it (double-click on Windows, or run `python3 LibreHeatmaps.py`). The interface opens in your browser.
3. On first launch, select **Import my GPS data**. For later imports, choose **Import GPS files or account ZIP**. With no imported routes, the map starts in Nashville, Tennessee. During imports, the map status shows processed and total activities as well as how many were added.

### Download your complete Strava data

1. Sign in to [Strava](https://www.strava.com/).
2. Go to **Settings → My Account**.
3. Under **Download My Account**, select **Get Started**.
4. Request your archive, download it when ready, then select the **entire ZIP as-is**—you do not need to extract it or choose individual files.

> [!TIP]
> Keep the full export, including `activities.csv` and bike/gear files, to populate bike names, moving time, and elevation. Already imported? Choose **Activity statistics → Add activity details** and select the ZIP again. Your existing routes are preserved.

You can also import individual **FIT, FIT.GZ, GPX, GPX.GZ, TCX, and TCX.GZ** files. **Strava is optional.**

### Import your complete Garmin account export

Select the downloaded **Garmin account ZIP** directly. LibreHeatmaps reads FIT activity files inside Garmin's nested ZIP archives without requiring you to extract either archive. Garmin exports may contain thousands of FIT files with no GPS routes; these are skipped as **No GPS**, while usable tracks and matching Garmin activity summaries populate your local library. Importing again avoids duplicate activities. Only GPS activities appear on the heatmap.


## Explore your history

Open **Activity statistics** to switch between **Overview**, **Analytics**, **Activities**, **Data explorer**, and **Review anomalies**. Overview groups related totals (including moving and elapsed time, and average and peak speed), personal records, trends, milestones, and bike mileage. Analytics offers eight charts covering speed, distance, moving time, and activity frequency, with adjustable date ranges, weekly/monthly/yearly grouping, average or median speed, vertical scaling, per-chart display styles, expandable charts, data tables, and local CSV exports. Chart display preferences are saved in your browser. Because the database preserves GPS route geometry rather than a complete timestamp series, these are **per-activity comparisons**, not second-by-second ride speed profiles. Activities lists individual GPS routes. **Data explorer** provides a searchable, sortable, paginated table with adjustable columns for dates, distances, moving/elapsed time, average and peak speed, heart rate, average power (watts), elevation, gear and source files. Missing measurements are shown as unavailable. Export the filtered table as a CSV; power is displayed only when imported data actually contains it.

**Garmin & advanced data:** In **Settings → Garmin & advanced activity data**, enable **Show advanced activity insights** to reveal an additional Statistics view and Data Explorer columns for any recorded calories, device temperature, weather temperature, wind, humidity, cadence, or maximum power. Monthly charts show available calories, temperatures, wind, average power, and cadence; their source values can be inspected in tables. These measurements are imported only if present in the original FIT files or Strava metadata. Weather observations are not inferred from your routes. For older imported FIT activities, reimport their source files, your complete Garmin account ZIP, or Strava ZIP to enrich the local records without duplicating activities. Device-recorded temperature is distinct from ambient weather observations.

Selecting a record opens its route over a dark online map when map tiles are enabled, or a local route outline otherwise.

On the main map, scroll over a particular road or route to zoom toward the mouse pointer. Use **Create heatmap PNG** to open a separate framing editor: drag to choose the geographic area, zoom into a location, select wide, landscape, square, or portrait framing, and export a high-resolution PNG with a dark, light, or transparent background. When **Online map tiles** is enabled, the capture editor previews a dark or light map depending on the selected PNG background; transparent previews show a checkerboard. Fine zoom steps (0.25 levels), a precision slider, and a route-aware Fit control help frame the area. The exported PNG includes only the GPS heatmap on a solid dark, light, or transparent background, never third-party map imagery.

In **Settings → Statistics & data quality**, you can optionally filter unlikely cycling speeds of **50 mph (80.5 km/h) or more** from speed statistics and Analytics. This filter is **off by default**. A separate **experimental** speed-anomaly filter can compare speeds against your own rides of the same activity type, using robust median-based thresholds when enough comparable rides exist. Both filters are opt-in, can exclude legitimate exceptional rides, and never change the original routes or non-speed totals. When either filter is on, **Settings** shows the number of activities currently excluded across the entire library before you open the review; the **Statistics → Review anomalies** navigation also shows the excluded count for your current selection. Only flagged rides or previously approved exceptions display the speed-review panel in an individual activity preview. **Statistics → Review anomalies** lists flagged rides and any approved exceptions. Open an activity to inspect its route and recorded metrics, then choose **Include this ride’s speed** if the recording is accurate; use **Return to automatic filtering** to reverse your decision. Exceptions are saved locally per activity, persist across restarts, and affect speed calculations and Analytics without changing the GPS data. Adjust them in Settings and choose **Save settings**; LibreHeatmaps confirms the saved values before reporting that changes are applied.

## Privacy

Tracks, imported account data, and statistics stay on **your local hardware**.

> [!IMPORTANT]
> **Optional online map tiles** contact their providers and disclose your IP address and the map area viewed. Disable **Online map tiles** to avoid those requests. Manual update checks contact GitHub. Settings displays your local data folder.

Never attach an unredacted GPS export or database to a public [GitHub issue](https://github.com/Gubna-Tech/LibreHeatmaps/issues).

<div align="center">

<sub>MIT License · Copyright (c) 2026 Gubna. Independent of Strava, Garmin, and third-party map providers.</sub>

</div>
