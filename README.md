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
3. On first launch, select **Import my GPS data**. For later imports, choose **Import Strava ZIP, activity, or CSV**. With no imported routes, the map starts in Nashville, Tennessee.

### Download your complete Strava data

1. Sign in to [Strava](https://www.strava.com/).
2. Go to **Settings → My Account**.
3. Under **Download My Account**, select **Get Started**.
4. Request your archive, download it when ready, then select the **entire ZIP as-is**—you do not need to extract it or choose individual files.

> [!TIP]
> Keep the full export, including `activities.csv` and bike/gear files, to populate bike names, moving time, and elevation. Already imported? Choose **Activity statistics → Add activity details** and select the ZIP again. Your existing routes are preserved.

You can also import individual **FIT, FIT.GZ, GPX, GPX.GZ, TCX, and TCX.GZ** files. **Strava is optional.**

## Explore your history

Open **Activity statistics** to switch between **Overview** and **Activities**. Overview groups related totals (including moving and elapsed time, and average and peak speed), personal records, trends, milestones, and bike mileage. Activities lists individual GPS routes.

Selecting a record opens its route over a dark online map when map tiles are enabled, or a local route outline otherwise.

## Privacy

Tracks, imported account data, and statistics stay on **your local hardware**.

> [!IMPORTANT]
> **Optional online map tiles** contact their providers and disclose your IP address and the map area viewed. Disable **Online map tiles** to avoid those requests. Manual update checks contact GitHub. Settings displays your local data folder.

Never attach an unredacted GPS export or database to a public [GitHub issue](https://github.com/Gubna-Tech/LibreHeatmaps/issues).

---

<div align="center">

<sub>MIT License · Copyright (c) 2026 Gubna. Independent of Strava, Garmin, and third-party map providers.</sub>

</div>
