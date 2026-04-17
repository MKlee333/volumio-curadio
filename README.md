# Volumio Curated Radio (`curated_radio`)

Volumio music service plugin that adds a separate curated radio source with:

- `Interesting Stations` (saved by you)
- `Verified Streams`
- `New Findings`
- `Needs Review`
- `Inactive`
- grouped views by country and genre

The plugin keeps an internal SQLite database, periodically discovers new stations, and verifies stream URLs.

## Repository Layout

This repository is the plugin root expected by Volumio:

- `index.js` plugin controller
- `package.json` Volumio plugin metadata
- `config.json` plugin defaults
- `UIConfig.json` plugin settings UI
- `scripts/radio_worker.py` database / discovery / verification worker
- `i18n/` translations
- `curated_radio.svg` source icon
- `install.sh` / `uninstall.sh`

## Install on Volumio

1. SSH into Volumio.
2. Clone this repository:
   ```bash
   cd /data/plugins/music_service
   git clone https://github.com/MKlee333/volumio-curadio.git curated_radio
   ```
3. Restart Volumio:
   ```bash
   volumio vrestart
   ```
4. In UI: `Plugins > Music Services`, enable `Curated Radio`.

## Main Settings

Open `Plugins > Music Services > Curated Radio` and configure:

- `Discovery profile prompt`
- `Editorial source URLs / feeds`
- `Curated tag seeds`
- `Blocked tag patterns`
- `Blocked name patterns`
- `Refresh interval (minutes)`

## Favorites / Saved Stations

- Native Volumio list favorites are supported from station lists.
- The plugin also has an internal saved list (`Interesting Stations`) with buttons:
  - `Save current station`
  - `Remove current saved station`

## Development Notes

- The Python worker schema auto-migrates existing DBs.
- Temporary local artifacts (`*.db`, test JSON, `__pycache__`) are excluded by `.gitignore`.

## License

GNU GPL v3.0 or later. See [LICENSE](LICENSE).
