'use strict';

const libQ = require('kew');
const fs = require('fs');
const path = require('path');
const execFile = require('child_process').execFile;

module.exports = ControllerCuratedRadio;

function ControllerCuratedRadio(context) {
  this.context = context;
  this.commandRouter = this.context.coreCommand;
  this.logger = this.context.logger;
  this.configManager = this.context.configManager;
  this.serviceName = 'curated_radio';
  this.refreshTimer = null;
  this.workerBusy = false;
  this.mpdPlugin = null;
}

ControllerCuratedRadio.prototype.onVolumioStart = function() {
  const configFile = this.commandRouter.pluginManager.getConfigurationFile(this.context, 'config.json');
  this.config = new (require('v-conf'))();
  this.config.loadFile(configFile);
  return libQ.resolve();
};

ControllerCuratedRadio.prototype.getConfigurationFiles = function() {
  return ['config.json'];
};

ControllerCuratedRadio.prototype.onStart = function() {
  this.mpdPlugin = this.commandRouter.pluginManager.getPlugin('music_service', 'mpd');
  this.loadI18nStrings();
  this.addToBrowseSources();
  this._scheduleRefreshTimer();

  return this.syncDatabase(false)
    .then(() => {
      if (this._isEnabled() && this.config.get('refreshOnStartup')) {
        return this._runMaintenanceCycle(false);
      }
      return libQ.resolve();
    });
};

ControllerCuratedRadio.prototype.onStop = function() {
  if (this.refreshTimer) {
    clearInterval(this.refreshTimer);
    this.refreshTimer = null;
  }
  this.commandRouter.volumioRemoveToBrowseSources(this.getI18nString('PLUGIN_NAME'));
  return libQ.resolve();
};

ControllerCuratedRadio.prototype.onRestart = function() {
  return libQ.resolve();
};

ControllerCuratedRadio.prototype.getUIConfig = function() {
  const defer = libQ.defer();
  const langCode = this.commandRouter.sharedVars.get('language_code');

  this.commandRouter.i18nJson(
    path.join(__dirname, 'i18n', 'strings_' + langCode + '.json'),
    path.join(__dirname, 'i18n', 'strings_en.json'),
    path.join(__dirname, 'UIConfig.json')
  ).then((uiconf) => {
    this._setUIValue(uiconf, 'enabled', !!this.config.get('enabled'));
    this._setUIValue(uiconf, 'databasePath', this.config.get('databasePath'));
    this._setUIValue(uiconf, 'seedJsonPath', this.config.get('seedJsonPath'));
    this._setUIValue(uiconf, 'findingsJsonPath', this.config.get('findingsJsonPath'));
    this._setUIValue(uiconf, 'autoRefresh', !!this.config.get('autoRefresh'));
    this._setUIValue(uiconf, 'autoDiscover', !!this.config.get('autoDiscover'));
    this._setUIValue(uiconf, 'refreshIntervalMinutes', String(this.config.get('refreshIntervalMinutes') || 360));
    this._setUIValue(uiconf, 'discoveryApiBase', this.config.get('discoveryApiBase') || 'https://de1.api.radio-browser.info');
    this._setUIValue(uiconf, 'discoveryLimit', String(this.config.get('discoveryLimit') || 80));
    this._setUIValue(uiconf, 'discoveryProfilePrompt', this.config.get('discoveryProfilePrompt') || '');
    this._setUIValue(uiconf, 'editorialSourceUrls', this.config.get('editorialSourceUrls') || '');
    this._setUIValue(uiconf, 'curatedTagSeeds', this.config.get('curatedTagSeeds') || '');
    this._setUIValue(uiconf, 'blockedTagPatterns', this.config.get('blockedTagPatterns') || '');
    this._setUIValue(uiconf, 'blockedNamePatterns', this.config.get('blockedNamePatterns') || '');
    this._setUIValue(uiconf, 'lastSyncSummary', this.config.get('lastSyncSummary') || '');
    defer.resolve(uiconf);
  }).fail((err) => defer.reject(err));

  return defer.promise;
};

ControllerCuratedRadio.prototype.saveBasicConfig = function(data) {
  this.config.set('enabled', !!data.enabled);
  this.config.set('databasePath', String(data.databasePath || '').trim() || '/data/INTERNAL/curated-radio.db');
  this.config.set('seedJsonPath', String(data.seedJsonPath || '').trim() || '/data/favourites/my-web-radio');
  this.config.set('findingsJsonPath', String(data.findingsJsonPath || '').trim() || '/data/INTERNAL/curated-radio-findings.json');
  this.config.set('autoRefresh', !!data.autoRefresh);
  this.config.set('autoDiscover', !!data.autoDiscover);
  this.config.set('refreshIntervalMinutes', this._clampInt(data.refreshIntervalMinutes, 10, 10080, 360));
  this.config.set('discoveryApiBase', String(data.discoveryApiBase || '').trim() || 'https://de1.api.radio-browser.info');
  this.config.set('discoveryLimit', this._clampInt(data.discoveryLimit, 10, 500, 80));
  this.config.set('discoveryProfilePrompt', String(data.discoveryProfilePrompt || '').trim() || 'prefer: community radio, underground, eclectic, freeform, resident djs, mixes, archives, cultural talk, local scenes; avoid: mainstream, hits, christmas, xmas, chart, commercial, easy listening; sources: radio browser, station homepage');
  this.config.set('editorialSourceUrls', String(data.editorialSourceUrls || '').trim());
  this.config.set('curatedTagSeeds', String(data.curatedTagSeeds || '').trim() || 'underground,eclectic,electronic,experimental,leftfield,ambient,downtempo,house,techno,dub,dubstep,jungle,garage,lofi,hip hop,jazz,soul,reggae,community');
  this.config.set('blockedTagPatterns', String(data.blockedTagPatterns || '').trim() || 'christmas,xmas,pop,hits,charts,top 40,oldies,schlager,country,religious,talk,news,sports');
  this.config.set('blockedNamePatterns', String(data.blockedNamePatterns || '').trim() || 'radio paradise,mango,christmas,xmas');
  this._scheduleRefreshTimer();
  this.commandRouter.pushToastMessage('success', this.getI18nString('PLUGIN_NAME'), this.getI18nString('SETTINGS_SAVED'));
  return this.syncDatabase(false);
};

ControllerCuratedRadio.prototype.runSyncNow = function() {
  this.commandRouter.pushToastMessage('info', this.getI18nString('PLUGIN_NAME'), this.getI18nString('SYNC_STARTED'));
  return this.syncDatabase(false);
};

ControllerCuratedRadio.prototype.runRefreshNow = function() {
  this.commandRouter.pushToastMessage('info', this.getI18nString('PLUGIN_NAME'), this.getI18nString('REFRESH_STARTED'));
  return this.refreshDatabase(false);
};

ControllerCuratedRadio.prototype.runDiscoverNow = function() {
  this.commandRouter.pushToastMessage('info', this.getI18nString('PLUGIN_NAME'), this.getI18nString('DISCOVERY_STARTED'));
  return this.discoverFindings(false)
    .then(() => this.syncDatabase(false))
    .then(() => this.refreshDatabase(false));
};

ControllerCuratedRadio.prototype.addToFavourites = function(data) {
  return this._withFavouritePayload(data, (payload) => this.commandRouter.playListManager.addToFavourites(payload));
};

ControllerCuratedRadio.prototype.removeFromFavourites = function(data) {
  return this._withFavouritePayload(data, (payload) => this.commandRouter.playListManager.removeFromFavourites(payload.title || payload.uri, payload.service, payload.uri));
};

ControllerCuratedRadio.prototype.saveCurrentInteresting = function() {
  return this._saveCurrentInterestingValue(1);
};

ControllerCuratedRadio.prototype.removeCurrentInteresting = function() {
  return this._saveCurrentInterestingValue(0);
};

ControllerCuratedRadio.prototype.addToBrowseSources = function() {
  this.commandRouter.volumioAddToBrowseSources({
    name: this.getI18nString('PLUGIN_NAME'),
    uri: 'curatedradio',
    plugin_type: 'music_service',
    plugin_name: 'curated_radio',
    albumart: '/albumart?sourceicon=music_service/curated_radio/curated_radio.svg'
  });
};

ControllerCuratedRadio.prototype.handleBrowseUri = function(curUri) {
  curUri = (curUri || '').replace(/^\/+/, '');

  if (!curUri.startsWith('curatedradio')) {
    return libQ.reject(new Error('Invalid URI'));
  }

  const parts = curUri.split('/');
  if (parts.length === 1 || !parts[1]) {
    return this.getRootContent();
  }

  if (parts[1] === 'verified') {
    return this.getSectionContent('verified', this.getI18nString('VERIFIED_STREAMS'), 'curatedradio');
  }
  if (parts[1] === 'interesting') {
    return this.getSectionContent('interesting', this.getI18nString('INTERESTING_STATIONS'), 'curatedradio');
  }
  if (parts[1] === 'findings') {
    return this.getSectionContent('findings', this.getI18nString('NEW_FINDINGS'), 'curatedradio');
  }
  if (parts[1] === 'review') {
    return this.getSectionContent('review', this.getI18nString('NEEDS_REVIEW'), 'curatedradio');
  }
  if (parts[1] === 'inactive') {
    return this.getSectionContent('inactive', this.getI18nString('INACTIVE'), 'curatedradio');
  }
  if (parts[1] === 'countries') {
    if (parts.length === 2) {
      return this.getGroupDirectory('country', this.getI18nString('BY_COUNTRY'), 'curatedradio');
    }
    return this.getGroupedStationContent('country', decodeURIComponent(parts.slice(2).join('/')), this.getI18nString('BY_COUNTRY'), 'curatedradio/countries');
  }
  if (parts[1] === 'genres') {
    if (parts.length === 2) {
      return this.getGroupDirectory('genre', this.getI18nString('BY_GENRE'), 'curatedradio');
    }
    return this.getGroupedStationContent('genre', decodeURIComponent(parts.slice(2).join('/')), this.getI18nString('BY_GENRE'), 'curatedradio/genres');
  }

  return libQ.resolve({
    navigation: {
      prev: { uri: 'curatedradio' },
      lists: []
    }
  });
};

ControllerCuratedRadio.prototype.getRootContent = function() {
  return this._runWorkerJson(['stats', '--db', this._getDatabasePath()]).then((stats) => {
    return {
      navigation: {
        prev: { uri: '/' },
        lists: [
          {
            title: this.getI18nString('ROOT_DESCRIPTION'),
            availableListViews: ['list', 'grid'],
            items: [
              this._buildFolderItem(this.getI18nString('INTERESTING_STATIONS'), 'curatedradio/interesting', stats.interesting_count),
              this._buildFolderItem(this.getI18nString('VERIFIED_STREAMS'), 'curatedradio/verified', stats.verified_count),
              this._buildFolderItem(this.getI18nString('NEW_FINDINGS'), 'curatedradio/findings', stats.findings_count),
              this._buildFolderItem(this.getI18nString('NEEDS_REVIEW'), 'curatedradio/review', stats.review_count),
              this._buildFolderItem(this.getI18nString('INACTIVE'), 'curatedradio/inactive', stats.inactive_count),
              this._buildFolderItem(this.getI18nString('BY_COUNTRY'), 'curatedradio/countries', stats.country_count),
              this._buildFolderItem(this.getI18nString('BY_GENRE'), 'curatedradio/genres', stats.genre_count)
            ]
          }
        ]
      }
    };
  });
};

ControllerCuratedRadio.prototype.getSectionContent = function(section, title, prevUri) {
  return this._runWorkerJson(['export-section', section, '--db', this._getDatabasePath(), '--limit', String(this._getMaxStationsPerSection())]).then((rows) => {
    return this._annotateFavouriteFlags(rows).then((annotatedRows) => ({
      navigation: {
        prev: { uri: prevUri },
        lists: [
          {
            title: title,
            availableListViews: ['list', 'grid'],
            items: annotatedRows.map((row) => this._mapStationToItem(row))
          }
        ]
      }
    }));
  });
};

ControllerCuratedRadio.prototype.getGroupDirectory = function(dimension, title, prevUri) {
  return this._runWorkerJson(['export-groups', dimension, '--db', this._getDatabasePath()]).then((rows) => {
    return {
      navigation: {
        prev: { uri: prevUri },
        lists: [
          {
            title: title,
            availableListViews: ['list'],
            items: rows.map((row) => {
              const base = dimension === 'country' ? 'curatedradio/countries/' : 'curatedradio/genres/';
              return {
                service: this.serviceName,
                type: 'folder',
                title: row.key,
                album: String(row.count) + ' stations',
                icon: 'fa fa-folder-open',
                uri: base + encodeURIComponent(row.key)
              };
            })
          }
        ]
      }
    };
  });
};

ControllerCuratedRadio.prototype.getGroupedStationContent = function(dimension, key, title, prevUri) {
  return this._runWorkerJson(['export-group-items', dimension, key, '--db', this._getDatabasePath(), '--limit', String(this._getMaxStationsPerSection())]).then((rows) => {
    return this._annotateFavouriteFlags(rows).then((annotatedRows) => ({
      navigation: {
        prev: { uri: prevUri },
        lists: [
          {
            title: title + ': ' + key,
            availableListViews: ['list', 'grid'],
            items: annotatedRows.map((row) => this._mapStationToItem(row))
          }
        ]
      }
    }));
  });
};

ControllerCuratedRadio.prototype.search = function(query) {
  const text = query && query.value ? String(query.value).trim() : '';
  if (text.length < 2) {
    return libQ.resolve(null);
  }

  return this._runWorkerJson(['search', text, '--db', this._getDatabasePath(), '--limit', String(this._getMaxStationsPerSection())]).then((rows) => {
    if (!rows.length) {
      return null;
    }
    return this._annotateFavouriteFlags(rows).then((annotatedRows) => ({
      title: this.getI18nString('PLUGIN_NAME'),
      icon: 'fa fa-globe',
      availableListViews: ['list', 'grid'],
      items: annotatedRows.map((row) => this._mapStationToItem(row))
    }));
  });
};

ControllerCuratedRadio.prototype.explodeUri = function(uri) {
  const parts = (uri || '').split('/');
  if (parts.length < 3 || parts[1] !== 'play') {
    return libQ.reject(new Error('Invalid station URI'));
  }

  return this._runWorkerJson(['lookup', parts[2], '--db', this._getDatabasePath()]).then((row) => {
    return {
      uri: row.stream_url,
      service: 'mpd',
      name: row.name,
      title: row.name,
      type: 'track',
      albumart: '',
      samplerate: '',
      bitdepth: '',
      trackType: 'webradio',
      channels: 2,
      duration: 0
    };
  });
};

ControllerCuratedRadio.prototype.clearAddPlayTracks = function(tracks) {
  if (!tracks || !tracks.length || !this.mpdPlugin) {
    return libQ.resolve();
  }

  const track = Array.isArray(tracks) ? tracks[0] : tracks;
  return this.explodeUri(track.uri)
    .then((exploded) => {
      return this.mpdPlugin.sendMpdCommand('stop', [])
        .then(() => this.mpdPlugin.sendMpdCommand('clear', []))
        .then(() => this.mpdPlugin.sendMpdCommand('add "' + exploded.uri + '"', []))
        .then(() => {
          this.commandRouter.stateMachine.setConsumeUpdateService('mpd');
          return this.mpdPlugin.sendMpdCommand('play', []);
        });
    });
};

ControllerCuratedRadio.prototype.syncDatabase = function(runRefreshAfterSync) {
  if (!this._isEnabled()) {
    return libQ.resolve();
  }

  const args = ['sync', '--db', this._getDatabasePath(), '--seed', this._getSeedJsonPath()];
  const findingsPath = this._getFindingsJsonPath();
  if (findingsPath) {
    args.push('--findings', findingsPath);
  }

  return this._runWorkerJson(args).then((summary) => {
    this._updateSummary(summary);
    if (runRefreshAfterSync) {
      return this.refreshDatabase(false).then(() => summary);
    }
    return summary;
  });
};

ControllerCuratedRadio.prototype.refreshDatabase = function(showToastOnError) {
  if (!this._isEnabled() || this.workerBusy) {
    return libQ.resolve();
  }

  this.workerBusy = true;
  return this._runWorkerJson(['refresh', '--db', this._getDatabasePath()])
    .then((summary) => {
      this._updateSummary(summary);
      return summary;
    })
    .fail((err) => {
      if (showToastOnError !== false) {
        this.commandRouter.pushToastMessage('error', this.getI18nString('PLUGIN_NAME'), this.getI18nString('WORKER_FAILED') + ': ' + err.message);
      }
      throw err;
    })
    .fin(() => {
      this.workerBusy = false;
    });
};

ControllerCuratedRadio.prototype.discoverFindings = function(showToastOnError) {
  if (!this._isEnabled() || this.workerBusy || !this._getFindingsJsonPath()) {
    return libQ.resolve();
  }

  this.workerBusy = true;
  return this._runWorkerJson([
    'discover',
    '--db', this._getDatabasePath(),
    '--output', this._getFindingsJsonPath(),
    '--api-base', this._getDiscoveryApiBase(),
    '--limit', String(this._getDiscoveryLimit()),
    '--profile-prompt', this._getDiscoveryProfilePrompt(),
    '--editorial-sources', this._getEditorialSourceUrls(),
    '--tags', this._getCuratedTagSeeds(),
    '--blocked-tags', this._getBlockedTagPatterns(),
    '--blocked-names', this._getBlockedNamePatterns()
  ]).then((summary) => {
    return summary;
  }).fail((err) => {
    if (showToastOnError !== false) {
      this.commandRouter.pushToastMessage('error', this.getI18nString('PLUGIN_NAME'), this.getI18nString('WORKER_FAILED') + ': ' + err.message);
    }
    throw err;
  }).fin(() => {
    this.workerBusy = false;
  });
};

ControllerCuratedRadio.prototype.loadI18nStrings = function() {
  try {
    const langCode = this.commandRouter.sharedVars.get('language_code');
    this.i18nStrings = JSON.parse(fs.readFileSync(path.join(__dirname, 'i18n', 'strings_' + langCode + '.json'), 'utf8'));
  } catch (e) {
    this.i18nStrings = JSON.parse(fs.readFileSync(path.join(__dirname, 'i18n', 'strings_en.json'), 'utf8'));
  }
  this.i18nDefaults = JSON.parse(fs.readFileSync(path.join(__dirname, 'i18n', 'strings_en.json'), 'utf8'));
};

ControllerCuratedRadio.prototype.getI18nString = function(key) {
  if (this.i18nStrings && typeof this.i18nStrings[key] !== 'undefined') {
    return this.i18nStrings[key];
  }
  if (this.i18nDefaults && typeof this.i18nDefaults[key] !== 'undefined') {
    return this.i18nDefaults[key];
  }
  return key;
};

ControllerCuratedRadio.prototype._runWorkerJson = function(args) {
  const defer = libQ.defer();
  const fullArgs = [path.join(__dirname, 'scripts', 'radio_worker.py')].concat(args);
  const options = { maxBuffer: 8 * 1024 * 1024 };

  execFile(this._getPythonCommand(), fullArgs, options, (error, stdout, stderr) => {
    if (error) {
      const detail = stderr ? stderr.trim() : error.message;
      this.logger.error('[curated_radio] worker failed: ' + detail);
      defer.reject(new Error(detail || 'worker error'));
      return;
    }

    try {
      defer.resolve(JSON.parse(stdout || '{}'));
    } catch (parseError) {
      this.logger.error('[curated_radio] invalid worker JSON: ' + parseError.message);
      defer.reject(parseError);
    }
  });

  return defer.promise;
};

ControllerCuratedRadio.prototype._buildFolderItem = function(title, uri, count) {
  return {
    service: this.serviceName,
    type: 'folder',
    title: title,
    album: String(count || 0) + ' items',
    icon: 'fa fa-folder-open',
    uri: uri
  };
};

ControllerCuratedRadio.prototype._mapStationToItem = function(row) {
  let subtitle = row.country || '';
  if (row.genre) {
    subtitle = subtitle ? subtitle + ' | ' + row.genre : row.genre;
  }
  const qualityLabel = this._formatQualityLabel(row);
  if (qualityLabel) {
    subtitle = subtitle ? subtitle + ' | ' + qualityLabel : qualityLabel;
  }
  if (row.last_status && row.last_status !== 'ok') {
    subtitle = subtitle ? subtitle + ' | ' + row.last_status : row.last_status;
  }

  return {
    service: 'webradio',
    type: 'webradio',
    title: row.is_interesting ? '* ' + row.name : row.name,
    artist: row.country || '',
    album: subtitle,
    albumart: '/albumart?sourceicon=music_service/curated_radio/curated_radio.svg',
    favorite: !!row.is_favourite,
    favourite: !!row.is_favourite,
    icon: 'fa fa-microphone',
    uri: row.stream_url
  };
};

ControllerCuratedRadio.prototype._updateSummary = function(summary) {
  if (!summary) {
    return;
  }
  const parts = [];
  if (typeof summary.total_stations !== 'undefined') {
    parts.push('stations ' + summary.total_stations);
  }
  if (typeof summary.interesting_count !== 'undefined') {
    parts.push('interesting ' + summary.interesting_count);
  }
  if (typeof summary.verified_count !== 'undefined') {
    parts.push('verified ' + summary.verified_count);
  }
  if (typeof summary.findings_count !== 'undefined') {
    parts.push('findings ' + summary.findings_count);
  }
  if (typeof summary.review_count !== 'undefined') {
    parts.push('review ' + summary.review_count);
  }
  if (typeof summary.inactive_count !== 'undefined') {
    parts.push('inactive ' + summary.inactive_count);
  }
  this.config.set('lastSyncSummary', parts.join(' | '));
};

ControllerCuratedRadio.prototype._saveCurrentInterestingValue = function(value) {
  const state = this.commandRouter.volumioGetState ? this.commandRouter.volumioGetState() : (this.commandRouter.stateMachine && this.commandRouter.stateMachine.getState ? this.commandRouter.stateMachine.getState() : null);
  const currentUri = state && (state.uri || state.trackUri || state.path || state.file) ? String(state.uri || state.trackUri || state.path || state.file).trim() : '';
  if (!currentUri) {
    this.commandRouter.pushToastMessage('error', this.getI18nString('PLUGIN_NAME'), this.getI18nString('CURRENT_STATION_MISSING'));
    return libQ.reject(new Error('No current stream URL found'));
  }

  return this._runWorkerJson([
    'mark-interesting-by-url',
    '--db', this._getDatabasePath(),
    '--url', currentUri,
    '--name', String((state && (state.album || state.artist || state.service || state.title)) || '').trim(),
    '--country', '',
    '--genre', '',
    '--value', String(value ? 1 : 0)
  ]).then((row) => {
    this.commandRouter.pushToastMessage('success', this.getI18nString('PLUGIN_NAME'), value ? this.getI18nString('CURRENT_STATION_SAVED') : this.getI18nString('CURRENT_STATION_REMOVED'));
    return row;
  });
};

ControllerCuratedRadio.prototype._withFavouritePayload = function(data, callback) {
  const input = data || {};
  const uri = input.uri ? String(input.uri) : '';
  if (!uri) {
    return libQ.reject(new Error('Missing URI'));
  }

  if (!uri.startsWith('curatedradio/play/')) {
    return callback({
      service: 'webradio',
      uri: uri,
      title: input.title || input.name || uri,
      albumart: input.albumart || '/albumart?sourceicon=music_service/curated_radio/curated_radio.svg'
    });
  }

  const parts = uri.split('/');
  return this._runWorkerJson(['lookup', parts[2], '--db', this._getDatabasePath()]).then((row) => {
    return callback({
      service: 'webradio',
      uri: row.stream_url,
      title: row.name,
      albumart: '/albumart?sourceicon=music_service/curated_radio/curated_radio.svg'
    });
  });
};

ControllerCuratedRadio.prototype._annotateFavouriteFlags = function(rows) {
  const defer = libQ.defer();
  const radioFavouritesPath = '/data/favourites/radio-favourites';

  fs.readFile(radioFavouritesPath, 'utf8', (err, content) => {
    let favouriteUris = new Set();

    if (!err && content) {
      try {
        const payload = JSON.parse(content);
        if (Array.isArray(payload)) {
          favouriteUris = new Set(payload.map((item) => item && item.uri ? String(item.uri).trim() : '').filter(Boolean));
        }
      } catch (parseError) {
        this.logger.warn('[curated_radio] could not parse radio favourites: ' + parseError.message);
      }
    }

    const annotated = (rows || []).map((row) => {
      const clone = Object.assign({}, row);
      clone.is_favourite = favouriteUris.has(String(row.stream_url || '').trim());
      return clone;
    });
    defer.resolve(annotated);
  });

  return defer.promise;
};

ControllerCuratedRadio.prototype._formatQualityLabel = function(row) {
  const bitrate = parseInt(row.bitrate, 10);
  const codec = row.codec ? String(row.codec).trim() : '';
  if (!Number.isNaN(bitrate) && bitrate > 0 && codec) {
    return bitrate + ' kbps ' + codec;
  }
  if (!Number.isNaN(bitrate) && bitrate > 0) {
    return bitrate + ' kbps';
  }
  if (codec) {
    return codec;
  }
  return '';
};

ControllerCuratedRadio.prototype._scheduleRefreshTimer = function() {
  if (this.refreshTimer) {
    clearInterval(this.refreshTimer);
    this.refreshTimer = null;
  }
  if (!this._isEnabled() || (!this.config.get('autoRefresh') && !this.config.get('autoDiscover'))) {
    return;
  }
  const intervalMinutes = this._clampInt(this.config.get('refreshIntervalMinutes'), 10, 10080, 360);
  this.refreshTimer = setInterval(() => {
    this._runMaintenanceCycle(false).fail(() => libQ.resolve());
  }, intervalMinutes * 60 * 1000);
};

ControllerCuratedRadio.prototype._runMaintenanceCycle = function(showToastOnError) {
  let sequence = libQ.resolve();

  if (this.config.get('autoDiscover') && this._getFindingsJsonPath()) {
    sequence = sequence
      .then(() => this.discoverFindings(showToastOnError))
      .then(() => this.syncDatabase(false));
  } else {
    sequence = sequence.then(() => this.syncDatabase(false));
  }

  if (this.config.get('autoRefresh')) {
    sequence = sequence.then(() => this.refreshDatabase(showToastOnError));
  }

  return sequence;
};

ControllerCuratedRadio.prototype._setUIValue = function(uiconf, id, value) {
  if (!uiconf || !uiconf.sections) {
    return;
  }
  uiconf.sections.forEach((section) => {
    if (!section.content) {
      return;
    }
    section.content.forEach((item) => {
      if (item.id === id) {
        item.value = value;
      }
    });
  });
};

ControllerCuratedRadio.prototype._clampInt = function(value, min, max, fallback) {
  const parsed = parseInt(value, 10);
  if (Number.isNaN(parsed)) {
    return fallback;
  }
  if (parsed < min) {
    return min;
  }
  if (parsed > max) {
    return max;
  }
  return parsed;
};

ControllerCuratedRadio.prototype._isEnabled = function() {
  return this.config.get('enabled') !== false;
};

ControllerCuratedRadio.prototype._getDatabasePath = function() {
  return this.config.get('databasePath') || '/data/INTERNAL/curated-radio.db';
};

ControllerCuratedRadio.prototype._getSeedJsonPath = function() {
  return this.config.get('seedJsonPath') || '/data/favourites/my-web-radio';
};

ControllerCuratedRadio.prototype._getFindingsJsonPath = function() {
  return this.config.get('findingsJsonPath') || '/data/INTERNAL/curated-radio-findings.json';
};

ControllerCuratedRadio.prototype._getMaxStationsPerSection = function() {
  return this._clampInt(this.config.get('maxStationsPerSection'), 10, 1000, 250);
};

ControllerCuratedRadio.prototype._getPythonCommand = function() {
  return this.config.get('pythonCommand') || 'python3';
};

ControllerCuratedRadio.prototype._getDiscoveryApiBase = function() {
  return this.config.get('discoveryApiBase') || 'https://de1.api.radio-browser.info';
};

ControllerCuratedRadio.prototype._getDiscoveryLimit = function() {
  return this._clampInt(this.config.get('discoveryLimit'), 10, 500, 80);
};

ControllerCuratedRadio.prototype._getDiscoveryProfilePrompt = function() {
  return this.config.get('discoveryProfilePrompt') || 'prefer: community radio, underground, eclectic, freeform, resident djs, mixes, archives, cultural talk, local scenes; avoid: mainstream, hits, christmas, xmas, chart, commercial, easy listening; sources: radio browser, station homepage';
};

ControllerCuratedRadio.prototype._getEditorialSourceUrls = function() {
  return this.config.get('editorialSourceUrls') || '';
};

ControllerCuratedRadio.prototype._getCuratedTagSeeds = function() {
  return this.config.get('curatedTagSeeds') || 'underground,eclectic,electronic,experimental,leftfield,ambient,downtempo,house,techno,dub,dubstep,jungle,garage,lofi,hip hop,jazz,soul,reggae,community';
};

ControllerCuratedRadio.prototype._getBlockedTagPatterns = function() {
  return this.config.get('blockedTagPatterns') || 'christmas,xmas,pop,hits,charts,top 40,oldies,schlager,country,religious,talk,news,sports';
};

ControllerCuratedRadio.prototype._getBlockedNamePatterns = function() {
  return this.config.get('blockedNamePatterns') || 'radio paradise,mango,christmas,xmas';
};
