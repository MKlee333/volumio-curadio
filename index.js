'use strict';

const libQ = require('kew');
const fs = require('fs');
const path = require('path');
const execFile = require('child_process').execFile;

const MIN_REFRESH_INTERVAL_MINUTES = 60;
const MAX_REFRESH_INTERVAL_MINUTES = 10080;
const DEFAULT_REFRESH_INTERVAL_MINUTES = 4320;
const MIN_DISCOVERY_LIMIT = 10;
const MAX_DISCOVERY_LIMIT = 120;
const DEFAULT_DISCOVERY_LIMIT = 30;
const MIN_DISCOVERY_CLOUD_TERM_LIMIT = 10;
const MAX_DISCOVERY_CLOUD_TERM_LIMIT = 80;
const DEFAULT_DISCOVERY_CLOUD_TERM_LIMIT = 40;
const MIN_DISCOVERY_AI_TERM_LIMIT = 0;
const MAX_DISCOVERY_AI_TERM_LIMIT = 12;
const DEFAULT_DISCOVERY_AI_TERM_LIMIT = 4;

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
  this.configFile = configFile;
  this.config = new (require('v-conf'))();
  this.config.loadFile(configFile);
  this._normalizeConfigSchema();
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

  setTimeout(() => {
    this.syncDatabase(false)
      .then(() => {
        if (this._isEnabled() && this._configBool('refreshOnStartup', true)) {
          return this._runMaintenanceCycle(false);
        }
        return libQ.resolve();
      })
      .fail((err) => {
        this.logger.error('[curated_radio] startup sync failed: ' + (err && err.message ? err.message : err));
      });
  }, 0);

  return libQ.resolve();
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
    this._setUIValue(uiconf, 'enabled', this._configBool('enabled', true));
    this._setUIValue(uiconf, 'databasePath', this._configText('databasePath', '/data/INTERNAL/curated-radio.db'));
    this._setUIValue(uiconf, 'seedJsonPath', this._configText('seedJsonPath', '/data/favourites/my-web-radio'));
    this._setUIValue(uiconf, 'findingsJsonPath', this._configText('findingsJsonPath', '/data/INTERNAL/curated-radio-findings.json'));
    this._setUIValue(uiconf, 'autoRefresh', this._configBool('autoRefresh', true));
    this._setUIValue(uiconf, 'autoDiscover', this._configBool('autoDiscover', true));
    this._setUIValue(uiconf, 'refreshIntervalMinutes', this._getRefreshIntervalMinutes());
    this._setUIValue(uiconf, 'discoveryApiBase', this._configText('discoveryApiBase', 'https://de1.api.radio-browser.info'));
    this._setUIValue(uiconf, 'discoveryLimit', this._getDiscoveryLimit());
    this._setUIValue(uiconf, 'discoveryProfilePrompt', this._configText('discoveryProfilePrompt', ''));
    this._setUIValue(uiconf, 'editorialSourceUrls', this._configText('editorialSourceUrls', ''));
    this._setUIValue(uiconf, 'curatedTagSeeds', this._configText('curatedTagSeeds', ''));
    this._setUIValue(uiconf, 'blockedTagPatterns', this._configText('blockedTagPatterns', ''));
    this._setUIValue(uiconf, 'blockedNamePatterns', this._configText('blockedNamePatterns', ''));
    this._setUIValue(uiconf, 'discoveryCloudTermLimit', this._getDiscoveryCloudTermLimit());
    this._setUIValue(uiconf, 'discoveryAiEnabled', this._configBool('discoveryAiEnabled', false));
    this._setUIValue(uiconf, 'discoveryAiApiBase', this._configText('discoveryAiApiBase', 'https://api.openai.com/v1'));
    this._setUIValue(uiconf, 'discoveryAiModel', this._configText('discoveryAiModel', 'gpt-4o-mini'));
    this._setUIValue(uiconf, 'discoveryAiApiKey', this._configText('discoveryAiApiKey', ''));
    this._setUIValue(uiconf, 'discoveryAiTermLimit', this._getDiscoveryAiTermLimit());
    this._setUIValue(uiconf, 'lastSyncSummary', this._configText('lastSyncSummary', ''));
    this._setUIValue(uiconf, 'lastCrawledAt', this._formatUiTimestamp(this._configText('lastCrawledAt', '')));
    this._setUIValue(uiconf, 'lastCheckedAt', this._formatUiTimestamp(this._configText('lastCheckedAt', '')));
    defer.resolve(uiconf);
  }).fail((err) => defer.reject(err));

  return defer.promise;
};

ControllerCuratedRadio.prototype.saveBasicConfig = function(data) {
  this.config.set('enabled', this._boolInput(data.enabled, true));
  this.config.set('databasePath', this._textInput(data.databasePath) || '/data/INTERNAL/curated-radio.db');
  this.config.set('seedJsonPath', this._textInput(data.seedJsonPath) || '/data/favourites/my-web-radio');
  this.config.set('findingsJsonPath', this._textInput(data.findingsJsonPath) || '/data/INTERNAL/curated-radio-findings.json');
  this.config.set('autoRefresh', this._boolInput(data.autoRefresh, true));
  this.config.set('autoDiscover', this._boolInput(data.autoDiscover, true));
  this.config.set('refreshIntervalMinutes', this._clampInt(data.refreshIntervalMinutes, MIN_REFRESH_INTERVAL_MINUTES, MAX_REFRESH_INTERVAL_MINUTES, DEFAULT_REFRESH_INTERVAL_MINUTES));
  this.config.set('discoveryApiBase', this._textInput(data.discoveryApiBase) || 'https://de1.api.radio-browser.info');
  this.config.set('discoveryLimit', this._clampInt(data.discoveryLimit, MIN_DISCOVERY_LIMIT, MAX_DISCOVERY_LIMIT, DEFAULT_DISCOVERY_LIMIT));
  this.config.set('discoveryProfilePrompt', this._textInput(data.discoveryProfilePrompt) || 'prefer: community radio, underground, eclectic, freeform, resident djs, mixes, archives, cultural talk, local scenes; avoid: mainstream, hits, christmas, xmas, chart, commercial, easy listening; sources: radio browser, station homepage');
  this.config.set('editorialSourceUrls', this._textInput(data.editorialSourceUrls));
  this.config.set('curatedTagSeeds', this._textInput(data.curatedTagSeeds) || 'underground,eclectic,electronic,experimental,leftfield,ambient,downtempo,house,techno,dub,dubstep,jungle,garage,lofi,hip hop,jazz,soul,reggae,community');
  this.config.set('blockedTagPatterns', this._textInput(data.blockedTagPatterns) || 'christmas,xmas,pop,hits,charts,top 40,oldies,schlager,country,religious,talk,news,sports');
  this.config.set('blockedNamePatterns', this._textInput(data.blockedNamePatterns) || 'radio paradise,mango,christmas,xmas');
  this.config.set('discoveryCloudTermLimit', this._clampInt(data.discoveryCloudTermLimit, MIN_DISCOVERY_CLOUD_TERM_LIMIT, MAX_DISCOVERY_CLOUD_TERM_LIMIT, DEFAULT_DISCOVERY_CLOUD_TERM_LIMIT));
  this.config.set('discoveryAiEnabled', this._boolInput(data.discoveryAiEnabled, false));
  this.config.set('discoveryAiApiBase', this._textInput(data.discoveryAiApiBase) || 'https://api.openai.com/v1');
  this.config.set('discoveryAiModel', this._textInput(data.discoveryAiModel) || 'gpt-4o-mini');
  this.config.set('discoveryAiApiKey', this._textInput(data.discoveryAiApiKey));
  this.config.set('discoveryAiTermLimit', this._clampInt(data.discoveryAiTermLimit, MIN_DISCOVERY_AI_TERM_LIMIT, MAX_DISCOVERY_AI_TERM_LIMIT, DEFAULT_DISCOVERY_AI_TERM_LIMIT));
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

ControllerCuratedRadio.prototype.saveCurrentStatic = function() {
  return this._saveCurrentStaticValue(1);
};

ControllerCuratedRadio.prototype.removeCurrentStatic = function() {
  return this._saveCurrentStaticValue(0);
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
      const checkedAt = new Date().toISOString();
      this.config.set('lastCheckedAt', checkedAt);
      if (!summary || typeof summary !== 'object') {
        summary = {};
      }
      summary.last_checked_at = checkedAt;
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

  const aiApiKey = this._getDiscoveryAiApiKey();
  const workerEnv = aiApiKey ? { CURADIO_AI_API_KEY: aiApiKey } : null;

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
    '--blocked-names', this._getBlockedNamePatterns(),
    '--cloud-term-limit', String(this._getDiscoveryCloudTermLimit()),
    '--ai-enabled', this._configBool('discoveryAiEnabled', false) ? '1' : '0',
    '--ai-api-base', this._getDiscoveryAiApiBase(),
    '--ai-model', this._getDiscoveryAiModel(),
    '--ai-term-limit', String(this._getDiscoveryAiTermLimit())
  ], workerEnv).then((summary) => {
    const crawledAt = new Date().toISOString();
    this.config.set('lastCrawledAt', crawledAt);
    if (!summary || typeof summary !== 'object') {
      summary = {};
    }
    summary.last_crawled_at = crawledAt;
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

ControllerCuratedRadio.prototype._runWorkerJson = function(args, envVars) {
  const defer = libQ.defer();
  const fullArgs = [path.join(__dirname, 'scripts', 'radio_worker.py')].concat(args);
  const options = {
    maxBuffer: 8 * 1024 * 1024,
    timeout: 6 * 60 * 1000,
    env: Object.assign({}, process.env, envVars || {})
  };

  execFile(this._getPythonCommand(), fullArgs, options, (error, stdout, stderr) => {
    if (error) {
      let detail = stderr ? stderr.trim() : '';
      if (!detail) {
        detail = error.killed ? 'worker timeout after ' + Math.round(options.timeout / 1000) + 's' : error.message;
      }
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
  if (parseInt(row.is_static_url, 10) === 1) {
    const staticMarker = this.getI18nString('STATIC_URL_MARKER');
    subtitle = subtitle ? subtitle + ' | ' + staticMarker : staticMarker;
  }
  if (row.last_error && String(row.last_error).indexOf('static-url candidate:') === 0) {
    const candidateText = String(row.last_error).replace('static-url candidate:', 'candidate').trim();
    subtitle = subtitle ? subtitle + ' | ' + candidateText : candidateText;
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
  if (typeof summary.term_cloud_size !== 'undefined') {
    parts.push('cloud ' + summary.term_cloud_size);
  }
  if (typeof summary.accepted_tag_stage !== 'undefined') {
    parts.push('tag-stage ' + summary.accepted_tag_stage);
  }
  if (typeof summary.accepted_term_cloud_stage !== 'undefined') {
    parts.push('term-stage ' + summary.accepted_term_cloud_stage);
  }
  if (summary.ai_used) {
    parts.push('ai-terms ' + (summary.ai_term_count || 0));
  }
  this.config.set('lastSyncSummary', parts.join(' | '));
  if (typeof summary.last_crawled_at !== 'undefined' && summary.last_crawled_at) {
    this.config.set('lastCrawledAt', summary.last_crawled_at);
  }
  if (typeof summary.last_checked_at !== 'undefined' && summary.last_checked_at) {
    this.config.set('lastCheckedAt', summary.last_checked_at);
  }
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

ControllerCuratedRadio.prototype._saveCurrentStaticValue = function(value) {
  const state = this.commandRouter.volumioGetState ? this.commandRouter.volumioGetState() : (this.commandRouter.stateMachine && this.commandRouter.stateMachine.getState ? this.commandRouter.stateMachine.getState() : null);
  const currentUri = state && (state.uri || state.trackUri || state.path || state.file) ? String(state.uri || state.trackUri || state.path || state.file).trim() : '';
  if (!currentUri) {
    this.commandRouter.pushToastMessage('error', this.getI18nString('PLUGIN_NAME'), this.getI18nString('CURRENT_STATION_MISSING'));
    return libQ.reject(new Error('No current stream URL found'));
  }

  return this._runWorkerJson([
    'mark-static-by-url',
    '--db', this._getDatabasePath(),
    '--url', currentUri,
    '--name', String((state && (state.album || state.artist || state.service || state.title)) || '').trim(),
    '--country', '',
    '--genre', '',
    '--value', String(value ? 1 : 0)
  ]).then((row) => {
    this.commandRouter.pushToastMessage('success', this.getI18nString('PLUGIN_NAME'), value ? this.getI18nString('CURRENT_STATION_STATIC_SAVED') : this.getI18nString('CURRENT_STATION_STATIC_REMOVED'));
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
  if (!this._isEnabled() || (!this._configBool('autoRefresh', true) && !this._configBool('autoDiscover', true))) {
    return;
  }
  const intervalMinutes = this._getRefreshIntervalMinutes();
  this.refreshTimer = setInterval(() => {
    this._runMaintenanceCycle(false).fail(() => libQ.resolve());
  }, intervalMinutes * 60 * 1000);
};

ControllerCuratedRadio.prototype._runMaintenanceCycle = function(showToastOnError) {
  let sequence = libQ.resolve();

  if (this._configBool('autoDiscover', true) && this._getFindingsJsonPath()) {
    sequence = sequence
      .then(() => this.discoverFindings(showToastOnError))
      .then(() => this.syncDatabase(false));
  } else {
    sequence = sequence.then(() => this.syncDatabase(false));
  }

  if (this._configBool('autoRefresh', true)) {
    sequence = sequence.then(() => this.refreshDatabase(showToastOnError));
  }

  return sequence;
};

ControllerCuratedRadio.prototype._unwrapInputValue = function(value) {
  if (value && typeof value === 'object' && Object.prototype.hasOwnProperty.call(value, 'value')) {
    return value.value;
  }
  return value;
};

ControllerCuratedRadio.prototype._textInput = function(value) {
  const unwrapped = this._unwrapInputValue(value);
  return String(unwrapped == null ? '' : unwrapped).trim();
};

ControllerCuratedRadio.prototype._boolInput = function(value, fallback) {
  const unwrapped = this._unwrapInputValue(value);
  if (typeof unwrapped === 'boolean') {
    return unwrapped;
  }
  if (typeof unwrapped === 'number') {
    return unwrapped !== 0;
  }
  if (typeof unwrapped === 'string') {
    const token = unwrapped.trim().toLowerCase();
    if (token === 'true' || token === '1' || token === 'yes' || token === 'on') {
      return true;
    }
    if (token === 'false' || token === '0' || token === 'no' || token === 'off') {
      return false;
    }
  }
  return fallback;
};

ControllerCuratedRadio.prototype._configValue = function(key, fallback) {
  const value = this._unwrapInputValue(this.config.get(key));
  if (typeof value === 'undefined' || value === null) {
    return fallback;
  }
  return value;
};

ControllerCuratedRadio.prototype._configText = function(key, fallback) {
  const value = this._configValue(key, fallback);
  return String(value == null ? '' : value);
};

ControllerCuratedRadio.prototype._configBool = function(key, fallback) {
  return this._boolInput(this.config.get(key), fallback);
};

ControllerCuratedRadio.prototype._configInt = function(key, fallback) {
  return this._clampInt(this.config.get(key), -2147483648, 2147483647, fallback);
};

ControllerCuratedRadio.prototype._defaultConfigValues = function() {
  return {
    enabled: true,
    pythonCommand: 'python3',
    databasePath: '/data/INTERNAL/curated-radio.db',
    seedJsonPath: '/data/favourites/my-web-radio',
    findingsJsonPath: '/data/INTERNAL/curated-radio-findings.json',
    autoRefresh: true,
    autoDiscover: true,
    refreshIntervalMinutes: DEFAULT_REFRESH_INTERVAL_MINUTES,
    discoveryApiBase: 'https://de1.api.radio-browser.info',
    discoveryLimit: DEFAULT_DISCOVERY_LIMIT,
    discoveryProfilePrompt: 'prefer: community radio, underground, eclectic, freeform, resident djs, mixes, archives, cultural talk, local scenes; avoid: mainstream, hits, christmas, xmas, chart, commercial, easy listening; sources: radio browser, station homepage',
    editorialSourceUrls: '',
    curatedTagSeeds: 'underground,eclectic,electronic,experimental,leftfield,ambient,downtempo,house,techno,dub,dubstep,jungle,garage,lofi,hip hop,jazz,soul,reggae,community',
    blockedTagPatterns: 'christmas,xmas,pop,hits,charts,top 40,oldies,schlager,country,religious,talk,news,sports',
    blockedNamePatterns: 'radio paradise,mango,christmas,xmas',
    discoveryCloudTermLimit: DEFAULT_DISCOVERY_CLOUD_TERM_LIMIT,
    discoveryAiEnabled: false,
    discoveryAiApiBase: 'https://api.openai.com/v1',
    discoveryAiModel: 'gpt-4o-mini',
    discoveryAiApiKey: '',
    discoveryAiTermLimit: DEFAULT_DISCOVERY_AI_TERM_LIMIT,
    refreshOnStartup: true,
    maxStationsPerSection: 250,
    lastSyncSummary: 'Not synced yet',
    lastCrawledAt: '',
    lastCheckedAt: '',
  };
};

ControllerCuratedRadio.prototype._normalizeConfigType = function(typeHint, value) {
  const typeToken = String(typeHint || '').trim().toLowerCase();
  if (typeToken === 'boolean') {
    return 'boolean';
  }
  if (typeToken === 'number' || typeToken === 'integer' || typeToken === 'float') {
    return 'number';
  }
  if (typeToken === 'string') {
    return 'string';
  }
  if (typeof value === 'boolean') {
    return 'boolean';
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return 'number';
  }
  return 'string';
};

ControllerCuratedRadio.prototype._normalizeConfigSchema = function() {
  let raw = {};
  try {
    raw = JSON.parse(fs.readFileSync(this.configFile, 'utf8'));
  } catch (e) {
    raw = {};
  }

  const defaults = this._defaultConfigValues();
  const normalized = {};
  let needsRewrite = false;

  Object.keys(defaults).forEach((key) => {
    const defaultValue = defaults[key];
    const fallbackType = this._normalizeConfigType(null, defaultValue);
    const rawEntry = raw && Object.prototype.hasOwnProperty.call(raw, key) ? raw[key] : undefined;

    if (rawEntry && typeof rawEntry === 'object' && Object.prototype.hasOwnProperty.call(rawEntry, 'value')) {
      const normalizedType = this._normalizeConfigType(rawEntry.type, rawEntry.value);
      normalized[key] = {
        type: normalizedType,
        value: rawEntry.value
      };
      if (rawEntry.type !== normalizedType) {
        needsRewrite = true;
      }
      return;
    }

    const normalizedValue = typeof rawEntry === 'undefined' ? defaultValue : this._unwrapInputValue(rawEntry);
    normalized[key] = {
      type: fallbackType,
      value: normalizedValue
    };
    needsRewrite = true;
  });

  Object.keys(raw || {}).forEach((key) => {
    if (Object.prototype.hasOwnProperty.call(normalized, key)) {
      return;
    }
    const rawEntry = raw[key];
    if (rawEntry && typeof rawEntry === 'object' && Object.prototype.hasOwnProperty.call(rawEntry, 'value')) {
      const normalizedType = this._normalizeConfigType(rawEntry.type, rawEntry.value);
      normalized[key] = {
        type: normalizedType,
        value: rawEntry.value
      };
      if (rawEntry.type !== normalizedType) {
        needsRewrite = true;
      }
      return;
    }
    normalized[key] = {
      type: this._normalizeConfigType(null, rawEntry),
      value: this._unwrapInputValue(rawEntry)
    };
    needsRewrite = true;
  });

  if (needsRewrite) {
    fs.writeFileSync(this.configFile, JSON.stringify(normalized, null, 2), 'utf8');
    this.config.loadFile(this.configFile);
    this.logger.info('[curated_radio] normalized config schema to v-conf typed values');
  }
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

ControllerCuratedRadio.prototype._formatUiTimestamp = function(value) {
  if (!value) {
    return '-';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleString();
};

ControllerCuratedRadio.prototype._clampInt = function(value, min, max, fallback) {
  const parsed = parseInt(this._unwrapInputValue(value), 10);
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
  return this._configBool('enabled', true);
};

ControllerCuratedRadio.prototype._getDatabasePath = function() {
  return this._configText('databasePath', '/data/INTERNAL/curated-radio.db');
};

ControllerCuratedRadio.prototype._getSeedJsonPath = function() {
  return this._configText('seedJsonPath', '/data/favourites/my-web-radio');
};

ControllerCuratedRadio.prototype._getFindingsJsonPath = function() {
  return this._configText('findingsJsonPath', '/data/INTERNAL/curated-radio-findings.json');
};

ControllerCuratedRadio.prototype._getMaxStationsPerSection = function() {
  return this._clampInt(this._configValue('maxStationsPerSection', 250), 10, 1000, 250);
};

ControllerCuratedRadio.prototype._getRefreshIntervalMinutes = function() {
  return this._clampInt(this._configValue('refreshIntervalMinutes', DEFAULT_REFRESH_INTERVAL_MINUTES), MIN_REFRESH_INTERVAL_MINUTES, MAX_REFRESH_INTERVAL_MINUTES, DEFAULT_REFRESH_INTERVAL_MINUTES);
};

ControllerCuratedRadio.prototype._getPythonCommand = function() {
  return this._configText('pythonCommand', 'python3');
};

ControllerCuratedRadio.prototype._getDiscoveryApiBase = function() {
  return this._configText('discoveryApiBase', 'https://de1.api.radio-browser.info');
};

ControllerCuratedRadio.prototype._getDiscoveryLimit = function() {
  return this._clampInt(this._configValue('discoveryLimit', DEFAULT_DISCOVERY_LIMIT), MIN_DISCOVERY_LIMIT, MAX_DISCOVERY_LIMIT, DEFAULT_DISCOVERY_LIMIT);
};

ControllerCuratedRadio.prototype._getDiscoveryProfilePrompt = function() {
  return this._configText('discoveryProfilePrompt', 'prefer: community radio, underground, eclectic, freeform, resident djs, mixes, archives, cultural talk, local scenes; avoid: mainstream, hits, christmas, xmas, chart, commercial, easy listening; sources: radio browser, station homepage');
};

ControllerCuratedRadio.prototype._getEditorialSourceUrls = function() {
  return this._configText('editorialSourceUrls', '');
};

ControllerCuratedRadio.prototype._getCuratedTagSeeds = function() {
  return this._configText('curatedTagSeeds', 'underground,eclectic,electronic,experimental,leftfield,ambient,downtempo,house,techno,dub,dubstep,jungle,garage,lofi,hip hop,jazz,soul,reggae,community');
};

ControllerCuratedRadio.prototype._getBlockedTagPatterns = function() {
  return this._configText('blockedTagPatterns', 'christmas,xmas,pop,hits,charts,top 40,oldies,schlager,country,religious,talk,news,sports');
};

ControllerCuratedRadio.prototype._getBlockedNamePatterns = function() {
  return this._configText('blockedNamePatterns', 'radio paradise,mango,christmas,xmas');
};

ControllerCuratedRadio.prototype._getDiscoveryCloudTermLimit = function() {
  return this._clampInt(this._configValue('discoveryCloudTermLimit', DEFAULT_DISCOVERY_CLOUD_TERM_LIMIT), MIN_DISCOVERY_CLOUD_TERM_LIMIT, MAX_DISCOVERY_CLOUD_TERM_LIMIT, DEFAULT_DISCOVERY_CLOUD_TERM_LIMIT);
};

ControllerCuratedRadio.prototype._getDiscoveryAiApiBase = function() {
  return this._configText('discoveryAiApiBase', 'https://api.openai.com/v1');
};

ControllerCuratedRadio.prototype._getDiscoveryAiModel = function() {
  return this._configText('discoveryAiModel', 'gpt-4o-mini');
};

ControllerCuratedRadio.prototype._getDiscoveryAiApiKey = function() {
  return this._configText('discoveryAiApiKey', '') || process.env.OPENAI_API_KEY || '';
};

ControllerCuratedRadio.prototype._getDiscoveryAiTermLimit = function() {
  return this._clampInt(this._configValue('discoveryAiTermLimit', DEFAULT_DISCOVERY_AI_TERM_LIMIT), MIN_DISCOVERY_AI_TERM_LIMIT, MAX_DISCOVERY_AI_TERM_LIMIT, DEFAULT_DISCOVERY_AI_TERM_LIMIT);
};
