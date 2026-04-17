#!/usr/bin/env python3
import argparse
import hashlib
import datetime as dt
from html import unescape
import json
import os
import re
import sqlite3
import sys
import urllib.error
import urllib.parse
import urllib.request


PLAYLIST_CONTENT_TYPES = {
    'application/vnd.apple.mpegurl',
    'application/x-mpegurl',
    'audio/mpegurl',
    'audio/x-mpegurl',
    'audio/x-scpls',
    'application/pls+xml',
    'application/octet-stream',
}

STREAM_EXTENSIONS = ('.mp3', '.aac', '.ogg', '.opus', '.m3u', '.m3u8', '.pls', '.asx')
CURATION_SIGNAL_PATTERNS = (
    'underground',
    'eclectic',
    'experimental',
    'leftfield',
    'community',
    'independent',
    'college',
    'pirate',
    'selector',
    'selectors',
    'alternative',
)
GENERIC_NAME_PATTERNS = (
    'smooth jazz',
    'jazz radio',
    'deep house',
    'dance wave',
    'sunshine live',
    'intense radio',
    'radio swiss jazz',
    'hits',
    'top 40',
    'oldies',
)


def utc_now():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def connect_db(db_path):
    directory = os.path.dirname(db_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA foreign_keys=ON')
    conn.executescript(
        '''
        CREATE TABLE IF NOT EXISTS stations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            stream_url TEXT NOT NULL,
            normalized_name TEXT NOT NULL UNIQUE,
            normalized_url TEXT NOT NULL UNIQUE,
            country TEXT NOT NULL DEFAULT '',
            genre TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT '',
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_check_at TEXT,
            last_success_at TEXT,
            last_status TEXT NOT NULL DEFAULT 'pending',
            last_http_status INTEGER,
            last_error TEXT NOT NULL DEFAULT '',
            last_content_type TEXT NOT NULL DEFAULT '',
            last_resolved_url TEXT NOT NULL DEFAULT '',
            active INTEGER NOT NULL DEFAULT 1,
            needs_review INTEGER NOT NULL DEFAULT 0,
            is_new INTEGER NOT NULL DEFAULT 1,
            failure_count INTEGER NOT NULL DEFAULT 0,
            is_interesting INTEGER NOT NULL DEFAULT 0,
            is_static_url INTEGER NOT NULL DEFAULT 0,
            bitrate INTEGER NOT NULL DEFAULT 0,
            codec TEXT NOT NULL DEFAULT '',
            confidence REAL NOT NULL DEFAULT 0.5
        );
        CREATE TABLE IF NOT EXISTS evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_key TEXT NOT NULL,
            candidate_name TEXT NOT NULL DEFAULT '',
            candidate_url TEXT NOT NULL DEFAULT '',
            source_kind TEXT NOT NULL,
            source_name TEXT NOT NULL DEFAULT '',
            source_url TEXT NOT NULL DEFAULT '',
            evidence_hash TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL DEFAULT '',
            excerpt TEXT NOT NULL DEFAULT '',
            confidence REAL NOT NULL DEFAULT 0.0,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_evidence_candidate_key ON evidence(candidate_key);
        CREATE TABLE IF NOT EXISTS url_change_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id INTEGER NOT NULL,
            station_name TEXT NOT NULL DEFAULT '',
            old_stream_url TEXT NOT NULL DEFAULT '',
            new_stream_url TEXT NOT NULL DEFAULT '',
            normalized_new_url TEXT NOT NULL DEFAULT '',
            candidate_name TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT '',
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            FOREIGN KEY(station_id) REFERENCES stations(id) ON DELETE CASCADE,
            UNIQUE(station_id, normalized_new_url, status)
        );
        CREATE INDEX IF NOT EXISTS idx_url_change_reviews_status ON url_change_reviews(status, station_id);
        '''
    )
    ensure_station_schema(conn)
    return conn


def ensure_station_schema(conn):
    columns = {row[1] for row in conn.execute('PRAGMA table_info(stations)').fetchall()}
    if 'is_interesting' not in columns:
        conn.execute("ALTER TABLE stations ADD COLUMN is_interesting INTEGER NOT NULL DEFAULT 0")
    if 'is_static_url' not in columns:
        conn.execute("ALTER TABLE stations ADD COLUMN is_static_url INTEGER NOT NULL DEFAULT 0")
    if 'bitrate' not in columns:
        conn.execute("ALTER TABLE stations ADD COLUMN bitrate INTEGER NOT NULL DEFAULT 0")
    if 'codec' not in columns:
        conn.execute("ALTER TABLE stations ADD COLUMN codec TEXT NOT NULL DEFAULT ''")
    conn.commit()


def normalize_name(value):
    text = (value or '').strip().lower()
    text = re.sub(r'\[[^\]]*\]', '', text)
    text = re.sub(r'[^a-z0-9]+', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def normalize_url(value):
    text = (value or '').strip()
    parsed = urllib.parse.urlparse(text)
    path = parsed.path or ''
    if path.endswith('/'):
        path = path[:-1]
    rebuilt = urllib.parse.urlunparse((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        path,
        '',
        parsed.query,
        ''
    ))
    return rebuilt


def int_value(value, default=0):
    try:
        return int(value or default)
    except (TypeError, ValueError):
        return int(default)


def stream_quality_key(bitrate, codec, url_value):
    codec_bonus = 0
    codec_value = normalize_token(codec)
    if codec_value == 'flac':
        codec_bonus = 4000
    elif codec_value in ('aac', 'aac+', 'aacp', 'opus'):
        codec_bonus = 700
    elif codec_value in ('mp3', 'mpeg'):
        codec_bonus = 400
    url_bonus = 20 if str(url_value or '').startswith('https://') else 0
    return (int_value(bitrate, 0), codec_bonus, url_bonus)


def parse_name_metadata(name):
    match = re.search(r'^(.*?)\s*\[(.*?)\]$', (name or '').strip())
    if not match:
        return (name or '').strip(), '', ''
    display_name = match.group(1).strip()
    meta = match.group(2).strip()
    if '|' in meta:
        country, genre = meta.split('|', 1)
        return display_name, country.strip(), genre.strip()
    return display_name, meta, ''


def parse_station_items(file_path):
    if not file_path or not os.path.exists(file_path):
        return []
    with open(file_path, 'r', encoding='utf-8') as handle:
        payload = json.load(handle)
    items = []
    if not isinstance(payload, list):
        return items
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        name = (entry.get('name') or entry.get('title') or '').strip()
        stream_url = (entry.get('uri') or entry.get('url') or '').strip()
        if not name or not stream_url:
            continue
        plain_name, country, genre = parse_name_metadata(name)
        items.append({
            'name': name,
            'plain_name': plain_name,
            'stream_url': stream_url,
            'normalized_name': normalize_name(name),
            'normalized_url': normalize_url(stream_url),
            'country': country,
            'genre': genre,
            'bitrate': int_value(entry.get('bitrate'), 0),
            'codec': (entry.get('codec') or '').strip(),
            'is_interesting': 1 if entry.get('is_interesting') else 0,
        })
    return items


def http_get_json(url_value):
    request = urllib.request.Request(
        url_value,
        headers={
            'User-Agent': 'CuratedRadio/0.2',
            'Accept': 'application/json',
        }
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        payload = response.read().decode('utf-8')
    return json.loads(payload)


def fetch_text_document(url_value):
    request = urllib.request.Request(
        url_value,
        headers={
            'User-Agent': 'CuratedRadio/0.2',
            'Accept': 'text/html,application/xhtml+xml,text/plain;q=0.8,*/*;q=0.5',
        }
    )
    with urllib.request.urlopen(request, timeout=8) as response:
        raw = response.read(65536)
        content_type = (response.headers.get('Content-Type') or '').lower()
        final_url = response.geturl()
    if not any(token in content_type for token in ('html', 'text', 'xml', 'rss', 'atom')) and not raw.lstrip().startswith(b'<'):
        return {
            'url': final_url,
            'title': '',
            'text': '',
            'raw': '',
            'content_type': content_type,
        }
    try:
        text = raw.decode('utf-8', errors='ignore')
    except Exception:
        text = raw.decode('latin-1', errors='ignore')
    title_match = re.search(r'<title[^>]*>(.*?)</title>', text, re.I | re.S)
    title = strip_markup(title_match.group(1)) if title_match else ''
    text_content = strip_markup(text)
    return {
        'url': final_url,
        'title': title[:500],
        'text': text_content[:12000],
        'raw': text[:200000],
        'content_type': content_type,
    }


def base_display_name(value):
    text = (value or '').strip()
    text = re.sub(r'\s*\[[^\]]*\]\s*$', '', text)
    return text.strip()


def station_tags(raw_tags):
    tags = tokenize_genres(raw_tags)
    deduped = []
    seen = set()
    for tag in tags:
        lowered = tag.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(tag)
    return deduped[:4]


def csv_tokens(value):
    if not value:
        return []
    return [item.strip() for item in re.split(r'[,;\n]+', value) if item.strip()]


def split_prompt_items(value):
    if not value:
        return []
    return [item.strip() for item in re.split(r'[,;\n]+', value) if item.strip()]


def strip_markup(value):
    text = value or ''
    text = re.sub(r'(?is)<script.*?</script>|<style.*?</style>', ' ', text)
    text = re.sub(r'(?s)<[^>]+>', ' ', text)
    text = unescape(text)
    return re.sub(r'\s+', ' ', text).strip()


def normalize_token(value):
    return re.sub(r'\s+', ' ', (value or '').strip().lower())


def matches_pattern_list(value, patterns):
    lowered = normalize_token(value)
    for pattern in patterns:
        token = normalize_token(pattern)
        if token and token in lowered:
            return True
    return False


def parse_profile_prompt(prompt_text):
    prompt_text = (prompt_text or '').strip()
    profile = {
        'prefer': [],
        'avoid': [],
        'sources': [],
        'must': [],
    }
    if not prompt_text:
        return profile

    parts = re.split(r'(?i)\b(prefer|avoid|sources|must)\s*:\s*', prompt_text)
    if len(parts) <= 1:
        profile['prefer'] = split_prompt_items(prompt_text)
        return profile

    prefix = parts[0].strip()
    if prefix:
        profile['prefer'].extend(split_prompt_items(prefix))

    idx = 1
    while idx + 1 < len(parts):
        key = parts[idx].strip().lower()
        value = parts[idx + 1].strip()
        if key in profile:
            profile[key].extend(split_prompt_items(value))
        idx += 2
    return profile


def entry_tags(entry):
    return [normalize_token(tag) for tag in station_tags(entry.get('tags') or '')]


def format_discovered_name(entry):
    name = base_display_name(entry.get('name') or '')
    country = (entry.get('country') or 'Unknown').strip() or 'Unknown'
    tags = station_tags(entry.get('tags') or '')
    genre = ' / '.join(tags)
    if genre:
        return '{} [{} | {}]'.format(name, country, genre)
    return '{} [{}]'.format(name, country)


def is_generic_format_name(value):
    name = normalize_token(base_display_name(value))
    if not name:
        return True
    for pattern in GENERIC_NAME_PATTERNS:
        if pattern in name:
            return True
    tokens = [token for token in re.split(r'[^a-z0-9]+', name) if token]
    generic_words = {
        'radio', 'fm', 'am', 'jazz', 'smooth', 'deep', 'house', 'dance',
        'live', 'lounge', 'classic', 'classics', 'hits', 'blues', 'trance',
        'electronic', 'eurodance', 'oldies', 'soul', 'ambient', 'reggae',
        'hip', 'hop', 'groove', 'station'
    }
    if tokens and all(token.isdigit() or token in generic_words for token in tokens):
        return True
    return False


def evidence_hash_for(candidate_key, source_kind, source_url, title):
    raw = '||'.join([candidate_key, source_kind, source_url or '', title or ''])
    return hashlib.sha1(raw.encode('utf-8')).hexdigest()


def source_name_from_url(url_value):
    host = urllib.parse.urlparse(url_value or '').netloc.lower()
    host = re.sub(r'^www\.', '', host)
    return host or 'editorial_source'


def parse_source_directives(profile_sources, editorial_sources_csv):
    source_urls = []
    source_tokens = []
    for item in list(profile_sources or []) + csv_tokens(editorial_sources_csv):
        token = (item or '').strip()
        if not token:
            continue
        if token.startswith('http://') or token.startswith('https://'):
            source_urls.append(token)
        else:
            source_tokens.append(normalize_token(token))
    return {
        'source_urls': list(dict.fromkeys(source_urls)),
        'source_tokens': list(dict.fromkeys(source_tokens)),
    }


def extract_xml_field(block, tag_name):
    match = re.search(r'<{0}\b[^>]*>(.*?)</{0}>'.format(re.escape(tag_name)), block, re.I | re.S)
    if match:
        return strip_markup(match.group(1))
    return ''


def extract_feed_link(block):
    href_match = re.search(r'<link\b[^>]*href=["\']([^"\']+)["\']', block, re.I)
    if href_match:
        return href_match.group(1).strip()
    text_match = re.search(r'<link\b[^>]*>(.*?)</link>', block, re.I | re.S)
    if text_match:
        link = strip_markup(text_match.group(1))
        if link.startswith('http://') or link.startswith('https://'):
            return link
    return ''


def extract_editorial_documents(source_urls):
    documents = []
    for source_url in source_urls:
        try:
            doc = fetch_text_document(source_url)
        except Exception:
            continue
        raw = doc.get('raw') or ''
        source_name = source_name_from_url(doc.get('url') or source_url)
        lowered_raw = raw.lower()
        if '<rss' in lowered_raw or '<feed' in lowered_raw or '<item' in lowered_raw or '<entry' in lowered_raw:
            blocks = re.findall(r'(?is)<item\b.*?</item>|<entry\b.*?</entry>', raw)
            for block in blocks[:25]:
                title = extract_xml_field(block, 'title')
                summary = (
                    extract_xml_field(block, 'description') or
                    extract_xml_field(block, 'summary') or
                    extract_xml_field(block, 'content')
                )
                link = extract_feed_link(block) or doc.get('url') or source_url
                text = strip_markup(summary)[:4000]
                if not title and not text:
                    continue
                documents.append({
                    'source_kind': 'editorial_feed',
                    'source_name': source_name,
                    'source_url': source_url,
                    'url': link,
                    'title': title[:300],
                    'text': text,
                })
        else:
            if doc.get('title') or doc.get('text'):
                documents.append({
                    'source_kind': 'editorial_page',
                    'source_name': source_name,
                    'source_url': source_url,
                    'url': doc.get('url') or source_url,
                    'title': (doc.get('title') or '')[:300],
                    'text': (doc.get('text') or '')[:6000],
                })
    return documents


def editorial_mention_evidence(editorial_docs, station_name, profile):
    normalized_name = normalize_token(base_display_name(station_name))
    if not normalized_name or len(normalized_name) < 3:
        return None
    escaped_name = r'\b' + re.escape(normalized_name) + r'\b'
    best = None
    for doc in editorial_docs:
        text = normalize_token((doc.get('title') or '') + ' ' + (doc.get('text') or ''))
        if not text or re.search(escaped_name, text) is None:
            continue
        prefer_hits = [item for item in profile.get('prefer', []) if normalize_token(item) and normalize_token(item) in text]
        avoid_hits = [item for item in profile.get('avoid', []) if normalize_token(item) and normalize_token(item) in text]
        curation_hits = [item for item in CURATION_SIGNAL_PATTERNS if item in text]
        score = 35
        title_text = normalize_token(doc.get('title') or '')
        if re.search(escaped_name, title_text):
            score += 30
        score += len(prefer_hits) * 10
        score += len(curation_hits) * 6
        score -= len(avoid_hits) * 14
        if score < 40:
            continue
        confidence = min(0.92, 0.42 + score / 100.0)
        excerpt = (doc.get('text') or '')[:240]
        candidate = {
            'source_kind': doc.get('source_kind') or 'editorial',
            'source_name': doc.get('source_name') or 'editorial_source',
            'source_url': doc.get('url') or doc.get('source_url') or '',
            'title': (doc.get('title') or '')[:160],
            'excerpt': excerpt,
            'confidence': confidence,
            'score': score,
            'matches': prefer_hits[:8] + curation_hits[:8],
            'document_url': doc.get('url') or '',
            'source_page_url': doc.get('source_url') or '',
        }
        if best is None or candidate['score'] > best['score']:
            best = candidate
    return best


def upsert_evidence(conn, candidate_key, candidate_name, candidate_url, source_kind, source_name, source_url, title, excerpt, confidence, metadata=None):
    now = utc_now()
    hashed = evidence_hash_for(candidate_key, source_kind, source_url, title)
    metadata_json = json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True)
    existing = conn.execute('SELECT id FROM evidence WHERE evidence_hash = ?', (hashed,)).fetchone()
    if existing is None:
        conn.execute(
            '''
            INSERT INTO evidence (
                candidate_key, candidate_name, candidate_url, source_kind, source_name,
                source_url, evidence_hash, title, excerpt, confidence,
                first_seen_at, last_seen_at, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                candidate_key,
                candidate_name,
                candidate_url,
                source_kind,
                source_name,
                source_url or '',
                hashed,
                title or '',
                excerpt or '',
                float(confidence or 0.0),
                now,
                now,
                metadata_json,
            )
        )
    else:
        conn.execute(
            '''
            UPDATE evidence
            SET candidate_name = ?, candidate_url = ?, source_name = ?, source_url = ?,
                title = ?, excerpt = ?, confidence = ?, last_seen_at = ?, metadata_json = ?
            WHERE id = ?
            ''',
            (
                candidate_name,
                candidate_url,
                source_name,
                source_url or '',
                title or '',
                excerpt or '',
                float(confidence or 0.0),
                now,
                metadata_json,
                existing['id'],
            )
        )


def homepage_curation_evidence(homepage_url, station_name):
    doc = fetch_text_document(homepage_url)
    text = normalize_token(doc.get('text') or '')
    title = doc.get('title') or ''
    if not text:
        return None
    positive_patterns = (
        'schedule', 'resident', 'residents', 'show', 'shows', 'archive', 'archives',
        'presenter', 'presenters', 'host', 'hosts', 'community', 'collective',
        'selector', 'selectors', 'curated', 'programmes', 'programs', 'mix',
        'mixes', 'session', 'sessions', 'broadcast', 'broadcasts', 'listen back',
        'freeform', 'eclectic', 'underground', 'experimental'
    )
    negative_patterns = (
        'top 40', 'hits', 'christmas', 'xmas', 'easy listening', 'oldies',
        'breaking news', 'sports radio', 'country hits'
    )
    matches = [pattern for pattern in positive_patterns if pattern in text]
    negatives = [pattern for pattern in negative_patterns if pattern in text]
    score = len(matches) * 12 - len(negatives) * 20
    if station_name and normalize_token(base_display_name(station_name)) in normalize_token(title + ' ' + doc.get('text', '')[:2000]):
        score += 15
    if score < 20:
        return None
    excerpt_source = doc.get('text') or ''
    excerpt = excerpt_source[:240]
    return {
        'source_kind': 'homepage',
        'source_name': 'station_homepage',
        'source_url': doc.get('url') or homepage_url,
        'title': title[:160],
        'excerpt': excerpt,
        'confidence': min(0.95, 0.45 + score / 100.0),
        'score': score,
        'matches': matches[:8],
    }


def upsert_station(conn, station, source_name):
    now = utc_now()
    is_new = 1 if source_name == 'findings' else 0
    new_bitrate = int_value(station.get('bitrate'), 0)
    new_codec = (station.get('codec') or '').strip()
    existing = conn.execute(
        '''
        SELECT * FROM stations
        WHERE normalized_url = ? OR normalized_name = ?
        LIMIT 1
        ''',
        (station['normalized_url'], station['normalized_name'])
    ).fetchone()

    if existing is None:
        conn.execute(
            '''
            INSERT INTO stations (
                name, stream_url, normalized_name, normalized_url, country, genre,
                source, first_seen_at, last_seen_at, updated_at, last_status,
                active, needs_review, is_new, is_interesting, bitrate, codec, confidence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 1, 0, ?, ?, ?, ?, ?)
            ''',
            (
                station['name'],
                station['stream_url'],
                station['normalized_name'],
                station['normalized_url'],
                station['country'],
                station['genre'],
                source_name,
                now,
                now,
                now,
                is_new,
                int_value(station.get('is_interesting'), 0),
                new_bitrate,
                new_codec,
                0.55,
            )
        )
        return

    new_name = station['name'] if len(station['name']) >= len(existing['name']) else existing['name']
    existing_bitrate = int_value(existing['bitrate'], 0)
    existing_codec = existing['codec'] or ''
    existing_quality = stream_quality_key(existing_bitrate, existing_codec, existing['stream_url'])
    incoming_quality = stream_quality_key(new_bitrate, new_codec, station['stream_url'])
    existing_normalized_url = normalize_url(existing['stream_url'])
    incoming_url_changed = station['normalized_url'] != existing_normalized_url
    is_static_url = int_value(existing['is_static_url'], 0) == 1
    static_conflict = is_static_url and incoming_url_changed
    if static_conflict:
        queue_url_change_review(conn, existing, station, source_name)
    prefer_incoming_stream = not static_conflict and (station['normalized_url'] == existing['normalized_url'] or incoming_quality > existing_quality)
    new_url = station['stream_url'] if prefer_incoming_stream else existing['stream_url']
    new_country = station['country'] or existing['country']
    new_genre = station['genre'] or existing['genre']
    status = existing['last_status']
    needs_review = existing['needs_review']
    review_note = existing['last_error'] or ''
    if static_conflict:
        needs_review = 1
        review_note = 'static-url candidate: ' + station['stream_url']
    elif normalize_url(existing['stream_url']) != normalize_url(new_url):
        status = 'pending'
        needs_review = 0
        review_note = ''
    merged_is_interesting = 1 if int_value(existing['is_interesting'], 0) or int_value(station.get('is_interesting'), 0) else 0
    merged_bitrate = max(existing_bitrate, new_bitrate) if normalize_url(new_url) == station['normalized_url'] else existing_bitrate
    if prefer_incoming_stream and new_bitrate:
        merged_bitrate = new_bitrate
    merged_codec = new_codec if prefer_incoming_stream and new_codec else existing_codec
    if static_conflict:
        merged_source = existing['source']
    else:
        merged_source = existing['source'] if merged_is_interesting and source_name == 'findings' and existing['source'] == 'seed' else source_name

    conn.execute(
        '''
        UPDATE stations
        SET name = ?, stream_url = ?, normalized_name = ?, normalized_url = ?,
            country = ?, genre = ?, source = ?, last_seen_at = ?, updated_at = ?,
            last_status = ?, needs_review = ?, last_error = ?, is_new = ?, is_interesting = ?,
            bitrate = ?, codec = ?, active = 1
        WHERE id = ?
        ''',
        (
            new_name,
            new_url,
            normalize_name(new_name),
            normalize_url(new_url),
            new_country,
            new_genre,
            merged_source,
            now,
            now,
            status,
            needs_review,
            review_note,
            is_new,
            merged_is_interesting,
            merged_bitrate,
            merged_codec,
            existing['id'],
        )
    )


def queue_url_change_review(conn, existing_row, station, source_name):
    station_id = int(existing_row['id'])
    old_url = existing_row['stream_url']
    new_url = station['stream_url']
    normalized_new_url = normalize_url(new_url)
    if not normalized_new_url or normalized_new_url == normalize_url(old_url):
        return

    now = utc_now()
    existing_review = conn.execute(
        '''
        SELECT id FROM url_change_reviews
        WHERE station_id = ? AND normalized_new_url = ? AND status = 'pending'
        LIMIT 1
        ''',
        (station_id, normalized_new_url)
    ).fetchone()

    if existing_review is None:
        conn.execute(
            '''
            INSERT INTO url_change_reviews (
                station_id, station_name, old_stream_url, new_stream_url, normalized_new_url,
                candidate_name, source, first_seen_at, last_seen_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            ''',
            (
                station_id,
                existing_row['name'] or station['name'],
                old_url,
                new_url,
                normalized_new_url,
                station['name'] or '',
                source_name,
                now,
                now,
            )
        )
    else:
        conn.execute(
            '''
            UPDATE url_change_reviews
            SET station_name = ?, old_stream_url = ?, new_stream_url = ?,
                candidate_name = ?, source = ?, last_seen_at = ?
            WHERE id = ?
            ''',
            (
                existing_row['name'] or station['name'],
                old_url,
                new_url,
                station['name'] or '',
                source_name,
                now,
                existing_review['id'],
            )
        )


def sync_database(conn, seed_path, findings_path):
    imported = 0
    seed_items = parse_station_items(seed_path)
    findings_items = parse_station_items(findings_path)
    reconcile_findings_source(conn, findings_items)
    for source_name, items in (('seed', seed_items), ('findings', findings_items)):
        for station in items:
            upsert_station(conn, station, source_name)
            imported += 1
    conn.commit()
    return stats(conn, imported_count=imported)


def reconcile_findings_source(conn, findings_items):
    current_names = {item['normalized_name'] for item in findings_items}
    current_urls = {item['normalized_url'] for item in findings_items}
    rows = conn.execute(
        '''
        SELECT id, normalized_name, normalized_url, is_interesting
        FROM stations
        WHERE source = 'findings' AND is_new = 1
        '''
    ).fetchall()
    for row in rows:
        if row['normalized_name'] in current_names or row['normalized_url'] in current_urls:
            continue
        if int_value(row['is_interesting'], 0):
            conn.execute(
                '''
                UPDATE stations
                SET is_new = 0, source = 'interesting', updated_at = ?
                WHERE id = ?
                ''',
                (utc_now(), row['id'])
            )
            continue
        conn.execute('DELETE FROM stations WHERE id = ?', (row['id'],))


def looks_like_stream(url_value, content_type):
    content_type = (content_type or '').split(';', 1)[0].strip().lower()
    parsed = urllib.parse.urlparse(url_value or '')
    lowered_path = (parsed.path or '').lower()
    if content_type.startswith('audio/'):
        return True
    if content_type in PLAYLIST_CONTENT_TYPES:
        return True
    return lowered_path.endswith(STREAM_EXTENSIONS)


def codec_from_content_type(content_type):
    lowered = (content_type or '').lower()
    if 'aac' in lowered:
        return 'AAC'
    if 'opus' in lowered:
        return 'Opus'
    if 'ogg' in lowered or 'vorbis' in lowered:
        return 'Ogg'
    if 'mpeg' in lowered or 'mp3' in lowered:
        return 'MP3'
    if 'flac' in lowered:
        return 'FLAC'
    return ''


def verify_url(url_value):
    request = urllib.request.Request(
        url_value,
        headers={'User-Agent': 'CuratedRadio/0.1', 'Icy-MetaData': '1'}
    )
    with urllib.request.urlopen(request, timeout=12) as response:
        content_type = response.headers.get('Content-Type', '')
        status_code = getattr(response, 'status', 200)
        final_url = response.geturl()
        response.read(1024)
        return {
            'status_code': status_code,
            'content_type': content_type,
            'resolved_url': final_url,
        }


def refresh_database(conn):
    rows = conn.execute(
        '''
        SELECT * FROM stations
        WHERE active = 1 OR last_status IN ('pending', 'review')
        ORDER BY id
        '''
    ).fetchall()
    now = utc_now()
    for row in rows:
        try:
            result = verify_url(row['stream_url'])
            status_code = result['status_code']
            content_type = result['content_type']
            resolved_url = result['resolved_url']
            if 200 <= status_code < 400 and looks_like_stream(resolved_url or row['stream_url'], content_type):
                codec_value = row['codec'] or codec_from_content_type(content_type)
                conn.execute(
                    '''
                    UPDATE stations
                    SET last_check_at = ?, last_success_at = ?, last_status = 'ok',
                        last_http_status = ?, last_error = '', last_content_type = ?,
                        last_resolved_url = ?, active = 1, needs_review = 0,
                        failure_count = 0, codec = ?, confidence = 0.95
                    WHERE id = ?
                    ''',
                    (now, now, status_code, content_type, resolved_url, codec_value, row['id'])
                )
            else:
                conn.execute(
                    '''
                    UPDATE stations
                    SET last_check_at = ?, last_status = 'review', last_http_status = ?,
                        last_error = '', last_content_type = ?, last_resolved_url = ?,
                        active = 1, needs_review = 1, confidence = 0.55
                    WHERE id = ?
                    ''',
                    (now, status_code, content_type, resolved_url, row['id'])
                )
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ValueError) as exc:
            failure_count = int(row['failure_count'] or 0) + 1
            active = 0 if failure_count >= 3 else 1
            conn.execute(
                '''
                UPDATE stations
                SET last_check_at = ?, last_status = ?, last_http_status = ?,
                    last_error = ?, active = ?, needs_review = ?,
                    failure_count = ?, confidence = ?
                WHERE id = ?
                ''',
                (
                    now,
                    'inactive' if active == 0 else 'review',
                    getattr(exc, 'code', None),
                    str(exc),
                    active,
                    1,
                    failure_count,
                    0.2 if active == 0 else 0.4,
                    row['id'],
                )
            )
    conn.commit()
    return stats(conn)


def discovery_score(entry, matched_tag_count):
    votes = int(entry.get('votes') or 0)
    clickcount = int(entry.get('clickcount') or 0)
    bitrate = int(entry.get('bitrate') or 0)
    favicon_bonus = 10 if entry.get('favicon') else 0
    ssl_bonus = 10 if str(entry.get('url_resolved') or entry.get('url') or '').startswith('https://') else 0
    homepage_bonus = 25 if entry.get('homepage') else 0
    popularity_penalty = 0
    if votes > 500:
        popularity_penalty += min(votes - 500, 1500)
    if clickcount > 5000:
        popularity_penalty += min(clickcount - 5000, 4000)
    return matched_tag_count * 200 + votes * 3 + min(clickcount, 1500) + min(bitrate, 320) + favicon_bonus + ssl_bonus + homepage_bonus - popularity_penalty


def build_discovery_urls(api_base, limit, tag_seeds):
    request_limit = max(25, min(int(limit), 100))
    base = api_base.rstrip('/')
    urls = []
    for tag in tag_seeds:
        encoded_tag = urllib.parse.quote(tag)
        urls.append('{}/json/stations/bytag/{}?hidebroken=true&order=votes&reverse=true&limit={}'.format(base, encoded_tag, request_limit))
    return urls


def discover_stations(conn, output_path, api_base, limit, profile_prompt, editorial_sources_csv, tag_seeds_csv, blocked_tags_csv, blocked_names_csv):
    known_rows = query_rows(conn, 'SELECT normalized_name, normalized_url FROM stations')
    known_names = {row['normalized_name'] for row in known_rows}
    known_urls = {row['normalized_url'] for row in known_rows}
    profile = parse_profile_prompt(profile_prompt)
    tag_seeds = csv_tokens(tag_seeds_csv)
    blocked_tags = csv_tokens(blocked_tags_csv)
    blocked_names = csv_tokens(blocked_names_csv)
    prompt_prefers = profile['prefer'] + profile['must']
    prompt_avoids = profile['avoid']
    source_directives = parse_source_directives(profile.get('sources', []), editorial_sources_csv)
    editorial_docs = extract_editorial_documents(source_directives['source_urls'])
    tag_seeds = list(dict.fromkeys(tag_seeds + prompt_prefers))
    blocked_tags = list(dict.fromkeys(blocked_tags + prompt_avoids))
    blocked_names = list(dict.fromkeys(blocked_names + prompt_avoids))
    normalized_tag_seeds = [normalize_token(tag) for tag in tag_seeds]
    normalized_prompt_prefers = [normalize_token(item) for item in prompt_prefers]

    candidates_by_name = {}

    for url_value in build_discovery_urls(api_base, limit, tag_seeds):
        try:
            payload = http_get_json(url_value)
        except Exception:
            continue
        if not isinstance(payload, list):
            continue
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            if str(entry.get('lastcheckok', '1')).lower() in ('0', 'false', 'no'):
                continue
            tags = entry_tags(entry)
            if not tags:
                continue
            matched_tags = [tag for tag in tags if any(seed in tag for seed in normalized_tag_seeds)]
            if not matched_tags:
                continue
            has_curation_signal = any(any(signal in tag for signal in CURATION_SIGNAL_PATTERNS) for tag in tags)
            if not has_curation_signal:
                if len(matched_tags) < 2:
                    continue
                if is_generic_format_name(entry.get('name') or ''):
                    continue
            if any(matches_pattern_list(tag, blocked_tags) for tag in tags):
                continue
            if matches_pattern_list(entry.get('name') or '', blocked_names):
                continue
            prompt_preference_matches = [tag for tag in tags if any(pref in tag for pref in normalized_prompt_prefers if pref)]
            name = format_discovered_name(entry)
            stream_url = (entry.get('url_resolved') or entry.get('url') or '').strip()
            if not name or not stream_url:
                continue
            normalized_name = normalize_name(name)
            normalized_url = normalize_url(stream_url)
            if not normalized_name or not normalized_url:
                continue
            if normalized_name in known_names or normalized_url in known_urls:
                continue
            if not stream_url.startswith('http://') and not stream_url.startswith('https://'):
                continue
            bitrate_value = int_value(entry.get('bitrate'), 0)
            codec_value = (entry.get('codec') or '').strip()
            candidate_key = normalized_url or normalized_name
            rb_confidence = min(0.9, 0.35 + len(matched_tags) * 0.08 + (0.05 if has_curation_signal else 0))
            upsert_evidence(
                conn,
                candidate_key,
                name,
                stream_url,
                'radio_browser',
                'radio_browser',
                entry.get('homepage') or '',
                entry.get('name') or name,
                'tags=' + ','.join(tags[:6]),
                rb_confidence,
                {
                    'matched_tags': matched_tags,
                    'prompt_preference_matches': prompt_preference_matches,
                    'votes': entry.get('votes'),
                    'clickcount': entry.get('clickcount'),
                    'homepage': entry.get('homepage'),
                    'country': entry.get('country'),
                    'bitrate': bitrate_value,
                    'codec': codec_value,
                }
            )
            candidate = {
                '_candidate_key': candidate_key,
                '_normalized_name': normalized_name,
                '_normalized_url': normalized_url,
                'service': 'webradio',
                'title': '',
                'name': name,
                'uri': stream_url,
                'bitrate': bitrate_value,
                'codec': codec_value,
                '_homepage': (entry.get('homepage') or '').strip(),
                '_matched_tags': matched_tags,
                '_prompt_preference_matches': prompt_preference_matches,
                '_has_curation_signal': has_curation_signal,
                '_score': discovery_score(entry, len(matched_tags)) + len(prompt_preference_matches) * 120 + bitrate_value * 6,
            }
            existing_candidate = candidates_by_name.get(normalized_name)
            if existing_candidate is None or stream_quality_key(candidate['bitrate'], candidate['codec'], candidate['uri']) > stream_quality_key(existing_candidate['bitrate'], existing_candidate['codec'], existing_candidate['uri']):
                candidates_by_name[normalized_name] = candidate

    candidates = list(candidates_by_name.values())
    candidates.sort(key=lambda item: (-item['_score'], -int_value(item.get('bitrate'), 0), item['name'].lower()))
    selected = []
    max_items = max(1, int(limit))
    for item in candidates:
        homepage_ev = None
        editorial_ev = None
        if item.get('_homepage'):
            try:
                homepage_ev = homepage_curation_evidence(item['_homepage'], item['name'])
            except Exception:
                homepage_ev = None
        if editorial_docs:
            try:
                editorial_ev = editorial_mention_evidence(editorial_docs, item['name'], profile)
            except Exception:
                editorial_ev = None
        evidence_sources = {'radio_browser'}
        total_confidence = 0.45
        if homepage_ev is not None:
            evidence_sources.add('homepage')
            total_confidence += homepage_ev['confidence']
            upsert_evidence(
                conn,
                item['_candidate_key'],
                item['name'],
                item['uri'],
                homepage_ev['source_kind'],
                homepage_ev['source_name'],
                homepage_ev['source_url'],
                homepage_ev['title'],
                homepage_ev['excerpt'],
                homepage_ev['confidence'],
                {
                    'score': homepage_ev['score'],
                    'matches': homepage_ev['matches'],
                }
            )
        if editorial_ev is not None:
            evidence_sources.add('editorial')
            total_confidence += editorial_ev['confidence']
            upsert_evidence(
                conn,
                item['_candidate_key'],
                item['name'],
                item['uri'],
                editorial_ev['source_kind'],
                editorial_ev['source_name'],
                editorial_ev['source_url'],
                editorial_ev['title'],
                editorial_ev['excerpt'],
                editorial_ev['confidence'],
                {
                    'score': editorial_ev['score'],
                    'matches': editorial_ev['matches'],
                    'document_url': editorial_ev['document_url'],
                    'source_page_url': editorial_ev['source_page_url'],
                }
            )
        elif not item.get('_has_curation_signal'):
            continue

        if profile['must'] and not item.get('_prompt_preference_matches') and homepage_ev is None and editorial_ev is None:
            continue

        if len(evidence_sources) < 2:
            continue

        item['_score'] += int(total_confidence * 100)
        selected.append(item)
        if len(selected) >= max_items:
            break

    for item in selected:
        item.pop('_score', None)
        item.pop('_candidate_key', None)
        item.pop('_homepage', None)
        item.pop('_matched_tags', None)
        item.pop('_prompt_preference_matches', None)
        item.pop('_has_curation_signal', None)

    directory = os.path.dirname(output_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as handle:
        json.dump(selected, handle, ensure_ascii=False, indent=2)
    conn.commit()

    return {
        'candidate_count': len(candidates),
        'written_count': len(selected),
        'editorial_source_count': len(source_directives['source_urls']),
        'editorial_document_count': len(editorial_docs),
        'output_path': output_path,
    }


def query_rows(conn, sql, params=()):
    rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def export_section(conn, section, limit):
    limit = max(1, int(limit))
    if section == 'interesting':
        return query_rows(
            conn,
            '''
            SELECT * FROM stations
            WHERE is_interesting = 1
            ORDER BY updated_at DESC, name COLLATE NOCASE
            LIMIT ?
            ''',
            (limit,)
        )
    if section == 'verified':
        return query_rows(
            conn,
            '''
            SELECT * FROM stations
            WHERE last_status = 'ok' AND active = 1
            ORDER BY name COLLATE NOCASE
            LIMIT ?
            ''',
            (limit,)
        )
    if section == 'findings':
        return query_rows(
            conn,
            '''
            SELECT * FROM stations
            WHERE is_new = 1 AND active = 1
            ORDER BY updated_at DESC, name COLLATE NOCASE
            LIMIT ?
            ''',
            (limit,)
        )
    if section == 'review':
        return query_rows(
            conn,
            '''
            SELECT * FROM stations
            WHERE needs_review = 1 AND active = 1
            ORDER BY updated_at DESC, name COLLATE NOCASE
            LIMIT ?
            ''',
            (limit,)
        )
    if section == 'inactive':
        return query_rows(
            conn,
            '''
            SELECT * FROM stations
            WHERE active = 0 OR last_status = 'inactive'
            ORDER BY updated_at DESC, name COLLATE NOCASE
            LIMIT ?
            ''',
            (limit,)
        )
    raise ValueError('Unknown section: ' + section)


def tokenize_genres(value):
    raw = (value or '').strip()
    if not raw:
        return []
    parts = re.split(r'[/,;|]+', raw)
    return [part.strip() for part in parts if part.strip()]


def export_groups(conn, dimension):
    rows = query_rows(conn, 'SELECT country, genre FROM stations WHERE active = 1')
    counts = {}
    if dimension == 'country':
        for row in rows:
            key = row['country'] or 'Unknown'
            counts[key] = counts.get(key, 0) + 1
    elif dimension == 'genre':
        for row in rows:
            genres = tokenize_genres(row['genre']) or ['Unknown']
            for key in genres:
                counts[key] = counts.get(key, 0) + 1
    else:
        raise ValueError('Unknown dimension: ' + dimension)
    grouped = [{'key': key, 'count': value} for key, value in counts.items()]
    grouped.sort(key=lambda item: (-item['count'], item['key'].lower()))
    return grouped


def export_group_items(conn, dimension, key, limit):
    limit = max(1, int(limit))
    rows = query_rows(conn, 'SELECT * FROM stations WHERE active = 1 ORDER BY name COLLATE NOCASE')
    matched = []
    for row in rows:
        if dimension == 'country':
            row_key = row['country'] or 'Unknown'
            if row_key == key:
                matched.append(row)
        elif dimension == 'genre':
            genres = tokenize_genres(row['genre']) or ['Unknown']
            if key in genres:
                matched.append(row)
        else:
            raise ValueError('Unknown dimension: ' + dimension)
    return matched[:limit]


def lookup_station(conn, station_id):
    row = conn.execute('SELECT * FROM stations WHERE id = ?', (station_id,)).fetchone()
    if row is None:
        raise ValueError('Unknown station id')
    return dict(row)


def lookup_station_by_url(conn, url_value):
    normalized_url = normalize_url(url_value)
    row = conn.execute(
        '''
        SELECT * FROM stations
        WHERE normalized_url = ? OR normalized_url = ? OR last_resolved_url = ? OR last_resolved_url = ?
        ORDER BY is_interesting DESC, bitrate DESC, updated_at DESC
        LIMIT 1
        ''',
        (normalized_url, url_value.strip(), normalized_url, url_value.strip())
    ).fetchone()
    if row is None:
        raise ValueError('Unknown station url')
    return dict(row)


def set_station_interesting(conn, station_id, value):
    now = utc_now()
    conn.execute(
        '''
        UPDATE stations
        SET is_interesting = ?, updated_at = ?
        WHERE id = ?
        ''',
        (1 if value else 0, now, int(station_id))
    )
    conn.commit()
    return lookup_station(conn, int(station_id))


def set_station_static(conn, station_id, value):
    now = utc_now()
    conn.execute(
        '''
        UPDATE stations
        SET is_static_url = ?, updated_at = ?
        WHERE id = ?
        ''',
        (1 if value else 0, now, int(station_id))
    )
    conn.commit()
    return lookup_station(conn, int(station_id))


def set_station_interesting_by_url(conn, url_value, name='', country='', genre='', bitrate=0, codec='', value=1):
    try:
        row = lookup_station_by_url(conn, url_value)
        return set_station_interesting(conn, row['id'], value)
    except ValueError:
        if not value:
            return {
                'name': name or url_value,
                'stream_url': url_value,
                'is_interesting': 0,
            }

    normalized_url = normalize_url(url_value)
    normalized_name = normalize_name(name or url_value)
    now = utc_now()
    conn.execute(
        '''
        INSERT INTO stations (
            name, stream_url, normalized_name, normalized_url, country, genre,
            source, first_seen_at, last_seen_at, updated_at, last_status,
            active, needs_review, is_new, is_interesting, bitrate, codec, confidence
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 1, 0, 0, 1, ?, ?, ?)
        ''',
        (
            name or url_value,
            url_value,
            normalized_name,
            normalized_url,
            country or '',
            genre or '',
            'interesting',
            now,
            now,
            now,
            int_value(bitrate, 0),
            codec or '',
            0.6,
        )
    )
    conn.commit()
    return lookup_station_by_url(conn, url_value)


def set_station_static_by_url(conn, url_value, name='', country='', genre='', bitrate=0, codec='', value=1):
    try:
        row = lookup_station_by_url(conn, url_value)
        return set_station_static(conn, row['id'], value)
    except ValueError:
        if not value:
            return {
                'name': name or url_value,
                'stream_url': url_value,
                'is_static_url': 0,
            }

    normalized_url = normalize_url(url_value)
    normalized_name = normalize_name(name or url_value)
    now = utc_now()
    conn.execute(
        '''
        INSERT INTO stations (
            name, stream_url, normalized_name, normalized_url, country, genre,
            source, first_seen_at, last_seen_at, updated_at, last_status,
            active, needs_review, is_new, is_interesting, is_static_url, bitrate, codec, confidence
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 1, 0, 0, 0, ?, ?, ?, ?)
        ''',
        (
            name or url_value,
            url_value,
            normalized_name,
            normalized_url,
            country or '',
            genre or '',
            'manual',
            now,
            now,
            now,
            1 if value else 0,
            int_value(bitrate, 0),
            codec or '',
            0.6,
        )
    )
    conn.commit()
    return lookup_station_by_url(conn, url_value)


def search_stations(conn, term, limit):
    limit = max(1, int(limit))
    pattern = '%' + term.strip() + '%'
    return query_rows(
        conn,
        '''
        SELECT * FROM stations
        WHERE name LIKE ? OR country LIKE ? OR genre LIKE ?
        ORDER BY
            CASE WHEN is_interesting = 1 THEN 0 ELSE 1 END,
            CASE WHEN last_status = 'ok' THEN 0 ELSE 1 END,
            name COLLATE NOCASE
        LIMIT ?
        ''',
        (pattern, pattern, pattern, limit)
    )


def stats(conn, imported_count=0):
    def scalar(sql):
        return conn.execute(sql).fetchone()[0]

    return {
        'imported_count': imported_count,
        'total_stations': scalar('SELECT COUNT(*) FROM stations'),
        'interesting_count': scalar('SELECT COUNT(*) FROM stations WHERE is_interesting = 1'),
        'verified_count': scalar("SELECT COUNT(*) FROM stations WHERE last_status = 'ok' AND active = 1"),
        'findings_count': scalar('SELECT COUNT(*) FROM stations WHERE is_new = 1 AND active = 1'),
        'review_count': scalar('SELECT COUNT(*) FROM stations WHERE needs_review = 1 AND active = 1'),
        'inactive_count': scalar("SELECT COUNT(*) FROM stations WHERE active = 0 OR last_status = 'inactive'"),
        'country_count': len(export_groups(conn, 'country')),
        'genre_count': len(export_groups(conn, 'genre')),
    }


def build_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command', required=True)

    sync_parser = subparsers.add_parser('sync')
    sync_parser.add_argument('--db', required=True)
    sync_parser.add_argument('--seed', required=True)
    sync_parser.add_argument('--findings', default='')

    refresh_parser = subparsers.add_parser('refresh')
    refresh_parser.add_argument('--db', required=True)

    discover_parser = subparsers.add_parser('discover')
    discover_parser.add_argument('--db', required=True)
    discover_parser.add_argument('--output', required=True)
    discover_parser.add_argument('--api-base', required=True)
    discover_parser.add_argument('--limit', default='80')
    discover_parser.add_argument('--profile-prompt', default='')
    discover_parser.add_argument('--editorial-sources', default='')
    discover_parser.add_argument('--tags', required=True)
    discover_parser.add_argument('--blocked-tags', default='')
    discover_parser.add_argument('--blocked-names', default='')

    stats_parser = subparsers.add_parser('stats')
    stats_parser.add_argument('--db', required=True)

    section_parser = subparsers.add_parser('export-section')
    section_parser.add_argument('section')
    section_parser.add_argument('--db', required=True)
    section_parser.add_argument('--limit', default='250')

    groups_parser = subparsers.add_parser('export-groups')
    groups_parser.add_argument('dimension')
    groups_parser.add_argument('--db', required=True)

    group_items_parser = subparsers.add_parser('export-group-items')
    group_items_parser.add_argument('dimension')
    group_items_parser.add_argument('key')
    group_items_parser.add_argument('--db', required=True)
    group_items_parser.add_argument('--limit', default='250')

    lookup_parser = subparsers.add_parser('lookup')
    lookup_parser.add_argument('station_id')
    lookup_parser.add_argument('--db', required=True)

    lookup_url_parser = subparsers.add_parser('lookup-by-url')
    lookup_url_parser.add_argument('--db', required=True)
    lookup_url_parser.add_argument('--url', required=True)

    interesting_parser = subparsers.add_parser('mark-interesting')
    interesting_parser.add_argument('--db', required=True)
    interesting_parser.add_argument('--station-id', required=True)
    interesting_parser.add_argument('--value', default='1')

    static_parser = subparsers.add_parser('mark-static')
    static_parser.add_argument('--db', required=True)
    static_parser.add_argument('--station-id', required=True)
    static_parser.add_argument('--value', default='1')

    interesting_url_parser = subparsers.add_parser('mark-interesting-by-url')
    interesting_url_parser.add_argument('--db', required=True)
    interesting_url_parser.add_argument('--url', required=True)
    interesting_url_parser.add_argument('--name', default='')
    interesting_url_parser.add_argument('--country', default='')
    interesting_url_parser.add_argument('--genre', default='')
    interesting_url_parser.add_argument('--bitrate', default='0')
    interesting_url_parser.add_argument('--codec', default='')
    interesting_url_parser.add_argument('--value', default='1')

    static_url_parser = subparsers.add_parser('mark-static-by-url')
    static_url_parser.add_argument('--db', required=True)
    static_url_parser.add_argument('--url', required=True)
    static_url_parser.add_argument('--name', default='')
    static_url_parser.add_argument('--country', default='')
    static_url_parser.add_argument('--genre', default='')
    static_url_parser.add_argument('--bitrate', default='0')
    static_url_parser.add_argument('--codec', default='')
    static_url_parser.add_argument('--value', default='1')

    search_parser = subparsers.add_parser('search')
    search_parser.add_argument('term')
    search_parser.add_argument('--db', required=True)
    search_parser.add_argument('--limit', default='250')

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    conn = connect_db(args.db)
    try:
        if args.command == 'sync':
            payload = sync_database(conn, args.seed, args.findings)
        elif args.command == 'refresh':
            payload = refresh_database(conn)
        elif args.command == 'discover':
            payload = discover_stations(
                conn,
                args.output,
                args.api_base,
                args.limit,
                args.profile_prompt,
                args.editorial_sources,
                args.tags,
                args.blocked_tags,
                args.blocked_names
            )
        elif args.command == 'stats':
            payload = stats(conn)
        elif args.command == 'export-section':
            payload = export_section(conn, args.section, args.limit)
        elif args.command == 'export-groups':
            payload = export_groups(conn, args.dimension)
        elif args.command == 'export-group-items':
            payload = export_group_items(conn, args.dimension, args.key, args.limit)
        elif args.command == 'lookup':
            payload = lookup_station(conn, int(args.station_id))
        elif args.command == 'lookup-by-url':
            payload = lookup_station_by_url(conn, args.url)
        elif args.command == 'mark-interesting':
            payload = set_station_interesting(conn, int(args.station_id), int_value(args.value, 1))
        elif args.command == 'mark-static':
            payload = set_station_static(conn, int(args.station_id), int_value(args.value, 1))
        elif args.command == 'mark-interesting-by-url':
            payload = set_station_interesting_by_url(
                conn,
                args.url,
                name=args.name,
                country=args.country,
                genre=args.genre,
                bitrate=int_value(args.bitrate, 0),
                codec=args.codec,
                value=int_value(args.value, 1)
            )
        elif args.command == 'mark-static-by-url':
            payload = set_station_static_by_url(
                conn,
                args.url,
                name=args.name,
                country=args.country,
                genre=args.genre,
                bitrate=int_value(args.bitrate, 0),
                codec=args.codec,
                value=int_value(args.value, 1)
            )
        elif args.command == 'search':
            payload = search_stations(conn, args.term, args.limit)
        else:
            raise ValueError('Unsupported command')
        json.dump(payload, sys.stdout, ensure_ascii=False)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
