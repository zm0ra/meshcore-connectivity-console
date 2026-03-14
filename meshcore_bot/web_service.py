from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from .database import BotDatabase


INDEX_HTML = """<!doctype html>
<html lang=\"pl\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>MeshCore Bot</title>
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\">
  <style>
    :root {
      color-scheme: light;
      --bg: #edf0ec;
      --panel: rgba(255, 255, 255, 0.82);
      --panel-strong: rgba(255, 255, 255, 0.94);
      --ink: #15212a;
      --muted: #6a7883;
      --line: rgba(21, 33, 42, 0.1);
      --green: #2e8b57;
      --blue: #2c71d1;
      --red: #c64a3d;
      --yellow: #cfaa38;
      --orange: #db7d31;
      --unknown: #98a4ad;
      --shadow: 0 18px 42px rgba(21, 33, 42, 0.12);
    }
    html, body {
      margin: 0;
      height: 100%;
      background: var(--bg);
      color: var(--ink);
      font-family: Georgia, 'Iowan Old Style', serif;
    }
    #app {
      position: relative;
      width: 100%;
      height: 100%;
      overflow: hidden;
    }
    #map {
      position: absolute;
      inset: 0;
      background: #e8eeeb;
    }
    .overlay {
      position: absolute;
      z-index: 1000;
      background: var(--panel);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
      backdrop-filter: blur(14px);
    }
    #sidebar {
      top: 16px;
      right: 16px;
      bottom: 16px;
      width: min(372px, calc(100vw - 32px));
      border-radius: 20px;
      display: grid;
      grid-template-rows: auto 1fr;
      overflow: hidden;
    }
    #map-legend {
      left: 16px;
      bottom: 16px;
      border-radius: 14px;
      padding: 10px 12px;
      max-width: 250px;
      font-size: 0.74rem;
      color: var(--muted);
    }
    .summary-strip {
      padding: 12px;
      border-bottom: 1px solid var(--line);
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 6px;
    }
    .summary-card {
      padding: 8px 7px;
      border-radius: 10px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.52);
      text-align: center;
    }
    .summary-card strong {
      display: block;
      font-size: 0.93rem;
      line-height: 1.1;
    }
    .summary-card span {
      display: block;
      margin-top: 2px;
      color: var(--muted);
      font-size: 0.68rem;
      line-height: 1.15;
    }
    .list-shell {
      overflow: auto;
      padding: 10px 10px 14px;
    }
    .list-toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin: 2px 2px 10px;
      padding: 9px 10px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.48);
    }
    .list-toolbar label {
      color: var(--muted);
      font-size: 0.7rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .sort-select {
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.72);
      color: var(--ink);
      padding: 5px 10px;
      font: inherit;
      font-size: 0.72rem;
    }
    .section-heading {
      margin: 10px 2px 6px;
      color: var(--muted);
      font-size: 0.7rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .node-list {
      display: grid;
      gap: 6px;
    }
    .node-row {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.54);
      overflow: hidden;
    }
    .node-row.active {
      background: var(--panel-strong);
      border-color: rgba(44, 113, 209, 0.24);
    }
    .node-row-button {
      width: 100%;
      border: 0;
      background: transparent;
      color: inherit;
      padding: 8px 9px;
      display: grid;
      grid-template-columns: auto 1fr auto;
      gap: 8px;
      align-items: center;
      text-align: left;
      cursor: pointer;
      font: inherit;
    }
    .node-row-button:hover {
      background: rgba(255, 255, 255, 0.28);
    }
    .status-dot {
      width: 8px;
      height: 8px;
      border-radius: 999px;
      box-shadow: 0 0 0 2px rgba(255, 255, 255, 0.96);
      flex: 0 0 auto;
    }
    .node-main {
      min-width: 0;
    }
    .node-name {
      display: block;
      font-size: 0.84rem;
      line-height: 1.2;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .node-age {
      display: block;
      margin-top: 2px;
      color: var(--muted);
      font-size: 0.72rem;
      line-height: 1.1;
    }
    .node-state-tag {
      color: var(--muted);
      font-size: 0.68rem;
      white-space: nowrap;
    }
    .node-expand {
      padding: 0 9px 10px;
      display: grid;
      gap: 10px;
    }
    .detail-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 6px;
    }
    .detail-cell {
      padding: 7px 8px;
      border-radius: 10px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.42);
      font-size: 0.73rem;
      color: var(--muted);
      line-height: 1.22;
    }
    .detail-cell strong {
      display: block;
      color: var(--ink);
      font-size: 0.76rem;
      margin-bottom: 2px;
    }
    .expand-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      padding-top: 2px;
    }
    .expand-head strong {
      font-size: 0.82rem;
    }
    .ghost-button {
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.5);
      border-radius: 999px;
      color: var(--muted);
      padding: 3px 8px;
      cursor: pointer;
      font: inherit;
      font-size: 0.7rem;
    }
    .neighbor-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.71rem;
    }
    .neighbor-table th,
    .neighbor-table td {
      padding: 5px 4px;
      border-bottom: 1px solid rgba(21, 33, 42, 0.08);
      text-align: left;
      vertical-align: top;
    }
    .neighbor-table th {
      color: var(--muted);
      font-weight: 600;
      font-size: 0.68rem;
    }
    .neighbor-table button {
      border: 0;
      background: transparent;
      padding: 0;
      color: inherit;
      text-align: left;
      cursor: pointer;
      font: inherit;
      line-height: 1.2;
    }
    .neighbor-table tr.active {
      background: rgba(44, 113, 209, 0.08);
    }
    .chart-shell {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.46);
      padding: 8px;
    }
    .chart-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 6px;
    }
    .chart-title {
      font-size: 0.76rem;
      line-height: 1.2;
    }
    .chart-title strong {
      display: block;
      font-size: 0.8rem;
    }
    .chart-meta {
      color: var(--muted);
      font-size: 0.68rem;
      white-space: nowrap;
    }
    #signal-chart {
      width: 100%;
      height: 152px;
      display: block;
    }
    .empty-note {
      color: var(--muted);
      font-size: 0.74rem;
      line-height: 1.3;
      padding: 4px 0 2px;
    }
    .legend-group + .legend-group {
      margin-top: 9px;
    }
    .legend-title {
      display: block;
      margin-bottom: 4px;
      color: var(--ink);
      font-size: 0.73rem;
    }
    .legend-row {
      display: flex;
      align-items: center;
      gap: 6px;
      margin-top: 4px;
    }
    .legend-node,
    .legend-line {
      flex: 0 0 auto;
    }
    .legend-node {
      width: 8px;
      height: 8px;
      border-radius: 999px;
      box-shadow: 0 0 0 2px rgba(255, 255, 255, 0.96);
    }
    .legend-line {
      width: 18px;
      height: 0;
      border-top-width: 2px;
      border-top-style: solid;
    }
    .leaflet-control-attribution {
      opacity: 0.7;
    }
    .node-label-icon,
    .link-label-icon {
      background: transparent;
      border: 0;
      transform: translate(-50%, -50%);
    }
    .node-label-chip {
      border: 1px solid rgba(21, 33, 42, 0.1);
      border-radius: 10px;
      background: rgba(255, 255, 255, 0.76);
      box-shadow: 0 8px 18px rgba(21, 33, 42, 0.08);
      color: var(--ink);
      padding: 5px 8px;
      white-space: nowrap;
      font-size: 0.72rem;
      line-height: 1.2;
      pointer-events: none;
    }
    .node-label-chip strong {
      font-size: 0.74rem;
      font-weight: 600;
    }
    .node-label-chip .label-meta {
      display: block;
      margin-top: 2px;
      color: var(--muted);
      font-size: 0.68rem;
    }
    .signal-label-chip {
      border: 1px solid rgba(21, 33, 42, 0.08);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.74);
      box-shadow: 0 8px 18px rgba(21, 33, 42, 0.06);
      color: var(--ink);
      padding: 4px 8px;
      font-family: 'SFMono-Regular', ui-monospace, monospace;
      font-size: 0.66rem;
      line-height: 1.2;
      text-align: center;
      white-space: nowrap;
      pointer-events: none;
    }
    .signal-label-chip strong,
    .signal-label-chip span {
      display: block;
    }
    @media (max-width: 860px) {
      #sidebar {
        left: 12px;
        right: 12px;
        top: auto;
        bottom: 12px;
        width: auto;
        max-height: 56vh;
      }
      #map-legend {
        left: 12px;
        top: 12px;
        bottom: auto;
        max-width: 190px;
      }
      .summary-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }
  </style>
</head>
<body>
  <div id=\"app\">
    <div id=\"map\"></div>
    <div id=\"map-legend\" class=\"overlay\">
      <div class=\"legend-group\">
        <span class=\"legend-title\">Repeatery</span>
        <div class=\"legend-row\"><span class=\"legend-node\" style=\"background:#2e8b57\"></span><span>dane dostępne</span></div>
        <div class=\"legend-row\"><span class=\"legend-node\" style=\"background:#2c71d1\"></span><span>znany / bez pobranych danych</span></div>
        <div class=\"legend-row\"><span class=\"legend-node\" style=\"background:#c64a3d\"></span><span>nieaktywny &gt; 24h</span></div>
      </div>
      <div class=\"legend-group\">
        <span class=\"legend-title\">Połączenia</span>
        <div class=\"legend-row\"><span class=\"legend-line\" style=\"border-top-color:#2e8b57\"></span><span>mocne</span></div>
        <div class=\"legend-row\"><span class=\"legend-line\" style=\"border-top-color:#cfaa38\"></span><span>średnie</span></div>
        <div class=\"legend-row\"><span class=\"legend-line\" style=\"border-top-color:#db7d31\"></span><span>słabe</span></div>
        <div class=\"legend-row\"><span class=\"legend-line\" style=\"border-top-color:#c64a3d\"></span><span>bardzo słabe</span></div>
      </div>
    </div>
    <aside id=\"sidebar\" class=\"overlay\">
      <section class=\"summary-strip\">
        <div id=\"summary\" class=\"summary-grid\"></div>
      </section>
      <section class=\"list-shell\">
        <div id=\"node-sections\"></div>
      </section>
    </aside>
  </div>
  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\"></script>
  <script>
    const ACTIVE_THRESHOLD_MS = 24 * 60 * 60 * 1000;
    const LOW_ZOOM_LABEL_THRESHOLD = 10;
    const HIGH_ZOOM_LABEL_THRESHOLD = 12;
    const MAX_COLLISION_LABELS = 18;
    const map = L.map('map', { zoomControl: true, preferCanvas: true }).setView([53.43, 14.55], 8);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
      subdomains: 'abcd',
      maxZoom: 20,
      attribution: '&copy; OpenStreetMap contributors &copy; CARTO'
    }).addTo(map);
    const markersLayer = L.layerGroup().addTo(map);
    const halosLayer = L.layerGroup().addTo(map);
    const linksLayer = L.layerGroup().addTo(map);
    const labelsLayer = L.layerGroup().addTo(map);
    const linkLabelsLayer = L.layerGroup().addTo(map);
    let latestState = null;
    let selectedSourceId = null;
    let selectedNeighborId = null;
    let hoveredNodeId = null;
    let nodeSortMode = 'last_advert';
    let hasFitBounds = false;

    function formatWhen(value) {
      if (!value) return 'brak';
      return new Date(value).toLocaleString();
    }

    function formatShortWhen(value) {
      if (!value) return 'brak';
      return new Date(value).toLocaleString([], {
        year: 'numeric',
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
      });
    }

    function timeAgo(value) {
      if (!value) return 'brak';
      const elapsed = Math.max(0, Date.now() - new Date(value).getTime());
      const seconds = Math.floor(elapsed / 1000);
      if (seconds < 60) return `${seconds}s temu`;
      if (seconds < 3600) return `${Math.floor(seconds / 60)} min temu`;
      if (seconds < 86400) return `${Math.floor(seconds / 3600)} h temu`;
      return `${Math.floor(seconds / 86400)} d temu`;
    }

    function humanizeSeconds(value) {
      if (typeof value !== 'number' || !Number.isFinite(value)) return 'brak';
      if (value < 60) return `${Math.round(value)} s`;
      if (value < 3600) {
        const minutes = Math.floor(value / 60);
        const seconds = Math.round(value % 60);
        return seconds ? `${minutes} min ${seconds} s` : `${minutes} min`;
      }
      if (value < 86400) {
        const hours = Math.floor(value / 3600);
        const minutes = Math.floor((value % 3600) / 60);
        return minutes ? `${hours} h ${minutes} min` : `${hours} h`;
      }
      const days = Math.floor(value / 86400);
      const hours = Math.floor((value % 86400) / 3600);
      return hours ? `${days} d ${hours} h` : `${days} d`;
    }

    function isInactive(node) {
      if (!node.last_advert_at) return true;
      return Date.now() - new Date(node.last_advert_at).getTime() > ACTIVE_THRESHOLD_MS;
    }

    function nodeState(node) {
      if (isInactive(node)) return 'inactive';
      return node.data_fetch_ok ? 'ok' : 'missing';
    }

    function nodeStateRank(node) {
      const state = nodeState(node);
      if (state === 'ok') return 0;
      if (state === 'missing') return 1;
      return 2;
    }

    function nodeColor(node) {
      const state = nodeState(node);
      if (state === 'ok') return '#2e8b57';
      if (state === 'missing') return '#2c71d1';
      return '#c64a3d';
    }

    function isFiniteCoordinate(latitude, longitude) {
      return Number.isFinite(latitude) && Number.isFinite(longitude) && !(Math.abs(latitude) < 0.01 && Math.abs(longitude) < 0.01);
    }

    function haversineKm(aLat, aLon, bLat, bLon) {
      const toRad = (value) => value * Math.PI / 180;
      const dLat = toRad(bLat - aLat);
      const dLon = toRad(bLon - aLon);
      const sa = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(dLon / 2) ** 2;
      return 6371 * 2 * Math.atan2(Math.sqrt(sa), Math.sqrt(1 - sa));
    }

    function median(values) {
      if (!values.length) return null;
      const sorted = values.slice().sort((left, right) => left - right);
      const middle = Math.floor(sorted.length / 2);
      return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
    }

    function deriveMapNodes(nodes) {
      const candidates = nodes.filter((node) => isFiniteCoordinate(node.latitude, node.longitude));
      if (candidates.length <= 2) return candidates;
      const centerLat = median(candidates.map((node) => node.latitude));
      const centerLon = median(candidates.map((node) => node.longitude));
      return candidates.filter((node) => haversineKm(centerLat, centerLon, node.latitude, node.longitude) <= 1200);
    }

    function relevantNodes(state) {
      return state.nodes || [];
    }

    function getSelectedNode(state) {
      return (state.nodes || []).find((node) => node.identity_hex === selectedSourceId) || null;
    }

    function getSelectedLinks(state) {
      if (!selectedSourceId) return [];
      return ((state.management?.map_links) || [])
        .filter((link) => link.source_identity_hex === selectedSourceId)
        .sort((left, right) => ((right.snr ?? -999) - (left.snr ?? -999)));
    }

    function getSelectedMapLinks(state) {
      return getSelectedLinks(state)
        .filter((link) => isFiniteCoordinate(link.source_latitude, link.source_longitude))
        .filter((link) => isFiniteCoordinate(link.target_latitude, link.target_longitude));
    }

    function selectedNeighborIds(state) {
      return new Set(getSelectedLinks(state).map((link) => link.target_identity_hex));
    }

    function nodeStateLabel(node) {
      const state = nodeState(node);
      if (state === 'ok') return 'dane';
      if (state === 'missing') return 'brak danych';
      return 'nieaktywny';
    }

    function compareIsoTimesDesc(leftValue, rightValue) {
      const leftTime = leftValue ? new Date(leftValue).getTime() : 0;
      const rightTime = rightValue ? new Date(rightValue).getTime() : 0;
      return rightTime - leftTime;
    }

    function compareNodeNames(left, right) {
      return (left.name || left.hash_prefix_hex).localeCompare(right.name || right.hash_prefix_hex);
    }

    function sortNodes(nodes) {
      return nodes.slice().sort((left, right) => {
        const rankDiff = nodeStateRank(left) - nodeStateRank(right);
        if (rankDiff !== 0) return rankDiff;

        if (nodeSortMode === 'alphabetical') {
          const nameDiff = compareNodeNames(left, right);
          if (nameDiff !== 0) return nameDiff;
          return compareIsoTimesDesc(left.last_advert_at, right.last_advert_at);
        }

        if (nodeSortMode === 'last_data') {
          const dataDiff = compareIsoTimesDesc(left.last_data_at, right.last_data_at);
          if (dataDiff !== 0) return dataDiff;
          const advertDiff = compareIsoTimesDesc(left.last_advert_at, right.last_advert_at);
          if (advertDiff !== 0) return advertDiff;
          return compareNodeNames(left, right);
        }

        const advertDiff = compareIsoTimesDesc(left.last_advert_at, right.last_advert_at);
        if (advertDiff !== 0) return advertDiff;
        const dataDiff = compareIsoTimesDesc(left.last_data_at, right.last_data_at);
        if (dataDiff !== 0) return dataDiff;
        return compareNodeNames(left, right);
      });
    }

    function fitInitialBounds(bounds) {
      if (!bounds.length) return;
      map.fitBounds(bounds, {
        paddingTopLeft: [18, 18],
        paddingBottomRight: [18, 18],
        maxZoom: 10,
      });
      hasFitBounds = true;
    }

    function fitSelectedRepeater(selectedNode, visibleNodes) {
      if (!selectedNode || !isFiniteCoordinate(selectedNode.latitude, selectedNode.longitude)) return;
      const bounds = [[selectedNode.latitude, selectedNode.longitude]];
      for (const node of visibleNodes) {
        if (node.identity_hex === selectedSourceId) continue;
        bounds.push([node.latitude, node.longitude]);
      }
      if (bounds.length > 1) {
        map.flyToBounds(bounds, {
          paddingTopLeft: [36, 36],
          paddingBottomRight: [360, 36],
          maxZoom: 12,
          duration: 0.6,
        });
        return;
      }
      map.flyTo([selectedNode.latitude, selectedNode.longitude], Math.max(map.getZoom(), 11), { duration: 0.5 });
    }

    function renderSummary(state) {
      const nodes = relevantNodes(state);
      const html = [
        { label: 'znane', value: nodes.length },
        { label: 'z danymi', value: nodes.filter((node) => !isInactive(node) && node.data_fetch_ok).length },
        { label: 'oczekujące', value: nodes.filter((node) => !isInactive(node) && !node.data_fetch_ok).length },
        { label: 'nieaktywne', value: nodes.filter((node) => isInactive(node)).length },
      ].map((item) => `<div class=\"summary-card\"><strong>${item.value}</strong><span>${item.label}</span></div>`).join('');
      document.getElementById('summary').innerHTML = html;
    }

    function selectNode(identityHex) {
      if (selectedSourceId === identityHex) {
        clearSelection();
        return;
      }
      selectedSourceId = identityHex;
      selectedNeighborId = null;
      if (!latestState) return;
      const selectedNode = getSelectedNode(latestState);
      const allMapNodes = deriveMapNodes(sortNodes(relevantNodes(latestState)));
      const neighborIds = selectedNeighborIds(latestState);
      const visibleNodes = allMapNodes.filter((node) => node.identity_hex === selectedSourceId || neighborIds.has(node.identity_hex));
      fitSelectedRepeater(selectedNode, visibleNodes);
      render(latestState);
    }

    function clearSelection() {
      selectedSourceId = null;
      selectedNeighborId = null;
      render(latestState);
    }

    function lineSignalMetric(link) {
      if (typeof link.snr === 'number') {
        return { value: link.snr, label: `SNR ${link.snr.toFixed(1)} dB`, short: `SNR ${link.snr.toFixed(1)}`, kind: 'SNR' };
      }
      if (typeof link.rssi === 'number') {
        return { value: link.rssi, label: `RSSI ${link.rssi} dBm`, short: `RSSI ${link.rssi}`, kind: 'RSSI' };
      }
      return { value: null, label: 'b/d', short: 'b/d', kind: 'sygnał' };
    }

    function describeProbeResult(node) {
      if (node.last_probe_status === 'failed' && node.last_data_at) {
        return 'nieudane po zapisaniu danych';
      }
      if (node.last_probe_status) {
        return node.last_probe_status;
      }
      return node.data_fetch_ok ? 'dane zapisane' : 'oczekuje';
    }

    function linkLabel(link, sourceNode) {
      const metric = lineSignalMetric(link);
      const distance = neighborDistanceKm(sourceNode, link);
      const metricLine = metric.value !== null ? `${metric.kind}: ${metric.value.toFixed(1)} ${metric.kind === 'RSSI' ? 'dBm' : 'dB'}` : 'sygnał: b/d';
      const distanceLine = distance !== null ? `dyst: ${distance.toFixed(1)} km` : 'dyst: -';
      return `<strong>${metricLine}</strong><span>${distanceLine}</span>`;
    }

    function lineColor(link) {
      const metric = lineSignalMetric(link);
      if (metric.value === null) return '#98a4ad';
      if (metric.value >= 10) return '#2e8b57';
      if (metric.value >= 5) return '#cfaa38';
      if (metric.value >= 0) return '#db7d31';
      return '#c64a3d';
    }

    function markerStyle(node, isolated, selected, neighbor) {
      const color = nodeColor(node);
      if (selected) {
        return { radius: 10, color, weight: 2.8, fillColor: color, fillOpacity: 0.97, opacity: 1 };
      }
      if (neighbor) {
        return { radius: 7, color, weight: 1.8, fillColor: color, fillOpacity: 0.88, opacity: 0.92 };
      }
      if (isolated) {
        return { radius: 4, color, weight: 1, fillColor: color, fillOpacity: 0.16, opacity: 0.2 };
      }
      return { radius: 5, color, weight: 1.2, fillColor: color, fillOpacity: 0.82, opacity: 0.85 };
    }

    function estimateLabelRect(point, html) {
      const text = html.replace(/<[^>]+>/g, ' ');
      const lines = html.includes('label-meta') ? 2 : 1;
      const width = Math.min(180, Math.max(66, text.trim().length * 5.4));
      const height = lines === 2 ? 38 : 24;
      return {
        left: point.x - (width / 2),
        right: point.x + (width / 2),
        top: point.y - height - 18,
        bottom: point.y - 18,
      };
    }

    function rectsOverlap(left, right) {
      return !(left.right < right.left || left.left > right.right || left.bottom < right.top || left.top > right.bottom);
    }

    function labelHtml(node, zoom, forced, neighborIds) {
      const shortName = node.name || node.hash_prefix_hex;
      if (selectedNeighborId) {
        if (node.identity_hex !== selectedSourceId && node.identity_hex !== selectedNeighborId) return null;
        return `<div class=\"node-label-chip\"><strong>${shortName}</strong><span class=\"label-meta\">ostatni advert: ${formatShortWhen(node.last_advert_at)}</span></div>`;
      }
      const inspectionNeighbor = Boolean(selectedSourceId) && node.identity_hex !== selectedSourceId && neighborIds.has(node.identity_hex);
      if (inspectionNeighbor) {
        return `<div class=\"node-label-chip\"><strong>${shortName}</strong><span class=\"label-meta\">ostatni advert: ${formatShortWhen(node.last_advert_at)}</span></div>`;
      }
      if (forced || zoom >= HIGH_ZOOM_LABEL_THRESHOLD) {
        return `<div class=\"node-label-chip\"><strong>${shortName}</strong><span class=\"label-meta\">ostatni advert: ${formatShortWhen(node.last_advert_at)}</span></div>`;
      }
      if (zoom >= LOW_ZOOM_LABEL_THRESHOLD) {
        return `<div class=\"node-label-chip\"><strong>${shortName}</strong></div>`;
      }
      return null;
    }

    function labelPriority(node, neighborIds) {
      if (node.identity_hex === selectedSourceId) return 4;
      if (neighborIds.has(node.identity_hex)) return 3;
      if (node.identity_hex === hoveredNodeId) return 2;
      return 1;
    }

    function renderLabels(nodes, neighborIds) {
      labelsLayer.clearLayers();
      const zoom = map.getZoom();
      const showAllNeighborLabels = Boolean(selectedSourceId) && !selectedNeighborId;
      const candidates = [];
      for (const node of nodes) {
        const forced = node.identity_hex === selectedSourceId || node.identity_hex === hoveredNodeId || (showAllNeighborLabels && neighborIds.has(node.identity_hex));
        const html = labelHtml(node, zoom, forced, neighborIds);
        if (!html) continue;
        candidates.push({
          node,
          html,
          forced,
          priority: labelPriority(node, neighborIds),
          point: map.latLngToContainerPoint([node.latitude, node.longitude]),
        });
      }
      candidates.sort((left, right) => right.priority - left.priority);
      const occupied = [];
      let count = 0;
      for (const candidate of candidates) {
        const rect = estimateLabelRect(candidate.point, candidate.html);
        const overlaps = occupied.some((item) => rectsOverlap(item, rect));
        if (overlaps && !candidate.forced) continue;
        if (!candidate.forced && !showAllNeighborLabels && count >= MAX_COLLISION_LABELS) continue;
        occupied.push(rect);
        count += 1;
        L.marker([candidate.node.latitude, candidate.node.longitude], {
          icon: L.divIcon({ className: 'node-label-icon', html: candidate.html, iconSize: null }),
          interactive: false,
          zIndexOffset: candidate.priority * 100,
        }).addTo(labelsLayer);
      }
    }

    function renderLinkLabels(selectedLinks, sourceNode) {
      linkLabelsLayer.clearLayers();
      const alwaysVisible = Boolean(selectedSourceId);
      for (const link of selectedLinks) {
        if (selectedNeighborId && link.target_identity_hex !== selectedNeighborId) continue;
        const midpoint = [
          (link.source_latitude + link.target_latitude) / 2,
          (link.source_longitude + link.target_longitude) / 2,
        ];
        L.marker(midpoint, {
          icon: L.divIcon({ className: 'link-label-icon', html: `<div class=\"signal-label-chip\">${linkLabel(link, sourceNode)}</div>`, iconSize: null }),
          interactive: false,
          opacity: alwaysVisible ? 1 : 0,
          zIndexOffset: 2000,
        }).addTo(linkLabelsLayer);
      }
    }

    function neighborDistanceKm(sourceNode, link) {
      if (!sourceNode || !isFiniteCoordinate(sourceNode.latitude, sourceNode.longitude)) return null;
      if (!isFiniteCoordinate(link.target_latitude, link.target_longitude)) return null;
      return haversineKm(sourceNode.latitude, sourceNode.longitude, link.target_latitude, link.target_longitude);
    }

    function selectedHistoryRows(state, node, neighborId) {
      if (!node || !neighborId) return [];
      return ((state.management?.signal_history || {})[node.identity_hex] || [])
        .filter((row) => row.target_identity_hex === neighborId || row.target_hash_prefix_hex === neighborId)
        .sort((left, right) => new Date(left.collected_at) - new Date(right.collected_at));
    }

    function renderSignalChart(node, neighborLink, historyRows) {
      if (!node) return '<div class=\"empty-note\">Wybierz repeater, aby obejrzeć jego bezpośrednich sąsiadów.</div>';
      if (!neighborLink) return '<div class=\"empty-note\">Wybierz wiersz sąsiada, aby obejrzeć historię sygnału.</div>';
      if (historyRows.length < 2) {
        return `
          <div class=\"chart-shell\">
            <div class=\"chart-head\">
              <div class=\"chart-title\"><strong>${neighborLink.target_name}</strong><span>historia ${lineSignalMetric(neighborLink).kind}</span></div>
              <div class=\"chart-meta\">ostatnio ${lineSignalMetric(neighborLink).label}</div>
            </div>
            <div class=\"empty-note\">Dla tego połączenia zapisano na razie ${historyRows.length} prób${historyRows.length === 1 ? 'kę' : historyRows.length < 5 ? 'ki' : 'ek'}. Wykres pojawi się po zebraniu co najmniej 2 próbek.</div>
          </div>
        `;
      }
      const values = historyRows.map((row) => row.snr).filter((value) => value !== null && value !== undefined);
      const times = historyRows.map((row) => new Date(row.collected_at).getTime());
      const minValue = Math.min(...values);
      const maxValue = Math.max(...values);
      const minTime = Math.min(...times);
      const maxTime = Math.max(...times);
      const leftPad = 28;
      const topPad = 10;
      const width = 272;
      const height = 110;
      const valueSpan = Math.max(1, maxValue - minValue);
      const timeSpan = Math.max(1, maxTime - minTime);
      const grid = [0, 0.5, 1].map((ratio) => {
        const y = topPad + ratio * height;
        const value = (maxValue - (ratio * valueSpan)).toFixed(1);
        return `<line x1=\"${leftPad}\" y1=\"${y}\" x2=\"${leftPad + width}\" y2=\"${y}\" stroke=\"rgba(21,33,42,0.08)\" stroke-width=\"1\" />` +
          `<text x=\"4\" y=\"${y + 4}\" fill=\"#6a7883\" font-size=\"10\">${value}</text>`;
      }).join('');
      const path = historyRows.map((row, index) => {
        const x = leftPad + ((new Date(row.collected_at).getTime() - minTime) / timeSpan) * width;
        const y = topPad + ((maxValue - row.snr) / valueSpan) * height;
        return `${index === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`;
      }).join(' ');
      const points = historyRows.map((row) => {
        const x = leftPad + ((new Date(row.collected_at).getTime() - minTime) / timeSpan) * width;
        const y = topPad + ((maxValue - row.snr) / valueSpan) * height;
        return `<circle cx=\"${x.toFixed(1)}\" cy=\"${y.toFixed(1)}\" r=\"2.2\" fill=\"${lineColor(neighborLink)}\" />`;
      }).join('');
      return `
        <div class=\"chart-shell\">
          <div class=\"chart-head\">
            <div class=\"chart-title\"><strong>${neighborLink.target_name}</strong><span>historia SNR</span></div>
            <div class=\"chart-meta\">ostatnio ${lineSignalMetric(neighborLink).label}</div>
          </div>
          <svg id=\"signal-chart\" viewBox=\"0 0 320 152\" preserveAspectRatio=\"none\">
            ${grid}
            <path d=\"${path}\" fill=\"none\" stroke=\"${lineColor(neighborLink)}\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" />
            ${points}
            <text x=\"${leftPad}\" y=\"144\" fill=\"#6a7883\" font-size=\"10\">${timeAgo(new Date(minTime).toISOString())}</text>
            <text x=\"${leftPad + width - 22}\" y=\"144\" fill=\"#6a7883\" font-size=\"10\">teraz</text>
          </svg>
        </div>
      `;
    }

    function renderExpandedNode(node, state) {
      const selectedLinks = getSelectedLinks(state);
      if (!selectedLinks.length || (selectedNeighborId && !selectedLinks.some((link) => link.target_identity_hex === selectedNeighborId))) {
        selectedNeighborId = null;
      }
      const selectedLink = selectedLinks.find((link) => link.target_identity_hex === selectedNeighborId) || null;
      const historyRows = selectedHistoryRows(state, node, selectedNeighborId);
      const neighborRows = selectedLinks.length ? `
        <table class=\"neighbor-table\">
          <thead>
            <tr>
              <th>Sąsiad</th>
              <th>Ostatnio widziany</th>
              <th>Sygnał</th>
              <th>Dystans</th>
            </tr>
          </thead>
          <tbody>
            ${selectedLinks.map((link) => {
              const distance = neighborDistanceKm(node, link);
              const activeClass = link.target_identity_hex === selectedNeighborId ? ' class=\"active\"' : '';
              return `
                <tr${activeClass}>
                  <td><button type=\"button\" data-neighbor=\"${link.target_identity_hex}\">${link.target_name}</button></td>
                  <td>${typeof link.last_heard_seconds === 'number' ? humanizeSeconds(link.last_heard_seconds) : timeAgo(link.collected_at)}</td>
                  <td>${lineSignalMetric(link).label}</td>
                  <td>${distance === null ? '-' : `${distance.toFixed(1)} km`}</td>
                </tr>
              `;
            }).join('')}
          </tbody>
        </table>
      ` : '<div class=\"empty-note\">Dla tego repeatera nie ma jeszcze zapisanych połączeń sąsiedzkich.</div>';
      return `
        <div class=\"node-expand\">
          <div class=\"expand-head\">
            <strong>Inspekcja</strong>
            <button type=\"button\" class=\"ghost-button\" data-clear-selection=\"1\">Wyczyść fokus</button>
          </div>
          <div class=\"detail-grid\">
            <div class=\"detail-cell\"><strong>Rola</strong>${node.role || 'Repeater'}</div>
            <div class=\"detail-cell\"><strong>Ostatni advert</strong>${formatWhen(node.last_advert_at)}</div>
            <div class=\"detail-cell\"><strong>Ostatnie dane</strong>${formatWhen(node.last_data_at)}</div>
            <div class=\"detail-cell\"><strong>Ostatnie udane pobranie</strong>${formatWhen(node.last_successful_probe_at)}</div>
            <div class=\"detail-cell\"><strong>Wynik ostatniej próby</strong>${describeProbeResult(node)}</div>
            <div class=\"detail-cell\"><strong>Ostatnia próba</strong>${formatWhen(node.last_probe_at)}</div>
          </div>
          <div>
            <div class=\"expand-head\"><strong>Bezpośredni sąsiedzi</strong><span class=\"node-state-tag\">${selectedLinks.length}</span></div>
            ${neighborRows}
          </div>
          ${renderSignalChart(node, selectedLink, historyRows)}
        </div>
      `;
    }

    function rowHtml(node, state) {
      return `
        <div class=\"node-row${node.identity_hex === selectedSourceId ? ' active' : ''}\">
          <button type=\"button\" class=\"node-row-button\" data-node=\"${node.identity_hex}\">
            <span class=\"status-dot\" style=\"background:${nodeColor(node)}\"></span>
            <span class=\"node-main\">
              <span class=\"node-name\">${node.name || node.hash_prefix_hex}</span>
              <span class=\"node-age\">ostatni advert: ${formatShortWhen(node.last_advert_at)}</span>
            </span>
            <span class=\"node-state-tag\">${nodeStateLabel(node)}</span>
          </button>
          ${node.identity_hex === selectedSourceId ? renderExpandedNode(node, state) : ''}
        </div>
      `;
    }

    function renderNodeSections(state) {
      const container = document.getElementById('node-sections');
      const nodes = sortNodes(relevantNodes(state));
      const selectedNode = selectedSourceId ? nodes.find((node) => node.identity_hex === selectedSourceId) : null;
      const others = nodes.filter((node) => node.identity_hex !== selectedSourceId);
      let html = '';
      html += `
        <div class="list-toolbar">
          <label for="sort-mode">Sortowanie</label>
          <select id="sort-mode" class="sort-select" data-sort-mode="1">
            <option value="last_advert"${nodeSortMode === 'last_advert' ? ' selected' : ''}>ostatni advert</option>
            <option value="last_data"${nodeSortMode === 'last_data' ? ' selected' : ''}>ostatnie dane</option>
            <option value="alphabetical"${nodeSortMode === 'alphabetical' ? ' selected' : ''}>alfabetycznie</option>
          </select>
        </div>
      `;
      if (selectedNode) {
        html += '<div class=\"section-heading\">Wybrany repeater</div>';
        html += `<div class=\"node-list\">${rowHtml(selectedNode, state)}</div>`;
      }
      html += `<div class=\"section-heading\">${selectedNode ? 'Pozostałe repeatery' : 'Repeatery'}</div>`;
      html += `<div class=\"node-list\">${others.length ? others.map((node) => rowHtml(node, state)).join('') : '<div class=\"empty-note\">Brak innych repeaterów.</div>'}</div>`;
      container.innerHTML = html;
      for (const button of container.querySelectorAll('[data-node]')) {
        button.addEventListener('click', () => selectNode(button.dataset.node));
      }
      for (const select of container.querySelectorAll('[data-sort-mode]')) {
        select.addEventListener('change', () => {
          nodeSortMode = select.value;
          render(latestState);
        });
      }
      for (const button of container.querySelectorAll('[data-clear-selection]')) {
        button.addEventListener('click', clearSelection);
      }
      for (const button of container.querySelectorAll('[data-neighbor]')) {
        button.addEventListener('click', () => {
          selectedNeighborId = button.dataset.neighbor;
          render(latestState);
        });
      }
    }

    function renderMap(state) {
      markersLayer.clearLayers();
      halosLayer.clearLayers();
      linksLayer.clearLayers();
      labelsLayer.clearLayers();
      linkLabelsLayer.clearLayers();
      const allMapNodes = deriveMapNodes(sortNodes(relevantNodes(state)));
      const neighborIds = selectedNeighborIds(state);
      const selectedLinks = getSelectedMapLinks(state);
      const sourceNode = getSelectedNode(state);
      const nodes = selectedSourceId
        ? allMapNodes.filter((node) => node.identity_hex === selectedSourceId || neighborIds.has(node.identity_hex))
        : allMapNodes;
      const bounds = [];
      for (const node of nodes) {
        const selected = node.identity_hex === selectedSourceId;
        const neighbor = neighborIds.has(node.identity_hex);
        const isolated = Boolean(selectedNeighborId) && node.identity_hex !== selectedSourceId && node.identity_hex !== selectedNeighborId;
        if (selected) {
          L.circleMarker([node.latitude, node.longitude], {
            radius: 15,
            color: nodeColor(node),
            weight: 1,
            fillColor: nodeColor(node),
            fillOpacity: 0.08,
            opacity: 0.36,
          }).addTo(halosLayer);
        }
        const marker = L.circleMarker([node.latitude, node.longitude], markerStyle(node, isolated, selected, neighbor)).addTo(markersLayer);
        marker.on('click', (event) => {
          L.DomEvent.stopPropagation(event);
          selectNode(node.identity_hex);
        });
        marker.on('mouseover', () => {
          hoveredNodeId = node.identity_hex;
          renderLabels(nodes, neighborIds);
        });
        marker.on('mouseout', () => {
          if (hoveredNodeId === node.identity_hex) hoveredNodeId = null;
          renderLabels(nodes, neighborIds);
        });
        if (selected) marker.bringToFront();
        bounds.push([node.latitude, node.longitude]);
      }
      for (const link of selectedLinks) {
        const polyline = L.polyline([
          [link.source_latitude, link.source_longitude],
          [link.target_latitude, link.target_longitude],
        ], {
          color: lineColor(link),
          weight: selectedNeighborId && link.target_identity_hex === selectedNeighborId ? 3.2 : 2,
          opacity: selectedNeighborId && link.target_identity_hex !== selectedNeighborId ? 0.18 : 0.82,
        }).addTo(linksLayer);
        polyline.on('mouseover', () => {
          if (selectedLinks.length > 6) {
            const midpoint = [
              (link.source_latitude + link.target_latitude) / 2,
              (link.source_longitude + link.target_longitude) / 2,
            ];
            const transient = L.marker(midpoint, {
              icon: L.divIcon({ className: 'link-label-icon', html: `<div class=\"signal-label-chip\">${linkLabel(link, sourceNode)}</div>`, iconSize: null }),
              interactive: false,
              zIndexOffset: 2000,
            }).addTo(linkLabelsLayer);
            polyline.once('mouseout', () => linkLabelsLayer.removeLayer(transient));
          }
        });
        polyline.on('click', (event) => {
          L.DomEvent.stopPropagation(event);
          selectedNeighborId = link.target_identity_hex;
          render(latestState);
        });
        bounds.push([link.source_latitude, link.source_longitude]);
        bounds.push([link.target_latitude, link.target_longitude]);
      }
      renderLabels(nodes, neighborIds);
      renderLinkLabels(selectedLinks, sourceNode);
      if (!hasFitBounds && bounds.length) fitInitialBounds(bounds);
    }

    function render(state) {
      latestState = state;
      renderSummary(state);
      renderNodeSections(state);
      renderMap(state);
    }

    async function refresh() {
      const response = await fetch('/api/state');
      const state = await response.json();
      render(state);
    }

    map.on('click', () => {
      hoveredNodeId = null;
      if (selectedSourceId) clearSelection();
    });
    map.on('zoomend', () => {
      if (latestState) renderMap(latestState);
    });

    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""


def create_app(database: BotDatabase) -> FastAPI:
    app = FastAPI(title="meshcore-bot", version="0.1.0")

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok", "database": database.snapshot_overview()})

    @app.get("/api/state")
    async def api_state() -> JSONResponse:
        return JSONResponse(
            {
                "overview": database.snapshot_overview(),
                "nodes": database.list_repeaters_for_web(),
                "probe_jobs": database.list_probe_jobs(limit=100),
                "management": {
                    "map_links": database.latest_repeater_neighbor_links(limit_repeaters=128),
                    "signal_history": database.repeater_neighbor_signal_history(limit_samples_per_source=128),
                },
            }
        )

    @app.get("/", response_class=HTMLResponse)
    async def root() -> HTMLResponse:
        return HTMLResponse(INDEX_HTML)

    return app
