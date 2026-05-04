"""HTTP API and simple browser viewer."""

from __future__ import annotations

from html import escape
from secrets import compare_digest
from urllib.parse import parse_qs, urlencode

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware

from .service import MeshcoreTCPBotService


INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MeshCore TCP Bot</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
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
      width: min(336px, calc(100vw - 32px));
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
      max-width: 220px;
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
    }
    .node-label-chip {
      border: 1px solid rgba(21, 33, 42, 0.1);
      border-radius: 10px;
      background: rgba(255, 255, 255, 0.95);
      box-shadow: 0 10px 24px rgba(21, 33, 42, 0.1);
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
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.94);
      box-shadow: 0 8px 18px rgba(21, 33, 42, 0.08);
      color: var(--ink);
      padding: 2px 6px;
      font-family: 'SFMono-Regular', ui-monospace, monospace;
      font-size: 0.66rem;
      line-height: 1;
      pointer-events: none;
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
  <div id="app">
    <div id="map"></div>
    <div id="map-legend" class="overlay">
      <div class="legend-group">
        <span class="legend-title">Nodes</span>
        <div class="legend-row"><span class="legend-node" style="background:#2e8b57"></span><span>data fetched</span></div>
        <div class="legend-row"><span class="legend-node" style="background:#2c71d1"></span><span>known / unreachable</span></div>
        <div class="legend-row"><span class="legend-node" style="background:#c64a3d"></span><span>inactive &gt; 24h</span></div>
      </div>
      <div class="legend-group">
        <span class="legend-title">Links</span>
        <div class="legend-row"><span class="legend-line" style="border-top-color:#2e8b57"></span><span>strong</span></div>
        <div class="legend-row"><span class="legend-line" style="border-top-color:#cfaa38"></span><span>medium</span></div>
        <div class="legend-row"><span class="legend-line" style="border-top-color:#db7d31"></span><span>weak</span></div>
        <div class="legend-row"><span class="legend-line" style="border-top-color:#c64a3d"></span><span>very weak / stale</span></div>
      </div>
    </div>
    <aside id="sidebar" class="overlay">
      <section class="summary-strip">
        <div id="summary" class="summary-grid"></div>
      </section>
      <section class="list-shell">
        <div id="node-sections"></div>
      </section>
    </aside>
  </div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
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
    let hasFitBounds = false;

    function formatWhen(value) {
      if (!value) return 'unknown';
      return new Date(value).toLocaleString();
    }

    function timeAgo(value) {
      if (!value) return 'unknown';
      const elapsed = Math.max(0, Date.now() - new Date(value).getTime());
      const seconds = Math.floor(elapsed / 1000);
      if (seconds < 60) return `${seconds}s ago`;
      if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
      if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
      return `${Math.floor(seconds / 86400)}d ago`;
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
      return state.nodes.filter((node) => node.role === 'Repeater' || node.role === 'Room Server');
    }

    function getSelectedNode(state) {
      return state.nodes.find((node) => node.identity_hex === selectedSourceId) || null;
    }

    function getSelectedLinks(state) {
      if (!selectedSourceId) return [];
      const visibleIds = new Set(deriveMapNodes(relevantNodes(state)).map((node) => node.identity_hex));
      return (state.management?.map_links || [])
        .filter((link) => link.source_identity_hex === selectedSourceId)
        .filter((link) => visibleIds.has(link.target_identity_hex))
        .filter((link) => isFiniteCoordinate(link.source_latitude, link.source_longitude) && isFiniteCoordinate(link.target_latitude, link.target_longitude))
        .sort((left, right) => ((right.rssi ?? right.snr ?? -999) - (left.rssi ?? left.snr ?? -999)));
    }

    function selectedNeighborIds(state) {
      return new Set(getSelectedLinks(state).map((link) => link.target_identity_hex));
    }

    function nodeStateLabel(node) {
      const state = nodeState(node);
      if (state === 'ok') return 'data';
      if (state === 'missing') return 'no data';
      return 'inactive';
    }

    function sortNodes(nodes) {
      return nodes.slice().sort((left, right) => {
        const rankDiff = nodeStateRank(left) - nodeStateRank(right);
        if (rankDiff !== 0) return rankDiff;
        const leftTime = left.last_advert_at ? new Date(left.last_advert_at).getTime() : 0;
        const rightTime = right.last_advert_at ? new Date(right.last_advert_at).getTime() : 0;
        if (rightTime !== leftTime) return rightTime - leftTime;
        return (left.name || left.hash_prefix_hex).localeCompare(right.name || right.hash_prefix_hex);
      });
    }

    function renderSummary(state) {
      const nodes = relevantNodes(state);
      const html = [
        { label: 'known', value: nodes.length },
        { label: 'data', value: nodes.filter((node) => !isInactive(node) && node.data_fetch_ok).length },
        { label: 'pending', value: nodes.filter((node) => !isInactive(node) && !node.data_fetch_ok).length },
        { label: 'inactive', value: nodes.filter((node) => isInactive(node)).length },
      ].map((item) => `<div class="summary-card"><strong>${item.value}</strong><span>${item.label}</span></div>`).join('');
      document.getElementById('summary').innerHTML = html;
    }

    function selectNode(identityHex) {
      if (selectedSourceId === identityHex) {
        clearSelection();
        return;
      }
      selectedSourceId = identityHex;
      if (!latestState) return;
      const selectedNode = getSelectedNode(latestState);
      const selectedLinks = getSelectedLinks(latestState);
      selectedNeighborId = selectedLinks.length ? (selectedNeighborId && selectedLinks.some((link) => link.target_identity_hex === selectedNeighborId) ? selectedNeighborId : selectedLinks[0].target_identity_hex) : null;
      if (selectedNode && isFiniteCoordinate(selectedNode.latitude, selectedNode.longitude)) {
        const bounds = [[selectedNode.latitude, selectedNode.longitude]];
        for (const link of selectedLinks) {
          if (isFiniteCoordinate(link.target_latitude, link.target_longitude)) {
            bounds.push([link.target_latitude, link.target_longitude]);
          }
        }
        if (bounds.length > 1) {
          map.flyToBounds(bounds, {
            paddingTopLeft: [36, 36],
            paddingBottomRight: [380, 36],
            maxZoom: 11,
            duration: 0.6,
          });
        } else {
          map.flyTo([selectedNode.latitude, selectedNode.longitude], Math.max(map.getZoom(), 10), { duration: 0.5 });
        }
      }
      render(latestState);
    }

    function clearSelection() {
      selectedSourceId = null;
      selectedNeighborId = null;
      render(latestState);
    }

    function lineSignalMetric(link) {
      if (typeof link.rssi === 'number') {
        return { value: link.rssi, label: `${link.rssi.toFixed(1)} dBm`, short: link.rssi.toFixed(1), kind: 'RSSI' };
      }
      if (typeof link.snr === 'number') {
        return { value: link.snr, label: `${link.snr.toFixed(1)} dB`, short: link.snr.toFixed(1), kind: 'SNR' };
      }
      return { value: null, label: 'n/a', short: 'n/a', kind: 'signal' };
    }

    function lineColor(link) {
      const metric = lineSignalMetric(link);
      if (metric.value === null) return '#98a4ad';
      if (typeof link.rssi === 'number') {
        if (metric.value >= -95) return '#2e8b57';
        if (metric.value >= -105) return '#cfaa38';
        if (metric.value >= -115) return '#db7d31';
        return '#c64a3d';
      }
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
      const inspectionNeighbor = Boolean(selectedSourceId) && node.identity_hex !== selectedSourceId && neighborIds.has(node.identity_hex);
      if (inspectionNeighbor) {
        return `<div class="node-label-chip"><strong>${shortName}</strong></div>`;
      }
      if (forced || zoom >= HIGH_ZOOM_LABEL_THRESHOLD) {
        return `<div class="node-label-chip"><strong>${shortName}</strong><span class="label-meta">last advert: ${timeAgo(node.last_advert_at)}</span></div>`;
      }
      if (zoom >= LOW_ZOOM_LABEL_THRESHOLD) {
        return `<div class="node-label-chip"><strong>${shortName}</strong></div>`;
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
      const candidates = [];
      for (const node of nodes) {
        const forced = node.identity_hex === selectedSourceId || node.identity_hex === hoveredNodeId;
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
        if (!candidate.forced && count >= MAX_COLLISION_LABELS) continue;
        occupied.push(rect);
        count += 1;
        L.marker([candidate.node.latitude, candidate.node.longitude], {
          icon: L.divIcon({
            className: 'node-label-icon',
            html: candidate.html,
            iconSize: null,
          }),
          interactive: false,
          zIndexOffset: candidate.priority * 100,
        }).addTo(labelsLayer);
      }
    }

    function renderLinkLabels(selectedLinks) {
      linkLabelsLayer.clearLayers();
      const alwaysVisible = selectedLinks.length <= 6;
      for (const link of selectedLinks) {
        const metric = lineSignalMetric(link);
        if (metric.value === null) continue;
        const midpoint = [
          (link.source_latitude + link.target_latitude) / 2,
          (link.source_longitude + link.target_longitude) / 2,
        ];
        L.marker(midpoint, {
          icon: L.divIcon({
            className: 'link-label-icon',
            html: `<div class="signal-label-chip">${metric.short}</div>`,
            iconSize: null,
          }),
          interactive: false,
          opacity: alwaysVisible ? 1 : 0,
          zIndexOffset: 2000,
        }).addTo(linkLabelsLayer);
      }
    }

    function neighborDistanceKm(sourceNode, link) {
      if (typeof link.distance_km === 'number') return link.distance_km;
      if (!sourceNode || !isFiniteCoordinate(sourceNode.latitude, sourceNode.longitude)) return null;
      if (!isFiniteCoordinate(link.target_latitude, link.target_longitude)) return null;
      return haversineKm(sourceNode.latitude, sourceNode.longitude, link.target_latitude, link.target_longitude);
    }

    function selectedHistoryRows(state, node, neighborId) {
      if (!node || !neighborId) return [];
      const historyKey = node.target_name || node.name || node.identity_hex;
      return ((state.management?.signal_history || {})[historyKey] || [])
        .filter((row) => (row.neighbor_identity_hex || row.neighbor_hash_prefix) === neighborId)
        .sort((left, right) => new Date(left.collected_at) - new Date(right.collected_at));
    }

    function renderSignalChart(node, neighborLink, historyRows) {
      if (!node) return '<div class="empty-note">Select a node to inspect direct neighbors.</div>';
      if (!neighborLink) return '<div class="empty-note">Select a neighbor row to inspect signal history.</div>';
      if (historyRows.length < 2) {
        return `
          <div class="chart-shell">
            <div class="chart-head">
              <div class="chart-title"><strong>${neighborLink.target_name}</strong><span>${lineSignalMetric(neighborLink).kind} history</span></div>
              <div class="chart-meta">latest ${lineSignalMetric(neighborLink).label}</div>
            </div>
            <div class="empty-note">Only ${historyRows.length} stored sample${historyRows.length === 1 ? '' : 's'} for this link so far. The history graph appears after at least 2 samples.</div>
          </div>
        `;
      }
      const metricName = historyRows.some((row) => typeof row.rssi === 'number') ? 'RSSI' : 'SNR';
      const values = historyRows.map((row) => row.rssi ?? row.snr).filter((value) => value !== null && value !== undefined);
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
        return `<line x1="${leftPad}" y1="${y}" x2="${leftPad + width}" y2="${y}" stroke="rgba(21,33,42,0.08)" stroke-width="1" />` +
          `<text x="4" y="${y + 4}" fill="#6a7883" font-size="10">${value}</text>`;
      }).join('');
      const path = historyRows.map((row, index) => {
        const signal = row.rssi ?? row.snr;
        const x = leftPad + ((new Date(row.collected_at).getTime() - minTime) / timeSpan) * width;
        const y = topPad + ((maxValue - signal) / valueSpan) * height;
        return `${index === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`;
      }).join(' ');
      const points = historyRows.map((row) => {
        const signal = row.rssi ?? row.snr;
        const x = leftPad + ((new Date(row.collected_at).getTime() - minTime) / timeSpan) * width;
        const y = topPad + ((maxValue - signal) / valueSpan) * height;
        return `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="2.2" fill="${lineColor(neighborLink)}" />`;
      }).join('');
      return `
        <div class="chart-shell">
          <div class="chart-head">
            <div class="chart-title"><strong>${neighborLink.target_name}</strong><span>${metricName} history</span></div>
            <div class="chart-meta">latest ${lineSignalMetric(neighborLink).label}</div>
          </div>
          <svg id="signal-chart" viewBox="0 0 320 152" preserveAspectRatio="none">
            ${grid}
            <path d="${path}" fill="none" stroke="${lineColor(neighborLink)}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
            ${points}
            <text x="${leftPad}" y="144" fill="#6a7883" font-size="10">${timeAgo(new Date(minTime).toISOString())}</text>
            <text x="${leftPad + width - 22}" y="144" fill="#6a7883" font-size="10">now</text>
          </svg>
        </div>
      `;
    }

    function renderExpandedNode(node, state) {
      const selectedLinks = getSelectedLinks(state);
      if (!selectedLinks.length) {
        selectedNeighborId = null;
      } else if (!selectedNeighborId || !selectedLinks.some((link) => link.target_identity_hex === selectedNeighborId)) {
        selectedNeighborId = selectedLinks[0].target_identity_hex;
      }
      const selectedLink = selectedLinks.find((link) => link.target_identity_hex === selectedNeighborId) || null;
      const historyRows = selectedHistoryRows(state, node, selectedNeighborId);
      const neighborRows = selectedLinks.length ? `
        <table class="neighbor-table">
          <thead>
            <tr>
              <th>Neighbor</th>
              <th>Last seen</th>
              <th>Signal</th>
              <th>Distance</th>
            </tr>
          </thead>
          <tbody>
            ${selectedLinks.map((link) => {
              const distance = neighborDistanceKm(node, link);
              const activeClass = link.target_identity_hex === selectedNeighborId ? ' class="active"' : '';
              return `
                <tr${activeClass}>
                  <td><button type="button" data-neighbor="${link.target_identity_hex}">${link.target_name}</button></td>
                  <td>${typeof link.last_heard_seconds === 'number' ? `${link.last_heard_seconds}s` : timeAgo(link.collected_at)}</td>
                  <td>${lineSignalMetric(link).label}</td>
                  <td>${distance === null ? '-' : `${distance.toFixed(1)} km`}</td>
                </tr>
              `;
            }).join('')}
          </tbody>
        </table>
      ` : '<div class="empty-note">No valid direct-neighbor links are currently available for this node.</div>';
      return `
        <div class="node-expand">
          <div class="expand-head">
            <strong>Inspection</strong>
            <button type="button" class="ghost-button" data-clear-selection="1">Clear</button>
          </div>
          <div class="detail-grid">
            <div class="detail-cell"><strong>Role</strong>${node.role || 'Unknown role'}</div>
            <div class="detail-cell"><strong>Last advert</strong>${formatWhen(node.last_advert_at)}</div>
          </div>
          <div>
            <div class="expand-head"><strong>Direct neighbors</strong><span class="node-state-tag">${selectedLinks.length}</span></div>
            ${neighborRows}
          </div>
          ${renderSignalChart(node, selectedLink, historyRows)}
        </div>
      `;
    }

    function rowHtml(node, state) {
      return `
        <div class="node-row${node.identity_hex === selectedSourceId ? ' active' : ''}">
          <button type="button" class="node-row-button" data-node="${node.identity_hex}">
            <span class="status-dot" style="background:${nodeColor(node)}"></span>
            <span class="node-main">
              <span class="node-name">${node.name || node.hash_prefix_hex}</span>
              <span class="node-age">${timeAgo(node.last_advert_at)}</span>
            </span>
            <span class="node-state-tag">${nodeStateLabel(node)}</span>
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
      if (selectedNode) {
        html += '<div class="section-heading">Selected node</div>';
        html += `<div class="node-list">${rowHtml(selectedNode, state)}</div>`;
      }
      html += `<div class="section-heading">${selectedNode ? 'Other nodes' : 'Nodes'}</div>`;
      html += `<div class="node-list">${others.length ? others.map((node) => rowHtml(node, state)).join('') : '<div class="empty-note">No other nodes available.</div>'}</div>`;
      container.innerHTML = html;
      for (const button of container.querySelectorAll('[data-node]')) {
        button.addEventListener('click', () => selectNode(button.dataset.node));
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
      const selectedLinks = getSelectedLinks(state);
      const nodes = selectedSourceId
        ? allMapNodes.filter((node) => node.identity_hex === selectedSourceId || neighborIds.has(node.identity_hex))
        : allMapNodes;
      const bounds = [];
      for (const node of nodes) {
        const selected = node.identity_hex === selectedSourceId;
        const neighbor = neighborIds.has(node.identity_hex);
        const isolated = false;
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
        const targetNode = state.nodes.find((node) => node.identity_hex === link.target_identity_hex);
        const inactiveLink = isInactive(getSelectedNode(state) || {}) || (targetNode ? isInactive(targetNode) : false);
        const polyline = L.polyline([
          [link.source_latitude, link.source_longitude],
          [link.target_latitude, link.target_longitude],
        ], {
          color: lineColor(link),
          weight: 2,
          opacity: 0.72,
          dashArray: inactiveLink ? '7 7' : null,
        }).addTo(linksLayer);
        polyline.on('mouseover', () => {
          if (selectedLinks.length > 6) {
            const midpoint = [
              (link.source_latitude + link.target_latitude) / 2,
              (link.source_longitude + link.target_longitude) / 2,
            ];
            const transient = L.marker(midpoint, {
              icon: L.divIcon({ className: 'link-label-icon', html: `<div class="signal-label-chip">${lineSignalMetric(link).short}</div>`, iconSize: null }),
              interactive: false,
              zIndexOffset: 2000,
            }).addTo(linkLabelsLayer);
            polyline.once('mouseout', () => linkLabelsLayer.removeLayer(transient));
          }
        });
        bounds.push([link.source_latitude, link.source_longitude]);
        bounds.push([link.target_latitude, link.target_longitude]);
      }
      renderLabels(nodes, neighborIds);
      renderLinkLabels(selectedLinks);
      if (!hasFitBounds && bounds.length) {
        map.fitBounds(bounds, { padding: [36, 36], maxZoom: 9 });
        hasFitBounds = true;
      }
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


def _checked(value: bool) -> str:
    return " checked" if value else ""


def _selected(current: str, expected: str) -> str:
    return " selected" if current == expected else ""


def _admin_layout(title: str, body: str, *, notice: str | None = None, error: str | None = None) -> str:
    banner = ""
    if notice:
        banner += f'<div class="banner notice">{escape(notice)}</div>'
    if error:
        banner += f'<div class="banner error">{escape(error)}</div>'
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <style>
    :root {{
      --bg: #eef1eb;
      --panel: #ffffff;
      --ink: #18242c;
      --muted: #667681;
      --line: #d8dfdb;
      --accent: #1f6a52;
      --accent-soft: #d9eee4;
      --warn: #8c4d1f;
      --warn-soft: #f4e5d5;
      --error: #9d3d3d;
      --error-soft: #f6dddd;
      --shadow: 0 18px 40px rgba(24, 36, 44, 0.08);
    }}
    body {{ margin: 0; background: linear-gradient(180deg, #edf1ea 0%, #e5ece7 100%); color: var(--ink); font-family: Georgia, 'Iowan Old Style', serif; }}
    .shell {{ max-width: 1320px; margin: 0 auto; padding: 24px; }}
    .head {{ display: flex; justify-content: space-between; align-items: center; gap: 16px; margin-bottom: 20px; }}
    .head h1 {{ margin: 0; font-size: 1.7rem; }}
    .head p {{ margin: 4px 0 0; color: var(--muted); }}
    .logout {{ border: 1px solid var(--line); border-radius: 999px; background: var(--panel); padding: 8px 14px; cursor: pointer; font: inherit; }}
    .banner {{ margin-bottom: 16px; padding: 12px 14px; border-radius: 14px; border: 1px solid var(--line); box-shadow: var(--shadow); }}
    .banner.notice {{ background: var(--accent-soft); border-color: #bfdccf; }}
    .banner.error {{ background: var(--error-soft); border-color: #ebc1c1; }}
    .grid {{ display: grid; gap: 16px; grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    .panel {{ background: rgba(255,255,255,0.92); border: 1px solid var(--line); border-radius: 22px; box-shadow: var(--shadow); padding: 18px; }}
    .panel.full {{ grid-column: 1 / -1; }}
    .panel h2 {{ margin: 0 0 4px; font-size: 1.05rem; }}
    .panel p {{ margin: 0 0 12px; color: var(--muted); font-size: 0.9rem; line-height: 1.35; }}
    .stack {{ display: grid; gap: 10px; }}
    .row {{ display: grid; gap: 10px; grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    .row.row-3 {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }}
    .row.row-4 {{ grid-template-columns: repeat(4, minmax(0, 1fr)); }}
    label {{ display: grid; gap: 4px; font-size: 0.8rem; color: var(--muted); }}
    input, textarea, select {{ width: 100%; box-sizing: border-box; border: 1px solid var(--line); border-radius: 12px; padding: 9px 10px; font: inherit; color: var(--ink); background: #fff; }}
    textarea {{ min-height: 88px; resize: vertical; }}
    .checkbox {{ display: flex; align-items: center; gap: 8px; color: var(--ink); }}
    .checkbox input {{ width: auto; }}
    .toolbar {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px; }}
    button {{ border: 1px solid var(--line); border-radius: 12px; padding: 9px 14px; background: #fff; color: var(--ink); cursor: pointer; font: inherit; }}
    button.primary {{ background: var(--accent); color: #fff; border-color: var(--accent); }}
    button.warn {{ background: var(--warn-soft); border-color: #dfc39e; color: var(--warn); }}
    button.danger {{ background: var(--error-soft); border-color: #e5b7b7; color: var(--error); }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.84rem; }}
    th, td {{ text-align: left; padding: 8px 6px; border-bottom: 1px solid var(--line); vertical-align: top; }}
    th {{ color: var(--muted); font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.06em; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, monospace; font-size: 0.75rem; word-break: break-all; }}
    .muted {{ color: var(--muted); }}
    .login {{ max-width: 420px; margin: 9vh auto; padding: 24px; }}
    @media (max-width: 980px) {{ .grid, .row, .row.row-3, .row.row-4 {{ grid-template-columns: 1fr; }} .shell {{ padding: 14px; }} }}
  </style>
</head>
<body>
  <div class="shell">
    {banner}
    {body}
  </div>
</body>
</html>"""


def _render_admin_login(service: MeshcoreTCPBotService, *, notice: str | None = None, error: str | None = None) -> str:
    return _admin_layout(
        "MeshCore Admin Login",
        f"""
        <section class="panel login">
          <h2>Admin Login</h2>
          <p>Log in with the configured admin password from Docker environment variables. It can be a normal password or a numeric-only secret.</p>
          <form method="post" action="/admin/login" class="stack">
            <label>Password
              <input type="password" name="password" autocomplete="current-password">
            </label>
            <div class="toolbar">
              <button type="submit" class="primary">Log In</button>
            </div>
          </form>
          <p class="muted">Configured: password={'yes' if service.config.admin.password else 'no'}.</p>
        </section>
        """,
        notice=notice,
        error=error,
    )


def _render_admin_dashboard(service: MeshcoreTCPBotService, *, notice: str | None = None, error: str | None = None) -> str:
    state = service.admin_snapshot()
    bot = state["bot"]
    commands = state["commands"]
    channels = state["channels"]
    endpoints = state["endpoints"]
    targets = state["management_targets"]
    identity = state["identity"]
    endpoint_names = [str(item["name"]) for item in endpoints]
    command_rows = "".join(
        f"""
        <tr>
          <td class="mono">{escape(name)}</td>
          <td>{'on' if settings.get('enabled', True) else 'off'}</td>
          <td class="mono">{escape(str(settings.get('response_template') or ''))}</td>
        </tr>
        """
        for name, settings in commands.items()
    )
    channel_rows = "".join(
        f"""
        <tr>
          <td class="mono">{escape(item['name'])}</td>
          <td>{'yes' if item.get('listen', True) else 'no'}</td>
          <td class="mono">{escape(item.get('psk') or '')}</td>
          <td>
            <form method="post" action="/admin/channel/delete"><input type="hidden" name="name" value="{escape(item['name'])}"><button type="submit" class="danger">Delete</button></form>
          </td>
        </tr>
        """
        for item in channels
    )
    endpoint_rows = "".join(
        f"""
        <tr>
          <td class="mono">{escape(item['name'])}</td>
          <td class="mono">{escape(item['raw_host'])}:{int(item.get('raw_port', 5002))}</td>
          <td class="mono">{escape(str(item.get('console_host') or ''))}:{escape(str(item.get('console_port') or ''))}</td>
          <td>{'yes' if item.get('enabled', True) else 'no'}</td>
          <td>
            <form method="post" action="/admin/endpoint/delete"><input type="hidden" name="name" value="{escape(item['name'])}"><button type="submit" class="danger">Delete</button></form>
          </td>
        </tr>
        """
        for item in endpoints
    )
    target_rows = "".join(
        f"""
        <tr>
          <td>{escape(str(item.get('name') or ''))}</td>
          <td>{escape(str(item.get('endpoint_name') or ''))}</td>
          <td class="mono">{escape(str(item.get('target_identity_hex') or item.get('target_hash_prefix') or ''))}</td>
          <td>{escape(str(item.get('prefer_role') or 'guest'))}</td>
          <td>{'yes' if item.get('enabled') else 'no'}</td>
          <td>
            <form method="post" action="/admin/target/delete"><input type="hidden" name="name" value="{escape(str(item.get('name') or ''))}"><button type="submit" class="danger">Delete</button></form>
          </td>
        </tr>
        """
        for item in targets
    )
    endpoint_options = "".join(
        f'<option value="{escape(name)}">{escape(name)}</option>'
        for name in endpoint_names
    )
    return _admin_layout(
        "MeshCore Admin",
        f"""
        <header class="head">
          <div>
            <h1>MeshCore Admin</h1>
            <p>Runtime settings below are persisted in SQLite and applied directly to the running bot.</p>
          </div>
          <form method="post" action="/admin/logout"><button type="submit" class="logout">Log Out</button></form>
        </header>
        <div class="grid">
          <section class="panel">
            <h2>Bot Settings</h2>
            <p>Name, replies, private-message behavior, and history limits.</p>
            <form method="post" action="/admin/settings/general" class="stack">
              <div class="row">
                <label>Bot name<input name="name" value="{escape(str(bot.get('name') or ''))}"></label>
                <label>Reply prefix<input name="reply_prefix" value="{escape(str(bot.get('reply_prefix') or ''))}"></label>
              </div>
              <div class="row row-4">
                <label>Command prefix<input name="command_prefix" value="{escape(str(bot.get('command_prefix') or '!'))}"></label>
                <label>Message history<input type="number" min="10" name="message_history_size" value="{int(bot.get('message_history_size', 200))}"></label>
                <label>Chart history points<input type="number" min="2" name="signal_history_limit" value="{int(bot.get('signal_history_limit', 32))}"></label>
                <label>Stored neighbor snapshots<input type="number" min="1" name="neighbor_snapshot_retention" value="{int(bot.get('neighbor_snapshot_retention', 96))}"></label>
              </div>
              <label class="checkbox"><input type="checkbox" name="private_messages_enabled" value="1"{_checked(bool(bot.get('private_messages_enabled', True)))}>Reply to private messages</label>
              <label>Private-message auto reply<textarea name="private_message_auto_response">{escape(str(bot.get('private_message_auto_response') or ''))}</textarea></label>
              <div class="toolbar"><button type="submit" class="primary">Save Bot Settings</button></div>
            </form>
          </section>
          <section class="panel">
            <h2>Bot Identity</h2>
            <p>Key rotation is dangerous. Existing private peers and management sessions will stop trusting the old bot identity.</p>
            <div class="stack">
              <div><strong>Public key</strong><div class="mono">{escape(identity['public_key_hex'])}</div></div>
              <div><strong>Private key</strong><div class="mono">{escape(identity['private_key_hex'])}</div></div>
              <div><strong>Identity file</strong><div class="mono">{escape(identity['path'])}</div></div>
              <form method="post" action="/admin/identity/regenerate" class="stack">
                <label>Type REGENERATE to confirm<input name="confirm" placeholder="REGENERATE"></label>
                <label>Admin password again<input type="password" name="password" autocomplete="current-password"></label>
                <div class="toolbar"><button type="submit" class="danger">Regenerate Identity</button></div>
              </form>
            </div>
          </section>
          <section class="panel full">
            <h2>Commands</h2>
            <p>Enable or disable each command and change its response template. Supported placeholders include reply_prefix, sender, path_len, snr_suffix, rssi_suffix, distance_suffix, trace, neighbors_summary, command_list, bot_name, and command_prefix.</p>
            <table><thead><tr><th>Command</th><th>Status</th><th>Current template</th></tr></thead><tbody>{command_rows}</tbody></table>
            <form method="post" action="/admin/settings/commands" class="stack" style="margin-top:12px;">
              {''.join(f'''<div class="panel" style="padding:12px;"><div class="row"><label class="checkbox"><input type="checkbox" name="{name}_enabled" value="1"{_checked(bool(settings.get('enabled', True)))}>Enable {escape(name)}</label></div><label>Response template<textarea name="{name}_template">{escape(str(settings.get('response_template') or ''))}</textarea></label></div>''' for name, settings in commands.items())}
              <div class="toolbar"><button type="submit" class="primary">Save Command Settings</button></div>
            </form>
          </section>
          <section class="panel full">
            <h2>Channels</h2>
            <p>Known hashtag channels. Listen controls whether the bot reacts on that channel.</p>
            <table><thead><tr><th>Name</th><th>Listen</th><th>PSK</th><th></th></tr></thead><tbody>{channel_rows}</tbody></table>
            <form method="post" action="/admin/channel/upsert" class="stack" style="margin-top:12px;">
              <div class="row row-3">
                <label>Channel name<input name="name" placeholder="bot-test"></label>
                <label>PSK (optional)<input name="psk"></label>
                <label class="checkbox"><input type="checkbox" name="listen" value="1" checked>Listen on this channel</label>
              </div>
              <div class="toolbar"><button type="submit" class="primary">Add / Update Channel</button></div>
            </form>
          </section>
          <section class="panel full">
            <h2>Endpoints</h2>
            <p>Raw TCP repeater connections the bot maintains.</p>
            <table><thead><tr><th>Name</th><th>Raw</th><th>CLI</th><th>Enabled</th><th></th></tr></thead><tbody>{endpoint_rows}</tbody></table>
            <form method="post" action="/admin/endpoint/upsert" class="stack" style="margin-top:12px;">
              <div class="row row-4">
                <label>Name<input name="name"></label>
                <label>Raw host<input name="raw_host"></label>
                <label>Raw port<input type="number" name="raw_port" value="5002"></label>
                <label class="checkbox"><input type="checkbox" name="enabled" value="1" checked>Enabled</label>
              </div>
              <div class="row row-4">
                <label>CLI host<input name="console_host"></label>
                <label>CLI port<input type="number" name="console_port" value="5001"></label>
                <label>Mirror host<input name="console_mirror_host"></label>
                <label>Mirror port<input type="number" name="console_mirror_port" value="5003"></label>
              </div>
              <div class="row">
                <label>Latitude<input name="latitude"></label>
                <label>Longitude<input name="longitude"></label>
              </div>
              <div class="toolbar"><button type="submit" class="primary">Add / Update Endpoint</button></div>
            </form>
          </section>
          <section class="panel full">
            <h2>Known Repeaters / Room Servers</h2>
            <p>Management targets the bot can log into. Admin passwords saved here are used by the live management logic.</p>
            <table><thead><tr><th>Name</th><th>Endpoint</th><th>Identity / prefix</th><th>Role</th><th>Enabled</th><th></th></tr></thead><tbody>{target_rows}</tbody></table>
            <form method="post" action="/admin/target/upsert" class="stack" style="margin-top:12px;">
              <div class="row row-4">
                <label>Name<input name="name"></label>
                <label>Endpoint<select name="endpoint_name">{endpoint_options}</select></label>
                <label>Identity hex<input name="target_identity_hex"></label>
                <label>Hash prefix<input name="target_hash_prefix"></label>
              </div>
              <div class="row row-4">
                <label>Guest password<input name="guest_password"></label>
                <label>Admin password<input name="admin_password"></label>
                <label>Preferred role<select name="prefer_role"><option value="guest">guest</option><option value="admin">admin</option></select></label>
                <label class="checkbox"><input type="checkbox" name="enabled" value="1" checked>Enabled</label>
              </div>
              <label>Notes<textarea name="notes"></textarea></label>
              <div class="toolbar"><button type="submit" class="primary">Add / Update Target</button></div>
            </form>
          </section>
        </div>
        """,
        notice=notice,
        error=error,
    )


async def _parse_form(request: Request) -> dict[str, str]:
    body = (await request.body()).decode("utf-8", errors="replace")
    parsed = parse_qs(body, keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}


def _admin_query(notice: str | None = None, error: str | None = None) -> str:
    payload: dict[str, str] = {}
    if notice:
        payload["notice"] = notice
    if error:
        payload["error"] = error
    if not payload:
        return "/admin"
    return "/admin?" + urlencode(payload)


def _admin_authenticated(request: Request) -> bool:
    return bool(request.session.get("meshcore_admin_authenticated"))


def _admin_has_credentials(service: MeshcoreTCPBotService) -> bool:
  return bool(service.config.admin.password)


def _admin_login_matches(service: MeshcoreTCPBotService, password: str) -> bool:
  return bool(service.config.admin.password and password and compare_digest(password, service.config.admin.password))


def create_app(service: MeshcoreTCPBotService) -> FastAPI:
    app = FastAPI(title="MeshCore TCP Bot", version="0.1.0")
    app.add_middleware(SessionMiddleware, secret_key=service.config.admin.session_secret or "meshcore-admin-disabled")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/state")
    async def api_state() -> dict[str, object]:
        snapshot = service.snapshot()
        return {
            "started_at": snapshot.started_at.isoformat(),
            "endpoints": snapshot.endpoints,
            "nodes": snapshot.nodes,
            "messages": snapshot.messages,
            "diagnostics": snapshot.diagnostics,
            "identity": snapshot.identity,
            "persistence": snapshot.persistence,
            "management": snapshot.management,
        }

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return INDEX_HTML

    @app.get("/admin", response_class=HTMLResponse)
    async def admin_page(request: Request) -> str:
      notice = request.query_params.get("notice")
      error = request.query_params.get("error")
      if not _admin_has_credentials(service):
        return _render_admin_login(service, error="Admin credentials are not available.")
      if not _admin_authenticated(request):
        return _render_admin_login(service, notice=notice, error=error)
      return _render_admin_dashboard(service, notice=notice, error=error)

    @app.post("/admin/login")
    async def admin_login(request: Request) -> RedirectResponse:
      if not _admin_has_credentials(service):
        return RedirectResponse(_admin_query(error="Admin credentials are not configured."), status_code=303)
      form = await _parse_form(request)
      if not _admin_login_matches(service, form.get("password", "")):
        return RedirectResponse(_admin_query(error="Invalid admin password."), status_code=303)
      request.session["meshcore_admin_authenticated"] = True
      return RedirectResponse(_admin_query(notice="Logged in."), status_code=303)

    @app.post("/admin/logout")
    async def admin_logout(request: Request) -> RedirectResponse:
      request.session.clear()
      return RedirectResponse(_admin_query(notice="Logged out."), status_code=303)

    @app.post("/admin/settings/general")
    async def admin_general_settings(request: Request) -> RedirectResponse:
        if not _admin_authenticated(request):
            return RedirectResponse(_admin_query(error="Login required."), status_code=303)
        form = await _parse_form(request)
        try:
            await service.update_general_settings(
                {
                    "name": form.get("name", "").strip() or service.config.bot.name,
                    "reply_prefix": form.get("reply_prefix", "").strip() or service.config.bot.reply_prefix,
                    "command_prefix": form.get("command_prefix", "").strip() or service.config.bot.command_prefix,
                    "message_history_size": int(form.get("message_history_size", "200") or 200),
                    "signal_history_limit": int(form.get("signal_history_limit", "32") or 32),
                    "neighbor_snapshot_retention": int(form.get("neighbor_snapshot_retention", "96") or 96),
                    "private_messages_enabled": "private_messages_enabled" in form,
                    "private_message_auto_response": form.get("private_message_auto_response", "").strip(),
                }
            )
        except Exception as exc:
            return RedirectResponse(_admin_query(error=str(exc)), status_code=303)
        return RedirectResponse(_admin_query(notice="Bot settings updated."), status_code=303)

    @app.post("/admin/settings/commands")
    async def admin_command_settings(request: Request) -> RedirectResponse:
        if not _admin_authenticated(request):
            return RedirectResponse(_admin_query(error="Login required."), status_code=303)
        form = await _parse_form(request)
        updates = {}
        for name in service.command_settings:
            updates[name] = {
                "enabled": f"{name}_enabled" in form,
                "response_template": form.get(f"{name}_template", "").strip(),
            }
        await service.update_command_settings(updates)
        return RedirectResponse(_admin_query(notice="Command settings updated."), status_code=303)

    @app.post("/admin/channel/upsert")
    async def admin_channel_upsert(request: Request) -> RedirectResponse:
        if not _admin_authenticated(request):
            return RedirectResponse(_admin_query(error="Login required."), status_code=303)
        form = await _parse_form(request)
        try:
            await service.upsert_channel_config(
                {
                    "name": form.get("name", "").strip(),
                    "psk": form.get("psk", "").strip(),
                    "listen": "listen" in form,
                },
                old_name=form.get("old_name") or None,
            )
        except Exception as exc:
            return RedirectResponse(_admin_query(error=str(exc)), status_code=303)
        return RedirectResponse(_admin_query(notice="Channel saved."), status_code=303)

    @app.post("/admin/channel/delete")
    async def admin_channel_delete(request: Request) -> RedirectResponse:
        if not _admin_authenticated(request):
            return RedirectResponse(_admin_query(error="Login required."), status_code=303)
        form = await _parse_form(request)
        try:
            await service.delete_channel_config(form.get("name", "").strip())
        except Exception as exc:
            return RedirectResponse(_admin_query(error=str(exc)), status_code=303)
        return RedirectResponse(_admin_query(notice="Channel deleted."), status_code=303)

    @app.post("/admin/endpoint/upsert")
    async def admin_endpoint_upsert(request: Request) -> RedirectResponse:
        if not _admin_authenticated(request):
            return RedirectResponse(_admin_query(error="Login required."), status_code=303)
        form = await _parse_form(request)
        try:
            await service.upsert_endpoint_config(
                {
                    "name": form.get("name", "").strip(),
                    "raw_host": form.get("raw_host", "").strip(),
                    "raw_port": form.get("raw_port", "5002").strip() or "5002",
                    "enabled": "enabled" in form,
                    "console_host": form.get("console_host", "").strip(),
                    "console_port": form.get("console_port", "").strip(),
                    "console_mirror_host": form.get("console_mirror_host", "").strip(),
                    "console_mirror_port": form.get("console_mirror_port", "").strip(),
                    "latitude": form.get("latitude", "").strip(),
                    "longitude": form.get("longitude", "").strip(),
                },
                old_name=form.get("old_name") or None,
            )
        except Exception as exc:
            return RedirectResponse(_admin_query(error=str(exc)), status_code=303)
        return RedirectResponse(_admin_query(notice="Endpoint saved."), status_code=303)

    @app.post("/admin/endpoint/delete")
    async def admin_endpoint_delete(request: Request) -> RedirectResponse:
        if not _admin_authenticated(request):
            return RedirectResponse(_admin_query(error="Login required."), status_code=303)
        form = await _parse_form(request)
        try:
            await service.delete_endpoint_config(form.get("name", "").strip())
        except Exception as exc:
            return RedirectResponse(_admin_query(error=str(exc)), status_code=303)
        return RedirectResponse(_admin_query(notice="Endpoint deleted."), status_code=303)

    @app.post("/admin/target/upsert")
    async def admin_target_upsert(request: Request) -> RedirectResponse:
        if not _admin_authenticated(request):
            return RedirectResponse(_admin_query(error="Login required."), status_code=303)
        form = await _parse_form(request)
        try:
            await service.upsert_management_target(
                {
                    "name": form.get("name", "").strip(),
                    "endpoint_name": form.get("endpoint_name", "").strip(),
                    "target_identity_hex": form.get("target_identity_hex", "").strip(),
                    "target_hash_prefix": form.get("target_hash_prefix", "").strip(),
                    "guest_password": form.get("guest_password", ""),
                    "admin_password": form.get("admin_password", ""),
                    "prefer_role": form.get("prefer_role", "guest").strip(),
                    "enabled": "enabled" in form,
                    "notes": form.get("notes", "").strip(),
                },
                old_name=form.get("old_name") or None,
            )
        except Exception as exc:
            return RedirectResponse(_admin_query(error=str(exc)), status_code=303)
        return RedirectResponse(_admin_query(notice="Target saved."), status_code=303)

    @app.post("/admin/target/delete")
    async def admin_target_delete(request: Request) -> RedirectResponse:
        if not _admin_authenticated(request):
            return RedirectResponse(_admin_query(error="Login required."), status_code=303)
        form = await _parse_form(request)
        try:
            await service.delete_management_target(form.get("name", "").strip())
        except Exception as exc:
            return RedirectResponse(_admin_query(error=str(exc)), status_code=303)
        return RedirectResponse(_admin_query(notice="Target deleted."), status_code=303)

    @app.post("/admin/identity/regenerate")
    async def admin_identity_regenerate(request: Request) -> RedirectResponse:
      if not _admin_authenticated(request):
        return RedirectResponse(_admin_query(error="Login required."), status_code=303)
      form = await _parse_form(request)
      if form.get("confirm", "").strip().upper() != "REGENERATE":
        return RedirectResponse(_admin_query(error="Type REGENERATE to confirm key rotation."), status_code=303)
      if not _admin_login_matches(service, form.get("password", "")):
        return RedirectResponse(_admin_query(error="Correct admin password is required for key rotation."), status_code=303)
      try:
        await service.regenerate_identity()
      except Exception as exc:
        return RedirectResponse(_admin_query(error=str(exc)), status_code=303)
      return RedirectResponse(_admin_query(notice="Bot identity regenerated."), status_code=303)

    return app