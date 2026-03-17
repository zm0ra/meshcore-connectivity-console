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
      --bg: #e8ece7;
      --panel: rgba(248, 250, 248, 0.96);
      --panel-strong: #ffffff;
      --section: rgba(21, 33, 42, 0.045);
      --ink: #15212a;
      --muted: #6a7883;
      --line: rgba(21, 33, 42, 0.1);
      --line-strong: rgba(21, 33, 42, 0.16);
      --green: #2e8b57;
      --blue: #2c71d1;
      --red: #c64a3d;
      --yellow: #cfaa38;
      --orange: #db7d31;
      --unknown: #98a4ad;
      --shadow: 0 20px 48px rgba(21, 33, 42, 0.14);
      --shadow-soft: 0 8px 22px rgba(21, 33, 42, 0.08);
    }
    html, body {
      margin: 0;
      height: 100%;
      background: var(--bg);
      color: var(--ink);
      font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      font-variant-numeric: tabular-nums;
      -webkit-text-size-adjust: 100%;
    }
    body {
      overflow: hidden;
    }
    #app {
      position: relative;
      width: 100%;
      height: 100%;
      min-height: 100dvh;
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
      backdrop-filter: blur(10px);
    }
    #sidebar {
      top: 16px;
      right: 16px;
      bottom: 16px;
      width: min(438px, calc(100vw - 32px));
      border-radius: 24px;
      display: grid;
      grid-template-rows: auto auto 1fr auto;
      overflow: hidden;
      background: rgba(246, 248, 246, 0.98);
      border-color: rgba(21, 33, 42, 0.08);
    }
    .sheet-toggle {
      display: none;
      width: 100%;
      border: 0;
      border-bottom: 1px solid var(--line);
      background: transparent;
      padding: 8px 14px 6px;
      cursor: pointer;
      text-align: center;
      font: inherit;
      color: var(--muted);
    }
    .sheet-toggle span {
      display: inline-block;
      vertical-align: middle;
    }
    .sheet-handle {
      width: 42px;
      height: 5px;
      border-radius: 999px;
      background: rgba(21, 33, 42, 0.18);
      box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.8);
    }
    .sheet-label {
      display: none;
      margin-left: 8px;
      font-size: 0.72rem;
      letter-spacing: 0.04em;
      text-transform: uppercase;
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
      padding: 14px 16px 12px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(255, 255, 255, 0.84), rgba(248, 250, 248, 0.72));
    }
    .summary-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 6px;
    }
    .summary-card {
      padding: 10px 8px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.88);
      box-shadow: var(--shadow-soft);
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
      padding: 12px 14px 16px;
    }
    .list-toolbar {
      display: grid;
      gap: 10px;
      margin: 0 0 12px;
      padding: 14px;
      border: 1px solid rgba(21, 33, 42, 0.08);
      border-radius: 18px;
      background: var(--section);
    }
    .list-toolbar label {
      color: var(--muted);
      font-size: 0.7rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .toolbar-cluster {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: space-between;
    }
    .toolbar-meta {
      display: flex;
      align-items: center;
      justify-content: flex-start;
      gap: 8px;
      flex-wrap: wrap;
    }
    .toolbar-meta-group {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .toolbar-toggle-button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 36px;
      padding: 8px 12px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.92);
      color: var(--muted);
      font: inherit;
      font-size: 0.74rem;
      font-weight: 600;
      cursor: pointer;
      white-space: nowrap;
    }
    .toolbar-toggle-button.active {
      background: rgba(44, 113, 209, 0.14);
      border-color: rgba(44, 113, 209, 0.16);
      color: var(--ink);
    }
    .toolbar-note {
      color: var(--muted);
      font-size: 0.74rem;
      line-height: 1.35;
    }
    .toolbar-head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
    }
    .toolbar-head-main {
      display: grid;
      gap: 6px;
      min-width: 0;
    }
    .toolbar-head-actions {
      display: inline-flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      flex-wrap: wrap;
      flex: 0 0 auto;
    }
    .toolbar-title {
      font-size: 0.98rem;
      line-height: 1.1;
      letter-spacing: -0.01em;
    }
    .toolbar-subtitle {
      color: var(--muted);
      font-size: 0.76rem;
      line-height: 1.32;
    }
    .primary-toggle,
    .secondary-toggle,
    .filter-toggle {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 3px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.6);
      flex-wrap: wrap;
    }
    .primary-toggle {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 6px;
      padding: 6px;
      border-radius: 16px;
      background: rgba(21, 33, 42, 0.06);
    }
    .secondary-toggle,
    .filter-toggle {
      margin-bottom: 8px;
    }
    .segmented-button {
      border: 0;
      border-radius: 12px;
      background: transparent;
      color: var(--muted);
      padding: 8px 12px;
      font: inherit;
      font-size: 0.77rem;
      font-weight: 600;
      cursor: pointer;
      white-space: nowrap;
    }
    .segmented-button.active {
      background: rgba(44, 113, 209, 0.18);
      color: var(--ink);
      box-shadow: inset 0 0 0 1px rgba(44, 113, 209, 0.16), 0 1px 0 rgba(255, 255, 255, 0.9);
    }
    .segmented-button:disabled,
    .segmented-button.disabled {
      opacity: 0.44;
      color: var(--muted);
      cursor: not-allowed;
      box-shadow: none;
    }
    .mobile-view-toggle {
      display: none;
      align-items: center;
      gap: 4px;
      padding: 3px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.6);
    }
    .view-button {
      border: 0;
      border-radius: 999px;
      background: transparent;
      color: var(--muted);
      padding: 4px 9px;
      font: inherit;
      font-size: 0.7rem;
      cursor: pointer;
    }
    .view-button.active {
      background: rgba(44, 113, 209, 0.14);
      color: var(--ink);
    }
    .sort-select {
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.92);
      color: var(--ink);
      padding: 8px 10px;
      font: inherit;
      font-size: 0.76rem;
    }
    .lang-toggle {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 4px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.82);
    }
    .lang-button {
      border: 0;
      border-radius: 999px;
      background: transparent;
      color: var(--muted);
      padding: 4px 9px;
      font: inherit;
      font-size: 0.7rem;
      cursor: pointer;
    }
    .lang-button.active {
      background: rgba(44, 113, 209, 0.14);
      color: var(--ink);
    }
    .toolbar-head .lang-toggle {
      flex: 0 0 auto;
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
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.94);
      box-shadow: var(--shadow-soft);
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
      background: rgba(255, 255, 255, 0.84);
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
      background: rgba(255, 255, 255, 0.88);
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
    .panel-stack {
      display: grid;
      gap: 10px;
    }
    .panel-section {
      display: grid;
      gap: 8px;
      padding: 10px;
      border: 1px solid rgba(21, 33, 42, 0.08);
      border-radius: 16px;
      background: rgba(21, 33, 42, 0.03);
    }
    .panel-card {
      padding: 10px 11px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.94);
      box-shadow: none;
    }
    .panel-card strong {
      display: block;
      font-size: 0.84rem;
      line-height: 1.15;
    }
    .panel-card span {
      display: block;
      margin-top: 4px;
      color: var(--muted);
      font-size: 0.72rem;
      line-height: 1.3;
    }
    .panel-section-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: -2px;
    }
    .panel-section-title {
      font-size: 0.7rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
    }
    .panel-section-note {
      color: var(--muted);
      font-size: 0.69rem;
      line-height: 1.2;
      text-align: right;
    }
    .answer-strip {
      display: grid;
      gap: 8px;
      padding: 12px;
      border: 1px solid rgba(21, 33, 42, 0.08);
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.96);
    }
    .answer-head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
    }
    .answer-title {
      display: grid;
      gap: 3px;
      min-width: 0;
    }
    .answer-title strong {
      display: block;
      font-size: 0.94rem;
      line-height: 1.1;
    }
    .answer-title span {
      display: block;
      color: var(--muted);
      font-size: 0.72rem;
      line-height: 1.24;
    }
    .answer-kicker {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 24px;
      padding: 4px 9px;
      border-radius: 999px;
      background: rgba(21, 33, 42, 0.05);
      color: var(--muted);
      font-size: 0.67rem;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      white-space: nowrap;
    }
    .answer-kicker.alert {
      background: rgba(198, 74, 61, 0.1);
      color: var(--red);
    }
    .answer-metrics {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .answer-stat {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      min-height: 26px;
      padding: 4px 10px;
      border-radius: 999px;
      border: 1px solid rgba(21, 33, 42, 0.08);
      background: rgba(21, 33, 42, 0.04);
      font-size: 0.69rem;
      font-weight: 600;
      letter-spacing: 0.03em;
      white-space: nowrap;
    }
    .answer-stat strong {
      font-size: 0.76rem;
      line-height: 1;
    }
    .answer-state {
      color: var(--ink);
      font-size: 0.79rem;
      line-height: 1.28;
    }
    .answer-state.muted {
      color: var(--muted);
    }
    .relation-grid,
    .route-result-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
    }
    .route-result-grid {
      grid-template-columns: repeat(2, minmax(0, 1fr));
      align-items: stretch;
    }
    .relation-card,
    .route-card {
      padding: 10px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.94);
      box-shadow: none;
    }
    .route-card {
      display: grid;
      align-content: start;
      gap: 8px;
      min-height: 0;
    }
    .relation-card strong,
    .route-card strong {
      display: block;
      font-size: 0.86rem;
      line-height: 1.1;
    }
    .relation-card span,
    .route-card span {
      display: block;
      margin-top: 3px;
      color: var(--muted);
      font-size: 0.68rem;
    }
    .relation-list {
      display: grid;
      gap: 5px;
    }
    .relation-item {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      align-items: start;
      gap: 8px;
      padding: 8px 10px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.94);
      box-shadow: none;
    }
    .relation-main {
      min-width: 0;
      display: grid;
      gap: 2px;
    }
    .relation-main strong {
      font-size: 0.78rem;
      line-height: 1.2;
    }
    .relation-main span {
      color: var(--muted);
      font-size: 0.68rem;
      line-height: 1.18;
    }
    .relation-badges {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      flex-wrap: wrap;
      justify-content: flex-end;
      flex: 0 0 auto;
    }
    .direction-chip,
    .stale-chip {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 58px;
      padding: 3px 8px;
      border-radius: 999px;
      font-size: 0.67rem;
      font-weight: 600;
      line-height: 1.2;
    }
    .direction-chip {
      background: rgba(46, 139, 87, 0.12);
      color: var(--ink);
    }
    .stale-chip {
      background: rgba(198, 74, 61, 0.12);
      color: var(--red);
    }
    .route-controls {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
      align-items: end;
    }
    .route-control-bar {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
      align-items: stretch;
    }
    .route-picker-note {
      color: var(--muted);
      font-size: 0.72rem;
      line-height: 1.22;
      text-align: left;
    }
    .route-picker-note strong {
      color: var(--ink);
      font-size: 0.74rem;
    }
    .route-endpoint {
      display: grid;
      grid-template-columns: 34px minmax(0, 1fr);
      align-items: center;
      gap: 6px;
      padding: 10px 12px;
      min-height: 0;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.94);
      box-shadow: none;
      text-align: left;
      cursor: pointer;
      font: inherit;
      color: var(--ink);
    }
    .route-endpoint.active {
      border-color: rgba(44, 113, 209, 0.24);
      box-shadow: inset 0 0 0 1px rgba(44, 113, 209, 0.16), var(--shadow-soft);
    }
    .route-endpoint.route-endpoint-target.active {
      border-color: rgba(207, 170, 56, 0.28);
      box-shadow: inset 0 0 0 1px rgba(207, 170, 56, 0.18), var(--shadow-soft);
    }
    .route-endpoint-label {
      grid-row: 1 / span 2;
      grid-column: 1;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 34px;
      height: 34px;
      border-radius: 999px;
      background: rgba(44, 113, 209, 0.08);
      color: var(--muted);
      font-size: 0.67rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .route-endpoint-target .route-endpoint-label {
      background: rgba(207, 170, 56, 0.12);
    }
    .route-endpoint-name {
      grid-column: 2;
      display: block;
      font-size: 0.9rem;
      line-height: 1.15;
      word-break: break-word;
    }
    .field-stack {
      display: grid;
      gap: 5px;
    }
    .field-stack label {
      color: var(--muted);
      font-size: 0.68rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .route-select {
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.96);
      color: var(--ink);
      padding: 8px 10px;
      font: inherit;
      font-size: 0.78rem;
    }
    .route-status-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      flex-wrap: wrap;
    }
    .route-status-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 88px;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 0.68rem;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    .route-status-badge.ok {
      background: rgba(46, 139, 87, 0.14);
      color: var(--green);
    }
    .route-status-badge.no {
      background: rgba(198, 74, 61, 0.12);
      color: var(--red);
    }
    .route-meta {
      color: var(--muted);
      font-size: 0.72rem;
      line-height: 1.2;
    }
    .route-card-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      flex-wrap: wrap;
    }
    .route-direction-chip {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 54px;
      padding: 4px 10px;
      border-radius: 999px;
      font-size: 0.67rem;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      background: rgba(21, 33, 42, 0.06);
      color: var(--ink);
    }
    .route-direction-chip.forward {
      background: rgba(44, 113, 209, 0.12);
      color: var(--blue);
    }
    .route-direction-chip.backward {
      background: rgba(207, 170, 56, 0.16);
      color: #9c7b13;
    }
    .route-path {
      display: grid;
      gap: 6px;
      align-content: start;
      font-size: 0.74rem;
      margin-top: 0;
      justify-items: center;
    }
    .route-hop-row {
      width: 100%;
      display: flex;
      justify-content: center;
      position: relative;
    }
    .route-hop-row + .route-hop-row::before {
      content: '';
      position: absolute;
      top: -7px;
      left: 50%;
      width: 1px;
      height: 8px;
      background: rgba(21, 33, 42, 0.16);
      transform: translateX(-50%);
    }
    .route-step {
      padding: 5px 9px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.96);
      min-width: 0;
      text-align: center;
    }
    .route-empty {
      display: grid;
      gap: 4px;
      align-content: center;
      min-height: 92px;
      text-align: left;
    }
    .route-empty strong {
      font-size: 0.8rem;
    }
    .route-empty span {
      margin-top: 0;
      font-size: 0.71rem;
    }
    .compact-note {
      padding: 8px 10px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.92);
      color: var(--muted);
      font-size: 0.72rem;
      line-height: 1.28;
    }
    .compact-note strong {
      display: block;
      margin-bottom: 2px;
      color: var(--ink);
      font-size: 0.78rem;
      line-height: 1.2;
    }
    .mobile-map-stack {
      display: grid;
      gap: 8px;
    }
    .mobile-analysis-tabs {
      display: none;
    }
    .mobile-summary-card {
      display: grid;
      gap: 8px;
      padding: 10px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.95);
    }
    .mobile-summary-head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 8px;
    }
    .mobile-summary-title {
      display: grid;
      gap: 3px;
      min-width: 0;
    }
    .mobile-summary-title strong {
      font-size: 0.84rem;
      line-height: 1.15;
    }
    .mobile-summary-title span {
      color: var(--muted);
      font-size: 0.71rem;
      line-height: 1.2;
    }
    .mobile-summary-count {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 24px;
      padding: 4px 8px;
      border-radius: 999px;
      background: rgba(21, 33, 42, 0.05);
      color: var(--ink);
      font-size: 0.68rem;
      font-weight: 700;
      white-space: nowrap;
    }
    .mobile-relation-list {
      display: grid;
      gap: 5px;
    }
    .mobile-relation-button {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.92);
      color: inherit;
      padding: 8px 10px;
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: start;
      text-align: left;
      cursor: pointer;
      font: inherit;
    }
    .mobile-relation-button.active {
      border-color: rgba(44, 113, 209, 0.24);
      background: rgba(255, 255, 255, 0.98);
    }
    .mobile-relation-main {
      min-width: 0;
      display: grid;
      gap: 2px;
    }
    .mobile-relation-main strong {
      font-size: 0.78rem;
      line-height: 1.2;
    }
    .mobile-relation-main span {
      color: var(--muted);
      font-size: 0.68rem;
      line-height: 1.18;
    }
    .mobile-relation-meta {
      display: grid;
      gap: 4px;
      justify-items: end;
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
    .legend-arrow {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 18px;
      height: 18px;
      border-radius: 999px;
      border: 1px solid rgba(21, 33, 42, 0.08);
      background: rgba(255, 255, 255, 0.82);
      color: var(--ink);
      font-size: 0.8rem;
      line-height: 1;
    }
    .legend-line.dashed {
      border-top-style: dashed;
    }
    .leaflet-control-attribution {
      opacity: 0.7;
    }
    .node-label-icon,
    .link-label-icon,
    .line-arrow-icon {
      background: transparent;
      border: 0;
      transform: translate(-50%, -50%);
    }
    .line-arrow-chip {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 19px;
      height: 19px;
      border-radius: 999px;
      border: 1px solid rgba(21, 33, 42, 0.1);
      background: rgba(255, 255, 255, 0.9);
      box-shadow: 0 6px 14px rgba(21, 33, 42, 0.12);
      color: var(--ink);
      font-size: 13px;
      font-weight: 700;
      text-shadow: none;
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
    .node-label-chip.focused {
      border-color: rgba(21, 33, 42, 0.18);
      background: rgba(255, 255, 255, 0.92);
      box-shadow: 0 10px 24px rgba(21, 33, 42, 0.14);
    }
    .node-label-chip.active-peer {
      background: rgba(255, 255, 255, 0.86);
    }
    .signal-label-chip strong,
    .signal-label-chip span {
      display: block;
    }
    @media (max-width: 860px) {
      body {
        overflow: auto;
      }
      #app {
        display: flex;
        flex-direction: column;
        height: auto;
        min-height: 100dvh;
        overflow: visible;
        gap: 10px;
        padding-bottom: max(12px, env(safe-area-inset-bottom));
      }
      #map {
        position: relative;
        inset: auto;
        order: 1;
        flex: 0 0 clamp(180px, 26dvh, 240px);
        min-height: clamp(180px, 26dvh, 240px);
      }
      #sidebar {
        position: relative;
        order: 3;
        left: auto;
        right: auto;
        top: auto;
        bottom: auto;
        width: auto;
        max-height: none;
        margin: 0 12px 0;
        border-radius: 20px;
        background: rgba(248, 250, 248, 0.98);
      }
      #map-legend {
        position: relative;
        order: 2;
        left: auto;
        right: auto;
        top: auto;
        bottom: auto;
        max-width: none;
        margin: 0 12px;
        padding: 10px 12px;
        border-radius: 16px;
        font-size: 0.7rem;
      }
      .summary-strip {
        padding: 14px 12px 10px;
      }
      .summary-grid {
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 8px;
      }
      .summary-card {
        padding: 8px 4px;
      }
      .summary-card strong {
        font-size: 0.78rem;
      }
      .summary-card span {
        font-size: 0.6rem;
      }
      .list-shell {
        padding: 12px 12px 18px;
      }
      .list-toolbar {
        gap: 8px;
        padding: 12px;
      }
      .toolbar-cluster {
        justify-content: space-between;
      }
      .mobile-view-toggle {
        display: inline-flex;
      }
      .mobile-analysis-tabs {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
      }
      .primary-toggle,
      .secondary-toggle,
      .filter-toggle {
        width: 100%;
        justify-content: flex-start;
      }
      .primary-toggle {
        grid-template-columns: 1fr 1fr;
      }
      .toolbar-meta {
        flex-direction: column;
        align-items: stretch;
      }
      .toolbar-head {
        align-items: stretch;
      }
      .toolbar-head-actions {
        justify-content: space-between;
      }
      .toolbar-meta-group {
        justify-content: space-between;
      }
      .sort-select {
        min-height: 38px;
        font-size: 0.94rem;
        padding: 7px 12px;
      }
      .lang-button {
        min-height: 34px;
        padding: 5px 12px;
        font-size: 0.82rem;
      }
      .node-row-button {
        gap: 10px;
        padding: 11px 11px;
      }
      .node-name {
        white-space: normal;
        overflow: visible;
        text-overflow: clip;
        font-size: 0.82rem;
      }
      .node-age {
        font-size: 0.78rem;
      }
      .node-state-tag {
        align-self: start;
        font-size: 0.76rem;
      }
      .node-expand {
        padding: 0 11px 12px;
      }
      .detail-grid {
        grid-template-columns: 1fr;
      }
      .detail-cell {
        font-size: 0.76rem;
      }
      .expand-head {
        align-items: flex-start;
        flex-direction: column;
      }
      .neighbor-table {
        display: block;
        overflow-x: auto;
        white-space: nowrap;
      }
      .relation-grid,
      .route-result-grid,
      .route-control-bar,
      .route-controls {
        grid-template-columns: 1fr;
      }
      .relation-item {
        grid-template-columns: 1fr;
      }
      .relation-badges {
        justify-content: flex-start;
      }
      .chart-head {
        align-items: flex-start;
        flex-direction: column;
      }
      .chart-meta {
        white-space: normal;
      }
      .legend-group + .legend-group {
        margin-top: 12px;
      }
      #map-legend .legend-row {
        display: grid;
        grid-template-columns: auto 1fr;
        align-items: center;
        column-gap: 8px;
      }
      .leaflet-left .leaflet-control {
        margin-left: 10px;
      }
      .leaflet-top .leaflet-control {
        margin-top: 10px;
      }
    }
    @media (max-width: 860px) and (orientation: portrait) {
      #app {
        display: block;
        height: 100dvh;
        min-height: 100dvh;
        overflow: hidden;
        padding-bottom: 0;
      }
      #map {
        position: absolute;
        inset: 0;
        display: block;
        min-height: auto;
        height: auto;
      }
      #sidebar {
        position: absolute;
        left: 10px;
        right: 10px;
        top: auto;
        bottom: max(10px, env(safe-area-inset-bottom));
        width: auto;
        height: min(34dvh, 280px);
        max-height: min(34dvh, 280px);
        margin: 0;
        overflow: hidden;
        border-radius: 18px;
        z-index: 1200;
        transition: height 180ms ease, max-height 180ms ease, transform 180ms ease;
      }
      #sidebar.sheet-collapsed {
        height: min(18dvh, 148px);
        max-height: min(18dvh, 148px);
      }
      #sidebar.sheet-expanded {
        height: min(74dvh, 640px);
        max-height: min(74dvh, 640px);
      }
      .sheet-toggle {
        display: block;
      }
      .sheet-label {
        display: inline-block;
      }
      #map-legend {
        display: none;
      }
      .summary-strip {
        display: none;
      }
      .list-shell {
        padding: 8px 10px 10px;
        overflow: auto;
        overscroll-behavior: contain;
      }
      .list-toolbar {
        margin: 0 0 8px;
      }
      .section-heading {
        margin-top: 6px;
      }
    }
    @media (max-width: 520px) {
      #map {
        flex-basis: clamp(150px, 22dvh, 200px);
        min-height: clamp(150px, 22dvh, 200px);
      }
      #sidebar {
        margin: 0 10px 0;
      }
      #map-legend {
        margin: 0 10px;
        font-size: 0.66rem;
      }
      .summary-strip {
        padding: 12px 10px 8px;
      }
      .list-shell {
        padding: 10px 10px 16px;
      }
    }
  </style>
</head>
<body>
  <div id=\"app\">
    <div id=\"map\"></div>
    <div id=\"map-legend\" class=\"overlay\"></div>
    <aside id=\"sidebar\" class=\"overlay\">
      <button id=\"sheet-toggle\" class=\"sheet-toggle\" type=\"button\" aria-expanded=\"false\"><span class=\"sheet-handle\"></span><span class=\"sheet-label\"></span></button>
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
    const LINK_STALE_SECONDS = 6 * 60 * 60;
    const LOW_ZOOM_LABEL_THRESHOLD = 10;
    const HIGH_ZOOM_LABEL_THRESHOLD = 12;
    const MAX_COLLISION_LABELS = 18;
    const TRANSLATIONS = {
      pl: {
        unknown: 'brak',
        legendRepeaters: 'Repeatery',
        legendLinks: 'Połączenia',
        legendDataAvailable: 'dane dostępne',
        legendKnownNoData: 'znany / bez pobranych danych',
        legendInactive: 'nieaktywny > 24h',
        legendStrong: 'mocne',
        legendMedium: 'średnie',
        legendWeak: 'słabe',
        legendVeryWeak: 'bardzo słabe',
        legendDashed: 'stare dane',
        legendArrow: 'kierunek',
        summaryKnown: 'znane',
        summaryWithData: 'z danymi',
        summaryPending: 'oczekujące',
        summaryInactive: 'nieaktywne',
        archivedToggle: '>24h',
        archivedToggleCount: (count) => `>24h ${count}`,
        answerSelectedRepeater: 'Wybrany repeater',
        mobileMapTitle: 'Mapa relacji',
        mobileMapEmpty: 'Wybierz repeater, aby pokazać relacje na mapie.',
        mobileMapVisible: 'widoczne',
        mobileMapListTitle: 'Najbliższe relacje',
        mobileMapNoRows: 'Brak relacji dla tego trybu.',
        mobileMapPickRepeater: 'Wybierz repeater i tryb kierunku.',
        mobileMapDirectionOut: 'Na mapie: Widzę',
        mobileMapDirectionIn: 'Na mapie: Mnie widzą',
        mobileAnalysisWidze: 'Widzę',
        mobileAnalysisWidza: 'Mnie widzą',
        mobileAnalysisMutual: '2-way',
        mobileAnalysisRoute: 'Trasa',
        connectivityStateOut: (count) => `${count} bezpośrednich relacji wychodzących.`,
        connectivityStateIn: (count) => `${count} repeaterów widzi ten punkt.`,
        connectivityStateMutual: (count) => `${count} relacji wzajemnych.`,
        connectivityStateNoOwnData: 'Brak własnych danych sąsiedztwa. Dostępne tylko relacje inbound.',
        connectivityStateNoVisible: 'Brak relacji dla bieżącego widoku.',
        routeStateIdle: 'Wybierz A i B, aby porównać obie strony niezależnie.',
        routeStateReady: 'Wyniki A->B i B->A są liczone oddzielnie.',
        routeStateSameNode: 'A i B muszą wskazywać różne punkty.',
        routeResultsTitle: 'Wynik trasy',
        statusData: 'dane',
        statusNoData: 'brak danych',
        statusInactive: 'nieaktywny',
        probeFailedAfterData: 'nieudane po zapisaniu danych',
        probeDataSaved: 'dane zapisane',
        probePending: 'oczekuje',
        signalMissing: 'sygnał: b/d',
        distanceMissing: 'dyst: -',
        distancePrefix: 'dyst',
        lastAdvertLabel: 'ostatni advert',
        chartHistory: 'historia',
        chartLatest: 'ostatnio',
        chartSNRHistory: 'historia SNR',
        chartNow: 'teraz',
        emptySelectRepeater: 'Wybierz repeater, aby obejrzeć jego bezpośrednich sąsiadów.',
        emptySelectNeighbor: 'Wybierz wiersz sąsiada, aby obejrzeć historię sygnału.',
        emptyNoNeighborLinks: 'Dla tego repeatera nie ma jeszcze zapisanych połączeń sąsiedzkich.',
        emptyNoOtherRepeaters: 'Brak innych repeaterów.',
        inspection: 'Inspekcja',
        clearFocus: 'Wyczyść fokus',
        role: 'Rola',
        lastAdvert: 'Ostatni advert',
        lastData: 'Ostatnie dane',
        lastSuccessfulProbe: 'Ostatnie udane pobranie',
        lastProbeResult: 'Wynik ostatniej próby',
        lastProbeAttempt: 'Ostatnia próba',
        directNeighbors: 'Bezpośredni sąsiedzi',
        neighbor: 'Sąsiad',
        lastSeen: 'Ostatnio widziany',
        signal: 'Sygnał',
        distance: 'Dystans',
        selectedRepeater: 'Wybrany repeater',
        otherRepeaters: 'Pozostałe repeatery',
        repeaters: 'Repeatery',
        sortLabel: 'Sortowanie',
        sortLastAdvert: 'ostatni advert',
        sortLastData: 'ostatnie dane',
        sortAlphabetical: 'alfabetycznie',
        viewMap: 'Mapa',
        viewList: 'Lista',
        viewLabel: 'Widok',
        panelMap: 'Mapa',
        panelConnectivity: 'Łączność',
        panelRoute: 'Trasa',
        panelAnalysis: 'Analiza',
        focusRepeater: 'Fokus',
        relationModeOut: 'Widzę',
        relationModeIn: 'Mnie widzą',
        relationModeMutual: '2-way',
        relationFilterAll: 'Wszystkie',
        relationFilterTwoWay: '2-way',
        relationFilterOut: 'Out',
        relationFilterIn: 'In',
        relationDirectOut: 'bezposrednio widze',
        relationDirectIn: 'bezposrednio widza',
        relationNodeSees: (name) => `${name} widzi`,
        relationNodeSeenBy: (name) => `${name} widziany przez`,
        relationNodeMutual: (name) => `${name} 2-way`,
        connectivityHint: 'Wybierz repeater.',
        connectivitySelect: 'Repeater',
        connectivityVisible: 'Widoczne relacje',
        connectivityCountShort: 'rel.',
        connectivityNoRows: 'Brak relacji dla wybranego widoku.',
        connectivitySummaryTitle: 'Podsumowanie',
        connectivityVisibleTitle: 'Widoczne relacje',
        connectivityFilterHint: 'W warstwie porównania pokazuj tylko jeden typ.',
        connectivitySummaryOut: 'widze',
        connectivitySummaryIn: 'widza',
        connectivitySummaryMutual: 'wzajemne',
        connectivitySummaryOneWay: '1-way',
        connectivityTablePeer: 'Repeater',
        connectivityTableType: 'Typ',
        connectivityTableOut: 'A->B',
        connectivityTableIn: 'B->A',
        connectivityTableAge: 'Ostatnio',
        connectivityTableSignal: 'SNR',
        relationTypeOut: 'ode mnie',
        relationTypeIn: 'do mnie',
        relationTypeMutual: '2-way',
        staleShort: 'stare',
        routeSource: 'Start',
        routeTarget: 'Cel',
        routeSwap: 'Zamien',
        routeForward: 'A->B',
        routeBackward: 'B->A',
        routePickHint: 'Wybierz z mapy',
        routeSelectedA: 'A',
        routeSelectedB: 'B',
        routeUnset: 'nie ustawiono',
        routeStatusYes: 'trasa jest',
        routeStatusNo: 'brak trasy',
        routeNoSelection: 'Ustaw A i B.',
        routeSameNode: 'Start i cel musza byc rozne.',
        routeNoPath: 'Brak trasy.',
        routeHopCount: 'hopow',
        routeUsesStale: 'uzyto starych linkow',
        routeFreshOnly: 'swieze linki',
        languageLabel: 'Język',
        sheetExpand: 'Rozwin',
        sheetCollapse: 'Zwin',
        toolbarMapTitle: 'Repeaters',
        toolbarMapSubtitle: 'Wybierz punkt na mapie lub z listy.',
        toolbarConnectivityTitle: 'Łączność',
        toolbarConnectivitySubtitle: 'Kto widzi kogo.',
        toolbarRouteTitle: 'Trasa',
        toolbarRouteSubtitle: 'Ustaw A i B.',
        routeTapTarget: 'Wybierz z mapy A albo B.',
        routeTapTargetSource: 'Kliknij mapę, aby ustawić A.',
        routeTapTargetTarget: 'Kliknij mapę, aby ustawić B.',
        routeTapTargetReady: 'Kliknij mapę, aby zmienić A albo B.',
        roleDefault: 'Repeater',
        kindSignal: 'sygnał',
        noDataShort: 'b/d',
        storedSamples: (count) => `Dla tego połączenia zapisano na razie ${count} prób${count === 1 ? 'kę' : count < 5 ? 'ki' : 'ek'}. Wykres pojawi się po zebraniu co najmniej 2 próbek.`,
        agoSeconds: (count) => `${count}s temu`,
        agoMinutes: (count) => `${count} min temu`,
        agoHours: (count) => `${count} h temu`,
        agoDays: (count) => `${count} d temu`,
      },
      en: {
        unknown: 'unknown',
        legendRepeaters: 'Repeaters',
        legendLinks: 'Links',
        legendDataAvailable: 'data available',
        legendKnownNoData: 'known / no data fetched',
        legendInactive: 'inactive > 24h',
        legendStrong: 'strong',
        legendMedium: 'medium',
        legendWeak: 'weak',
        legendVeryWeak: 'very weak',
        legendDashed: 'stale data',
        legendArrow: 'direction',
        summaryKnown: 'known',
        summaryWithData: 'with data',
        summaryPending: 'pending',
        summaryInactive: 'inactive',
        archivedToggle: '>24h',
        archivedToggleCount: (count) => `>24h ${count}`,
        answerSelectedRepeater: 'Selected repeater',
        mobileMapTitle: 'Relation map',
        mobileMapEmpty: 'Select a repeater to show relations on the map.',
        mobileMapVisible: 'visible',
        mobileMapListTitle: 'Closest relations',
        mobileMapNoRows: 'No relations for this mode.',
        mobileMapPickRepeater: 'Select a repeater and direction mode.',
        mobileMapDirectionOut: 'Map: Out',
        mobileMapDirectionIn: 'Map: Seen by',
        mobileAnalysisWidze: 'Out',
        mobileAnalysisWidza: 'Seen by',
        mobileAnalysisMutual: '2-way',
        mobileAnalysisRoute: 'Route',
        connectivityStateOut: (count) => `${count} direct outgoing relations.`,
        connectivityStateIn: (count) => `${count} repeaters can see this node.`,
        connectivityStateMutual: (count) => `${count} mutual relations.`,
        connectivityStateNoOwnData: 'No own neighbor snapshot. Only inbound relations are available.',
        connectivityStateNoVisible: 'No relations match the current view.',
        routeStateIdle: 'Select A and B to compare both directions independently.',
        routeStateReady: 'A->B and B->A are calculated separately.',
        routeStateSameNode: 'A and B must point to different nodes.',
        routeResultsTitle: 'Route result',
        statusData: 'data',
        statusNoData: 'no data',
        statusInactive: 'inactive',
        probeFailedAfterData: 'failed after data snapshot',
        probeDataSaved: 'data saved',
        probePending: 'pending',
        signalMissing: 'signal: n/a',
        distanceMissing: 'dist: -',
        distancePrefix: 'dist',
        lastAdvertLabel: 'last advert',
        chartHistory: 'history',
        chartLatest: 'latest',
        chartSNRHistory: 'SNR history',
        chartNow: 'now',
        emptySelectRepeater: 'Select a repeater to inspect its direct neighbors.',
        emptySelectNeighbor: 'Select a neighbor row to inspect signal history.',
        emptyNoNeighborLinks: 'No stored neighbor links are available yet for this repeater.',
        emptyNoOtherRepeaters: 'No other repeaters available.',
        inspection: 'Inspection',
        clearFocus: 'Clear focus',
        role: 'Role',
        lastAdvert: 'Last advert',
        lastData: 'Last data',
        lastSuccessfulProbe: 'Last successful fetch',
        lastProbeResult: 'Last probe result',
        lastProbeAttempt: 'Last probe attempt',
        directNeighbors: 'Direct neighbors',
        neighbor: 'Neighbor',
        lastSeen: 'Last seen',
        signal: 'Signal',
        distance: 'Distance',
        selectedRepeater: 'Selected repeater',
        otherRepeaters: 'Other repeaters',
        repeaters: 'Repeaters',
        sortLabel: 'Sort',
        sortLastAdvert: 'last advert',
        sortLastData: 'last data fetch',
        sortAlphabetical: 'alphabetical',
        viewMap: 'Map',
        viewList: 'List',
        viewLabel: 'View',
        panelMap: 'Map',
        panelConnectivity: 'Connectivity',
        panelRoute: 'Route',
        panelAnalysis: 'Analysis',
        focusRepeater: 'Focus',
        relationModeOut: 'Out',
        relationModeIn: 'Seen by',
        relationModeMutual: '2-way',
        relationFilterAll: 'All',
        relationFilterTwoWay: '2-way',
        relationFilterOut: 'Out',
        relationFilterIn: 'In',
        relationDirectOut: 'directly seen',
        relationDirectIn: 'directly seeing me',
        relationNodeSees: (name) => `${name} sees`,
        relationNodeSeenBy: (name) => `${name} seen by`,
        relationNodeMutual: (name) => `${name} 2-way`,
        connectivityHint: 'Select a repeater.',
        connectivitySelect: 'Repeater',
        connectivityVisible: 'Visible relations',
        connectivityCountShort: 'rel.',
        connectivityNoRows: 'No relations match the current view.',
        connectivitySummaryTitle: 'Summary',
        connectivityVisibleTitle: 'Visible relations',
        connectivityFilterHint: 'Show one relation type at a time in compare mode.',
        connectivitySummaryOut: 'outgoing',
        connectivitySummaryIn: 'incoming',
        connectivitySummaryMutual: 'mutual',
        connectivitySummaryOneWay: 'one-way',
        connectivityTablePeer: 'Repeater',
        connectivityTableType: 'Type',
        connectivityTableOut: 'A->B',
        connectivityTableIn: 'B->A',
        connectivityTableAge: 'Last seen',
        connectivityTableSignal: 'SNR',
        relationTypeOut: 'from me',
        relationTypeIn: 'to me',
        relationTypeMutual: '2-way',
        staleShort: 'stale',
        routeSource: 'Source',
        routeTarget: 'Target',
        routeSwap: 'Swap',
        routeForward: 'A->B',
        routeBackward: 'B->A',
        routePickHint: 'Pick from map',
        routeSelectedA: 'A',
        routeSelectedB: 'B',
        routeUnset: 'not set',
        routeStatusYes: 'route found',
        routeStatusNo: 'no route',
        routeNoSelection: 'Set A and B.',
        routeSameNode: 'Source and target must be different.',
        routeNoPath: 'No route available.',
        routeHopCount: 'hops',
        routeUsesStale: 'stale links used',
        routeFreshOnly: 'fresh links',
        languageLabel: 'Language',
        sheetExpand: 'Expand',
        sheetCollapse: 'Collapse',
        toolbarMapTitle: 'Repeaters',
        toolbarMapSubtitle: 'Pick a node on the map or from the list.',
        toolbarConnectivityTitle: 'Connectivity',
        toolbarConnectivitySubtitle: 'Who sees whom.',
        toolbarRouteTitle: 'Route',
        toolbarRouteSubtitle: 'Set A and B.',
        routeTapTarget: 'Pick A or B from the map.',
        routeTapTargetSource: 'Click the map to set A.',
        routeTapTargetTarget: 'Click the map to set B.',
        routeTapTargetReady: 'Click the map to change A or B.',
        roleDefault: 'Repeater',
        kindSignal: 'signal',
        noDataShort: 'n/a',
        storedSamples: (count) => `Only ${count} stored sample${count === 1 ? '' : 's'} for this link so far. The history chart appears after at least 2 samples.`,
        agoSeconds: (count) => `${count}s ago`,
        agoMinutes: (count) => `${count}m ago`,
        agoHours: (count) => `${count}h ago`,
        agoDays: (count) => `${count}d ago`,
      },
    };
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
    let currentLanguage = localStorage.getItem('meshcoreDashboardLanguage') || 'pl';
    let currentPanel = localStorage.getItem('meshcoreDashboardPanel') || 'map';
    let connectivityDirection = localStorage.getItem('meshcoreDashboardConnectivityDirection') || 'out';
    let connectivityFilter = '2way';
    let showArchived = localStorage.getItem('meshcoreDashboardShowArchived') === 'true';
    let routeSourceId = null;
    let routeTargetId = null;
    let routeActiveEndpoint = 'source';
    let hasFitBounds = false;
    let pendingRefreshState = null;
    let sidebarSheetState = localStorage.getItem('meshcoreDashboardSheetState') || 'collapsed';

    function strings() {
      return TRANSLATIONS[currentLanguage] || TRANSLATIONS.pl;
    }

    function tr(key) {
      return strings()[key];
    }

    function trFormat(key, value) {
      const entry = tr(key);
      return typeof entry === 'function' ? entry(value) : entry;
    }

    function isSidebarInteractionActive() {
      const activeElement = document.activeElement;
      if (!activeElement) return false;
      if (!activeElement.closest || !activeElement.closest('#sidebar')) return false;
      const tagName = activeElement.tagName;
      return tagName === 'SELECT' || tagName === 'OPTION' || tagName === 'INPUT' || tagName === 'TEXTAREA';
    }

    function flushPendingRefresh() {
      if (!pendingRefreshState || isSidebarInteractionActive()) return;
      const state = pendingRefreshState;
      pendingRefreshState = null;
      render(state);
    }

    function syncSidebarSheetState() {
      const sidebar = document.getElementById('sidebar');
      const toggle = document.getElementById('sheet-toggle');
      if (!sidebar || !toggle) return;
      if (!isPortraitMobileView()) {
        sidebar.classList.remove('sheet-collapsed', 'sheet-expanded');
        toggle.setAttribute('aria-expanded', 'true');
        const label = toggle.querySelector('.sheet-label');
        if (label) label.textContent = '';
        return;
      }
      sidebar.classList.toggle('sheet-collapsed', sidebarSheetState === 'collapsed');
      sidebar.classList.toggle('sheet-expanded', sidebarSheetState !== 'collapsed');
      toggle.setAttribute('aria-expanded', sidebarSheetState === 'collapsed' ? 'false' : 'true');
      const label = toggle.querySelector('.sheet-label');
      if (label) label.textContent = sidebarSheetState === 'collapsed' ? tr('sheetExpand') : tr('sheetCollapse');
      localStorage.setItem('meshcoreDashboardSheetState', sidebarSheetState);
    }

    function toggleSidebarSheet() {
      sidebarSheetState = sidebarSheetState === 'collapsed' ? 'expanded' : 'collapsed';
      syncSidebarSheetState();
    }

    function setLanguage(language) {
      if (!TRANSLATIONS[language]) return;
      currentLanguage = language;
      localStorage.setItem('meshcoreDashboardLanguage', language);
      document.documentElement.lang = language;
      renderLegend();
      if (latestState) render(latestState);
    }

    function isPortraitMobileView() {
      return window.matchMedia('(max-width: 860px) and (orientation: portrait)').matches;
    }

    function isAnalysisPanel() {
      return currentPanel === 'connectivity' || currentPanel === 'route';
    }

    function applyMobileView() {
      if (!isPortraitMobileView()) {
        document.body.dataset.mobileView = 'split';
        window.requestAnimationFrame(() => map.invalidateSize(false));
        return;
      }
      const view = currentPanel === 'map' ? 'map' : 'list';
      document.body.dataset.mobileView = view;
      if (view === 'map') {
        window.requestAnimationFrame(() => map.invalidateSize(false));
      }
    }

    function setPanel(panel) {
      if (!['map', 'connectivity', 'route'].includes(panel)) return;
      currentPanel = panel;
      if (panel === 'route' && !routeSourceId && selectedSourceId) {
        routeSourceId = selectedSourceId;
      }
      if (isPortraitMobileView()) {
        sidebarSheetState = panel === 'map' ? 'collapsed' : 'expanded';
      }
      localStorage.setItem('meshcoreDashboardPanel', panel);
      applyMobileView();
      if (latestState) render(latestState);
    }

    function hasOwnNeighborData(node) {
      return Boolean(node?.last_data_at);
    }

    function setConnectivityDirection(direction) {
      if (!['out', 'in', 'mutual'].includes(direction)) return;
      const node = latestState ? selectedConnectivityNode(latestState) : null;
      if ((direction === 'out' || direction === 'mutual') && node && !hasOwnNeighborData(node)) {
        return;
      }
      connectivityDirection = direction;
      localStorage.setItem('meshcoreDashboardConnectivityDirection', direction);
      if (latestState) render(latestState);
    }

    function setShowArchived(value) {
      showArchived = Boolean(value);
      localStorage.setItem('meshcoreDashboardShowArchived', showArchived ? 'true' : 'false');
      if (latestState) render(latestState);
    }

    function renderLegend() {
      const legend = document.getElementById('map-legend');
      legend.innerHTML = `
        <div class="legend-group">
          <span class="legend-title">${tr('legendRepeaters')}</span>
          <div class="legend-row"><span class="legend-node" style="background:#2e8b57"></span><span>${tr('legendDataAvailable')}</span></div>
          <div class="legend-row"><span class="legend-node" style="background:#2c71d1"></span><span>${tr('legendKnownNoData')}</span></div>
          <div class="legend-row"><span class="legend-node" style="background:#c64a3d"></span><span>${tr('legendInactive')}</span></div>
        </div>
        <div class="legend-group">
          <span class="legend-title">${tr('legendLinks')}</span>
          <div class="legend-row"><span class="legend-line" style="border-top-color:#2e8b57"></span><span>${tr('legendStrong')}</span></div>
          <div class="legend-row"><span class="legend-line" style="border-top-color:#cfaa38"></span><span>${tr('legendMedium')}</span></div>
          <div class="legend-row"><span class="legend-line" style="border-top-color:#db7d31"></span><span>${tr('legendWeak')}</span></div>
          <div class="legend-row"><span class="legend-line" style="border-top-color:#c64a3d"></span><span>${tr('legendVeryWeak')}</span></div>
          <div class="legend-row"><span class="legend-arrow">➜</span><span>${tr('legendArrow')}</span></div>
          <div class="legend-row"><span class="legend-line dashed" style="border-top-color:#6a7883"></span><span>${tr('legendDashed')}</span></div>
        </div>
      `;
    }

    function formatWhen(value) {
      if (!value) return tr('unknown');
      return new Date(value).toLocaleString();
    }

    function formatShortWhen(value) {
      if (!value) return tr('unknown');
      return new Date(value).toLocaleString([], {
        year: 'numeric',
        month: 'short',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
      });
    }

    function timeAgo(value) {
      if (!value) return tr('unknown');
      const elapsed = Math.max(0, Date.now() - new Date(value).getTime());
      const seconds = Math.floor(elapsed / 1000);
      if (seconds < 60) return tr('agoSeconds')(seconds);
      if (seconds < 3600) return tr('agoMinutes')(Math.floor(seconds / 60));
      if (seconds < 86400) return tr('agoHours')(Math.floor(seconds / 3600));
      return tr('agoDays')(Math.floor(seconds / 86400));
    }

    function humanizeSeconds(value) {
      if (typeof value !== 'number' || !Number.isFinite(value)) return tr('unknown');
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
      const nodes = state.nodes || [];
      if (showArchived) return nodes;
      return nodes.filter((node) => !isInactive(node));
    }

    function archivedNodeCount(state) {
      return (state.nodes || []).filter((node) => isInactive(node)).length;
    }

    function normalizeVisibleSelections(state) {
      const visibleIds = new Set(relevantNodes(state).map((node) => node.identity_hex));
      if (selectedSourceId && !visibleIds.has(selectedSourceId)) {
        selectedSourceId = null;
        selectedNeighborId = null;
      }
      if (selectedNeighborId && !visibleIds.has(selectedNeighborId)) {
        selectedNeighborId = null;
      }
      if (routeSourceId && !visibleIds.has(routeSourceId)) {
        routeSourceId = null;
      }
      if (routeTargetId && !visibleIds.has(routeTargetId)) {
        routeTargetId = null;
      }
      if (hoveredNodeId && !visibleIds.has(hoveredNodeId)) {
        hoveredNodeId = null;
      }
    }

    function connectivityData(state) {
      const nodes = sortNodes(relevantNodes(state));
      const nodeIndex = new Map(nodes.map((node) => [node.identity_hex, node]));
      const edges = [];
      const pairSet = new Set();
      for (const link of (state.management?.map_links || [])) {
        if (!nodeIndex.has(link.source_identity_hex) || !nodeIndex.has(link.target_identity_hex)) continue;
        if (link.source_identity_hex === link.target_identity_hex) continue;
        const ageSeconds = typeof link.last_heard_seconds === 'number'
          ? link.last_heard_seconds
          : Math.max(0, Math.floor((Date.now() - new Date(link.collected_at).getTime()) / 1000));
        const edge = {
          ...link,
          age_seconds: ageSeconds,
          stale: ageSeconds > LINK_STALE_SECONDS,
          mutual: false,
        };
        pairSet.add(`${edge.source_identity_hex}|${edge.target_identity_hex}`);
        edges.push(edge);
      }
      for (const edge of edges) {
        edge.mutual = pairSet.has(`${edge.target_identity_hex}|${edge.source_identity_hex}`);
      }
      const relationMap = new Map(nodes.map((node) => [node.identity_hex, { outgoing: [], incoming: [], mutual: [], oneWayOutgoing: [], oneWayIncoming: [] }]));
      for (const edge of edges) {
        relationMap.get(edge.source_identity_hex)?.outgoing.push(edge);
        relationMap.get(edge.target_identity_hex)?.incoming.push(edge);
        if (edge.mutual) {
          relationMap.get(edge.source_identity_hex)?.mutual.push(edge);
        } else {
          relationMap.get(edge.source_identity_hex)?.oneWayOutgoing.push(edge);
          relationMap.get(edge.target_identity_hex)?.oneWayIncoming.push(edge);
        }
      }
      return {
        nodes,
        nodeIndex,
        edges,
        relationMap,
        summary: {
          directed: edges.length,
          mutual: edges.filter((edge) => edge.mutual).length / 2,
          oneWay: edges.filter((edge) => !edge.mutual).length,
          stale: edges.filter((edge) => edge.stale).length,
        },
      };
    }

    function selectedConnectivityNode(state) {
      const data = connectivityData(state);
      return data.nodeIndex.get(selectedSourceId) || null;
    }

    function relationRows(state, nodeId, filter = null) {
      if (!nodeId) return [];
      const data = connectivityData(state);
      const relations = data.relationMap.get(nodeId);
      if (!relations) return [];
      const peers = new Map();
      for (const edge of relations.outgoing) {
        const row = peers.get(edge.target_identity_hex) || { peerId: edge.target_identity_hex, outEdge: null, inEdge: null };
        row.outEdge = edge;
        peers.set(edge.target_identity_hex, row);
      }
      for (const edge of relations.incoming) {
        const row = peers.get(edge.source_identity_hex) || { peerId: edge.source_identity_hex, outEdge: null, inEdge: null };
        row.inEdge = edge;
        peers.set(edge.source_identity_hex, row);
      }
      return Array.from(peers.values()).map((row) => {
        const peerNode = data.nodeIndex.get(row.peerId);
        const relationType = row.outEdge && row.inEdge ? '2way' : row.outEdge ? 'out' : 'in';
        const freshestAge = Math.min(
          row.outEdge?.age_seconds ?? Number.POSITIVE_INFINITY,
          row.inEdge?.age_seconds ?? Number.POSITIVE_INFINITY,
        );
        return {
          ...row,
          peerName: peerNode?.name || row.peerId.slice(0, 8),
          relationType,
          freshestAge: Number.isFinite(freshestAge) ? freshestAge : null,
          stale: Boolean(row.outEdge?.stale || row.inEdge?.stale),
        };
      }).filter((row) => {
        if (!filter) return true;
        return row.relationType === filter;
      }).sort((left, right) => {
        const typeRank = { '2way': 0, out: 1, in: 2 };
        if (typeRank[left.relationType] !== typeRank[right.relationType]) {
          return typeRank[left.relationType] - typeRank[right.relationType];
        }
        return left.peerName.localeCompare(right.peerName);
      });
    }

    function directRelationRows(state, nodeId, direction) {
      if (!nodeId) return [];
      const data = connectivityData(state);
      const relations = data.relationMap.get(nodeId);
      if (!relations) return [];
      const edges = direction === 'out' ? relations.outgoing : relations.incoming;
      return edges.map((edge) => {
        const peerId = direction === 'out' ? edge.target_identity_hex : edge.source_identity_hex;
        const peerNode = data.nodeIndex.get(peerId);
        return {
          peerName: peerNode?.name || peerId.slice(0, 8),
          relationType: direction,
          stale: Boolean(edge.stale),
          metricText: lineSignalMetric(edge).label,
          ageText: humanizeSeconds(edge.age_seconds),
        };
      }).sort((left, right) => left.peerName.localeCompare(right.peerName));
    }

    function routePath(edges, sourceId, targetId) {
      if (!sourceId || !targetId || sourceId === targetId) return null;
      const adjacency = new Map();
      for (const edge of edges) {
        const bucket = adjacency.get(edge.source_identity_hex) || [];
        bucket.push(edge);
        adjacency.set(edge.source_identity_hex, bucket);
      }
      for (const bucket of adjacency.values()) {
        bucket.sort((left, right) => ((right.snr ?? -999) - (left.snr ?? -999)) || (left.age_seconds - right.age_seconds));
      }
      const queue = [[sourceId]];
      const visited = new Set([sourceId]);
      while (queue.length) {
        const path = queue.shift();
        const current = path[path.length - 1];
        if (current === targetId) return path;
        for (const edge of (adjacency.get(current) || [])) {
          if (visited.has(edge.target_identity_hex)) continue;
          visited.add(edge.target_identity_hex);
          queue.push(path.concat(edge.target_identity_hex));
        }
      }
      return null;
    }

    function buildRouteResult(state, sourceId, targetId) {
      const data = connectivityData(state);
      const freshEdges = data.edges.filter((edge) => !edge.stale);
      const freshPath = routePath(freshEdges, sourceId, targetId);
      const path = freshPath || routePath(data.edges, sourceId, targetId);
      if (!path) {
        return { path: null, usesStale: false };
      }
      return { path, usesStale: !freshPath };
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
      if (state === 'ok') return tr('statusData');
      if (state === 'missing') return tr('statusNoData');
      return tr('statusInactive');
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

    function overlayInsets(basePadding) {
      const insets = { top: basePadding, right: basePadding, bottom: basePadding, left: basePadding };
      const mapElement = document.getElementById('map');
      const sidebar = document.getElementById('sidebar');
      if (!mapElement || !sidebar) return insets;

      const mapRect = mapElement.getBoundingClientRect();
      const sidebarRect = sidebar.getBoundingClientRect();
      if (!mapRect.width || !mapRect.height || !sidebarRect.width || !sidebarRect.height) return insets;

      const horizontalMid = mapRect.left + (mapRect.width / 2);
      const verticalMid = mapRect.top + (mapRect.height / 2);
      const overlapRight = Math.max(0, mapRect.right - sidebarRect.left);
      const overlapLeft = Math.max(0, sidebarRect.right - mapRect.left);
      const overlapBottom = Math.max(0, mapRect.bottom - sidebarRect.top);
      const overlapTop = Math.max(0, sidebarRect.bottom - mapRect.top);

      if (sidebarRect.left >= horizontalMid - 40) {
        insets.right += overlapRight;
      } else if (sidebarRect.right <= horizontalMid + 40) {
        insets.left += overlapLeft;
      }

      if (sidebarRect.top >= verticalMid - 40) {
        insets.bottom += overlapBottom;
      } else if (sidebarRect.bottom <= verticalMid + 40) {
        insets.top += overlapTop;
      }

      return insets;
    }

    function offsetLatLngForInsets(latlng, zoom, insets) {
      const projected = map.project(latlng, zoom);
      const shifted = L.point(
        projected.x + ((insets.right - insets.left) / 2),
        projected.y + ((insets.bottom - insets.top) / 2),
      );
      return map.unproject(shifted, zoom);
    }

    function fitInitialBounds(bounds) {
      if (!bounds.length) return;
      const insets = overlayInsets(18);
      map.fitBounds(bounds, {
        paddingTopLeft: [insets.left, insets.top],
        paddingBottomRight: [insets.right, insets.bottom],
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
      const insets = overlayInsets(36);
      if (bounds.length > 1) {
        map.flyToBounds(bounds, {
          paddingTopLeft: [insets.left, insets.top],
          paddingBottomRight: [insets.right, insets.bottom],
          maxZoom: 12,
          duration: 0.6,
        });
        return;
      }
      const targetZoom = Math.max(map.getZoom(), 11);
      const centeredTarget = offsetLatLngForInsets([selectedNode.latitude, selectedNode.longitude], targetZoom, insets);
      map.flyTo(centeredTarget, targetZoom, { duration: 0.5 });
    }

    function fitNodeCollection(nodes, focusId = null) {
      const visible = nodes.filter((node) => isFiniteCoordinate(node.latitude, node.longitude));
      if (!visible.length) return;
      const bounds = visible.map((node) => [node.latitude, node.longitude]);
      if (bounds.length === 1) {
        const targetNode = visible[0];
        const insets = overlayInsets(36);
        const targetZoom = Math.max(map.getZoom(), 11);
        const centeredTarget = offsetLatLngForInsets([targetNode.latitude, targetNode.longitude], targetZoom, insets);
        map.flyTo(centeredTarget, targetZoom, { duration: 0.5 });
        return;
      }
      const insets = overlayInsets(30);
      map.flyToBounds(bounds, {
        paddingTopLeft: [insets.left, insets.top],
        paddingBottomRight: [insets.right, insets.bottom],
        maxZoom: focusId ? 12 : 10,
        duration: 0.6,
      });
    }

    function focusConnectivitySelection(state) {
      const data = connectivityData(state);
      const focusId = selectedSourceId;
      if (!focusId) return;
      const focusNode = data.nodeIndex.get(focusId);
      const canInspectOwnData = hasOwnNeighborData(focusNode);
      let visibleIds = new Set([focusId]);
      if (connectivityDirection === 'out' && canInspectOwnData) {
        for (const edge of data.edges.filter((edge) => edge.source_identity_hex === focusId)) {
          visibleIds.add(edge.target_identity_hex);
        }
      } else if (connectivityDirection === 'in') {
        for (const edge of data.edges.filter((edge) => edge.target_identity_hex === focusId)) {
          visibleIds.add(edge.source_identity_hex);
        }
      } else if (canInspectOwnData) {
        for (const edge of data.edges.filter((edge) => edge.source_identity_hex === focusId && edge.mutual)) {
          visibleIds.add(edge.target_identity_hex);
        }
      }
      fitNodeCollection(data.nodes.filter((node) => visibleIds.has(node.identity_hex)), focusId);
    }

    function focusRouteSelection(state) {
      const data = connectivityData(state);
      const ids = new Set([routeSourceId, routeTargetId].filter(Boolean));
      if (!ids.size) return;
      if (routeSourceId && routeTargetId && routeSourceId !== routeTargetId) {
        const forward = buildRouteResult(state, routeSourceId, routeTargetId);
        const backward = buildRouteResult(state, routeTargetId, routeSourceId);
        for (const identityHex of (forward.path || [])) ids.add(identityHex);
        for (const identityHex of (backward.path || [])) ids.add(identityHex);
      }
      fitNodeCollection(data.nodes.filter((node) => ids.has(node.identity_hex)), routeSourceId || routeTargetId);
    }

    function renderSummary(state) {
      const nodes = relevantNodes(state);
      const html = [
        { label: tr('summaryKnown'), value: nodes.length },
        { label: tr('summaryWithData'), value: nodes.filter((node) => !isInactive(node) && node.data_fetch_ok).length },
        { label: tr('summaryPending'), value: nodes.filter((node) => !isInactive(node) && !node.data_fetch_ok).length },
        { label: tr('summaryInactive'), value: nodes.filter((node) => isInactive(node)).length },
      ].map((item) => `<div class=\"summary-card\"><strong>${item.value}</strong><span>${item.label}</span></div>`).join('');
      document.getElementById('summary').innerHTML = html;
    }

    function renderPrimaryTabs() {
      const isMobile = isPortraitMobileView();
      if (isMobile) {
        return `
          <div class="primary-toggle" role="group" aria-label="${tr('viewLabel')}">
            <button type="button" class="segmented-button${currentPanel === 'map' ? ' active' : ''}" data-panel="map">${tr('panelMap')}</button>
            <button type="button" class="segmented-button${isAnalysisPanel() ? ' active' : ''}" data-panel="connectivity">${tr('panelAnalysis')}</button>
          </div>
        `;
      }
      return `
        <div class="primary-toggle" role="group" aria-label="${tr('viewLabel')}">
          <button type="button" class="segmented-button${currentPanel === 'map' ? ' active' : ''}" data-panel="map">${tr('panelMap')}</button>
          <button type="button" class="segmented-button${currentPanel === 'connectivity' ? ' active' : ''}" data-panel="connectivity">${tr('panelConnectivity')}</button>
          <button type="button" class="segmented-button${currentPanel === 'route' ? ' active' : ''}" data-panel="route">${tr('panelRoute')}</button>
        </div>
      `;
    }

    function renderAnalysisTabs() {
      if (!isPortraitMobileView() || !isAnalysisPanel()) return '';
      const selectedNode = latestState ? selectedConnectivityNode(latestState) : null;
      const canInspectOwnData = !selectedNode || hasOwnNeighborData(selectedNode);
      return `
        <div class="secondary-toggle mobile-analysis-tabs" role="group" aria-label="${tr('panelAnalysis')}">
          <button type="button" class="segmented-button${currentPanel === 'connectivity' && connectivityDirection === 'out' ? ' active' : ''}" data-mobile-analysis="out"${canInspectOwnData ? '' : ' disabled'}>${tr('mobileAnalysisWidze')}</button>
          <button type="button" class="segmented-button${currentPanel === 'connectivity' && connectivityDirection === 'in' ? ' active' : ''}" data-mobile-analysis="in">${tr('mobileAnalysisWidza')}</button>
          <button type="button" class="segmented-button${currentPanel === 'connectivity' && connectivityDirection === 'mutual' ? ' active' : ''}" data-mobile-analysis="mutual"${canInspectOwnData ? '' : ' disabled'}>${tr('mobileAnalysisMutual')}</button>
          <button type="button" class="segmented-button${currentPanel === 'route' ? ' active' : ''}" data-mobile-analysis="route">${tr('mobileAnalysisRoute')}</button>
        </div>
      `;
    }

    function setMobileAnalysisMode(mode) {
      if (mode === 'route') {
        setPanel('route');
        return;
      }
      currentPanel = 'connectivity';
      localStorage.setItem('meshcoreDashboardPanel', currentPanel);
      setConnectivityDirection(mode);
    }

    function relationTypeLabel(type) {
      if (type === '2way') return tr('relationTypeMutual');
      if (type === 'out') return tr('relationTypeOut');
      return tr('relationTypeIn');
    }

    function connectivityModeLabel(node) {
      if (connectivityDirection === 'out') return tr('relationModeOut');
      if (connectivityDirection === 'in') return tr('relationModeIn');
      return tr('relationModeMutual');
    }

    function connectivityStateText(node, visibleCount, canInspectOwnData) {
      if (!canInspectOwnData) return tr('connectivityStateNoOwnData');
      if (visibleCount === 0) return tr('connectivityStateNoVisible');
      if (connectivityDirection === 'out') return trFormat('connectivityStateOut', visibleCount);
      if (connectivityDirection === 'in') return trFormat('connectivityStateIn', visibleCount);
      return trFormat('connectivityStateMutual', visibleCount);
    }

    function renderAnswerStrip(title, kicker, stateText, metrics = [], alert = false) {
      return `
        <div class="answer-strip">
          <div class="answer-head">
            <div class="answer-title">
              <strong>${title}</strong>
              <span class="answer-state${alert ? '' : ' muted'}">${stateText}</span>
            </div>
            ${kicker ? `<span class="answer-kicker${alert ? ' alert' : ''}">${kicker}</span>` : ''}
          </div>
          ${metrics.length ? `<div class="answer-metrics">${metrics.map((metric) => `<span class="answer-stat"><strong>${metric.value}</strong><span>${metric.label}</span></span>`).join('')}</div>` : ''}
        </div>
      `;
    }

    function activeRouteHint() {
      if (routeActiveEndpoint === 'source') return tr('routeTapTargetSource');
      if (routeActiveEndpoint === 'target') return tr('routeTapTargetTarget');
      return tr('routeTapTargetReady');
    }

    function connectivityVisibleRows(state, nodeId) {
      if (!nodeId) return [];
      const node = connectivityData(state).nodeIndex.get(nodeId);
      const canInspectOwnData = hasOwnNeighborData(node);
      if (connectivityDirection === 'out') {
        if (!canInspectOwnData) return [];
        return directRelationRows(state, nodeId, 'out');
      }
      if (connectivityDirection === 'in') {
        return directRelationRows(state, nodeId, 'in');
      }
      if (!canInspectOwnData) return [];
      const filtered = relationRows(state, nodeId, '2way').map((row) => ({
        peerName: row.peerName,
        relationType: row.relationType,
        stale: row.stale,
        metricText: `${tr('connectivityTableOut')}: ${row.outEdge ? lineSignalMetric(row.outEdge).short : '-'}`,
        ageText: row.freshestAge === null ? '-' : humanizeSeconds(row.freshestAge),
        secondaryText: `${tr('connectivityTableIn')}: ${row.inEdge ? lineSignalMetric(row.inEdge).short : '-'}`,
      }));
      return filtered;
    }

    function mobileMapRows(state, nodeId) {
      if (!nodeId) return [];
      const data = connectivityData(state);
      const node = data.nodeIndex.get(nodeId);
      const canInspectOwnData = hasOwnNeighborData(node);
      const edges = connectivityDirection === 'out'
        ? (canInspectOwnData ? data.edges.filter((edge) => edge.source_identity_hex === nodeId) : [])
        : data.edges.filter((edge) => edge.target_identity_hex === nodeId);
      return edges.map((edge) => {
        const peerId = connectivityDirection === 'out' ? edge.target_identity_hex : edge.source_identity_hex;
        const peerNode = data.nodeIndex.get(peerId);
        return {
          peerId,
          peerName: peerNode?.name || peerId.slice(0, 8),
          stale: Boolean(edge.stale),
          metricText: lineSignalMetric(edge).short,
          ageText: humanizeSeconds(edge.age_seconds),
        };
      }).sort((left, right) => left.peerName.localeCompare(right.peerName));
    }

    function renderMobileMapPanel(state) {
      const data = connectivityData(state);
      const node = selectedConnectivityNode(state);
      const nodeOptions = data.nodes.map((candidate) => `<option value="${candidate.identity_hex}">${candidate.name}</option>`).join('');
      const selector = `
        <div class="field-stack">
          <label for="mobile-map-node">${tr('connectivitySelect')}</label>
          <select id="mobile-map-node" class="route-select" data-focus-node="1">
            <option value=""></option>
            ${nodeOptions}
          </select>
        </div>
      `;
      const canInspectOwnData = !node || hasOwnNeighborData(node);
      if (node && !canInspectOwnData && connectivityDirection === 'out') {
        connectivityDirection = 'in';
      }
      const directionButtons = `
        <div class="secondary-toggle" role="group" aria-label="${tr('panelMap')}">
          <button type="button" class="segmented-button${connectivityDirection === 'out' ? ' active' : ''}" data-connectivity-direction="out"${canInspectOwnData ? '' : ' disabled'}>${tr('relationModeOut')}</button>
          <button type="button" class="segmented-button${connectivityDirection === 'in' ? ' active' : ''}" data-connectivity-direction="in">${tr('relationModeIn')}</button>
        </div>
      `;
      if (!node) {
        return `<div class="mobile-map-stack">${selector}${directionButtons}${renderAnswerStrip(tr('mobileMapTitle'), '', tr('mobileMapPickRepeater'))}</div>`;
      }
      const rows = mobileMapRows(state, node.identity_hex);
      const listHtml = rows.length
        ? `<div class="mobile-relation-list">${rows.slice(0, 5).map((row) => `
            <button type="button" class="mobile-relation-button${selectedNeighborId === row.peerId ? ' active' : ''}" data-mobile-peer="${row.peerId}">
              <span class="mobile-relation-main">
                <strong>${row.peerName}</strong>
                <span>${row.metricText}</span>
                <span>${tr('connectivityTableAge')}: ${row.ageText}</span>
              </span>
              <span class="mobile-relation-meta">
                ${row.stale ? `<span class="stale-chip">${tr('staleShort')}</span>` : '<span></span>'}
              </span>
            </button>
          `).join('')}</div>`
        : `<div class="compact-note"><strong>${tr('mobileMapListTitle')}</strong>${tr('mobileMapNoRows')}</div>`;
      const directionLabel = connectivityDirection === 'out' ? tr('mobileMapDirectionOut') : tr('mobileMapDirectionIn');
      return `
        <div class="mobile-map-stack">
          ${selector}
          ${directionButtons}
          <div class="mobile-summary-card">
            <div class="mobile-summary-head">
              <div class="mobile-summary-title">
                <strong>${node.name}</strong>
                <span>${directionLabel}</span>
              </div>
              <span class="mobile-summary-count">${rows.length} ${tr('mobileMapVisible')}</span>
            </div>
            ${listHtml}
          </div>
        </div>
      `;
    }

    function renderRelationList(rows) {
      if (!rows.length) {
        return `<div class="compact-note"><strong>${tr('connectivityVisibleTitle')}</strong>${tr('connectivityNoRows')}</div>`;
      }
      return `
        <div class="relation-list">
          ${rows.map((row) => `
            <div class="relation-item">
              <div class="relation-main">
                <strong>${row.peerName}</strong>
                <span>${row.metricText}</span>
                <span>${tr('connectivityTableAge')}: ${row.ageText}</span>
                ${row.secondaryText ? `<span>${row.secondaryText}</span>` : ''}
              </div>
              <div class="relation-badges">
                <span class="direction-chip">${relationTypeLabel(row.relationType)}</span>
                ${row.stale ? `<span class="stale-chip">${tr('staleShort')}</span>` : ''}
              </div>
            </div>
          `).join('')}
        </div>
      `;
    }

    function renderConnectivityPanel(state) {
      const data = connectivityData(state);
      const node = selectedConnectivityNode(state);
      const nodeOptions = data.nodes.map((candidate) => `<option value="${candidate.identity_hex}">${candidate.name}</option>`).join('');
      const selector = `
        <div class="field-stack">
          <label for="connectivity-node">${tr('connectivitySelect')}</label>
          <select id="connectivity-node" class="route-select" data-focus-node="1">
            <option value=""></option>
            ${nodeOptions}
          </select>
        </div>
      `;
      if (!node) {
        return `<div class="panel-stack"><div class="panel-section">${selector}${renderAnswerStrip(tr('panelConnectivity'), '', tr('connectivityHint'))}</div></div>`;
      }
      const mutualRows = relationRows(state, node.identity_hex, '2way');
      const relations = data.relationMap.get(node.identity_hex) || { outgoing: [], incoming: [], mutual: [], oneWayOutgoing: [], oneWayIncoming: [] };
      const canInspectOwnData = hasOwnNeighborData(node);
      if (!canInspectOwnData && connectivityDirection !== 'in') {
        connectivityDirection = 'in';
      }
      const directionButtons = `
        <div class="secondary-toggle" role="group" aria-label="${tr('panelConnectivity')}">
          <button type="button" class="segmented-button${connectivityDirection === 'out' ? ' active' : ''}" data-connectivity-direction="out"${canInspectOwnData ? '' : ' disabled'}>${tr('relationModeOut')}</button>
          <button type="button" class="segmented-button${connectivityDirection === 'in' ? ' active' : ''}" data-connectivity-direction="in">${tr('relationModeIn')}</button>
          <button type="button" class="segmented-button${connectivityDirection === 'mutual' ? ' active' : ''}" data-connectivity-direction="mutual"${canInspectOwnData ? '' : ' disabled'}>${tr('relationModeMutual')}</button>
        </div>
      `;
      const visibleRows = connectivityVisibleRows(state, node.identity_hex);
      const heroCount = visibleRows.length;
      const summaryMetrics = [
        { value: relations.outgoing.length, label: tr('connectivitySummaryOut') },
        { value: relations.incoming.length, label: tr('connectivitySummaryIn') },
        { value: mutualRows.length, label: tr('connectivitySummaryMutual') },
      ];
      return `
        <div class="panel-stack">
          <div class="panel-section">
            ${selector}
            ${isPortraitMobileView() ? '' : directionButtons}
            ${renderAnswerStrip(node.name, connectivityModeLabel(node), connectivityStateText(node, heroCount, canInspectOwnData), summaryMetrics, !canInspectOwnData)}
          </div>
          <div class="panel-section">
            <div class="panel-section-head"><span class="panel-section-title">${tr('connectivityVisibleTitle')}</span><span class="panel-section-note">${heroCount} ${tr('connectivityCountShort')}</span></div>
            ${renderRelationList(visibleRows)}
          </div>
        </div>
      `;
    }

    function routeSummaryCard(title, routeResult, data) {
      const directionClass = title === tr('routeForward') ? 'forward' : 'backward';
      if (!routeResult.path) {
        return `<div class="route-card"><div class="route-card-head"><strong>${title}</strong><span class="route-direction-chip ${directionClass}">${title}</span></div><div class="route-status-row"><span class="route-status-badge no">${tr('routeStatusNo')}</span></div><div class="route-empty"><strong>${tr('routeNoPath')}</strong><span>${tr('routePickHint')}</span></div></div>`;
      }
      const pathHtml = routeResult.path.map((identityHex, index) => {
        const node = data.nodeIndex.get(identityHex);
        const name = node?.name || identityHex.slice(0, 8);
        return `<div class="route-hop-row"><span class="route-step">${name}</span></div>`;
      }).join('');
      return `
        <div class="route-card">
          <div class="route-card-head"><strong>${title}</strong><span class="route-direction-chip ${directionClass}">${title}</span></div>
          <div class="route-status-row"><span class="route-status-badge ok">${tr('routeStatusYes')}</span><span class="route-meta">${Math.max(0, routeResult.path.length - 1)} ${tr('routeHopCount')}${routeResult.usesStale ? `, ${tr('routeUsesStale')}` : `, ${tr('routeFreshOnly')}`}</span></div>
          <div class="route-path">${pathHtml}</div>
        </div>
      `;
    }

    function renderRoutePanel(state) {
      const data = connectivityData(state);
      const options = data.nodes.map((node) => `<option value="${node.identity_hex}">${node.name}</option>`).join('');
      let body = `<div class="panel-section">${renderAnswerStrip(tr('routeResultsTitle'), '', tr('routeStateIdle'))}</div>`;
      if (routeSourceId && routeTargetId) {
        if (routeSourceId === routeTargetId) {
          body = `<div class="panel-section">${renderAnswerStrip(tr('routeResultsTitle'), '', tr('routeStateSameNode'), [], true)}</div>`;
        } else {
          const forward = buildRouteResult(state, routeSourceId, routeTargetId);
          const backward = buildRouteResult(state, routeTargetId, routeSourceId);
          body = `<div class="panel-section">${renderAnswerStrip(tr('routeResultsTitle'), '', tr('routeStateReady'), [{ value: forward.path ? 'OK' : '-', label: tr('routeForward') }, { value: backward.path ? 'OK' : '-', label: tr('routeBackward') }])}<div class="route-result-grid">${routeSummaryCard(tr('routeForward'), forward, data)}${routeSummaryCard(tr('routeBackward'), backward, data)}</div></div>`;
        }
      }
      const sourceName = data.nodeIndex.get(routeSourceId)?.name || '-';
      const targetName = data.nodeIndex.get(routeTargetId)?.name || '-';
      return `
        <div class="panel-stack">
          <div class="panel-section">
            <div class="route-picker-note"><strong>${activeRouteHint()}</strong></div>
            <div class="route-control-bar">
              <button type="button" class="route-endpoint${routeActiveEndpoint === 'source' ? ' active' : ''}" data-route-active="source">
                <span class="route-endpoint-label">${tr('routeSelectedA')}</span>
                <strong class="route-endpoint-name">${routeSourceId ? sourceName : tr('routeUnset')}</strong>
              </button>
              <button type="button" class="route-endpoint route-endpoint-target${routeActiveEndpoint === 'target' ? ' active' : ''}" data-route-active="target">
                <span class="route-endpoint-label">${tr('routeSelectedB')}</span>
                <strong class="route-endpoint-name">${routeTargetId ? targetName : tr('routeUnset')}</strong>
              </button>
            </div>
            <div class="route-controls">
              <div class="field-stack">
                <label for="route-source">${tr('routeSource')}</label>
                <select id="route-source" class="route-select" data-route-source="1">
                  <option value=""></option>
                  ${options}
                </select>
              </div>
              <div></div>
              <div class="field-stack">
                <label for="route-target">${tr('routeTarget')}</label>
                <select id="route-target" class="route-select" data-route-target="1">
                  <option value=""></option>
                  ${options}
                </select>
              </div>
            </div>
          </div>
          ${body}
        </div>
      `;
    }

    function selectNode(identityHex) {
      if (selectedSourceId === identityHex) {
        clearSelection();
        return;
      }
      selectedSourceId = identityHex;
      selectedNeighborId = null;
      if (!latestState) return;
      if (currentPanel === 'connectivity') {
        render(latestState);
        return;
      }
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
      return { value: null, label: tr('noDataShort'), short: tr('noDataShort'), kind: tr('kindSignal') };
    }

    function describeProbeResult(node) {
      if (node.last_probe_status === 'failed' && node.last_data_at) {
        return tr('probeFailedAfterData');
      }
      if (node.last_probe_status) {
        return node.last_probe_status;
      }
      return node.data_fetch_ok ? tr('probeDataSaved') : tr('probePending');
    }

    function linkLabel(link, sourceNode) {
      const metric = lineSignalMetric(link);
      const distance = neighborDistanceKm(sourceNode, link);
      const metricLine = metric.value !== null ? `${metric.kind}: ${metric.value.toFixed(1)} ${metric.kind === 'RSSI' ? 'dBm' : 'dB'}` : tr('signalMissing');
      const distanceLine = distance !== null ? `${tr('distancePrefix')}: ${distance.toFixed(1)} km` : tr('distanceMissing');
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
        return { radius: 12, color: '#15212a', weight: 3.6, fillColor: color, fillOpacity: 1, opacity: 1 };
      }
      if (neighbor) {
        return { radius: 7.5, color, weight: 2, fillColor: color, fillOpacity: 0.9, opacity: 0.94 };
      }
      if (isolated) {
        return { radius: 4, color, weight: 1, fillColor: color, fillOpacity: 0.16, opacity: 0.2 };
      }
      return { radius: 5, color, weight: 1.2, fillColor: color, fillOpacity: 0.82, opacity: 0.85 };
    }

    function drawFocusHalo(node, strokeColor, fillColor, outerRadius = 18, innerRadius = 13) {
      if (!node || !isFiniteCoordinate(node.latitude, node.longitude)) return;
      L.circleMarker([node.latitude, node.longitude], {
        radius: outerRadius,
        color: strokeColor,
        weight: 1.4,
        fillColor,
        fillOpacity: 0.06,
        opacity: 0.34,
      }).addTo(halosLayer);
      L.circleMarker([node.latitude, node.longitude], {
        radius: innerRadius,
        color: strokeColor,
        weight: 1.8,
        fillColor,
        fillOpacity: 0.1,
        opacity: 0.52,
      }).addTo(halosLayer);
    }

    function addDirectionalArrow(sourceNode, targetNode, color, ratio = 0.58) {
      if (!sourceNode || !targetNode) return;
      const fromPoint = map.latLngToLayerPoint([sourceNode.latitude, sourceNode.longitude]);
      const toPoint = map.latLngToLayerPoint([targetNode.latitude, targetNode.longitude]);
      const angle = Math.atan2(toPoint.y - fromPoint.y, toPoint.x - fromPoint.x) * (180 / Math.PI);
      const lat = sourceNode.latitude + ((targetNode.latitude - sourceNode.latitude) * ratio);
      const lon = sourceNode.longitude + ((targetNode.longitude - sourceNode.longitude) * ratio);
      L.marker([lat, lon], {
        icon: L.divIcon({ className: 'line-arrow-icon', html: `<span class="line-arrow-chip" style="color:${color}; transform: rotate(${angle}deg)">➜</span>`, iconSize: null }),
        interactive: false,
        zIndexOffset: 1200,
      }).addTo(linksLayer);
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
      const isFocusedNode = node.identity_hex === selectedSourceId || node.identity_hex === routeSourceId || node.identity_hex === routeTargetId;
      const isActivePeer = neighborIds.has(node.identity_hex);
      const chipClass = `node-label-chip${isFocusedNode ? ' focused' : ''}${isActivePeer ? ' active-peer' : ''}`;
      if (selectedNeighborId) {
        if (node.identity_hex !== selectedSourceId && node.identity_hex !== selectedNeighborId) return null;
        return `<div class="${chipClass}"><strong>${shortName}</strong><span class="label-meta">${tr('lastAdvertLabel')}: ${formatShortWhen(node.last_advert_at)}</span></div>`;
      }
      const inspectionNeighbor = Boolean(selectedSourceId) && node.identity_hex !== selectedSourceId && neighborIds.has(node.identity_hex);
      if (inspectionNeighbor && zoom >= HIGH_ZOOM_LABEL_THRESHOLD) {
        return `<div class="${chipClass}"><strong>${shortName}</strong></div>`;
      }
      if (forced && isFocusedNode) {
        return `<div class="${chipClass}"><strong>${shortName}</strong><span class="label-meta">${tr('lastAdvertLabel')}: ${formatShortWhen(node.last_advert_at)}</span></div>`;
      }
      if (forced || zoom >= HIGH_ZOOM_LABEL_THRESHOLD) {
        return `<div class="${chipClass}"><strong>${shortName}</strong></div>`;
      }
      if (zoom >= LOW_ZOOM_LABEL_THRESHOLD && (isFocusedNode || node.identity_hex === hoveredNodeId)) {
        return `<div class="${chipClass}"><strong>${shortName}</strong></div>`;
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
        const forced = node.identity_hex === selectedSourceId
          || node.identity_hex === routeSourceId
          || node.identity_hex === routeTargetId
          || node.identity_hex === hoveredNodeId
          || (selectedNeighborId && node.identity_hex === selectedNeighborId);
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
      if (!node) return `<div class=\"empty-note\">${tr('emptySelectRepeater')}</div>`;
      if (!neighborLink) return `<div class=\"empty-note\">${tr('emptySelectNeighbor')}</div>`;
      if (historyRows.length < 2) {
        return `
          <div class=\"chart-shell\">
            <div class=\"chart-head\">
              <div class=\"chart-title\"><strong>${neighborLink.target_name}</strong><span>${tr('chartHistory')} ${lineSignalMetric(neighborLink).kind}</span></div>
              <div class=\"chart-meta\">${tr('chartLatest')} ${lineSignalMetric(neighborLink).label}</div>
            </div>
            <div class=\"empty-note\">${tr('storedSamples')(historyRows.length)}</div>
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
            <div class=\"chart-title\"><strong>${neighborLink.target_name}</strong><span>${tr('chartSNRHistory')}</span></div>
            <div class=\"chart-meta\">${tr('chartLatest')} ${lineSignalMetric(neighborLink).label}</div>
          </div>
          <svg id=\"signal-chart\" viewBox=\"0 0 320 152\" preserveAspectRatio=\"none\">
            ${grid}
            <path d=\"${path}\" fill=\"none\" stroke=\"${lineColor(neighborLink)}\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" />
            ${points}
            <text x=\"${leftPad}\" y=\"144\" fill=\"#6a7883\" font-size=\"10\">${timeAgo(new Date(minTime).toISOString())}</text>
            <text x=\"${leftPad + width - 22}\" y=\"144\" fill=\"#6a7883\" font-size=\"10\">${tr('chartNow')}</text>
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
              <th>${tr('neighbor')}</th>
              <th>${tr('lastSeen')}</th>
              <th>${tr('signal')}</th>
              <th>${tr('distance')}</th>
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
      ` : `<div class=\"empty-note\">${tr('emptyNoNeighborLinks')}</div>`;
      return `
        <div class=\"node-expand\">
          <div class=\"expand-head\">
            <strong>${tr('inspection')}</strong>
            <button type=\"button\" class=\"ghost-button\" data-clear-selection=\"1\">${tr('clearFocus')}</button>
          </div>
          <div class=\"detail-grid\">
            <div class=\"detail-cell\"><strong>${tr('role')}</strong>${node.role || tr('roleDefault')}</div>
            <div class=\"detail-cell\"><strong>${tr('lastAdvert')}</strong>${formatWhen(node.last_advert_at)}</div>
            <div class=\"detail-cell\"><strong>${tr('lastData')}</strong>${formatWhen(node.last_data_at)}</div>
            <div class=\"detail-cell\"><strong>${tr('lastSuccessfulProbe')}</strong>${formatWhen(node.last_successful_probe_at)}</div>
            <div class=\"detail-cell\"><strong>${tr('lastProbeResult')}</strong>${describeProbeResult(node)}</div>
            <div class=\"detail-cell\"><strong>${tr('lastProbeAttempt')}</strong>${formatWhen(node.last_probe_at)}</div>
          </div>
          <div>
            <div class=\"expand-head\"><strong>${tr('directNeighbors')}</strong><span class=\"node-state-tag\">${selectedLinks.length}</span></div>
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
              <span class=\"node-age\">${tr('lastAdvertLabel')}: ${formatShortWhen(node.last_advert_at)}</span>
            </span>
            <span class=\"node-state-tag\">${nodeStateLabel(node)}</span>
          </button>
          ${node.identity_hex === selectedSourceId && currentPanel === 'map' ? renderExpandedNode(node, state) : ''}
        </div>
      `;
    }

    function renderNodeSections(state) {
      const container = document.getElementById('node-sections');
      const nodes = sortNodes(relevantNodes(state));
      const selectedNode = selectedSourceId ? nodes.find((node) => node.identity_hex === selectedSourceId) : null;
      const others = nodes.filter((node) => node.identity_hex !== selectedSourceId);
      const panelTitle = currentPanel === 'connectivity'
        ? tr('toolbarConnectivityTitle')
        : currentPanel === 'route'
          ? tr('toolbarRouteTitle')
          : tr('toolbarMapTitle');
      const panelSubtitle = currentPanel === 'connectivity'
        ? tr('toolbarConnectivitySubtitle')
        : currentPanel === 'route'
          ? tr('toolbarRouteSubtitle')
          : tr('toolbarMapSubtitle');
      const archivedCount = archivedNodeCount(state);
      let html = '';
      const sortHtml = currentPanel === 'map' && !isPortraitMobileView()
        ? `
            <div class="toolbar-meta-group">
              <label for="sort-mode">${tr('sortLabel')}</label>
              <select id="sort-mode" class="sort-select" data-sort-mode="1">
                <option value="last_advert"${nodeSortMode === 'last_advert' ? ' selected' : ''}>${tr('sortLastAdvert')}</option>
                <option value="last_data"${nodeSortMode === 'last_data' ? ' selected' : ''}>${tr('sortLastData')}</option>
                <option value="alphabetical"${nodeSortMode === 'alphabetical' ? ' selected' : ''}>${tr('sortAlphabetical')}</option>
              </select>
            </div>
          `
        : '';
      const archivedHtml = `<button type="button" class="toolbar-toggle-button${showArchived ? ' active' : ''}" data-toggle-archived="1">${archivedCount ? trFormat('archivedToggleCount', archivedCount) : tr('archivedToggle')}</button>`;
      const metaHtml = `${sortHtml}`;
      const langHtml = `<div class="lang-toggle" role="group" aria-label="${tr('languageLabel')}"><button type="button" class="lang-button" data-global-language="pl">PL</button><button type="button" class="lang-button" data-global-language="en">EN</button></div>`;
      html += `
        <div class="list-toolbar">
          <div class="toolbar-head">
            <div class="toolbar-head-main">
              <strong class="toolbar-title">${panelTitle}</strong>
              <span class="toolbar-subtitle">${panelSubtitle}</span>
            </div>
            <div class="toolbar-head-actions">
              ${archivedHtml}
              ${langHtml}
            </div>
          </div>
          ${renderPrimaryTabs()}
          <div class="toolbar-meta">
            ${metaHtml}
          </div>
        </div>
      `;
      html += renderAnalysisTabs();
      if (currentPanel === 'connectivity') {
        html += renderConnectivityPanel(state);
      } else if (currentPanel === 'route') {
        html += renderRoutePanel(state);
      } else {
        if (isPortraitMobileView()) {
          html += renderMobileMapPanel(state);
          container.innerHTML = html;
          for (const button of container.querySelectorAll('[data-node]')) {
            button.addEventListener('click', () => selectNode(button.dataset.node));
          }
          for (const button of container.querySelectorAll('[data-panel]')) {
            button.addEventListener('click', () => setPanel(button.dataset.panel));
          }
          for (const button of container.querySelectorAll('[data-mobile-analysis]')) {
            button.addEventListener('click', () => setMobileAnalysisMode(button.dataset.mobileAnalysis));
          }
          for (const button of container.querySelectorAll('[data-connectivity-direction]')) {
            button.addEventListener('click', () => setConnectivityDirection(button.dataset.connectivityDirection));
          }
          for (const select of container.querySelectorAll('[data-focus-node]')) {
            select.value = selectedSourceId || '';
            select.addEventListener('change', () => {
              selectedSourceId = select.value || null;
              selectedNeighborId = null;
              render(latestState);
            });
          }
          for (const button of container.querySelectorAll('[data-toggle-archived]')) {
            button.addEventListener('click', () => setShowArchived(!showArchived));
          }
          for (const button of container.querySelectorAll('[data-mobile-peer]')) {
            button.addEventListener('click', () => {
              selectedNeighborId = selectedNeighborId === button.dataset.mobilePeer ? null : button.dataset.mobilePeer;
              render(latestState);
            });
          }
          for (const button of container.querySelectorAll('[data-global-language]')) {
            button.classList.toggle('active', button.dataset.globalLanguage === currentLanguage);
            button.onclick = () => setLanguage(button.dataset.globalLanguage);
          }
          return;
        }
        if (selectedNode) {
          html += `<div class="section-heading">${tr('selectedRepeater')}</div>`;
          html += `<div class="node-list">${rowHtml(selectedNode, state)}</div>`;
        }
        html += `<div class="section-heading">${selectedNode ? tr('otherRepeaters') : tr('repeaters')}</div>`;
        html += `<div class="node-list">${others.length ? others.map((node) => rowHtml(node, state)).join('') : `<div class="empty-note">${tr('emptyNoOtherRepeaters')}</div>`}</div>`;
      }
      container.innerHTML = html;
      for (const button of container.querySelectorAll('[data-node]')) {
        button.addEventListener('click', () => selectNode(button.dataset.node));
      }
      for (const button of container.querySelectorAll('[data-panel]')) {
        button.addEventListener('click', () => setPanel(button.dataset.panel));
      }
      for (const button of container.querySelectorAll('[data-mobile-analysis]')) {
        button.addEventListener('click', () => setMobileAnalysisMode(button.dataset.mobileAnalysis));
      }
      for (const button of container.querySelectorAll('[data-connectivity-direction]')) {
        button.addEventListener('click', () => setConnectivityDirection(button.dataset.connectivityDirection));
      }
      for (const select of container.querySelectorAll('[data-focus-node]')) {
        select.value = selectedSourceId || '';
        select.addEventListener('change', () => {
          selectedSourceId = select.value || null;
          selectedNeighborId = null;
          if (latestState) focusConnectivitySelection(latestState);
          render(latestState);
        });
      }
      for (const select of container.querySelectorAll('[data-sort-mode]')) {
        select.addEventListener('change', () => {
          nodeSortMode = select.value;
          render(latestState);
        });
      }
      for (const button of container.querySelectorAll('[data-toggle-archived]')) {
        button.addEventListener('click', () => setShowArchived(!showArchived));
      }
      for (const select of container.querySelectorAll('[data-route-source]')) {
        select.value = routeSourceId || '';
        select.addEventListener('change', () => {
          routeActiveEndpoint = 'source';
          routeSourceId = select.value || null;
          if (latestState) focusRouteSelection(latestState);
          render(latestState);
        });
      }
      for (const select of container.querySelectorAll('[data-route-target]')) {
        select.value = routeTargetId || '';
        select.addEventListener('change', () => {
          routeActiveEndpoint = 'target';
          routeTargetId = select.value || null;
          if (latestState) focusRouteSelection(latestState);
          render(latestState);
        });
      }
      for (const button of container.querySelectorAll('[data-route-active]')) {
        button.addEventListener('click', () => {
          routeActiveEndpoint = button.dataset.routeActive === 'target' ? 'target' : 'source';
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
      for (const button of container.querySelectorAll('[data-mobile-peer]')) {
        button.addEventListener('click', () => {
          selectedNeighborId = selectedNeighborId === button.dataset.mobilePeer ? null : button.dataset.mobilePeer;
          render(latestState);
        });
      }
    }

    function renderMap(state) {
      if (currentPanel === 'map' && isPortraitMobileView()) {
        renderMobileDirectionalMap(state);
        return;
      }
      if (currentPanel === 'connectivity') {
        renderConnectivityMap(state);
        return;
      }
      if (currentPanel === 'route') {
        renderRouteMap(state);
        return;
      }
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
          drawFocusHalo(node, nodeColor(node), nodeColor(node), 17, 12);
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

    function drawMapNodes(nodeMap, focusId, highlightedIds = new Set()) {
      const bounds = [];
      for (const node of nodeMap) {
        if (!isFiniteCoordinate(node.latitude, node.longitude)) continue;
        const selected = node.identity_hex === focusId;
        const neighbor = highlightedIds.has(node.identity_hex);
        const marker = L.circleMarker([node.latitude, node.longitude], markerStyle(node, false, selected, neighbor)).addTo(markersLayer);
        marker.on('click', (event) => {
          L.DomEvent.stopPropagation(event);
          if (currentPanel === 'route') {
            if (routeActiveEndpoint === 'target') {
              routeTargetId = node.identity_hex;
            } else {
              routeSourceId = node.identity_hex;
            }
            focusRouteSelection(latestState);
          } else {
            selectedSourceId = node.identity_hex;
            if (currentPanel === 'connectivity') {
              focusConnectivitySelection(latestState);
            }
          }
          render(latestState);
        });
        bounds.push([node.latitude, node.longitude]);
      }
      return bounds;
    }

    function renderConnectivityMap(state) {
      markersLayer.clearLayers();
      halosLayer.clearLayers();
      linksLayer.clearLayers();
      labelsLayer.clearLayers();
      linkLabelsLayer.clearLayers();
      const data = connectivityData(state);
      const focusId = selectedSourceId;
      const focusNode = focusId ? data.nodeIndex.get(focusId) : null;
      const canInspectOwnData = hasOwnNeighborData(focusNode);
      let edges = [];
      if (focusId) {
        if (connectivityDirection === 'out' && canInspectOwnData) {
          edges = data.edges.filter((edge) => edge.source_identity_hex === focusId);
        } else if (connectivityDirection === 'in') {
          edges = data.edges.filter((edge) => edge.target_identity_hex === focusId);
        } else if (canInspectOwnData) {
          edges = data.edges.filter((edge) => edge.source_identity_hex === focusId && edge.mutual);
        }
      }
      const highlightedIds = new Set();
      for (const edge of edges) {
        highlightedIds.add(edge.source_identity_hex);
        highlightedIds.add(edge.target_identity_hex);
      }
      const nodes = focusId
        ? data.nodes.filter((node) => highlightedIds.has(node.identity_hex))
        : data.nodes;
      const bounds = drawMapNodes(nodes, focusId, highlightedIds);
      if (focusId) {
        const focusNode = data.nodeIndex.get(focusId);
        drawFocusHalo(focusNode, '#15212a', '#15212a', 19, 14);
      }
      for (const edge of edges) {
        const sourceNode = data.nodeIndex.get(edge.source_identity_hex);
        const targetNode = data.nodeIndex.get(edge.target_identity_hex);
        if (!sourceNode || !targetNode) continue;
        if (!isFiniteCoordinate(sourceNode.latitude, sourceNode.longitude) || !isFiniteCoordinate(targetNode.latitude, targetNode.longitude)) continue;
        const color = edge.mutual ? '#2e8b57' : connectivityDirection === 'in' ? '#2c71d1' : '#cfaa38';
        L.polyline([
          [sourceNode.latitude, sourceNode.longitude],
          [targetNode.latitude, targetNode.longitude],
        ], {
          color,
          weight: edge.stale ? 1.5 : 2.6,
          opacity: edge.stale ? 0.4 : 0.84,
          dashArray: edge.stale ? '5 5' : null,
        }).addTo(linksLayer);
        if (connectivityDirection === 'mutual') {
          addDirectionalArrow(sourceNode, targetNode, color, 0.42);
          addDirectionalArrow(targetNode, sourceNode, color, 0.42);
        } else {
          addDirectionalArrow(sourceNode, targetNode, color);
        }
      }
      renderLabels(nodes.filter((node) => isFiniteCoordinate(node.latitude, node.longitude)), highlightedIds);
      if (!hasFitBounds && bounds.length) fitInitialBounds(bounds);
    }

    function renderMobileDirectionalMap(state) {
      markersLayer.clearLayers();
      halosLayer.clearLayers();
      linksLayer.clearLayers();
      labelsLayer.clearLayers();
      linkLabelsLayer.clearLayers();
      const data = connectivityData(state);
      const focusId = selectedSourceId;
      const focusNode = focusId ? data.nodeIndex.get(focusId) : null;
      const canInspectOwnData = hasOwnNeighborData(focusNode);
      if (focusNode && connectivityDirection === 'out' && !canInspectOwnData) {
        connectivityDirection = 'in';
      }
      const edges = focusId
        ? (connectivityDirection === 'out'
            ? (canInspectOwnData ? data.edges.filter((edge) => edge.source_identity_hex === focusId) : [])
            : data.edges.filter((edge) => edge.target_identity_hex === focusId))
        : [];
      const highlightedIds = new Set(focusId ? [focusId] : []);
      for (const edge of edges) {
        highlightedIds.add(edge.source_identity_hex);
        highlightedIds.add(edge.target_identity_hex);
      }
      const nodes = focusId ? data.nodes.filter((node) => highlightedIds.has(node.identity_hex)) : data.nodes;
      const bounds = drawMapNodes(nodes, focusId, highlightedIds);
      if (focusNode) {
        drawFocusHalo(focusNode, '#15212a', '#15212a', 19, 14);
      }
      for (const edge of edges) {
        const sourceNode = data.nodeIndex.get(edge.source_identity_hex);
        const targetNode = data.nodeIndex.get(edge.target_identity_hex);
        if (!sourceNode || !targetNode) continue;
        if (!isFiniteCoordinate(sourceNode.latitude, sourceNode.longitude) || !isFiniteCoordinate(targetNode.latitude, targetNode.longitude)) continue;
        const peerId = connectivityDirection === 'out' ? edge.target_identity_hex : edge.source_identity_hex;
        const isActive = !selectedNeighborId || selectedNeighborId === peerId;
        const color = connectivityDirection === 'in' ? '#2c71d1' : '#cfaa38';
        L.polyline([
          [sourceNode.latitude, sourceNode.longitude],
          [targetNode.latitude, targetNode.longitude],
        ], {
          color,
          weight: isActive ? 3.1 : 1.8,
          opacity: isActive ? 0.88 : 0.22,
          dashArray: edge.stale ? '5 5' : null,
        }).addTo(linksLayer);
        addDirectionalArrow(sourceNode, targetNode, color);
      }
      renderLabels(nodes.filter((node) => isFiniteCoordinate(node.latitude, node.longitude)), highlightedIds);
      if (!hasFitBounds && bounds.length) fitInitialBounds(bounds);
    }

    function renderRouteMap(state) {
      markersLayer.clearLayers();
      halosLayer.clearLayers();
      linksLayer.clearLayers();
      labelsLayer.clearLayers();
      linkLabelsLayer.clearLayers();
      const data = connectivityData(state);
      const allMapNodes = deriveMapNodes(data.nodes);
      const highlightedIds = new Set([routeSourceId, routeTargetId].filter(Boolean));
      const forward = routeSourceId && routeTargetId && routeSourceId !== routeTargetId ? buildRouteResult(state, routeSourceId, routeTargetId) : null;
      const backward = routeSourceId && routeTargetId && routeSourceId !== routeTargetId ? buildRouteResult(state, routeTargetId, routeSourceId) : null;
      const pathIds = new Set(forward?.path || []);
      for (const identityHex of (backward?.path || [])) pathIds.add(identityHex);
      for (const identityHex of pathIds) highlightedIds.add(identityHex);
      const bounds = drawMapNodes(allMapNodes, routeSourceId, highlightedIds);
      if (routeSourceId) {
        const sourceNode = data.nodeIndex.get(routeSourceId);
        drawFocusHalo(sourceNode, '#2c71d1', '#2c71d1', 16, 12);
      }
      if (routeTargetId) {
        const targetNode = data.nodeIndex.get(routeTargetId);
        drawFocusHalo(targetNode, '#cfaa38', '#cfaa38', 16, 12);
      }
      const drawRoute = (routeResult, color, dashArray = null) => {
        if (!routeResult?.path) return;
        for (let index = 0; index < routeResult.path.length - 1; index += 1) {
          const sourceNode = data.nodeIndex.get(routeResult.path[index]);
          const targetNode = data.nodeIndex.get(routeResult.path[index + 1]);
          if (!sourceNode || !targetNode) continue;
          if (!isFiniteCoordinate(sourceNode.latitude, sourceNode.longitude) || !isFiniteCoordinate(targetNode.latitude, targetNode.longitude)) continue;
          L.polyline([
            [sourceNode.latitude, sourceNode.longitude],
            [targetNode.latitude, targetNode.longitude],
          ], {
            color,
            weight: 3,
            opacity: 0.9,
            dashArray,
          }).addTo(linksLayer);
          addDirectionalArrow(sourceNode, targetNode, color, 0.54);
        }
      };
      drawRoute(forward, '#2c71d1');
      drawRoute(backward, '#cfaa38');
      const labelNodes = routeSourceId && routeTargetId
        ? allMapNodes.filter((node) => highlightedIds.has(node.identity_hex))
        : allMapNodes;
      renderLabels(labelNodes, highlightedIds);
      if (!hasFitBounds && bounds.length) fitInitialBounds(bounds);
    }

    function render(state) {
      latestState = state;
      normalizeVisibleSelections(state);
      renderLegend();
      renderSummary(state);
      renderNodeSections(state);
      for (const button of document.querySelectorAll('[data-global-language]')) {
        button.classList.toggle('active', button.dataset.globalLanguage === currentLanguage);
        button.onclick = () => setLanguage(button.dataset.globalLanguage);
      }
      syncSidebarSheetState();
      applyMobileView();
      renderMap(state);
    }

    async function refresh() {
      const response = await fetch('/api/state');
      const state = await response.json();
      if (isSidebarInteractionActive()) {
        pendingRefreshState = state;
        return;
      }
      render(state);
    }

    map.on('click', () => {
      hoveredNodeId = null;
      if (selectedSourceId) clearSelection();
    });
    map.on('zoomend', () => {
      if (latestState) renderMap(latestState);
    });
    const sheetToggle = document.getElementById('sheet-toggle');
    if (sheetToggle) {
      sheetToggle.addEventListener('click', toggleSidebarSheet);
    }
    window.addEventListener('resize', () => {
      applyMobileView();
      syncSidebarSheetState();
    });
    document.addEventListener('focusin', () => {
      if (!isSidebarInteractionActive()) return;
      pendingRefreshState = null;
    });
    document.addEventListener('focusout', () => {
      window.setTimeout(flushPendingRefresh, 0);
    });

    document.documentElement.lang = currentLanguage;
    applyMobileView();
    renderLegend();
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
