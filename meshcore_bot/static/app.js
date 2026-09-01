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
    legendObserved: 'trasa pakietu (zaobserwowana)',
    summaryKnown: 'znane',
    summaryNew: 'nowe 24h',
    summaryWithData: 'z danymi',
    summaryPending: 'bez danych',
    summaryInactive: 'nieaktywne',
    archivedToggle: '>24h',
    archivedToggleCount: (count) => `>24h ${count}`,
    archivedAutoFallback: 'Brak aktywnych punktów z ostatnich 24 godzin. Pokazuję archiwalne, żeby mapa nie była pusta.',
    answerSelectedRepeater: 'Wybrany punkt',
    mobileMapTitle: 'Połączenia na mapie',
    mobileMapEmpty: 'Wybierz punkt, aby pokazać relacje na mapie.',
    mobileMapVisible: 'widoczne',
    mobileMapListTitle: 'Najbliższe relacje',
    mobileMapNoRows: 'Brak relacji dla tego trybu.',
    mobileMapPickRepeater: 'Wybierz punkt i kierunek relacji.',
    mobileMapDirectionOut: 'Na mapie: Widzę',
    mobileMapDirectionIn: 'Na mapie: Mnie widzą',
    mobileAnalysisWidze: 'Widzę',
    mobileAnalysisWidza: 'Mnie widzą',
    mobileAnalysisMutual: 'Obie strony',
    mobileAnalysisRoute: 'Trasa',
    connectivityStateOut: (count) => `${count} bezpośrednich relacji wychodzących.`,
    connectivityStateIn: (count) => `${count} punktów widzi ten punkt.`,
    connectivityStateMutual: (count) => `${count} relacji wzajemnych.`,
    connectivityStateNoOwnData: 'Ten punkt nie ma jeszcze własnych danych sąsiedztwa. Możemy pokazać tylko kto go widzi.',
    connectivityStateNoVisible: 'Brak relacji dla bieżącego widoku.',
    routeStateIdle: 'Wybierz start i cel. Wynik pokażemy od razu w obu kierunkach.',
    routeStatePickTarget: 'Wybierz cel, a pokażemy wynik trasy i oba kierunki.',
    routeStatePickSource: 'Wybierz start, aby policzyć trasę do wybranego celu.',
    routeStateReady: 'Pokazujemy oba kierunki niezależnie, jeśli istnieją.',
    routeStateSameNode: 'A i B muszą wskazywać różne punkty.',
    routeResultsTitle: 'Wynik trasy',
    routeReachabilityTitle: 'Sugestie celów z A',
    routeReachabilityIdle: 'Wybierz punkt startowy, a pokażemy dokąd można dojść.',
    routeReachabilitySummary: (count) => `Z A można dojść do ${count} punktów.`,
    routeReachabilityEmpty: 'Dla wybranego startu nie ma jeszcze znanych celów osiągalnych jednokierunkowo.',
    routeReachabilityFreshShort: 'świeże',
    routeReachabilityStaleShort: 'stare',
    routeReachabilityAction: 'Ustaw jako cel',
    routeClearTarget: 'Usuń cel',
    routeProbePathTitle: 'Zapamiętana ścieżka do celu',
    routeProbePathSaved: 'z ostatniego udanego pobrania',
    routeProbePathAdvert: 'z ostatniego ogłoszenia',
    routeProbePathNoStored: 'Brak zapamiętanej bezpośredniej ścieżki do tego celu.',
    routeProbePathFallback: 'Ostatnie pobranie mogło przejść trasą rozgłoszeniową albo odpowiedź nie zwróciła ścieżki do ponownego użycia.',
    routeProbePathObserved: 'zapisano',
    routeProbePathSource: 'źródło',
    routeProbePathEndpoint: 'cel',
    routeProbePathBot: 'BOT',
    routeProbePathTarget: 'B',
    routeProbePathUnknownHop: (prefix) => `hop ${prefix}`,
    routeProbePathAmbiguousHop: (prefix, count) => `${prefix} (${count} możliwe)`,
    routeHistoricalRoute: 'historyczna trasa',
    routeHistoricalLinks: 'historyczne linki',
    routeHistoryFallback: 'Bieżące linki nie dają przejścia, ale w historii jest starsza trasa.',
    statusData: 'gotowe',
    statusNoData: 'brak danych',
    statusInactive: 'nieaktywny',
    probeFailedAfterData: 'nieudane po zapisaniu danych',
    probeDataSaved: 'dane zapisane',
    probePending: 'czeka na dane',
    signalMissing: 'sygnał: b/d',
    distanceMissing: 'dyst: -',
    distancePrefix: 'dyst',
    lastAdvertLabel: 'ostatnio widziany',
    lastDataLabel: 'dane sąsiedztwa',
    chartHistory: 'historia',
    chartLatest: 'ostatnio',
    chartSNRHistory: 'historia SNR',
    chartNow: 'teraz',
    emptySelectRepeater: 'Wybierz punkt, aby zobaczyć jego bezpośrednie połączenia.',
    emptySelectNeighbor: 'Wybierz sąsiada, aby zobaczyć historię sygnału.',
    emptyNoNeighborLinks: 'Dla tego punktu nie ma jeszcze zapisanych połączeń sąsiedzkich.',
    emptyNoOtherRepeaters: 'Brak innych punktów.',
    emptyNoSearchResults: 'Brak punktów pasujących do filtra.',
    inspection: 'Szczegóły punktu',
    clearFocus: 'Wyczyść wybór',
    probeQueueTitle: 'Pobierz dane',
    probeQueueAction: 'Dodaj',
    probeQueueBusy: 'Dodaję...',
    probeQueueQueued: 'dodano',
    probeQueuePending: 'czeka',
    probeQueueRunning: 'trwa',
    probeQueueCooldown: 'limit',
    probeQueueError: 'błąd',
    probeQueueHintQueuedNow: 'Dodano jako następne.',
    probeQueueHintQueuedAt: (when) => `Dodano, nie wcześniej niż ${when}.`,
    probeQueueHintPendingNow: 'Już czeka.',
    probeQueueHintPendingAt: (when) => `Czeka, nie wcześniej niż ${when}.`,
    probeQueueHintRunning: 'Pobranie trwa.',
    probeQueueHintCooldown: 'Pominięto przez limit lub cooldown.',
    probeQueueHintError: 'Nie udało się dodać.',
    role: 'Rola',
    firstSeen: 'Pierwsze wykrycie',
    firstSeenLabel: 'pierwsze wykrycie',
    lastAdvert: 'Ostatnio widziany',
    lastData: 'Dane sąsiedztwa',
    lastSuccessfulProbe: 'Ostatnie udane pobranie',
    lastProbeResult: 'Wynik ostatniej próby',
    lastProbeAttempt: 'Ostatnia próba',
    directNeighbors: 'Bezpośrednie połączenia',
    mapNodePositionMissing: 'Mapa nie narysuje połączeń od tego punktu, bo nie ma on poprawnej pozycji GPS.',
    mapNeighborPositionsMissing: (count) => `Mapa pomija ${count} połącze${count === 1 ? 'nie' : count < 5 ? 'nia' : 'ń'} do sąsiadów bez poprawnej pozycji GPS.`,
    neighbor: 'Sąsiad',
    lastSeen: 'Ostatnio widziany',
    signal: 'Sygnał',
    distance: 'Dystans',
    selectedRepeater: 'Wybrany punkt',
    otherRepeaters: 'Pozostałe punkty',
    repeaters: 'Punkty',
    sortLabel: 'Sortowanie',
    searchLabel: 'Szukaj punktu',
    searchPlaceholder: 'nazwa, prefix, hex (min. 2 znaki)',
    sortLastAdvert: 'ostatnio widziany',
    sortLastData: 'ostatnie dane',
    sortAlphabetical: 'alfabetycznie',
    viewMap: 'Mapa',
    viewList: 'Lista',
    viewLabel: 'Widok',
    panelMap: 'Mapa',
    panelNew: 'Nowe',
    panelConnectivity: 'Łączność',
    panelRoute: 'Trasy',
    panelAnalysis: 'Analiza',
    focusRepeater: 'Wybór',
    relationModeOut: 'Widzę',
    relationModeIn: 'Mnie widzą',
    relationModeMutual: 'Obie strony',
    relationFilterAll: 'Wszystkie',
    relationFilterTwoWay: 'Obie strony',
    relationFilterOut: 'Widzę',
    relationFilterIn: 'Mnie widzą',
    relationDirectOut: 'bezpośrednio widzę',
    relationDirectIn: 'bezpośrednio widzą',
    relationNodeSees: (name) => `${name} widzi`,
    relationNodeSeenBy: (name) => `${name} widziany przez`,
    relationNodeMutual: (name) => `${name} w obie strony`,
    connectivityHint: 'Wybierz punkt.',
    connectivitySelect: 'Punkt',
    connectivityVisible: 'Widoczne relacje',
    connectivityCountShort: 'rel.',
    connectivityNoRows: 'Brak relacji dla wybranego widoku.',
    connectivitySummaryTitle: 'Podsumowanie',
    connectivityVisibleTitle: 'Widoczne relacje',
    connectivityFilterHint: 'W warstwie porównania pokazuj tylko jeden typ.',
    connectivitySummaryOut: 'widzę',
    connectivitySummaryIn: 'widzą',
    connectivitySummaryMutual: 'wzajemne',
    connectivitySummaryOneWay: 'jednokierunkowe',
    connectivityTablePeer: 'Punkt',
    connectivityTableType: 'Typ',
    connectivityTableOut: 'A->B',
    connectivityTableIn: 'B->A',
    connectivityTableAge: 'Ostatnio',
    connectivityTableSignal: 'SNR',
    relationTypeOut: 'ode mnie',
    relationTypeIn: 'do mnie',
    relationTypeMutual: 'obie strony',
    staleShort: 'stare',
    routeSource: 'Start',
    routeTarget: 'Cel',
    routeSwap: 'Zamień',
    routeForward: 'A->B',
    routeBackward: 'B->A',
    routePickHint: 'Wybierz na mapie',
    routeSelectedA: 'A',
    routeSelectedB: 'B',
    routeUnset: 'nie ustawiono',
    routeStatusYes: 'trasa dostępna',
    routeStatusNo: 'brak trasy',
    routeNoSelection: 'Ustaw A i B.',
    routeSameNode: 'Start i cel muszą być różne.',
    routeNoPath: 'Brak trasy.',
    routeHopCount: 'hopów',
    routeUsesStale: 'użyto starych linków',
    routeFreshOnly: 'świeże linki',
    languageLabel: 'Język',
    sheetExpand: 'Rozwiń',
    sheetCollapse: 'Zwin',
    dataErrorStale: (detail) => `Brak świeżych danych z serwera (${detail}). Widok pokazuje ostatni znany stan.`,
    dataErrorRetry: 'Spróbuj ponownie',
    dataErrorDismiss: 'Zamknij komunikat',
    packetPathsTitle: 'Realne trasy pakietów',
    packetPathsHint: 'Ścieżki, którymi faktycznie przeszły adverty. Kliknij, aby zobaczyć trasę na mapie.',
    packetPathsLoading: 'Wczytuję trasy...',
    packetPathsEmpty: 'Brak zaobserwowanych tras w ostatnich 48 godzinach.',
    packetPathsUnresolved: (count) => `${count} hop${count === 1 ? '' : 'ów'} nierozpoznanych`,
    panelHide: 'Ukryj panel',
    panelShow: 'Pokaż panel',
    clusterSummary: (total, ok, missing, stale) => `Grupa ${total} punktów: ${ok} z danymi, ${missing} bez danych, ${stale} nieaktywnych`,
    toolbarMapTitle: 'Mapa sieci',
    toolbarMapSubtitle: 'Wybierz punkt z mapy lub listy, aby zobaczyć jego bezpośrednie połączenia.',
    toolbarNewTitle: 'Nowe punkty',
    toolbarNewSubtitle: 'Punkty wykryte po raz pierwszy w ostatnich 24 godzinach.',
    toolbarConnectivityTitle: 'Łączność',
    toolbarConnectivitySubtitle: 'Sprawdź, kto widzi wybrany punkt i kogo widzi on.',
    toolbarRouteTitle: 'Trasy',
    toolbarRouteSubtitle: 'Wybierz start i cel. Najpierw pokażemy wynik, potem szczegóły.',
    newRepeaters: 'Nowe punkty 24h',
    emptyNoNewRepeaters: 'Brak nowych punktów wykrytych w ostatnich 24 godzinach.',
    routeTapTarget: 'Wybierz na mapie start albo cel.',
    routeTapTargetSource: 'Kliknij punkt na mapie, aby ustawić start.',
    routeTapTargetTarget: 'Kliknij punkt na mapie, aby ustawić cel.',
    routeTapTargetReady: 'Kliknij punkt na mapie, aby zmienić start albo cel.',
    roleDefault: 'Repeater',
    kindSignal: 'sygnał',
    noDataShort: 'b/d',
    loadingSignalHistory: 'Ładowanie historii sygnału...',
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
    legendObserved: 'packet path (observed)',
    summaryKnown: 'known',
    summaryNew: 'new 24h',
    summaryWithData: 'with data',
    summaryPending: 'no data',
    summaryInactive: 'inactive',
    archivedToggle: '>24h',
    archivedToggleCount: (count) => `>24h ${count}`,
    archivedAutoFallback: 'No active nodes were seen in the last 24 hours. Showing archived ones so the map does not stay empty.',
    answerSelectedRepeater: 'Selected node',
    mobileMapTitle: 'Links on map',
    mobileMapEmpty: 'Select a node to show relations on the map.',
    mobileMapVisible: 'visible',
    mobileMapListTitle: 'Closest relations',
    mobileMapNoRows: 'No relations for this mode.',
    mobileMapPickRepeater: 'Select a node and relation direction.',
    mobileMapDirectionOut: 'Map: Out',
    mobileMapDirectionIn: 'Map: Seen by',
    mobileAnalysisWidze: 'Out',
    mobileAnalysisWidza: 'Seen by',
    mobileAnalysisMutual: 'Mutual',
    mobileAnalysisRoute: 'Route',
    connectivityStateOut: (count) => `${count} direct outgoing relations.`,
    connectivityStateIn: (count) => `${count} nodes can see this node.`,
    connectivityStateMutual: (count) => `${count} mutual relations.`,
    connectivityStateNoOwnData: 'This node has no own neighbor snapshot yet. We can only show who can see it.',
    connectivityStateNoVisible: 'No relations match the current view.',
    routeStateIdle: 'Set source and target. We will show both directions immediately.',
    routeStatePickTarget: 'Pick a target to calculate both directions.',
    routeStatePickSource: 'Pick a source to calculate the route to the selected target.',
    routeStateReady: 'Both directions are shown independently when available.',
    routeStateSameNode: 'A and B must point to different nodes.',
    routeResultsTitle: 'Route result',
    routeReachabilityTitle: 'Suggested targets from A',
    routeReachabilityIdle: 'Set a source and we will show which targets are reachable one-way.',
    routeReachabilitySummary: (count) => `${count} reachable destination${count === 1 ? '' : 's'} from A.`,
    routeReachabilityEmpty: 'No known one-way destinations are reachable from the selected A yet.',
    routeReachabilityFreshShort: 'fresh',
    routeReachabilityStaleShort: 'stale',
    routeReachabilityAction: 'Set as target',
    routeClearTarget: 'Clear target',
    routeProbePathTitle: 'Remembered path to target',
    routeProbePathSaved: 'from successful fetch',
    routeProbePathAdvert: 'from latest advert',
    routeProbePathNoStored: 'No remembered direct path to this target is stored yet.',
    routeProbePathFallback: 'The latest fetch may have used flood routing or the response did not return a reusable path.',
    routeProbePathObserved: 'stored',
    routeProbePathSource: 'source',
    routeProbePathEndpoint: 'target',
    routeProbePathBot: 'BOT',
    routeProbePathTarget: 'B',
    routeProbePathUnknownHop: (prefix) => `hop ${prefix}`,
    routeProbePathAmbiguousHop: (prefix, count) => `${prefix} (${count} matches)`,
    routeHistoricalRoute: 'historical route',
    routeHistoricalLinks: 'historical links',
    routeHistoryFallback: 'Current links no longer provide a route, but an older route still exists in history.',
    statusData: 'ready',
    statusNoData: 'no data',
    statusInactive: 'inactive',
    probeFailedAfterData: 'failed after data snapshot',
    probeDataSaved: 'data saved',
    probePending: 'waiting for data',
    signalMissing: 'signal: n/a',
    distanceMissing: 'dist: -',
    distancePrefix: 'dist',
    lastAdvertLabel: 'last seen',
    lastDataLabel: 'neighbor data',
    chartHistory: 'history',
    chartLatest: 'latest',
    chartSNRHistory: 'SNR history',
    chartNow: 'now',
    emptySelectRepeater: 'Select a node to inspect its direct links.',
    emptySelectNeighbor: 'Select a neighbor row to inspect signal history.',
    emptyNoNeighborLinks: 'No stored neighbor links are available yet for this node.',
    emptyNoOtherRepeaters: 'No other nodes available.',
    emptyNoSearchResults: 'No nodes match the current filter.',
    inspection: 'Node details',
    clearFocus: 'Clear selection',
    probeQueueTitle: 'Fetch data',
    probeQueueAction: 'Queue',
    probeQueueBusy: 'Queuing...',
    probeQueueQueued: 'queued',
    probeQueuePending: 'pending',
    probeQueueRunning: 'running',
    probeQueueCooldown: 'limit',
    probeQueueError: 'error',
    probeQueueHintQueuedNow: 'Queued as next.',
    probeQueueHintQueuedAt: (when) => `Queued, not before ${when}.`,
    probeQueueHintPendingNow: 'Already pending.',
    probeQueueHintPendingAt: (when) => `Pending, not before ${when}.`,
    probeQueueHintRunning: 'Fetch already running.',
    probeQueueHintCooldown: 'Skipped by cooldown or queue limit.',
    probeQueueHintError: 'Unable to queue the job.',
    role: 'Role',
    firstSeen: 'First seen',
    firstSeenLabel: 'first seen',
    lastAdvert: 'Last seen',
    lastData: 'Neighbor data',
    lastSuccessfulProbe: 'Last successful fetch',
    lastProbeResult: 'Last probe result',
    lastProbeAttempt: 'Last probe attempt',
    directNeighbors: 'Direct links',
    mapNodePositionMissing: 'The map cannot draw links from this node because it has no valid GPS position.',
    mapNeighborPositionsMissing: (count) => `The map skips ${count} link${count === 1 ? '' : 's'} to neighbors without a valid GPS position.`,
    neighbor: 'Neighbor',
    lastSeen: 'Last seen',
    signal: 'Signal',
    distance: 'Distance',
    selectedRepeater: 'Selected node',
    otherRepeaters: 'Other nodes',
    repeaters: 'Nodes',
    sortLabel: 'Sort',
    searchLabel: 'Find node',
    searchPlaceholder: 'name, prefix, hex (min. 2 chars)',
    sortLastAdvert: 'last seen',
    sortLastData: 'last data fetch',
    sortAlphabetical: 'alphabetical',
    viewMap: 'Map',
    viewList: 'List',
    viewLabel: 'View',
    panelMap: 'Map',
    panelNew: 'New',
    panelConnectivity: 'Connectivity',
    panelRoute: 'Routes',
    panelAnalysis: 'Analysis',
    focusRepeater: 'Focus',
    relationModeOut: 'Out',
    relationModeIn: 'Seen by',
    relationModeMutual: 'Mutual',
    relationFilterAll: 'All',
    relationFilterTwoWay: 'Mutual',
    relationFilterOut: 'Out',
    relationFilterIn: 'In',
    relationDirectOut: 'directly seen',
    relationDirectIn: 'directly seeing me',
    relationNodeSees: (name) => `${name} sees`,
    relationNodeSeenBy: (name) => `${name} seen by`,
    relationNodeMutual: (name) => `${name} mutual`,
    connectivityHint: 'Select a node.',
    connectivitySelect: 'Node',
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
    connectivityTablePeer: 'Node',
    connectivityTableType: 'Type',
    connectivityTableOut: 'A->B',
    connectivityTableIn: 'B->A',
    connectivityTableAge: 'Last seen',
    connectivityTableSignal: 'SNR',
    relationTypeOut: 'from me',
    relationTypeIn: 'to me',
    relationTypeMutual: 'mutual',
    staleShort: 'stale',
    routeSource: 'Source',
    routeTarget: 'Target',
    routeSwap: 'Swap',
    routeForward: 'A->B',
    routeBackward: 'B->A',
    routePickHint: 'Pick on map',
    routeSelectedA: 'A',
    routeSelectedB: 'B',
    routeUnset: 'not set',
    routeStatusYes: 'route available',
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
    dataErrorStale: (detail) => `No fresh data from the server (${detail}). You are looking at the last known state.`,
    dataErrorRetry: 'Try again',
    dataErrorDismiss: 'Dismiss message',
    packetPathsTitle: 'Observed packet paths',
    packetPathsHint: 'Paths adverts actually travelled. Click one to draw it on the map.',
    packetPathsLoading: 'Loading paths...',
    packetPathsEmpty: 'No observed paths in the last 48 hours.',
    packetPathsUnresolved: (count) => `${count} hop${count === 1 ? '' : 's'} unidentified`,
    panelHide: 'Hide panel',
    panelShow: 'Show panel',
    clusterSummary: (total, ok, missing, stale) => `Cluster of ${total} nodes: ${ok} with data, ${missing} without data, ${stale} inactive`,
    toolbarMapTitle: 'Network map',
    toolbarMapSubtitle: 'Select a node on the map or from the list to inspect direct links.',
    toolbarNewTitle: 'New nodes',
    toolbarNewSubtitle: 'Nodes first seen within the last 24 hours.',
    toolbarConnectivityTitle: 'Connectivity',
    toolbarConnectivitySubtitle: 'Check who can see the selected node and who it can see.',
    toolbarRouteTitle: 'Routes',
    toolbarRouteSubtitle: 'Pick source and target. We show the answer first, then the details.',
    newRepeaters: 'New nodes 24h',
    emptyNoNewRepeaters: 'No completely new nodes were first seen in the last 24 hours.',
    routeTapTarget: 'Pick source or target on the map.',
    routeTapTargetSource: 'Click a node on the map to set the source.',
    routeTapTargetTarget: 'Click a node on the map to set the target.',
    routeTapTargetReady: 'Click a node on the map to change source or target.',
    roleDefault: 'Repeater',
    kindSignal: 'signal',
    noDataShort: 'n/a',
    loadingSignalHistory: 'Loading signal history...',
    storedSamples: (count) => `Only ${count} stored sample${count === 1 ? '' : 's'} for this link so far. The history chart appears after at least 2 samples.`,
    agoSeconds: (count) => `${count}s ago`,
    agoMinutes: (count) => `${count}m ago`,
    agoHours: (count) => `${count}h ago`,
    agoDays: (count) => `${count}d ago`,
  },
};
const map = L.map('map', { zoomControl: true, preferCanvas: true }).setView([53.43, 14.55], 8);
// ponytail: CARTO basemaps now demand an API key; OSM standard tiles are keyless
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19,
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);
const markersLayer = (typeof L.markerClusterGroup === 'function')
  ? L.markerClusterGroup({
      showCoverageOnHover: false,
      spiderfyOnMaxZoom: true,
      zoomToBoundsOnClick: true,
      disableClusteringAtZoom: 15,
      maxClusterRadius: 48,
      spiderfyDistanceMultiplier: 1.6,
      chunkedLoading: true,
      iconCreateFunction(cluster) {
        let g = 0, b = 0, r = 0;
        try {
          for (const m of cluster.getAllChildMarkers()) {
            const s = m.options && m.options.nodeState;
            if (s === 'ok') g++;
            else if (s === 'missing') b++;
            else r++;
          }
        } catch {}
        const n = g + b + r || cluster.getChildCount();
        const size = n < 10 ? 40 : n < 50 ? 48 : n < 200 ? 56 : 64;
        // The old bubble crammed "●5 ●1 ●8" inside a 38px circle, which was
        // unreadable at map scale. The mix is a ring around the count instead:
        // proportion is legible at a glance, the number stays the loud part.
        const stops = [];
        let at = 0;
        for (const [count, color] of [[g, '#3fbf78'], [b, '#5b9cf0'], [r, '#e0604f']]) {
          if (!count) continue;
          const end = at + (count / n) * 360;
          stops.push(`${color} ${at}deg ${end}deg`);
          at = end;
        }
        if (!stops.length) stops.push('#5b9cf0 0deg 360deg');
        const label = trFormat('clusterSummary', n, g, b, r);
        return L.divIcon({
          html: `<div class="mc-cluster-bubble" style="--mix: conic-gradient(from -90deg, ${stops.join(', ')})" role="img" aria-label="${label}" title="${label}"><span class="mc-cluster-total">${n}</span></div>`,
          className: 'mc-cluster-icon',
          iconSize: [size, size],
        });
      },
    })
  : L.layerGroup();
markersLayer.addTo(map);
// Registry of ALL node markers, keyed by identityHex. Filled synchronously
// by attachNodeMarker() at marker-creation sites. Used by applyMapFilters().
const _allNodeMarkers = new Map();
window._allNodeMarkers = _allNodeMarkers;
// Synchronous helper: register a circleMarker and add to markersLayer ONLY
// if it passes the current filter (window._nodePasses). This guarantees
// archival/out-of-range nodes never enter the cluster group.
window.attachNodeMarker = function(marker, node) {
  try {
    const pk = node && node.identity_hex;
    if (pk) _allNodeMarkers.set(pk, marker);
    const pass = (typeof window._nodePasses === 'function') ? !!window._nodePasses(node) : true;
    if (pass) marker.addTo(markersLayer);
  } catch (e) {
    try { marker.addTo(markersLayer); } catch {}
  }
  return marker;
};
if (typeof markersLayer.on === 'function' && typeof markersLayer.zoomToShowLayer === 'function') {
  // Hover-spiderfy with grace window: keep cluster open while cursor moves to a child marker.
  let _unspiderTimer = null;
  let _activeCluster = null;
  const cancelUnspider = () => { if (_unspiderTimer) { clearTimeout(_unspiderTimer); _unspiderTimer = null; } };
  const scheduleUnspider = () => {
    cancelUnspider();
    _unspiderTimer = setTimeout(() => {
      if (_activeCluster) { try { _activeCluster.unspiderfy(); } catch {} }
      _activeCluster = null;
      _unspiderTimer = null;
    }, 550);
  };
  markersLayer.on('clustermouseover', (event) => {
    cancelUnspider();
    _activeCluster = event.layer;
    try { event.layer.spiderfy(); } catch {}
  });
  markersLayer.on('clustermouseout', scheduleUnspider);
  markersLayer.on('spiderfied', (event) => {
    for (const m of (event.markers || [])) {
      m.on('mouseover', cancelUnspider);
      m.on('mouseout', scheduleUnspider);
    }
  });
  markersLayer.on('unspiderfied', () => { _activeCluster = null; cancelUnspider(); });
}
const halosLayer = L.layerGroup().addTo(map);
const linksLayer = L.layerGroup().addTo(map);
const labelsLayer = L.layerGroup().addTo(map);
const linkLabelsLayer = L.layerGroup().addTo(map);
let latestState = null;
let latestManagement = null;
let signalHistoryByNode = {};
let selectedSourceId = null;
let selectedNeighborId = null;
let hoveredNodeId = null;
let nodeSortMode = 'last_advert';
let nodeSearchQuery = '';
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
let refreshTimerId = null;
let refreshInFlight = null;
let managementRefreshInFlight = null;
let latestStateEtag = null;
let latestManagementEtag = null;
let latestManagementLoaded = false;
// Observed packet paths come from the server already hop-resolved; the browser
// only has one byte per hop and cannot disambiguate them on its own.
let packetPaths = [];
let packetPathsLoaded = false;
let packetPathsInFlight = null;
let selectedPacketPathId = null;
let latestManagementIncludesHistorical = false;
let signalHistoryRefreshInFlightByNode = new Map();
let signalHistoryLoadedNodes = new Set();
let signalHistoryPendingNodes = new Set();
let sidebarSheetState = localStorage.getItem('meshcoreDashboardSheetState') || 'collapsed';
let pendingMapClearSelectionKey = null;
let pendingMapClearExpiresAt = 0;
let restoreDoubleClickZoomTimer = null;
let probeQueueFeedback = null;
let probeQueueBusyNodeId = null;
const BLANK_MAP_CLEAR_WINDOW_MS = 900;
const DOUBLE_CLICK_ZOOM_RESTORE_MS = 260;
const MIN_NODE_SEARCH_QUERY_LENGTH = 2;
const IDLE_REFRESH_INTERVAL_MS = 300000;
const ACTIVE_PROBE_REFRESH_INTERVAL_MS = 15000;
const ERROR_REFRESH_INTERVAL_MS = 60000;

function emptyManagementState() {
  return {
    has_active_probe_jobs: false,
    map_links: [],
    route_hints: {},
    historical_links: [],
  };
}

function mergeStateWithManagement(state, management = latestManagement) {
  if (!state) return null;
  return {
    ...state,
    management: {
      ...emptyManagementState(),
      ...(management || {}),
    },
  };
}

function commitState(state) {
  latestState = mergeStateWithManagement(state);
  return latestState;
}

function commitManagement(management, includesHistorical = false) {
  latestManagement = {
    ...emptyManagementState(),
    ...(management || {}),
  };
  latestManagementLoaded = true;
  latestManagementIncludesHistorical = includesHistorical;
  if (latestState) {
    latestState = mergeStateWithManagement(latestState, latestManagement);
  }
  return latestManagement;
}

function clearFocusedDataCache() {
  latestManagement = null;
  latestManagementEtag = null;
  latestManagementLoaded = false;
  latestManagementIncludesHistorical = false;
  signalHistoryByNode = {};
  signalHistoryRefreshInFlightByNode = new Map();
  signalHistoryLoadedNodes = new Set();
  signalHistoryPendingNodes = new Set();
  if (latestState) {
    latestState = mergeStateWithManagement(latestState, null);
  }
}

function selectedNodeNeedsManagement() {
  return Boolean(selectedSourceId && (currentPanel === 'map' || currentPanel === 'new'));
}

function currentPanelNeedsManagement() {
  return currentPanel === 'connectivity' || currentPanel === 'route' || selectedNodeNeedsManagement();
}

function selectedHistoryNodeKey(node) {
  if (!node) return null;
  return String(node.identity_hex || '');
}

function hasSignalHistoryLoaded(node) {
  const nodeKey = selectedHistoryNodeKey(node);
  if (!nodeKey) return false;
  return signalHistoryLoadedNodes.has(nodeKey);
}

function isSignalHistoryLoading(node) {
  const nodeKey = selectedHistoryNodeKey(node);
  if (!nodeKey) return false;
  return signalHistoryPendingNodes.has(nodeKey);
}

function strings() {
  return TRANSLATIONS[currentLanguage] || TRANSLATIONS.pl;
}

function tr(key) {
  return strings()[key];
}

function trFormat(key, ...values) {
  const entry = tr(key);
  return typeof entry === 'function' ? entry(...values) : entry;
}

// Shared HTML escaper: the feature module had its own copy, the main render path
// had none, which is how an escape call reached a scope that never defined it.
function escapeMarkupText(value) {
  return String(value ?? '').replace(/[<>&"']/g, (character) => ({
    '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;', "'": '&#39;',
  }[character]));
}

function normalizeSearchText(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .trim();
}

function effectiveNodeSearchQuery() {
  const query = normalizeSearchText(nodeSearchQuery);
  return query.length >= MIN_NODE_SEARCH_QUERY_LENGTH ? query : '';
}

function hasActiveNodeSearchQuery() {
  return Boolean(effectiveNodeSearchQuery());
}

function autoShowArchived(state) {
  if (currentPanel === 'new' || showArchived) return false;
  const nodes = state?.nodes || [];
  return nodes.length > 0 && nodes.every((node) => isInactive(node));
}

function archivedVisible(state) {
  // Archived toggle removed from toolbar; chip bar (≤1h/≤6h/≤24h/>24h/b/d) now handles age filtering.
  return true;
}

function nodeMatchesSearch(node) {
  const query = effectiveNodeSearchQuery();
  if (!query) return true;
  const haystack = normalizeSearchText(`${node.name || ''} ${node.hash_prefix_hex || ''} ${node.identity_hex || ''}`);
  return haystack.includes(query);
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

// The chip bar floats above the sheet, so it needs the sheet's REAL height, not a
// second formula that guesses it. One observer, one variable, no drift.
function publishSheetHeight() {
  const sidebar = document.getElementById('sidebar');
  const root = document.documentElement;
  if (!sidebar || !isPortraitMobileView()) {
    root.style.setProperty('--sheet-h', '0px');
    return;
  }
  root.style.setProperty('--sheet-h', `${Math.round(sidebar.getBoundingClientRect().height)}px`);
}

function watchSheetHeight() {
  const sidebar = document.getElementById('sidebar');
  if (!sidebar || typeof ResizeObserver !== 'function') return;
  new ResizeObserver(publishSheetHeight).observe(sidebar);
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
    publishSheetHeight();
    return;
  }
  sidebar.classList.toggle('sheet-collapsed', sidebarSheetState === 'collapsed');
  sidebar.classList.toggle('sheet-expanded', sidebarSheetState !== 'collapsed');
  toggle.setAttribute('aria-expanded', sidebarSheetState === 'collapsed' ? 'false' : 'true');
  const label = toggle.querySelector('.sheet-label');
  if (label) label.textContent = sidebarSheetState === 'collapsed' ? tr('sheetExpand') : tr('sheetCollapse');
  localStorage.setItem('meshcoreDashboardSheetState', sidebarSheetState);
  publishSheetHeight();
}

function toggleSidebarSheet() {
  sidebarSheetState = sidebarSheetState === 'collapsed' ? 'expanded' : 'collapsed';
  syncSidebarSheetState();
}

let panelCollapsed = localStorage.getItem('meshcoreDashboardPanelCollapsed') === 'true';

function syncPanelCollapsed() {
  const sidebar = document.getElementById('sidebar');
  const toggle = document.getElementById('panel-toggle');
  if (!sidebar || !toggle) return;
  sidebar.classList.toggle('panel-hidden', panelCollapsed);
  toggle.classList.toggle('is-collapsed', panelCollapsed);
  document.body.classList.toggle('panel-collapsed', panelCollapsed);
  toggle.setAttribute('aria-expanded', panelCollapsed ? 'false' : 'true');
  const label = panelCollapsed ? tr('panelShow') : tr('panelHide');
  toggle.setAttribute('aria-label', label);
  toggle.title = label;
  sidebar.setAttribute('aria-hidden', panelCollapsed ? 'true' : 'false');
  // Leaflet sizes itself from the container, so it needs a nudge after the slide.
  setTimeout(() => map.invalidateSize({ pan: false }), 240);
}

function togglePanelCollapsed() {
  panelCollapsed = !panelCollapsed;
  localStorage.setItem('meshcoreDashboardPanelCollapsed', panelCollapsed ? 'true' : 'false');
  syncPanelCollapsed();
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
  if (!['map', 'new', 'connectivity', 'route'].includes(panel)) return;
  resetPendingMapClear();
  currentPanel = panel;
  latestManagementLoaded = false;
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
  resetPendingMapClear();
  connectivityDirection = direction;
  localStorage.setItem('meshcoreDashboardConnectivityDirection', direction);
  if (latestState) render(latestState);
}

function setShowArchived(value) {
  resetPendingMapClear();
  showArchived = Boolean(value);
  localStorage.setItem('meshcoreDashboardShowArchived', showArchived ? 'true' : 'false');
  if (latestState) render(latestState);
}

function renderLegend() {
  const legend = document.getElementById('map-legend');
  legend.innerHTML = `
    <div class="legend-group">
      <span class="legend-title">${tr('legendRepeaters')}</span>
      <div class="legend-row"><span class="legend-node legend-node-solid" style="background:${STATUS_COLORS.ok}"></span><span>${tr('legendDataAvailable')}</span></div>
      <div class="legend-row"><span class="legend-node legend-node-dashed" style="background:${STATUS_COLORS.missing}"></span><span>${tr('legendKnownNoData')}</span></div>
      <div class="legend-row"><span class="legend-node legend-node-dotted" style="background:${STATUS_COLORS.silent}"></span><span>${tr('legendInactive')}</span></div>
    </div>
    <div class="legend-group">
      <span class="legend-title">${tr('legendLinks')}</span>
      <div class="legend-row"><span class="legend-line" style="border-top-color:${snrColor(12)}"></span><span>${tr('legendStrong')}</span></div>
      <div class="legend-row"><span class="legend-line" style="border-top-color:${snrColor(7)}"></span><span>${tr('legendMedium')}</span></div>
      <div class="legend-row"><span class="legend-line" style="border-top-color:${snrColor(2)}"></span><span>${tr('legendWeak')}</span></div>
      <div class="legend-row"><span class="legend-line" style="border-top-color:${snrColor(-6)}"></span><span>${tr('legendVeryWeak')}</span></div>
      <div class="legend-row"><span class="legend-arrow">➜</span><span>${tr('legendArrow')}</span></div>
      <div class="legend-row"><span class="legend-line dashed" style="border-top-color:#6a7883"></span><span>${tr('legendDashed')}</span></div>
      <div class="legend-row"><span class="legend-line legend-line-thick" style="border-top-color:#b45ef0"></span><span>${tr('legendObserved')}</span></div>
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

function isNewRepeater(node) {
  if (!node?.first_seen_at) return false;
  return Date.now() - new Date(node.first_seen_at).getTime() <= ACTIVE_THRESHOLD_MS;
}

function newRepeaterNodes(state) {
  return (state.nodes || []).filter((node) => isNewRepeater(node));
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

// Node status and link quality used to share the same five hexes, so a green dot
// and a green line meant unrelated things. Status now lives on a teal-to-slate
// axis; the warm red-to-green ramp belongs to SNR alone (see SNR_RAMP).
const STATUS_COLORS = {
  ok: '#17a2a2',
  missing: '#5b6cd6',
  silent: '#7d8896',
};

function nodeColor(node) {
  const state = nodeState(node);
  if (state === 'ok') return STATUS_COLORS.ok;
  if (state === 'missing') return STATUS_COLORS.missing;
  return STATUS_COLORS.silent;
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

// Single source of truth for "what is visible right now". The summary cards used
// to count from the unfiltered set while the list and map were filtered by a
// separate DOM-hiding pass, so the numbers contradicted the rows underneath them.
function relevantNodes(state) {
  const base = currentPanel === 'new' ? newRepeaterNodes(state) : (state.nodes || []);
  const passes = window._nodePasses;
  if (typeof passes !== 'function') return base;
  return base.filter(passes);
}

function archivedNodeCount(state) {
  if (currentPanel === 'new') return 0;
  return (state.nodes || []).filter((node) => isInactive(node)).length;
}

function normalizeVisibleSelections(state) {
  // A node picked by hand or arriving in the URL outranks the age filter - the
  // filter decides what to browse, not what the operator is allowed to look at.
  const visibleIds = new Set((state.nodes || []).map((node) => node.identity_hex));
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
  // Endpoints resolve against EVERY known node, not just the filtered ones: an
  // age filter narrows the browsing list, it must not amputate the neighbours of
  // a node the operator explicitly selected.
  const nodeIndex = new Map((state.nodes || []).map((node) => [node.identity_hex, node]));
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
  const historicalEdges = [];
  const historicalPairSet = new Set();
  for (const link of (state.management?.historical_links || [])) {
    if (!nodeIndex.has(link.source_identity_hex) || !nodeIndex.has(link.target_identity_hex)) continue;
    if (link.source_identity_hex === link.target_identity_hex) continue;
    const pairKey = `${link.source_identity_hex}|${link.target_identity_hex}`;
    if (pairSet.has(pairKey) || historicalPairSet.has(pairKey)) continue;
    const ageSeconds = typeof link.last_heard_seconds === 'number'
      ? link.last_heard_seconds
      : Math.max(0, Math.floor((Date.now() - new Date(link.collected_at).getTime()) / 1000));
    historicalEdges.push({
      ...link,
      age_seconds: ageSeconds,
      stale: true,
      historical: true,
      mutual: false,
    });
    historicalPairSet.add(pairKey);
  }
  for (const edge of edges) {
    edge.mutual = pairSet.has(`${edge.target_identity_hex}|${edge.source_identity_hex}`);
  }
  // Keyed by every known node for the same reason as nodeIndex: relations belong
  // to the node, not to whatever slice of the list is on screen.
  const relationMap = new Map([...nodeIndex.keys()].map((identityHex) => [identityHex, { outgoing: [], incoming: [], mutual: [], oneWayOutgoing: [], oneWayIncoming: [] }]));
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
    historicalEdges,
    relationMap,
    summary: {
      directed: edges.length,
      mutual: edges.filter((edge) => edge.mutual).length / 2,
      oneWay: edges.filter((edge) => !edge.mutual).length,
      stale: edges.filter((edge) => edge.stale).length,
      historical: historicalEdges.length,
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

function buildHistoricalRouteResult(state, sourceId, targetId) {
  const data = connectivityData(state);
  const path = routePath(data.historicalEdges, sourceId, targetId);
  return { path, usesHistorical: Boolean(path) };
}

function buildRouteReachability(state, sourceId) {
  const data = connectivityData(state);
  if (!sourceId) {
    return { destinations: [], highlightIds: new Set(), treeEdges: [] };
  }
  const destinations = [];
  const highlightIds = new Set([sourceId]);
  const treeEdges = new Map();
  for (const node of data.nodes) {
    const targetId = node.identity_hex;
    if (!targetId || targetId === sourceId) continue;
    const routeResult = buildRouteResult(state, sourceId, targetId);
    if (!routeResult.path) continue;
    highlightIds.add(targetId);
    destinations.push({
      identityHex: targetId,
      name: node.name || node.hash_prefix_hex || targetId.slice(0, 8),
      hopCount: Math.max(0, routeResult.path.length - 1),
      usesStale: routeResult.usesStale,
    });
    for (let index = 0; index < routeResult.path.length - 1; index += 1) {
      const edgeSourceId = routeResult.path[index];
      const edgeTargetId = routeResult.path[index + 1];
      const edgeKey = `${edgeSourceId}:${edgeTargetId}`;
      const previous = treeEdges.get(edgeKey);
      if (!previous || (previous.usesStale && !routeResult.usesStale)) {
        treeEdges.set(edgeKey, { sourceId: edgeSourceId, targetId: edgeTargetId, usesStale: routeResult.usesStale });
      }
    }
  }
  destinations.sort((left, right) => (left.hopCount - right.hopCount) || (Number(left.usesStale) - Number(right.usesStale)) || left.name.localeCompare(right.name));
  return { destinations, highlightIds, treeEdges: [...treeEdges.values()] };
}

function getSelectedNode(state) {
  return (state.nodes || []).find((node) => node.identity_hex === selectedSourceId) || null;
}

function routeHintForNode(state, identityHex) {
  if (!identityHex) return null;
  return (state.management?.route_hints || {})[identityHex] || null;
}

// Hops the server resolved for the path the operator picked. Unresolved ones stay
// null so the chain breaks instead of inventing a waypoint.
function observedHopsFromServer() {
  const row = packetPathById(selectedPacketPathId);
  if (!row) return null;
  const points = [];
  const origin = row.origin || {};
  points.push(isFiniteCoordinate(origin.latitude, origin.longitude)
    ? { latitude: origin.latitude, longitude: origin.longitude, name: origin.name }
    : null);
  for (const hop of row.hops || []) {
    points.push(isFiniteCoordinate(hop.latitude, hop.longitude)
      ? { latitude: hop.latitude, longitude: hop.longitude, name: hop.name }
      : null);
  }
  return points;
}

// Fallback for a node's own stored route hint, where the browser can only match
// one-byte prefixes and usually has to give up.
function observedHopsFromHint(state) {
  const targetId = routeTargetId || routeSourceId;
  if (!targetId) return null;
  const hint = routeHintForNode(state, targetId);
  const pathRow = hint?.latest_saved_path || hint?.latest_advert_path;
  if (!pathRow) return null;
  const decoded = decodeHintPath(state, targetId, pathRow);
  const points = decoded.steps.map((step) => (
    step.kind === 'resolved' && step.node && isFiniteCoordinate(step.node.latitude, step.node.longitude)
      ? step.node
      : null
  ));
  if (decoded.targetNode && isFiniteCoordinate(decoded.targetNode.latitude, decoded.targetNode.longitude)) {
    points.push(decoded.targetNode);
  }
  return points;
}

// Plot the hops a packet really passed through: resolved hops become map points,
// unresolved ones break the chain so the line never fakes a hop.
function drawObservedPath(state, data) {
  const hops = selectedPacketPathId ? observedHopsFromServer() : observedHopsFromHint(state);
  if (!hops || hops.length < 2) return;
  const color = '#b45ef0';
  let drawn = 0;
  for (let index = 0; index < hops.length - 1; index += 1) {
    const from = hops[index];
    const to = hops[index + 1];
    if (!from || !to) continue;
    L.polyline([[from.latitude, from.longitude], [to.latitude, to.longitude]], {
      color,
      weight: 5,
      opacity: 0.92,
      lineCap: 'round',
    }).addTo(linksLayer);
    addDirectionalArrow(from, to, color, 0.5);
    drawn += 1;
  }
  if (drawn) {
    for (const hop of hops) {
      if (!hop) continue;
      L.circleMarker([hop.latitude, hop.longitude], {
        radius: 6, color, weight: 2, fillColor: '#fff', fillOpacity: 1,
      }).addTo(linksLayer);
    }
  }
}

function decodeHintPath(state, targetId, pathRow) {
  const targetNode = (state.nodes || []).find((node) => node.identity_hex === targetId) || null;
  const normalizedHex = String(pathRow?.path_hex || '').trim().toUpperCase();
  const pathLen = Number(pathRow?.path_len || 0);
  const prefixes = [];
  for (let index = 0; index < normalizedHex.length; index += 2) {
    const prefixHex = normalizedHex.slice(index, index + 2);
    if (prefixHex.length === 2) prefixes.push(prefixHex);
  }
  const steps = prefixes.slice(0, pathLen || prefixes.length).map((prefixHex) => {
    const matches = (state.nodes || []).filter((node) => String(node.identity_hex || '').startsWith(prefixHex));
    if (matches.length === 1) {
      return {
        kind: 'resolved',
        label: matches[0].name || matches[0].hash_prefix_hex || prefixHex,
        // Kept so the hop can be plotted, not just printed.
        node: matches[0],
      };
    }
    if (matches.length > 1) {
      return {
        kind: 'ambiguous',
        label: tr('routeProbePathAmbiguousHop')(prefixHex, matches.length),
      };
    }
    return {
      kind: 'unknown',
      label: trFormat('routeProbePathUnknownHop', prefixHex),
    };
  });
  return { steps, targetNode };
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

function listNodes(state) {
  const nodes = sortNodes(relevantNodes(state));
  if (currentPanel === 'connectivity' || currentPanel === 'route') return nodes;
  const filtered = nodes.filter((node) => nodeMatchesSearch(node));
  if (!selectedSourceId) return filtered;
  const selectedNode = nodes.find((node) => node.identity_hex === selectedSourceId);
  if (!selectedNode || filtered.some((node) => node.identity_hex === selectedNode.identity_hex)) {
    return filtered;
  }
  return [selectedNode].concat(filtered);
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
  if (!selectedNode || !isFiniteCoordinate(selectedNode.latitude, selectedNode.longitude)) {
    if (visibleNodes.length) fitNodeCollection(visibleNodes, selectedSourceId);
    return;
  }
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
  if (routeSourceId) {
    const reachability = buildRouteReachability(state, routeSourceId);
    for (const destination of reachability.destinations) ids.add(destination.identityHex);
  }
  if (routeSourceId && routeTargetId && routeSourceId !== routeTargetId) {
    const forward = buildRouteResult(state, routeSourceId, routeTargetId);
    const backward = buildRouteResult(state, routeTargetId, routeSourceId);
    for (const identityHex of (forward.path || [])) ids.add(identityHex);
    for (const identityHex of (backward.path || [])) ids.add(identityHex);
  }
  fitNodeCollection(data.nodes.filter((node) => ids.has(node.identity_hex)), routeSourceId || routeTargetId);
}

function currentPanelCopy() {
  if (currentPanel === 'connectivity') {
    return { title: tr('toolbarConnectivityTitle'), subtitle: tr('toolbarConnectivitySubtitle') };
  }
  if (currentPanel === 'new') {
    return { title: tr('toolbarNewTitle'), subtitle: tr('toolbarNewSubtitle') };
  }
  if (currentPanel === 'route') {
    return { title: tr('toolbarRouteTitle'), subtitle: tr('toolbarRouteSubtitle') };
  }
  return { title: tr('toolbarMapTitle'), subtitle: tr('toolbarMapSubtitle') };
}

function routeSummaryValue(routeResult, historicalRouteResult = null) {
  if (routeResult?.path) return tr('routeStatusYes');
  if (historicalRouteResult?.path) return tr('routeHistoricalRoute');
  return tr('routeStatusNo');
}

function renderSummaryMetrics(cards, metricClass) {
  return cards.map((item) => `
    <div class="${metricClass}">
      <strong>${item.value}</strong>
      <span>${item.label}</span>
    </div>
  `).join('');
}

function buildPanelSummary(state) {
  const panelCopy = currentPanelCopy();
  const nodes = relevantNodes(state);
  const data = connectivityData(state);
  const selectedNode = selectedSourceId ? data.nodeIndex.get(selectedSourceId) || null : null;
  const defaultCards = [
    { label: currentPanel === 'new' ? tr('summaryNew') : tr('summaryKnown'), value: nodes.length },
    { label: tr('summaryWithData'), value: nodes.filter((node) => !isInactive(node) && node.data_fetch_ok).length },
    { label: tr('summaryPending'), value: nodes.filter((node) => !isInactive(node) && !node.data_fetch_ok).length },
    { label: tr('summaryInactive'), value: nodes.filter((node) => isInactive(node)).length },
  ];
  if (currentPanel === 'connectivity') {
    const node = selectedConnectivityNode(state);
    if (!node) {
      return { ...panelCopy, status: '', cards: defaultCards };
    }
    const relations = data.relationMap.get(node.identity_hex) || { outgoing: [], incoming: [] };
    return {
      title: panelCopy.title,
      subtitle: `${tr('selectedRepeater')}: ${node.name}`,
      status: connectivityModeLabel(node),
      cards: [
        { label: tr('connectivityVisible'), value: connectivityVisibleRows(state, node.identity_hex).length },
        { label: tr('connectivitySummaryOut'), value: relations.outgoing.length },
        { label: tr('connectivitySummaryIn'), value: relations.incoming.length },
        { label: tr('connectivitySummaryMutual'), value: relationRows(state, node.identity_hex, '2way').length },
      ],
    };
  }
  if (currentPanel === 'route') {
    const sourceNode = routeSourceId ? data.nodeIndex.get(routeSourceId) || null : null;
    const targetNode = routeTargetId ? data.nodeIndex.get(routeTargetId) || null : null;
    const hasRoutePair = Boolean(sourceNode && targetNode && routeSourceId !== routeTargetId);
    const reachability = sourceNode ? buildRouteReachability(state, routeSourceId) : null;
    const forward = hasRoutePair ? buildRouteResult(state, routeSourceId, routeTargetId) : null;
    const backward = hasRoutePair ? buildRouteResult(state, routeTargetId, routeSourceId) : null;
    const historicalForward = forward?.path || !hasRoutePair
      ? null
      : buildHistoricalRouteResult(state, routeSourceId, routeTargetId);
    const historicalBackward = backward?.path || !hasRoutePair
      ? null
      : buildHistoricalRouteResult(state, routeTargetId, routeSourceId);
    let subtitle = panelCopy.subtitle;
    let status = '';
    if (sourceNode && targetNode) {
      subtitle = `${tr('routeSource')}: ${sourceNode.name} | ${tr('routeTarget')}: ${targetNode.name}`;
      status = `${tr('routeForward')} / ${tr('routeBackward')}`;
    } else if (sourceNode) {
      subtitle = `${tr('routeSource')}: ${sourceNode.name}`;
      status = tr('routeStatePickTarget');
    } else if (targetNode) {
      subtitle = `${tr('routeTarget')}: ${targetNode.name}`;
      status = tr('routeStatePickSource');
    }
    return {
      title: panelCopy.title,
      subtitle,
      status,
      cards: [
        { label: tr('routeSelectedA'), value: sourceNode ? tr('statusData') : '-' },
        { label: tr('routeSelectedB'), value: targetNode ? tr('statusData') : '-' },
        hasRoutePair
          ? { label: tr('routeForward'), value: routeSummaryValue(forward, historicalForward) }
          : { label: tr('routeReachabilityFreshShort'), value: reachability ? reachability.destinations.length : '-' },
        hasRoutePair
          ? { label: tr('routeBackward'), value: routeSummaryValue(backward, historicalBackward) }
          : { label: tr('routeReachabilityStaleShort'), value: reachability ? reachability.destinations.filter((destination) => destination.usesStale).length : '-' },
      ],
    };
  }
  return {
    title: panelCopy.title,
    subtitle: selectedNode ? `${tr('selectedRepeater')}: ${selectedNode.name}` : panelCopy.subtitle,
    status: selectedNode ? nodeStateLabel(selectedNode) : '',
    cards: defaultCards,
  };
}

// Stats only. Title, subtitle and status live in the toolbar head, which is the
// single place the panel identifies itself.
function renderSummaryCards(summary) {
  return `<div class="summary-grid">${renderSummaryMetrics(summary.cards, 'summary-card')}</div>`;
}

function renderPrimaryTabs() {
  return `
    <div class="primary-toggle" role="group" aria-label="${tr('viewLabel')}">
      <button type="button" class="segmented-button${currentPanel === 'map' ? ' active' : ''}" data-panel="map">${tr('panelMap')}</button>
      <button type="button" class="segmented-button${currentPanel === 'connectivity' ? ' active' : ''}" data-panel="connectivity">${tr('panelConnectivity')}</button>
      <button type="button" class="segmented-button${currentPanel === 'route' ? ' active' : ''}" data-panel="route">${tr('panelRoute')}</button>
      <button type="button" class="segmented-button${currentPanel === 'new' ? ' active' : ''}" data-panel="new">${tr('panelNew')}</button>
    </div>
  `;
}

function renderAnalysisTabs() {
  return '';
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

function renderExpandablePanel(title, body, open = false) {
  return `
    <details class="panel-details"${open ? ' open' : ''}>
      <summary>${title}</summary>
      <div class="panel-details-body">${body}</div>
    </details>
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

function routeSummaryCard(title, routeResult, data, historicalRouteResult = null) {
  const directionClass = title === tr('routeForward') ? 'forward' : 'backward';
  if (!routeResult.path) {
    if (historicalRouteResult?.path) {
      const historicalHtml = historicalRouteResult.path.map((identityHex) => {
        const node = data.nodeIndex.get(identityHex);
        const name = node?.name || identityHex.slice(0, 8);
        return `<div class="route-hop-row"><span class="route-step">${name}</span></div>`;
      }).join('');
      return `
        <div class="route-card">
          <div class="route-card-head"><strong>${title}</strong><span class="route-direction-chip ${directionClass}">${title}</span></div>
          <div class="route-status-row"><span class="route-status-badge no">${tr('routeHistoricalRoute')}</span><span class="route-meta">${Math.max(0, historicalRouteResult.path.length - 1)} ${tr('routeHopCount')}, ${tr('routeHistoricalLinks')}</span></div>
          <div class="route-empty"><strong>${tr('routeHistoryFallback')}</strong></div>
          <div class="route-path">${historicalHtml}</div>
        </div>
      `;
    }
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

function renderRouteProbePathSection(state) {
  if (!routeTargetId) {
    return '';
  }
  const hint = routeHintForNode(state, routeTargetId);
  const savedPath = hint?.latest_saved_path || null;
  const advertPath = hint?.latest_advert_path || null;
  const chosenPath = savedPath || advertPath;
  const latestProbeRun = hint?.latest_probe_run || null;
  if (!chosenPath) {
    const message = latestProbeRun?.result === 'success'
      ? `${tr('routeProbePathNoStored')} ${tr('routeProbePathFallback')}`
      : tr('routeProbePathNoStored');
    const metrics = latestProbeRun?.endpoint_name
      ? [{ value: latestProbeRun.endpoint_name, label: tr('routeProbePathEndpoint') }]
      : [];
    return `<div class="panel-section">${renderExpandablePanel(tr('routeProbePathTitle'), renderAnswerStrip(tr('routeProbePathTitle'), '', message, metrics, true))}</div>`;
  }
  const decoded = decodeHintPath(state, routeTargetId, chosenPath);
  const pathSteps = [
    `<span class="route-hint-step">${tr('routeProbePathBot')}</span>`,
    ...decoded.steps.map((step) => `<span class="route-hint-arrow">&rarr;</span><span class="route-hint-step${step.kind === 'resolved' ? '' : ' uncertain'}">${step.label}</span>`),
    `<span class="route-hint-arrow">&rarr;</span><span class="route-hint-step">${decoded.targetNode?.name || tr('routeProbePathTarget')}</span>`,
  ].join('');
  const chips = [
    `<span class="route-hint-chip">${savedPath ? tr('routeProbePathSaved') : tr('routeProbePathAdvert')}</span>`,
  ];
  if (chosenPath.source) {
    chips.push(`<span class="route-hint-chip">${tr('routeProbePathSource')}: ${chosenPath.source}</span>`);
  }
  if (chosenPath.endpoint_name) {
    chips.push(`<span class="route-hint-chip">${tr('routeProbePathEndpoint')}: ${chosenPath.endpoint_name}</span>`);
  }
  if (chosenPath.observed_at) {
    chips.push(`<span class="route-hint-chip">${tr('routeProbePathObserved')}: ${formatShortWhen(chosenPath.observed_at)}</span>`);
  }
  const note = latestProbeRun?.result === 'success' && advertPath && !savedPath
    ? tr('routeProbePathFallback')
    : '';
  return `
    <div class="panel-section">
      ${renderExpandablePanel(
        tr('routeProbePathTitle'),
        `${renderAnswerStrip(tr('routeProbePathTitle'), '', savedPath ? tr('routeProbePathSaved') : tr('routeProbePathAdvert'), [{ value: Number(chosenPath.path_len || 0), label: tr('routeHopCount') }])}
        <div class="route-hint-shell">
          <div class="route-hint-meta">${chips.join('')}</div>
          <div class="route-hint-path">${pathSteps}</div>
          ${note ? `<div class="route-hint-note">${note}</div>` : ''}
        </div>`
      )}
    </div>
  `;
}

function renderRouteReachabilitySection(state) {
  if (routeTargetId) {
    return '';
  }
  if (!routeSourceId) {
    return `<div class="panel-section">${renderAnswerStrip(tr('routeReachabilityTitle'), '', tr('routeReachabilityIdle'))}</div>`;
  }
  const reachability = buildRouteReachability(state, routeSourceId);
  const freshCount = reachability.destinations.filter((destination) => !destination.usesStale).length;
  const staleCount = reachability.destinations.length - freshCount;
  if (!reachability.destinations.length) {
    return `<div class="panel-section">${renderExpandablePanel(
      tr('routeReachabilityTitle'),
      `${renderAnswerStrip(tr('routeReachabilityTitle'), '', tr('routeReachabilityEmpty'), [{ value: 0, label: tr('routeReachabilityFreshShort') }, { value: 0, label: tr('routeReachabilityStaleShort') }], true)}
      <div class="route-destination-empty"><strong>${tr('routeReachabilityEmpty')}</strong><span>${tr('routePickHint')}</span></div>`
    )}</div>`;
  }
  const destinationHtml = reachability.destinations.map((destination) => `
    <button type="button" class="route-destination-item${routeTargetId === destination.identityHex ? ' active' : ''}" data-route-destination="${destination.identityHex}">
      <span class="route-destination-main">
        <strong>${destination.name}</strong>
        <span>${destination.hopCount} ${tr('routeHopCount')}${destination.usesStale ? `, ${tr('routeUsesStale')}` : `, ${tr('routeFreshOnly')}`}</span>
      </span>
      <span class="route-destination-action">${routeTargetId === destination.identityHex ? tr('routeSelectedB') : tr('routeReachabilityAction')}</span>
    </button>
  `).join('');
  return `<div class="panel-section">${renderExpandablePanel(
    tr('routeReachabilityTitle'),
    `${renderAnswerStrip(tr('routeReachabilityTitle'), '', trFormat('routeReachabilitySummary', reachability.destinations.length), [{ value: freshCount, label: tr('routeReachabilityFreshShort') }, { value: staleCount, label: tr('routeReachabilityStaleShort') }])}
    <div class="route-destination-list">${destinationHtml}</div>`
  )}</div>`;
}

function renderRoutePanel(state) {
  const data = connectivityData(state);
  const options = data.nodes.map((node) => `<option value="${node.identity_hex}">${node.name}</option>`).join('');
  let body = '';
  if (routeSourceId && routeTargetId) {
    if (routeSourceId === routeTargetId) {
      body += `<div class="panel-section">${renderAnswerStrip(tr('routeResultsTitle'), '', tr('routeStateSameNode'), [], true)}</div>`;
    } else {
      const forward = buildRouteResult(state, routeSourceId, routeTargetId);
      const backward = buildRouteResult(state, routeTargetId, routeSourceId);
      const historicalForward = forward.path ? null : buildHistoricalRouteResult(state, routeSourceId, routeTargetId);
      const historicalBackward = backward.path ? null : buildHistoricalRouteResult(state, routeTargetId, routeSourceId);
      body += `<div class="panel-section">${renderAnswerStrip(tr('routeResultsTitle'), '', tr('routeStateReady'), [{ value: forward.path ? tr('routeStatusYes') : historicalForward?.path ? tr('routeHistoricalRoute') : '-', label: tr('routeForward') }, { value: backward.path ? tr('routeStatusYes') : historicalBackward?.path ? tr('routeHistoricalRoute') : '-', label: tr('routeBackward') }])}<div class="route-result-grid">${routeSummaryCard(tr('routeForward'), forward, data, historicalForward)}${routeSummaryCard(tr('routeBackward'), backward, data, historicalBackward)}</div></div>`;
    }
  } else if (routeSourceId) {
    body += `<div class="panel-section">${renderAnswerStrip(tr('routeResultsTitle'), '', tr('routeStatePickTarget'))}</div>`;
  } else if (routeTargetId) {
    body += `<div class="panel-section">${renderAnswerStrip(tr('routeResultsTitle'), '', tr('routeStatePickSource'))}</div>`;
  } else if (!routeSourceId && !routeTargetId) {
    body += `<div class="panel-section">${renderAnswerStrip(tr('routeResultsTitle'), '', tr('routeStateIdle'))}</div>`;
  }
  body += renderPacketPathsSection();
  body += renderRouteReachabilitySection(state);
  body += renderRouteProbePathSection(state);
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
          <div class="route-endpoint-stack">
            <button type="button" class="route-endpoint route-endpoint-target${routeActiveEndpoint === 'target' ? ' active' : ''}" data-route-active="target">
              <span class="route-endpoint-label">${tr('routeSelectedB')}</span>
              <strong class="route-endpoint-name">${routeTargetId ? targetName : tr('routeUnset')}</strong>
            </button>
            ${routeTargetId ? `<div class="route-actions"><button type="button" class="route-endpoint-clear" data-route-clear-target="1">${tr('routeClearTarget')}</button></div>` : ''}
          </div>
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

function activeMapSelectionKey() {
  if (currentPanel === 'route') return null;
  if (!selectedSourceId && !selectedNeighborId) return null;
  return `${currentPanel}:${selectedSourceId || ''}:${selectedNeighborId || ''}`;
}

function resetPendingMapClear() {
  pendingMapClearSelectionKey = null;
  pendingMapClearExpiresAt = 0;
}

function armBlankMapClear() {
  const selectionKey = activeMapSelectionKey();
  if (!selectionKey) {
    resetPendingMapClear();
    return false;
  }
  const now = Date.now();
  const shouldClear = pendingMapClearSelectionKey === selectionKey && pendingMapClearExpiresAt > now;
  pendingMapClearSelectionKey = selectionKey;
  pendingMapClearExpiresAt = now + BLANK_MAP_CLEAR_WINDOW_MS;
  return shouldClear;
}

function suppressUpcomingDoubleClickZoom() {
  if (!map.doubleClickZoom.enabled()) return;
  map.doubleClickZoom.disable();
  if (restoreDoubleClickZoomTimer !== null) {
    window.clearTimeout(restoreDoubleClickZoomTimer);
  }
  restoreDoubleClickZoomTimer = window.setTimeout(() => {
    map.doubleClickZoom.enable();
    restoreDoubleClickZoomTimer = null;
  }, DOUBLE_CLICK_ZOOM_RESTORE_MS);
}

function selectNode(identityHex) {
  resetPendingMapClear();
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
  resetPendingMapClear();
  selectedSourceId = null;
  selectedNeighborId = null;
  if (!latestState) return;
  render(latestState);
}

async function queueProbeJob(repeaterId) {
  if (!latestState) return;
  const numericRepeaterId = Number(repeaterId);
  if (!Number.isFinite(numericRepeaterId)) return;
  const node = (latestState.nodes || []).find((item) => Number(item.id) === numericRepeaterId);
  if (!node) return;

  probeQueueBusyNodeId = node.identity_hex;
  if (latestState) render(latestState);

  try {
    const response = await fetch('/api/probe-jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        repeater_id: numericRepeaterId,
      }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload?.detail || 'queue failed');
    }
    probeQueueFeedback = {
      identityHex: node.identity_hex,
      status: payload.status || 'error',
      scheduledAt: payload.scheduled_at || null,
    };
    await refresh(true);
    await refreshFocusedDataIfNeeded({ force: true });
  } catch (error) {
    probeQueueFeedback = {
      identityHex: node.identity_hex,
      status: 'error',
      scheduledAt: null,
      // The backend's own `detail` was thrown away here; the user saw a generic hint.
      detail: error && error.message ? error.message : null,
    };
    if (latestState) render(latestState);
  } finally {
    probeQueueBusyNodeId = null;
    if (latestState) render(latestState);
  }
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

function findReverseLink(link) {
  try {
    if (!link || link.source_identity_hex === link.target_identity_hex) return null;
    const all = (latestState && latestState.management && latestState.management.map_links) || [];
    return all.find((other) => other.source_identity_hex === link.target_identity_hex
      && other.target_identity_hex === link.source_identity_hex) || null;
  } catch { return null; }
}

function formatLinkMetric(link) {
  const metric = lineSignalMetric(link);
  if (metric.value === null) return tr('signalMissing');
  const unit = metric.kind === 'RSSI' ? 'dBm' : 'dB';
  return `${metric.kind}: ${metric.value.toFixed(1)} ${unit}`;
}

function linkLabel(link, sourceNode) {
  const distance = neighborDistanceKm(sourceNode, link);
  const forwardLine = formatLinkMetric(link);
  const reverse = findReverseLink(link);
  const distanceLine = distance !== null ? `${tr('distancePrefix')}: ${distance.toFixed(1)} km` : tr('distanceMissing');
  if (reverse) {
    const reverseLine = formatLinkMetric(reverse);
    return `<strong>→ ${forwardLine}</strong><strong>← ${reverseLine}</strong><span>${distanceLine}</span>`;
  }
  return `<strong>${forwardLine}</strong><span>${distanceLine}</span>`;
}

// Four buckets painted SNR 2 and SNR 9 the same colour. This is a continuous
// ramp over the usable range (-10..+15 dB), so neighbouring links differ visibly.
const SNR_RAMP = [
  { at: -10, rgb: [198, 74, 61] },
  { at: 0, rgb: [219, 125, 49] },
  { at: 5, rgb: [207, 170, 56] },
  { at: 10, rgb: [70, 160, 110] },
  { at: 15, rgb: [46, 139, 87] },
];

function snrColor(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return '#98a4ad';
  const v = Math.max(SNR_RAMP[0].at, Math.min(SNR_RAMP[SNR_RAMP.length - 1].at, value));
  for (let i = 0; i < SNR_RAMP.length - 1; i += 1) {
    const a = SNR_RAMP[i];
    const b = SNR_RAMP[i + 1];
    if (v > b.at) continue;
    const t = (v - a.at) / (b.at - a.at || 1);
    const mix = a.rgb.map((channel, idx) => Math.round(channel + (b.rgb[idx] - channel) * t));
    return `rgb(${mix.join(',')})`;
  }
  return `rgb(${SNR_RAMP[SNR_RAMP.length - 1].rgb.join(',')})`;
}

function lineColor(link) {
  return snrColor(lineSignalMetric(link).value);
}

// Neighbour data can be minutes or months old. Freshness fades continuously
// instead of flipping at a single 6h threshold: full strength up to an hour,
// then down to a faint trace at a week.
function linkFreshness(link) {
  const seconds = Number(link?.last_heard_seconds);
  if (!Number.isFinite(seconds) || seconds < 0) return 0.45;
  const hours = seconds / 3600;
  if (hours <= 1) return 1;
  if (hours >= 24 * 7) return 0.16;
  const ratio = Math.log(hours) / Math.log(24 * 7);
  return Math.max(0.16, 1 - 0.84 * ratio);
}

// Status must survive colour blindness, so the outline carries it too:
// solid ring = fresh data, dashed = known but no data, dotted = silent >24h.
function statusDash(node) {
  const state = nodeState(node);
  if (state === 'ok') return null;
  if (state === 'missing') return '4 3';
  return '1 3';
}

function markerStyle(node, isolated, selected, neighbor) {
  const color = nodeColor(node);
  const dashArray = statusDash(node);
  if (selected) {
    return { radius: 12, color: '#15212a', weight: 3.6, fillColor: color, fillOpacity: 1, opacity: 1, dashArray };
  }
  if (neighbor) {
    return { radius: 7.5, color, weight: 2, fillColor: color, fillOpacity: 0.9, opacity: 0.94, dashArray };
  }
  if (isolated) {
    return { radius: 4, color, weight: 1, fillColor: color, fillOpacity: 0.16, opacity: 0.2, dashArray };
  }
  return { radius: 5, color, weight: 1.2, fillColor: color, fillOpacity: 0.82, opacity: 0.85, dashArray };
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
  const isHovered = node.identity_hex === hoveredNodeId;
  const hoverMeta = isHovered ? `<span class="label-meta">${tr('lastAdvertLabel')}: ${formatShortWhen(node.last_advert_at)}</span>` : '';
  if (forced || zoom >= HIGH_ZOOM_LABEL_THRESHOLD) {
    return `<div class="${chipClass}"><strong>${shortName}</strong>${hoverMeta}</div>`;
  }
  if (zoom >= LOW_ZOOM_LABEL_THRESHOLD && (isFocusedNode || isHovered)) {
    return `<div class="${chipClass}"><strong>${shortName}</strong>${hoverMeta}</div>`;
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
  const seen = new Set();
  for (const link of selectedLinks) {
    if (selectedNeighborId && link.target_identity_hex !== selectedNeighborId) continue;
    if (link.source_identity_hex === link.target_identity_hex) continue;
    const key = link.target_identity_hex || `${link.target_latitude},${link.target_longitude}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const midpoint = [
      (link.source_latitude + link.target_latitude) / 2,
      (link.source_longitude + link.target_longitude) / 2,
    ];
    L.marker(midpoint, {
      icon: L.divIcon({ className: 'link-label-icon', html: `<div class="signal-label-chip">${linkLabel(link, sourceNode)}</div>`, iconSize: null }),
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
  return (signalHistoryByNode[node.identity_hex] || [])
    .filter((row) => row.target_identity_hex === neighborId || row.target_hash_prefix_hex === neighborId)
    .sort((left, right) => new Date(left.collected_at) - new Date(right.collected_at));
}

function probeJobsForNode(state, node) {
  if (!node) return [];
  return (state.probe_jobs || []).filter((job) => job.pubkey_hex === node.identity_hex);
}

function nextProbeJobForNode(state, node) {
  const activeJobs = probeJobsForNode(state, node)
    .filter((job) => job.status === 'pending' || job.status === 'running')
    .sort((left, right) => {
      const leftRank = left.status === 'running' ? 0 : 1;
      const rightRank = right.status === 'running' ? 0 : 1;
      if (leftRank !== rightRank) return leftRank - rightRank;
      const leftTime = left.scheduled_at ? new Date(left.scheduled_at).getTime() : 0;
      const rightTime = right.scheduled_at ? new Date(right.scheduled_at).getTime() : 0;
      if (leftTime !== rightTime) return leftTime - rightTime;
      return (left.id || 0) - (right.id || 0);
    });
  return activeJobs[0] || null;
}

function probeQueueFeedbackForNode(node) {
  if (!node || !probeQueueFeedback) return null;
  return probeQueueFeedback.identityHex === node.identity_hex ? probeQueueFeedback : null;
}

function probeQueueSummary(state, node) {
  if (!node) return null;
  const activeJob = nextProbeJobForNode(state, node);
  if (activeJob) {
    if (activeJob.status === 'running') {
      return {
        chip: tr('probeQueueRunning'),
        chipClass: 'busy',
        note: tr('probeQueueHintRunning'),
      };
    }
    if (activeJob.scheduled_at && new Date(activeJob.scheduled_at).getTime() > Date.now()) {
      return {
        chip: tr('probeQueuePending'),
        chipClass: 'pending',
        note: tr('probeQueueHintPendingAt')(formatShortWhen(activeJob.scheduled_at)),
      };
    }
    return {
      chip: tr('probeQueuePending'),
      chipClass: 'pending',
      note: tr('probeQueueHintPendingNow'),
    };
  }

  if (probeQueueBusyNodeId === node.identity_hex) {
    return {
      chip: tr('probeQueuePending'),
      chipClass: 'busy',
      note: tr('probeQueueBusy'),
    };
  }

  const feedback = probeQueueFeedbackForNode(node);
  if (!feedback) return null;
  if (feedback.status === 'queued') {
    return {
      chip: tr('probeQueueQueued'),
      chipClass: 'pending',
      note: feedback.scheduledAt
        ? tr('probeQueueHintQueuedAt')(formatShortWhen(feedback.scheduledAt))
        : tr('probeQueueHintQueuedNow'),
    };
  }
  if (feedback.status === 'already_pending') {
    return {
      chip: tr('probeQueuePending'),
      chipClass: 'pending',
      note: feedback.scheduledAt
        ? tr('probeQueueHintPendingAt')(formatShortWhen(feedback.scheduledAt))
        : tr('probeQueueHintPendingNow'),
    };
  }
  if (feedback.status === 'cooldown') {
    return {
      chip: tr('probeQueueCooldown'),
      chipClass: 'cooldown',
      note: tr('probeQueueHintCooldown'),
    };
  }
  if (feedback.status === 'error') {
    return {
      chip: tr('probeQueueError'),
      chipClass: 'error',
      note: feedback.detail ? `${tr('probeQueueHintError')} ${feedback.detail}` : tr('probeQueueHintError'),
    };
  }
  return null;
}

function renderProbeQueueCard(state, node) {
  const summary = probeQueueSummary(state, node);
  const isBusy = probeQueueBusyNodeId === node.identity_hex;
  return `
    <div class="probe-queue-card">
      <div class="expand-head">
        <strong>${tr('probeQueueTitle')}</strong>
        ${summary ? `<span class="probe-status-chip ${summary.chipClass}">${summary.chip}</span>` : ''}
      </div>
      <div class="probe-queue-controls">
        <button type="button" class="probe-submit-button" data-queue-probe="${node.id}" ${isBusy ? 'disabled' : ''}>${isBusy ? tr('probeQueueBusy') : tr('probeQueueAction')}</button>
      </div>
      ${summary?.note ? `<div class="probe-note">${summary.note}</div>` : ''}
    </div>
  `;
}

function renderSignalChart(node, neighborLink, historyRows) {
  if (!node) return `<div class="empty-note">${tr('emptySelectRepeater')}</div>`;
  if (!neighborLink) return `<div class="empty-note">${tr('emptySelectNeighbor')}</div>`;
  if (isSignalHistoryLoading(node) && !hasSignalHistoryLoaded(node)) {
    return `<div class="empty-note">${tr('loadingSignalHistory')}</div>`;
  }
  if (historyRows.length < 2) {
    return `
      <div class="chart-shell">
        <div class="chart-head">
          <div class="chart-title"><strong>${neighborLink.target_name}</strong><span>${tr('chartHistory')} ${lineSignalMetric(neighborLink).kind}</span></div>
          <div class="chart-meta">${tr('chartLatest')} ${lineSignalMetric(neighborLink).label}</div>
        </div>
        <div class="empty-note">${tr('storedSamples')(historyRows.length)}</div>
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
    return `<line x1="${leftPad}" y1="${y}" x2="${leftPad + width}" y2="${y}" stroke="rgba(21,33,42,0.08)" stroke-width="1" />` +
      `<text x="4" y="${y + 4}" fill="#6a7883" font-size="10">${value}</text>`;
  }).join('');
  const path = historyRows.map((row, index) => {
    const x = leftPad + ((new Date(row.collected_at).getTime() - minTime) / timeSpan) * width;
    const y = topPad + ((maxValue - row.snr) / valueSpan) * height;
    return `${index === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`;
  }).join(' ');
  const points = historyRows.map((row) => {
    const x = leftPad + ((new Date(row.collected_at).getTime() - minTime) / timeSpan) * width;
    const y = topPad + ((maxValue - row.snr) / valueSpan) * height;
    return `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="2.2" fill="${lineColor(neighborLink)}" />`;
  }).join('');
  return `
    <div class="chart-shell">
      <div class="chart-head">
        <div class="chart-title"><strong>${neighborLink.target_name}</strong><span>${tr('chartSNRHistory')}</span></div>
        <div class="chart-meta">${tr('chartLatest')} ${lineSignalMetric(neighborLink).label}</div>
      </div>
      <svg id="signal-chart" viewBox="0 0 320 152" preserveAspectRatio="none">
        ${grid}
        <path d="${path}" fill="none" stroke="${lineColor(neighborLink)}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        ${points}
        <text x="${leftPad}" y="144" fill="#6a7883" font-size="10">${timeAgo(new Date(minTime).toISOString())}</text>
        <text x="${leftPad + width - 22}" y="144" fill="#6a7883" font-size="10">${tr('chartNow')}</text>
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
    <table class="neighbor-table">
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
          const activeClass = link.target_identity_hex === selectedNeighborId ? ' class="active"' : '';
          return `
            <tr${activeClass}>
              <td><button type="button" data-neighbor="${link.target_identity_hex}">${link.target_name}</button></td>
              <td>${typeof link.last_heard_seconds === 'number' ? humanizeSeconds(link.last_heard_seconds) : timeAgo(link.collected_at)}</td>
              <td>${lineSignalMetric(link).label}</td>
              <td>${distance === null ? '-' : `${distance.toFixed(1)} km`}</td>
            </tr>
          `;
        }).join('')}
      </tbody>
    </table>
  ` : `<div class="empty-note">${tr('emptyNoNeighborLinks')}</div>`;
  return `
    <div class="node-expand">
      <div class="expand-head">
        <strong>${tr('inspection')}</strong>
        <button type="button" class="ghost-button" data-clear-selection="1">${tr('clearFocus')}</button>
      </div>
      <div class="detail-grid">
        <div class="detail-cell"><strong>${tr('role')}</strong>${node.role || tr('roleDefault')}</div>
        <div class="detail-cell"><strong>${tr('firstSeen')}</strong>${formatWhen(node.first_seen_at)}</div>
        <div class="detail-cell"><strong>${tr('lastAdvert')}</strong>${formatWhen(node.last_advert_at)}</div>
        <div class="detail-cell"><strong>${tr('lastData')}</strong>${formatWhen(node.last_data_at)}</div>
        <div class="detail-cell"><strong>${tr('lastSuccessfulProbe')}</strong>${formatWhen(node.last_successful_probe_at)}</div>
        <div class="detail-cell"><strong>${tr('lastProbeResult')}</strong>${describeProbeResult(node)}</div>
        <div class="detail-cell"><strong>${tr('lastProbeAttempt')}</strong>${formatWhen(node.last_probe_at)}</div>
      </div>
      ${renderProbeQueueCard(state, node)}
      <div>
        <div class="expand-head"><strong>${tr('directNeighbors')}</strong><span class="node-state-tag">${selectedLinks.length}</span></div>
        ${neighborRows}
      </div>
      ${renderSignalChart(node, selectedLink, historyRows)}
    </div>
  `;
}

function rowHtml(node, state) {
  const primaryAgeLabel = currentPanel === 'new' ? tr('firstSeenLabel') : tr('lastAdvertLabel');
  const primaryAgeValue = currentPanel === 'new' ? node.first_seen_at : node.last_advert_at;
  return `
    <div class="node-row${node.identity_hex === selectedSourceId ? ' active' : ''}">
      <button type="button" class="node-row-button" data-node="${node.identity_hex}">
        <span class="status-dot" style="background:${nodeColor(node)}"></span>
        <span class="node-main">
          <span class="node-name">${node.name || node.hash_prefix_hex}</span>
          <span class="node-age">${primaryAgeLabel}: ${formatShortWhen(primaryAgeValue)}</span>
          ${currentPanel === 'new' ? `<span class="node-age">${tr('lastAdvertLabel')}: ${formatShortWhen(node.last_advert_at)}</span>` : ''}
          <span class="node-age">${tr('lastDataLabel')}: ${formatShortWhen(node.last_data_at)}</span>
        </span>
        <span class="node-state-tag">${nodeStateLabel(node)}</span>
      </button>
      ${node.identity_hex === selectedSourceId && (currentPanel === 'map' || currentPanel === 'new') ? renderExpandedNode(node, state) : ''}
    </div>
  `;
}

function renderNodeSections(state) {
  const container = document.getElementById('node-sections');
  const allNodes = sortNodes(relevantNodes(state));
  const nodes = listNodes(state);
  const selectedNode = selectedSourceId ? allNodes.find((node) => node.identity_hex === selectedSourceId) : null;
  const others = nodes.filter((node) => node.identity_hex !== selectedSourceId);
  const summary = buildPanelSummary(state);
  const panelTitle = summary.title;
  const panelSubtitle = summary.subtitle;
  const archivedCount = archivedNodeCount(state);
  const archivedAutoFallback = autoShowArchived(state);
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
  const searchHtml = currentPanel === 'map' || currentPanel === 'new'
    ? `
        <div class="toolbar-meta-group toolbar-search">
          <label for="node-search">${tr('searchLabel')}</label>
          <input id="node-search" class="toolbar-search-input" type="search" data-node-search="1" placeholder="${tr('searchPlaceholder')}" />
        </div>
      `
    : '';
  const archivedHtml = '';
  const metaHtml = `${searchHtml}${sortHtml}`;
  const langHtml = `<div class="lang-toggle" role="group" aria-label="${tr('languageLabel')}"><button type="button" class="lang-button" data-global-language="pl">PL</button><button type="button" class="lang-button" data-global-language="en">EN</button></div>`;
  const archivedNoteHtml = archivedAutoFallback
    ? `<div class="toolbar-note"><strong>${tr('archivedToggle')}</strong> ${tr('archivedAutoFallback')}</div>`
    : '';
  html += `
    <div class="list-toolbar">
      <div class="toolbar-head">
        <div class="toolbar-head-main">
          <strong class="toolbar-title">${panelTitle}</strong>
          <span class="toolbar-subtitle">${panelSubtitle}</span>
        </div>
        <div class="toolbar-head-actions">
          ${summary.status ? `<span class="summary-badge">${summary.status}</span>` : ''}
          ${archivedHtml}
          ${langHtml}
        </div>
      </div>
      ${archivedNoteHtml}
      ${renderPrimaryTabs()}
      ${renderSummaryCards(summary)}
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
  } else if (currentPanel === 'new') {
    if (selectedNode) {
      html += `<div class="section-heading">${tr('selectedRepeater')}</div>`;
      html += `<div class="node-list">${rowHtml(selectedNode, state)}</div>`;
    }
    html += `<div class="section-heading">${tr('newRepeaters')}</div>`;
    html += `<div class="node-list">${others.length ? others.map((node) => rowHtml(node, state)).join('') : `<div class="empty-note">${hasActiveNodeSearchQuery() ? tr('emptyNoSearchResults') : tr('emptyNoNewRepeaters')}</div>`}</div>`;
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
      for (const input of container.querySelectorAll('[data-node-search]')) {
        input.value = nodeSearchQuery;
        input.addEventListener('input', () => {
          nodeSearchQuery = input.value || '';
          render(latestState);
        });
      }
      for (const button of container.querySelectorAll('[data-queue-probe]')) {
        button.addEventListener('click', () => {
          queueProbeJob(button.dataset.queueProbe);
        });
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
    html += `<div class="node-list">${others.length ? others.map((node) => rowHtml(node, state)).join('') : `<div class="empty-note">${hasActiveNodeSearchQuery() ? tr('emptyNoSearchResults') : tr('emptyNoOtherRepeaters')}</div>`}</div>`;
  }
  container.innerHTML = html;
  for (const button of container.querySelectorAll('[data-node]')) {
    button.addEventListener('click', () => selectNode(button.dataset.node));
  }
  for (const button of container.querySelectorAll('[data-panel]')) {
    button.addEventListener('click', () => setPanel(button.dataset.panel));
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
  for (const input of container.querySelectorAll('[data-node-search]')) {
    input.value = nodeSearchQuery;
    input.addEventListener('input', () => {
      nodeSearchQuery = input.value || '';
      render(latestState);
    });
  }
  for (const button of container.querySelectorAll('[data-queue-probe]')) {
    button.addEventListener('click', () => {
      queueProbeJob(button.dataset.queueProbe);
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
  for (const button of container.querySelectorAll('[data-packet-path]')) {
    button.addEventListener('click', () => {
      const id = button.dataset.packetPath;
      selectedPacketPathId = String(selectedPacketPathId) === String(id) ? null : id;
      const row = packetPathById(selectedPacketPathId);
      if (row) {
        const points = [row.origin, ...(row.hops || [])]
          .filter((hop) => hop && isFiniteCoordinate(hop.latitude, hop.longitude))
          .map((hop) => [hop.latitude, hop.longitude]);
        if (points.length > 1) map.fitBounds(points, { padding: [60, 60], maxZoom: 12 });
      }
      render(latestState);
    });
  }
  for (const button of container.querySelectorAll('[data-route-destination]')) {
    button.addEventListener('click', () => {
      routeActiveEndpoint = 'target';
      routeTargetId = button.dataset.routeDestination || null;
      if (latestState) focusRouteSelection(latestState);
      render(latestState);
    });
  }
  for (const button of container.querySelectorAll('[data-route-clear-target]')) {
    const clearTarget = () => {
      routeActiveEndpoint = 'target';
      routeTargetId = null;
      if (latestState) focusRouteSelection(latestState);
      render(latestState);
    };
    button.addEventListener('click', clearTarget);
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
  const passes = (n) => (typeof window._nodePasses === 'function') ? window._nodePasses(n) : true;
  const nodes = selectedSourceId
    ? allMapNodes.filter((node) => node.identity_hex === selectedSourceId || neighborIds.has(node.identity_hex))
    : (hasActiveNodeSearchQuery()
        ? allMapNodes.filter((node) => nodeMatchesSearch(node))
        : allMapNodes.filter(passes));
  const bounds = [];
  for (const node of nodes) {
    const selected = node.identity_hex === selectedSourceId;
    const neighbor = neighborIds.has(node.identity_hex);
    const isolated = Boolean(selectedNeighborId) && node.identity_hex !== selectedSourceId && node.identity_hex !== selectedNeighborId;
    if (selected) {
      drawFocusHalo(node, nodeColor(node), nodeColor(node), 17, 12);
    }
    const marker = L.circleMarker([node.latitude, node.longitude], { ...markerStyle(node, isolated, selected, neighbor), identityHex: node.identity_hex, nodeState: nodeState(node) });
    window.attachNodeMarker(marker, node);
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
  const drawnLinks = new Set();
  for (const link of selectedLinks) {
    if (link.source_identity_hex === link.target_identity_hex) continue;
    const linkKey = link.target_identity_hex || `${link.target_latitude},${link.target_longitude}`;
    if (drawnLinks.has(linkKey)) continue;
    drawnLinks.add(linkKey);
    const fresh = linkFreshness(link);
    const dimmed = selectedNeighborId && link.target_identity_hex !== selectedNeighborId;
    const polyline = L.polyline([
      [link.source_latitude, link.source_longitude],
      [link.target_latitude, link.target_longitude],
    ], {
      color: lineColor(link),
      // Strong signal also draws thicker, so quality survives a colour-blind eye.
      weight: selectedNeighborId && link.target_identity_hex === selectedNeighborId
        ? 3.6
        : 1.4 + 2.2 * Math.max(0, Math.min(1, ((lineSignalMetric(link).value ?? -10) + 10) / 25)),
      opacity: dimmed ? 0.18 : Math.max(0.22, 0.9 * fresh),
    }).addTo(linksLayer);
    // Direction used to live only in the hover label. One arrowhead means the
    // link was heard one way; two mean both ends hear each other.
    if (!dimmed) {
      const from = { latitude: link.source_latitude, longitude: link.source_longitude };
      const to = { latitude: link.target_latitude, longitude: link.target_longitude };
      const reverse = findReverseLink(link);
      addDirectionalArrow(from, to, lineColor(link), reverse ? 0.62 : 0.56);
      if (reverse) addDirectionalArrow(to, from, lineColor(reverse), 0.38);
    }
    polyline.on('mouseover', () => {
      if (selectedLinks.length > 6) {
        const midpoint = [
          (link.source_latitude + link.target_latitude) / 2,
          (link.source_longitude + link.target_longitude) / 2,
        ];
        const transient = L.marker(midpoint, {
          icon: L.divIcon({ className: 'link-label-icon', html: `<div class="signal-label-chip">${linkLabel(link, sourceNode)}</div>`, iconSize: null }),
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
    const marker = L.circleMarker([node.latitude, node.longitude], { ...markerStyle(node, false, selected, neighbor), identityHex: node.identity_hex, nodeState: nodeState(node) });
    window.attachNodeMarker(marker, node);
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
  const reachability = routeSourceId && !routeTargetId ? buildRouteReachability(state, routeSourceId) : null;
  const highlightedIds = new Set(reachability?.highlightIds || []);
  for (const identityHex of [routeSourceId, routeTargetId].filter(Boolean)) highlightedIds.add(identityHex);
  const forward = routeSourceId && routeTargetId && routeSourceId !== routeTargetId ? buildRouteResult(state, routeSourceId, routeTargetId) : null;
  const backward = routeSourceId && routeTargetId && routeSourceId !== routeTargetId ? buildRouteResult(state, routeTargetId, routeSourceId) : null;
  const historicalForward = routeSourceId && routeTargetId && routeSourceId !== routeTargetId && !forward?.path
    ? buildHistoricalRouteResult(state, routeSourceId, routeTargetId)
    : null;
  const historicalBackward = routeSourceId && routeTargetId && routeSourceId !== routeTargetId && !backward?.path
    ? buildHistoricalRouteResult(state, routeTargetId, routeSourceId)
    : null;
  const pathIds = new Set(forward?.path || []);
  for (const identityHex of (backward?.path || [])) pathIds.add(identityHex);
  for (const identityHex of (historicalForward?.path || [])) pathIds.add(identityHex);
  for (const identityHex of (historicalBackward?.path || [])) pathIds.add(identityHex);
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
  const drawReachabilityTree = (reachabilityResult) => {
    if (!reachabilityResult) return;
    for (const edge of reachabilityResult.treeEdges) {
      const sourceNode = data.nodeIndex.get(edge.sourceId);
      const targetNode = data.nodeIndex.get(edge.targetId);
      if (!sourceNode || !targetNode) continue;
      if (!isFiniteCoordinate(sourceNode.latitude, sourceNode.longitude) || !isFiniteCoordinate(targetNode.latitude, targetNode.longitude)) continue;
      const color = edge.usesStale ? 'rgba(156, 123, 19, 0.42)' : 'rgba(44, 113, 209, 0.34)';
      L.polyline([
        [sourceNode.latitude, sourceNode.longitude],
        [targetNode.latitude, targetNode.longitude],
      ], {
        color,
        weight: 2,
        opacity: 0.9,
        dashArray: edge.usesStale ? '6 6' : null,
      }).addTo(linksLayer);
      addDirectionalArrow(sourceNode, targetNode, color, 0.56);
    }
  };
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
  const drawHistoricalContext = (focusIds) => {
    for (const edge of data.historicalEdges) {
      if (!focusIds.has(edge.source_identity_hex) && !focusIds.has(edge.target_identity_hex)) continue;
      const sourceNode = data.nodeIndex.get(edge.source_identity_hex);
      const targetNode = data.nodeIndex.get(edge.target_identity_hex);
      if (!sourceNode || !targetNode) continue;
      if (!isFiniteCoordinate(sourceNode.latitude, sourceNode.longitude) || !isFiniteCoordinate(targetNode.latitude, targetNode.longitude)) continue;
      L.polyline([
        [sourceNode.latitude, sourceNode.longitude],
        [targetNode.latitude, targetNode.longitude],
      ], {
        color: 'rgba(122, 97, 0, 0.55)',
        weight: 2,
        opacity: 0.86,
        dashArray: '6 8',
      }).addTo(linksLayer);
      addDirectionalArrow(sourceNode, targetNode, 'rgba(122, 97, 0, 0.55)', 0.48);
    }
  };
  drawReachabilityTree(reachability);
  if (routeSourceId && !routeTargetId) {
    drawHistoricalContext(new Set([routeSourceId]));
  }
  drawRoute(forward, '#2c71d1');
  drawRoute(backward, '#cfaa38');
  drawRoute(historicalForward, 'rgba(44, 113, 209, 0.72)', '7 7');
  drawRoute(historicalBackward, 'rgba(207, 170, 56, 0.78)', '7 7');
  // The routes above are computed from the neighbour graph - a proposal. This one
  // is the path a packet actually took, decoded from path_hex; it used to exist
  // only as a row of text chips while the map showed the computed guess.
  drawObservedPath(state, data);
  const labelNodes = routeSourceId
    ? allMapNodes.filter((node) => highlightedIds.has(node.identity_hex))
    : allMapNodes;
  renderLabels(labelNodes, highlightedIds);
  if (!hasFitBounds && bounds.length) fitInitialBounds(bounds);
}

function render(state) {
  latestState = state;
  normalizeVisibleSelections(state);
  renderLegend();
  renderNodeSections(state);
  for (const button of document.querySelectorAll('[data-global-language]')) {
    button.classList.toggle('active', button.dataset.globalLanguage === currentLanguage);
    button.onclick = () => setLanguage(button.dataset.globalLanguage);
  }
  syncSidebarSheetState();
  applyMobileView();
  renderMap(state);
  void refreshFocusedDataIfNeeded();
}

function hasActiveProbeJobs(state) {
  return Boolean((state?.probe_jobs || []).some((job) => job.status === 'pending' || job.status === 'running'));
}

function refreshIntervalMs() {
  if (document.hidden) return null;
  return hasActiveProbeJobs(latestState) ? ACTIVE_PROBE_REFRESH_INTERVAL_MS : IDLE_REFRESH_INTERVAL_MS;
}

function scheduleRefresh(delayMs = null) {
  if (refreshTimerId !== null) {
    window.clearTimeout(refreshTimerId);
    refreshTimerId = null;
  }
  const nextDelay = delayMs ?? refreshIntervalMs();
  if (nextDelay === null) return;
  refreshTimerId = window.setTimeout(() => {
    void refresh();
  }, nextDelay);
}

async function refresh(force = false) {
  if (refreshInFlight) return refreshInFlight;
  refreshInFlight = (async () => {
    const headers = {};
    if (latestStateEtag && !force) {
      headers['If-None-Match'] = latestStateEtag;
    }
    const response = await fetch('/api/state', {
      headers,
      cache: force ? 'no-store' : 'default',
    });
    if (response.status === 304) {
      markHealth('ok');
      return;
    }
    if (!response.ok) {
      markHealth('bad', `HTTP ${response.status}`);
      throw new Error(`state refresh failed: ${response.status}`);
    }
    latestStateEtag = response.headers.get('etag') || latestStateEtag;
    const state = commitState(await response.json());
    markHealth('ok');
    if (isSidebarInteractionActive()) {
      pendingRefreshState = state;
      return;
    }
    render(state);
  })();

  try {
    await refreshInFlight;
  } catch (error) {
    console.error('Dashboard refresh failed', error);
    markHealth('bad', error && error.message ? error.message : error);
    scheduleRefresh(ERROR_REFRESH_INTERVAL_MS);
  } finally {
    refreshInFlight = null;
    if (refreshTimerId === null) {
      scheduleRefresh();
    }
  }
}

async function refreshManagement(force = false) {
  if (!currentPanelNeedsManagement()) {
    return;
  }
  const includeHistorical = currentPanel === 'connectivity';
  if (managementRefreshInFlight) return managementRefreshInFlight;
  managementRefreshInFlight = (async () => {
    const headers = {};
    if (latestManagementEtag && !force && latestManagementIncludesHistorical === includeHistorical) {
      headers['If-None-Match'] = latestManagementEtag;
    }
    const endpoint = includeHistorical ? '/api/management?include_historical=1' : '/api/management';
    const response = await fetch(endpoint, {
      headers,
      cache: force ? 'no-store' : 'default',
    });
    if (response.status === 304) {
      latestManagementLoaded = true;
      latestManagementIncludesHistorical = includeHistorical;
      return;
    }
    if (!response.ok) {
      throw new Error(`management refresh failed: ${response.status}`);
    }
    latestManagementEtag = response.headers.get('etag') || latestManagementEtag;
    commitManagement(await response.json(), includeHistorical);
    if (!latestState) {
      return;
    }
    if (isSidebarInteractionActive()) {
      pendingRefreshState = latestState;
      return;
    }
    render(latestState);
  })();

  try {
    await managementRefreshInFlight;
  } catch (error) {
    console.error('Dashboard management refresh failed', error);
  } finally {
    managementRefreshInFlight = null;
  }
}

async function refreshPacketPaths(force = false) {
  if (packetPathsInFlight) return packetPathsInFlight;
  if (packetPathsLoaded && !force) return;
  packetPathsInFlight = (async () => {
    try {
      const response = await fetch('/api/packet-paths?limit=60&hours=48', { cache: force ? 'no-store' : 'default' });
      if (!response.ok) throw new Error(`packet paths failed: ${response.status}`);
      const payload = await response.json();
      packetPaths = Array.isArray(payload.rows) ? payload.rows : [];
      packetPathsLoaded = true;
      if (latestState) render(latestState);
    } catch (error) {
      console.error('Packet paths refresh failed', error);
      showDataError(error && error.message ? error.message : 'packet paths');
    } finally {
      packetPathsInFlight = null;
    }
  })();
  return packetPathsInFlight;
}

function packetPathById(id) {
  return packetPaths.find((row) => String(row.id) === String(id)) || null;
}

function renderPacketPathsSection() {
  if (!packetPathsLoaded) {
    return `<div class="panel-section"><div class="section-heading">${tr('packetPathsTitle')}</div><p class="panel-note">${tr('packetPathsLoading')}</p></div>`;
  }
  if (!packetPaths.length) {
    return `<div class="panel-section"><div class="section-heading">${tr('packetPathsTitle')}</div><p class="panel-note">${tr('packetPathsEmpty')}</p></div>`;
  }
  const rows = packetPaths.slice(0, 20).map((row) => {
    const chain = [row.origin?.name || '?', ...row.hops.map((hop) => hop.name || `?·${hop.prefix_hex}`)]
      .map((label) => escapeMarkupText(label))
      .join(' <span class="packet-arrow">&rarr;</span> ');
    const unresolved = row.path_len - row.resolved_hops;
    return `
      <button type="button" class="packet-path-row${String(selectedPacketPathId) === String(row.id) ? ' is-on' : ''}" data-packet-path="${row.id}">
        <span class="packet-path-chain">${chain}</span>
        <span class="packet-path-meta">${formatShortWhen(row.observed_at)} · ${row.path_len} ${tr('routeHopCount')}${unresolved ? ` · ${trFormat('packetPathsUnresolved', unresolved)}` : ''}</span>
      </button>`;
  }).join('');
  return `
    <div class="panel-section">
      <div class="section-heading">${tr('packetPathsTitle')}</div>
      <p class="panel-note">${tr('packetPathsHint')}</p>
      <div class="packet-path-list">${rows}</div>
    </div>`;
}

async function refreshSignalHistory(node, force = false) {
  if (!node) return;
  const nodeKey = selectedHistoryNodeKey(node);
  if (!nodeKey) return;
  if (signalHistoryRefreshInFlightByNode.has(nodeKey)) {
    return signalHistoryRefreshInFlightByNode.get(nodeKey);
  }
  if (signalHistoryLoadedNodes.has(nodeKey) && !force) {
    return;
  }
  signalHistoryPendingNodes.add(nodeKey);
  const requestPromise = (async () => {
    const response = await fetch(`/api/repeaters/${encodeURIComponent(node.id)}/signal-history`, {
      cache: force ? 'no-store' : 'default',
    });
    if (!response.ok) {
      throw new Error(`signal history refresh failed: ${response.status}`);
    }
    const payload = await response.json();
    signalHistoryByNode = {
      ...signalHistoryByNode,
      [nodeKey]: Array.isArray(payload.rows) ? payload.rows : [],
    };
    signalHistoryLoadedNodes.add(nodeKey);
  })();
  // The guard has to be armed BEFORE anything can re-enter this function. The
  // render below reaches refreshFocusedDataIfNeeded, which calls straight back
  // in here; with the guard registered afterwards every level fired its own
  // fetch and its own full re-render - one click turned into dozens of requests
  // and a frozen tab.
  signalHistoryRefreshInFlightByNode.set(nodeKey, requestPromise);
  if (latestState) render(latestState);
  try {
    await requestPromise;
  } catch (error) {
    console.error('Dashboard signal history refresh failed', error);
  } finally {
    signalHistoryRefreshInFlightByNode.delete(nodeKey);
    signalHistoryPendingNodes.delete(nodeKey);
    if (latestState && selectedSourceId === node.identity_hex) {
      render(latestState);
    }
  }
}

// Belt to the braces above: render() calls this, and the fetches it starts can
// render again. One level at a time is enough; a nested call would only repeat
// work the outer one is already doing.
let focusedRefreshRunning = false;

async function refreshFocusedDataIfNeeded(options = {}) {
  const force = Boolean(options.force);
  if (!latestState || document.hidden) {
    return;
  }
  if (focusedRefreshRunning && !force) {
    return;
  }
  focusedRefreshRunning = true;
  try {
    await runFocusedDataRefresh(force);
  } finally {
    focusedRefreshRunning = false;
  }
}

async function runFocusedDataRefresh(force) {
  const includeHistorical = currentPanel === 'connectivity';
  const needsManagementRefresh = force || !latestManagementLoaded || (includeHistorical && !latestManagementIncludesHistorical);
  if (currentPanelNeedsManagement() && needsManagementRefresh) {
    await refreshManagement(force);
  }
  if (currentPanel === 'route') {
    void refreshPacketPaths(force);
  }
  const selectedNode = getSelectedNode(latestState);
  if (selectedNodeNeedsManagement() && selectedNeighborId && selectedNode) {
    await refreshSignalHistory(selectedNode, force);
  }
}

map.on('click', () => {
  hoveredNodeId = null;
  if (armBlankMapClear()) {
    suppressUpcomingDoubleClickZoom();
    clearSelection();
    return;
  }
  if (latestState) renderMap(latestState);
});
map.on('zoomend', () => {
  if (latestState) renderMap(latestState);
});
const sheetToggle = document.getElementById('sheet-toggle');
if (sheetToggle) {
  sheetToggle.addEventListener('click', toggleSidebarSheet);
}
watchSheetHeight();
publishSheetHeight();
document.getElementById('dataErrorClose')?.addEventListener('click', clearDataError);
document.getElementById('panel-toggle')?.addEventListener('click', togglePanelCollapsed);
syncPanelCollapsed();
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
document.addEventListener('visibilitychange', () => {
  if (document.hidden) {
    if (refreshTimerId !== null) {
      window.clearTimeout(refreshTimerId);
      refreshTimerId = null;
    }
    return;
  }
  void Promise.all([refresh(), refreshFocusedDataIfNeeded()]);
});

document.documentElement.lang = currentLanguage;
applyMobileView();
renderLegend();

// === Tura 3: theme toggle + health pill ===
(function setupTheme() {
  const root = document.documentElement;
  const stored = (() => { try { return localStorage.getItem('mc_theme'); } catch { return null; } })();
  const systemQuery = matchMedia('(prefers-color-scheme: dark)');
  const hasStoredChoice = stored === 'light' || stored === 'dark' || stored === 'blackout';
  if (stored === 'light' || stored === 'dark') root.setAttribute('data-theme', stored);
  else if (stored === 'blackout') { root.setAttribute('data-theme', 'dark'); root.setAttribute('data-blackout', '1'); }
  // data-theme must always be set: the palette follows prefers-color-scheme, but
  // component rules key off [data-theme="dark"]. Leaving it unset gives dark
  // variables with light component surfaces (white text on white panels).
  else root.setAttribute('data-theme', systemQuery.matches ? 'dark' : 'light');
  const button = document.getElementById('themeToggle');
  function effective() {
    return root.getAttribute('data-theme') || 'light';
  }
  function syncIcon() {
    if (!button) return;
    button.textContent = effective() === 'dark' ? '\u2600' : '\u263d';
  }
  syncIcon();
  button?.addEventListener('click', () => {
    const next = effective() === 'dark' ? 'light' : 'dark';
    root.setAttribute('data-theme', next);
    root.removeAttribute('data-blackout');
    try { localStorage.setItem('mc_theme', next); } catch {}
    syncIcon();
    try { const u = new URL(location.href); if (u.searchParams.get('theme') === 'blackout') { u.searchParams.delete('theme'); history.replaceState(null, '', u.toString()); } } catch {}
  });
  systemQuery.addEventListener?.('change', () => {
    if (!hasStoredChoice) root.setAttribute('data-theme', systemQuery.matches ? 'dark' : 'light');
    syncIcon();
  });
})();

// A failed fetch used to be console-only: the dashboard kept showing stale data
// with no sign anything was wrong. Now it says so, with the actual cause.
function showDataError(detail) {
  const banner = document.getElementById('dataError');
  if (!banner) return;
  const text = banner.querySelector('.data-error-text');
  if (text) text.textContent = trFormat('dataErrorStale', String(detail || 'brak odpowiedzi'));
  banner.hidden = false;
}

function clearDataError() {
  const banner = document.getElementById('dataError');
  if (banner) banner.hidden = true;
}

let lastHealthOkMs = 0;
function markHealth(state, detail) {
  const pill = document.getElementById('healthPill');
  if (state === 'ok') clearDataError();
  else if (state === 'bad') showDataError(detail);
  if (!pill) return;
  if (state === 'ok') lastHealthOkMs = Date.now();
  pill.classList.toggle('health-ok', state === 'ok');
  pill.classList.toggle('health-warn', state === 'warn');
  pill.classList.toggle('health-bad', state === 'bad');
  const txt = state === 'ok' ? 'live' : state === 'warn' ? 'opóźnione' : state === 'bad' ? 'błąd API' : '—';
  pill.textContent = txt;
  pill.title = lastHealthOkMs ? `Ostatnia odpowiedź: ${new Date(lastHealthOkMs).toLocaleTimeString('pl-PL')}` : 'Brak odpowiedzi';
}
setInterval(() => {
  if (!lastHealthOkMs) return;
  const age = Date.now() - lastHealthOkMs;
  const pill = document.getElementById('healthPill');
  if (!pill) return;
  if (pill.classList.contains('health-bad')) return;
  if (age > 120000) markHealth('bad');
  else if (age > 60000) markHealth('warn');
}, 5000);

void refresh(true);

/* ====================================================================
   BIG FEATURE BATCH (2026-05-11): clustering UX, chips, favs, watchlist,
   URL state, CSV/PNG export, countdown, blackout, map search,
   repeaters-only, compass, QR, compare, history, matrix, heatmap,
   sparkline + advert timeline (client-side ring buffer).
   Built as a self-contained module that hooks into the existing
   `render`, `refresh`, `selectNode` symbols without altering them.
   ==================================================================== */
(function bigFeatureBatch() {
  const LS = (k, fb) => { try { return localStorage.getItem(k) ?? fb; } catch { return fb; } };
  const LSset = (k, v) => { try { localStorage.setItem(k, v); } catch {} };
  const LSjson = (k, fb) => { try { return JSON.parse(localStorage.getItem(k) || 'null') ?? fb; } catch { return fb; } };
  const LSsetJson = (k, v) => { try { localStorage.setItem(k, JSON.stringify(v)); } catch {} };

  const state = {
    favs: new Set(LSjson('mc_favs', [])),
    watch: new Set(LSjson('mc_watch', [])),
    chip: LS('mc_chip', 'h24'),
    favOnly: LS('mc_favOnly', '0') === '1',
    watchOnly: LS('mc_watchOnly', '0') === '1',
    repeatersOnly: LS('mc_repOnly', '0') === '1',
    notif: LS('mc_notif', '0') === '1',
    heatmapOn: false,
    compareIds: [],
    signalBuffer: new Map(), // pubkey -> array of {t, rssi, snr}
    advertBuffer: new Map(), // pubkey -> array of last_advert_at timestamps (ms)
    lastWatchSeen: new Map(), // pubkey -> last_advert_at ms
  };

  // ------- helpers -------
  const $ = (id) => document.getElementById(id);
  const nodeKey = (n) => n.identity_hex || n.pubkey || '';
  const ageMs = (iso) => {
    if (!iso) return Infinity;
    const t = Date.parse(iso);
    if (Number.isNaN(t)) return Infinity;
    return Date.now() - t;
  };
  function nodesArr() { return (latestState && latestState.nodes) || []; }

  // ------- 1. URL state load/save -------
  function readUrlState() {
    try {
      const u = new URL(location.href);
      const p = u.searchParams;
      const node = p.get('node');
      const panel = p.get('panel');
      const chip = p.get('chip');
      const fav = p.get('fav');
      const watch = p.get('watch');
      const black = p.get('theme');
      if (node) selectedSourceId = node;
      if (panel && ['map','new','connectivity','route'].includes(panel)) currentPanel = panel;
      if (chip) state.chip = chip;
      if (fav === '1') state.favOnly = true;
      if (watch === '1') state.watchOnly = true;
      if (black === 'blackout') {
        document.documentElement.setAttribute('data-theme', 'dark');
        document.documentElement.setAttribute('data-blackout', '1');
        try { localStorage.setItem('mc_theme', 'blackout'); } catch {}
      }
    } catch {}
  }
  let urlSyncRaf = 0;
  function syncUrl() {
    if (urlSyncRaf) return;
    urlSyncRaf = requestAnimationFrame(() => {
      urlSyncRaf = 0;
      try {
        const u = new URL(location.href);
        const p = u.searchParams;
        if (selectedSourceId) p.set('node', selectedSourceId); else p.delete('node');
        if (currentPanel && currentPanel !== 'map') p.set('panel', currentPanel); else p.delete('panel');
        if (state.chip && state.chip !== 'all') p.set('chip', state.chip); else p.delete('chip');
        if (state.favOnly) p.set('fav', '1'); else p.delete('fav');
        if (state.watchOnly) p.set('watch', '1'); else p.delete('watch');
        const isBlack = document.documentElement.getAttribute('data-blackout') === '1';
        if (isBlack) p.set('theme', 'blackout'); else if (p.get('theme') === 'blackout') p.delete('theme');
        history.replaceState(null, '', u.toString());
      } catch {}
    });
  }

  // ------- 2. quick filter chips -------
  // 24h used to be the last rung, so 1390 of 1423 nodes fell into one ">24h"
  // bucket that said nothing. The scale now keeps going: days, then a month.
  const HOUR = 3600e3;
  const CHIPS = [
    { id: 'all',  label: 'Wszystkie',  test: () => true },
    { id: 'h1',   label: '≤1h',        test: (n) => ageMs(n.last_advert_at) <= HOUR },
    { id: 'h7',   label: '≤7h',        test: (n) => ageMs(n.last_advert_at) <= 7*HOUR },
    { id: 'h24',  label: '≤24h',       test: (n) => ageMs(n.last_advert_at) <= 24*HOUR },
    { id: 'd7',   label: '≤7d',        test: (n) => ageMs(n.last_advert_at) <= 7*24*HOUR },
    { id: 'd30',  label: '≤30d',       test: (n) => ageMs(n.last_advert_at) <= 30*24*HOUR },
    { id: 'old',  label: '>30d',       test: (n) => ageMs(n.last_advert_at) > 30*24*HOUR },
    { id: 'none', label: 'b/d',        test: (n) => !n.last_advert_at },
  ];
  function renderChips() {
    const bar = $('mcChipBar');
    if (!bar) return;
    // Old ids (h6) survive in bookmarks and localStorage; land them on the
    // nearest live bucket instead of silently selecting nothing.
    if (!CHIPS.some((c) => c.id === state.chip)) {
      state.chip = state.chip === 'h6' ? 'h7' : 'all';
      LSset('mc_chip', state.chip);
    }
    const nodes = nodesArr();
    bar.innerHTML = '';
    for (const c of CHIPS) {
      const n = nodes.filter(c.test).length;
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'mc-chip' + (state.chip === c.id ? ' is-on' : '');
      btn.innerHTML = `${c.label}<span class="mc-chip-count">${n}</span>`;
      btn.setAttribute('aria-pressed', state.chip === c.id ? 'true' : 'false');
      btn.addEventListener('click', () => {
        state.chip = c.id; LSset('mc_chip', c.id); syncUrl(); rerenderFiltered(); fitMapToFiltered();
      });
      bar.appendChild(btn);
    }
  }
  function chipPredicate() {
    const c = CHIPS.find((x) => x.id === state.chip) || CHIPS[0];
    return c.test;
  }

  // ------- 3. favourites + watchlist -------
  function toggleFav(pk) {
    if (state.favs.has(pk)) state.favs.delete(pk); else state.favs.add(pk);
    LSsetJson('mc_favs', [...state.favs]);
    applyAll();
  }
  function toggleWatch(pk) {
    if (state.watch.has(pk)) state.watch.delete(pk); else state.watch.add(pk);
    LSsetJson('mc_watch', [...state.watch]);
    applyAll();
  }

  // inject star + watch + qr buttons next to node names
  function injectRowActions() {
    const rows = document.querySelectorAll('.node-row, .node-card, [data-identity-hex], [data-node-id]');
    const seen = new Set();
    for (const el of rows) {
      const pk = el.getAttribute('data-identity-hex') || el.getAttribute('data-node-id');
      if (!pk || seen.has(el)) continue;
      seen.add(el);
      if (el.querySelector('.mc-node-actions')) continue;
      const nameEl = el.querySelector('.node-name, .node-title, .node-header, h3, h4, strong') || el;
      const wrap = document.createElement('span');
      wrap.className = 'mc-node-actions';
      const fav = document.createElement('button');
      fav.type = 'button'; fav.className = 'mc-fav-star' + (state.favs.has(pk) ? ' is-on' : '');
      fav.textContent = state.favs.has(pk) ? '★' : '☆';
      fav.title = 'Ulubiony'; fav.setAttribute('aria-label', 'Ulubiony');
      fav.addEventListener('click', (e) => { e.stopPropagation(); toggleFav(pk); });
      const watch = document.createElement('button');
      watch.type = 'button'; watch.className = 'mc-watch-btn' + (state.watch.has(pk) ? ' is-on' : '');
      watch.textContent = '👁'; watch.title = 'Obserwuj (notifikacje gdy zniknie)';
      watch.setAttribute('aria-label', 'Obserwuj');
      watch.addEventListener('click', (e) => { e.stopPropagation(); toggleWatch(pk); });
      const qr = document.createElement('button');
      qr.type = 'button'; qr.className = 'mc-qr-btn';
      qr.textContent = '⬚'; qr.title = 'Pokaż QR z linkiem';
      qr.setAttribute('aria-label', 'QR');
      qr.addEventListener('click', (e) => { e.stopPropagation(); showQrModal(pk); });
      wrap.appendChild(fav); wrap.appendChild(watch); wrap.appendChild(qr);
      try { nameEl.appendChild(wrap); } catch {}
    }
  }

  // ------- 4. filtering: chip + fav + watch + repeatersOnly -------
  function nodePasses(n) {
    if (!n) return false;
    const pk = nodeKey(n);
    if (!chipPredicate()(n)) return false;
    if (state.favOnly && !state.favs.has(pk)) return false;
    if (state.watchOnly && !state.watch.has(pk)) return false;
    if (state.repeatersOnly) {
      const role = String(n.role || '').toLowerCase();
      if (!(role.includes('repeater') || role.includes('rpt') || role === '')) return false;
    }
    return true;
  }
  // Expose for the synchronous attachNodeMarker() helper in render paths.
  window._nodePasses = nodePasses;
  function getFilteredNodes() { return nodesArr().filter(nodePasses); }
  function hideFilteredRows() {
    const nodes = nodesArr();
    const byPk = new Map(nodes.map((n) => [nodeKey(n), n]));
    const rows = document.querySelectorAll('.node-row, .node-card, [data-identity-hex], [data-node-id]');
    for (const el of rows) {
      const pk = el.getAttribute('data-identity-hex') || el.getAttribute('data-node-id');
      if (!pk) continue;
      const n = byPk.get(pk);
      if (!n) continue;
      el.style.display = nodePasses(n) ? '' : 'none';
    }
  }
  function applyMapFilters() {
      if (!latestState || !markersLayer) return;
      const reg = window._allNodeMarkers;
      if (!reg || !reg.size) return;
      const byPk = new Map(nodesArr().map((n) => [nodeKey(n), n]));
      // 1. Usuń wszystkie markery z mapy (ale nie z rejestru)
      try { markersLayer.clearLayers(); } catch {}
      // 2. Dodaj tylko te, które przechodzą filtr
      const toAdd = [];
      for (const [pk, marker] of reg.entries()) {
        const n = byPk.get(pk);
        if (n && nodePasses(n)) toAdd.push(marker);
      }
      if (toAdd.length) {
        if (typeof markersLayer.addLayers === 'function') {
          try { markersLayer.addLayers(toAdd); } catch {}
        } else {
          for (const m of toAdd) { try { markersLayer.addLayer(m); } catch {} }
        }
      }
  }
  function computeMapPadding() {
    const pad = { top: 60, right: 40, bottom: 90, left: 40 };
    try {
      const mapEl = document.getElementById('map');
      const sb = document.getElementById('sidebar');
      if (mapEl && sb) {
        const mr = mapEl.getBoundingClientRect();
        const sr = sb.getBoundingClientRect();
        const style = window.getComputedStyle(sb);
        const visible = sr.width > 0 && sr.height > 0
          && style.display !== 'none'
          && style.visibility !== 'hidden'
          && parseFloat(style.opacity || '1') > 0.05;
        if (visible) {
          const overlapRight = Math.max(0, mr.right - sr.left);
          const overlapLeft = Math.max(0, sr.right - mr.left);
          if (sr.left >= mr.left + mr.width / 2) {
            pad.right = Math.max(pad.right, Math.min(overlapRight + 20, mr.width * 0.6));
          } else if (sr.right <= mr.left + mr.width / 2) {
            pad.left = Math.max(pad.left, Math.min(overlapLeft + 20, mr.width * 0.6));
          } else {
            pad.bottom = Math.max(pad.bottom, Math.min(sr.height + 20, mr.height * 0.6));
          }
        }
      }
      const chipBar = document.getElementById('mcChipBar');
      if (chipBar) {
        const cr = chipBar.getBoundingClientRect();
        if (cr.height > 0) pad.bottom = Math.max(pad.bottom, cr.height + 20);
      }
    } catch {}
    return pad;
  }
  function fitMapToFiltered() {
    try {
      if (!map) return;
      const pts = getFilteredNodes()
        .filter((n) => isFiniteCoordinate(n.latitude, n.longitude))
        .map((n) => [n.latitude, n.longitude]);
      if (!pts.length) return;
      const pad = computeMapPadding();
      const opts = {
        paddingTopLeft: [pad.left, pad.top],
        paddingBottomRight: [pad.right, pad.bottom],
        maxZoom: 15,
        animate: true,
      };
      if (pts.length === 1) {
        map.setView(pts[0], Math.max(map.getZoom(), 12), { animate: true });
        try { map.panBy([(pad.left - pad.right) / 2, (pad.top - pad.bottom) / 2], { animate: true }); } catch {}
      } else {
        map.fitBounds(pts, opts);
      }
    } catch (e) { console.warn('fitMapToFiltered', e); }
  }

  // ------- 5. countdown + back-off -------
  let errorBackoff = 0;
  function fmtCountdown(ms) {
    if (ms == null || !isFinite(ms)) return '';
    if (ms <= 0) return 'odśw...';
    const s = Math.floor(ms / 1000);
    if (s < 60) return `↻ ${s}s`;
    const m = Math.floor(s / 60); return `↻ ${m}min`;
  }
  function getNextRefreshMs() {
    // mirror existing refreshIntervalMs() but provide approximate countdown
    try {
      if (document.hidden) return null;
      const base = (typeof hasActiveProbeJobs === 'function' && hasActiveProbeJobs(latestState))
        ? ACTIVE_PROBE_REFRESH_INTERVAL_MS : IDLE_REFRESH_INTERVAL_MS;
      const start = bigFeatureBatch.__lastRefreshAt || Date.now();
      return Math.max(0, (start + base + errorBackoff) - Date.now());
    } catch { return null; }
  }
  setInterval(() => {
    const el = $('mcCountdown'); if (!el) return;
    el.textContent = fmtCountdown(getNextRefreshMs());
  }, 1000);

  // ------- 6. blackout / theme (blackout = dark + data-blackout=1) -------
  function syncBlackoutBtn() {
    const btn = $('mcBtnBlackout'); if (!btn) return;
    btn.classList.toggle('is-on', document.documentElement.getAttribute('data-blackout') === '1');
  }
  $('mcBtnBlackout')?.addEventListener('click', () => {
    const root = document.documentElement;
    const isBlack = root.getAttribute('data-blackout') === '1';
    if (isBlack) {
      root.removeAttribute('data-blackout');
      LSset('mc_theme', 'dark');
    } else {
      root.setAttribute('data-theme', 'dark');
      root.setAttribute('data-blackout', '1');
      LSset('mc_theme', 'blackout');
    }
    syncBlackoutBtn(); syncUrl();
  });
  // honor stored blackout on startup
  if (LS('mc_theme') === 'blackout') {
    document.documentElement.setAttribute('data-theme', 'dark');
    document.documentElement.setAttribute('data-blackout', '1');
  }
  syncBlackoutBtn();

  // ------- 7. util-bar toggles -------
  function bindToggle(id, key, onChange) {
    const btn = $(id); if (!btn) return;
    const sync = () => {
      btn.classList.toggle('is-on', !!state[key]);
      btn.setAttribute('aria-pressed', state[key] ? 'true' : 'false');
    };
    sync();
    btn.addEventListener('click', () => {
      state[key] = !state[key];
      LSset('mc_' + key, state[key] ? '1' : '0');
      sync(); onChange && onChange(); syncUrl(); rerenderFiltered(); fitMapToFiltered();
    });
  }
  bindToggle('mcBtnFavOnly', 'favOnly');
  bindToggle('mcBtnWatchOnly', 'watchOnly');
  bindToggle('mcBtnRepeatersOnly', 'repeatersOnly');

  // ------- 8. notifications -------
  $('mcBtnNotif')?.addEventListener('click', async () => {
    try {
      if (!('Notification' in window)) { alert('Twoja przeglądarka nie obsługuje powiadomień.'); return; }
      if (Notification.permission === 'granted') {
        state.notif = !state.notif; LSset('mc_notif', state.notif ? '1' : '0');
        $('mcBtnNotif').classList.toggle('is-on', state.notif);
        return;
      }
      const perm = await Notification.requestPermission();
      if (perm === 'granted') {
        state.notif = true; LSset('mc_notif', '1');
        $('mcBtnNotif').classList.add('is-on');
        new Notification('MeshCore', { body: 'Powiadomienia o obserwowanych włączone.' });
      }
    } catch (e) { console.warn('notif', e); }
  });
  if (state.notif && 'Notification' in window && Notification.permission === 'granted') {
    $('mcBtnNotif')?.classList.add('is-on');
  }
  function maybeNotifyWatchlist() {
    if (!state.notif) return;
    if (!('Notification' in window) || Notification.permission !== 'granted') return;
    const nodes = nodesArr();
    const now = Date.now();
    for (const n of nodes) {
      const pk = nodeKey(n);
      if (!state.watch.has(pk)) continue;
      const last = n.last_advert_at ? Date.parse(n.last_advert_at) : 0;
      const prev = state.lastWatchSeen.get(pk) || 0;
      if (last) state.lastWatchSeen.set(pk, last);
      if (prev && last && (now - last > 30*60*1000) && (now - prev <= 30*60*1000)) {
        try {
          new Notification('MeshCore: węzeł zniknął', {
            body: `${n.name || pk.slice(0,8)} nie nadaje od ponad 30 min.`,
            tag: 'mc-watch-' + pk,
          });
        } catch {}
      }
    }
  }

  // ------- 9. heatmap layer -------
  let heatLayer = null;
  function refreshHeatLayer() {
    if (!state.heatmapOn) {
      if (heatLayer) { try { map.removeLayer(heatLayer); } catch {} heatLayer = null; }
      return;
    }
    const points = getFilteredNodes()
      .filter((n) => isFiniteCoordinate(n.latitude, n.longitude))
      .map((n) => {
        const a = ageMs(n.last_advert_at);
        const w = a < 3600e3 ? 1.0 : a < 6*3600e3 ? 0.7 : a < 24*3600e3 ? 0.4 : 0.15;
        return [n.latitude, n.longitude, w];
      });
    if (heatLayer) try { map.removeLayer(heatLayer); } catch {}
    if (typeof L.heatLayer === 'function') {
      heatLayer = L.heatLayer(points, { radius: 28, blur: 22, minOpacity: 0.25 }).addTo(map);
    }
  }
  $('mcBtnHeatmap')?.addEventListener('click', () => {
    state.heatmapOn = !state.heatmapOn;
    $('mcBtnHeatmap').classList.toggle('is-on', state.heatmapOn);
    $('mcBtnHeatmap').setAttribute('aria-pressed', state.heatmapOn ? 'true' : 'false');
    refreshHeatLayer();
  });

  // The floating map search is gone; the sidebar search filters the node list,
  // and picking a row selects and focuses the node.
  function escapeText(s) {
    return String(s || '').replace(/[<>&"']/g, (c) => ({ '<':'&lt;','>':'&gt;','&':'&amp;','"':'&quot;',"'":'&#39;' }[c]));
  }
  // ------- 11. CSV export -------
  $('mcBtnCsv')?.addEventListener('click', () => {
    const cols = ['identity_hex','name','role','last_advert_at','first_seen_at','latitude','longitude','rssi','snr'];
    const lines = [cols.join(',')];
    for (const n of nodesArr()) {
      lines.push(cols.map((k) => {
        let v = n[k];
        if (v == null) return '';
        v = String(v).replace(/"/g, '""');
        return /[",\n]/.test(v) ? `"${v}"` : v;
      }).join(','));
    }
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `meshcore-nodes-${new Date().toISOString().slice(0,16).replace(':','')}.csv`;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 5000);
  });

  // ------- 12. PNG screenshot via html2canvas (dynamic) -------
  let html2canvasLoading = null;
  function ensureHtml2canvas() {
    if (window.html2canvas) return Promise.resolve(window.html2canvas);
    if (html2canvasLoading) return html2canvasLoading;
    html2canvasLoading = new Promise((res, rej) => {
      const s = document.createElement('script');
      s.src = 'https://cdn.jsdelivr.net/npm/html2canvas@1.4.1/dist/html2canvas.min.js';
      s.onload = () => res(window.html2canvas); s.onerror = rej;
      document.head.appendChild(s);
    });
    return html2canvasLoading;
  }
  $('mcBtnPng')?.addEventListener('click', async () => {
    const btn = $('mcBtnPng'); const orig = btn.textContent;
    // Disabled, not just relabelled: '...' alone let a second click start a
    // parallel render of the same map.
    btn.disabled = true; btn.textContent = '...';
    try {
      const h2c = await ensureHtml2canvas();
      const canvas = await h2c(document.getElementById('map'), { useCORS: true, allowTaint: true, backgroundColor: null });
      const a = document.createElement('a');
      a.href = canvas.toDataURL('image/png');
      a.download = `meshcore-map-${new Date().toISOString().slice(0,16).replace(':','')}.png`;
      document.body.appendChild(a); a.click(); a.remove();
    } catch (e) {
      console.warn('png export', e);
      // Inline banner instead of a blocking alert(), matching every other message in the UI.
      showDataError(`PNG: ${e && e.message ? e.message : 'export failed'}`);
    }
    finally { btn.disabled = false; btn.textContent = orig; }
  });

  // ------- 13. modal helper -------
  function showModal(html) {
    const old = document.querySelector('.mc-modal-backdrop');
    if (old) old.remove();
    // Keyboard users were dropped at the top of the page on close: remember the
    // trigger, move focus in, hand it back on the way out.
    const opener = document.activeElement;
    const back = document.createElement('div');
    back.className = 'mc-modal-backdrop';
    back.innerHTML = `<div class="mc-modal" role="dialog" aria-modal="true"><button class="mc-modal-close" aria-label="Zamknij">×</button>${html}</div>`;
    const close = () => {
      back.remove();
      if (opener && typeof opener.focus === 'function' && document.contains(opener)) opener.focus();
    };
    back.addEventListener('click', (e) => { if (e.target === back) close(); });
    back.querySelector('.mc-modal-close').addEventListener('click', close);
    back.addEventListener('keydown', (e) => {
      if (e.key !== 'Tab') return;
      const stops = back.querySelectorAll('button, a[href], input, select, textarea, [tabindex]:not([tabindex="-1"])');
      if (!stops.length) return;
      const first = stops[0];
      const last = stops[stops.length - 1];
      if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
      else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
    });
    document.body.appendChild(back);
    back.querySelector('.mc-modal-close').focus();
    back.close = close;
    return back;
  }

  // ------- 14. QR modal -------
  function showQrModal(pk) {
    const u = new URL(location.href); u.searchParams.set('node', pk); u.searchParams.delete('panel');
    const link = u.toString();
    let svg = '';
    try {
      if (window.qrcode) {
        const q = window.qrcode(0, 'M');
        q.addData(link); q.make();
        svg = q.createSvgTag({ scalable: true, cellSize: 6, margin: 2 });
      }
    } catch (e) { console.warn('qr', e); }
    const html = `
      <h3>QR z linkiem do węzła</h3>
      <p style="font-size:.8rem;color:var(--muted);word-break:break-all">${escapeText(link)}</p>
      <div style="display:flex;justify-content:center;padding:10px 0">${svg || '<em>Generator QR niedostępny.</em>'}</div>
      <p style="font-size:.75rem;color:var(--muted)">Zeskanuj, aby otworzyć ten widok z wybranym węzłem.</p>
    `;
    showModal(html);
  }

  // ------- 15. compare modal -------
  function nodeSummaryHtml(n) {
    if (!n) return '<em>Brak danych.</em>';
    const rows = [
      ['Nazwa', n.name],
      ['Rola', n.role],
      ['Hex', n.identity_hex],
      ['Ostatnio widziany', n.last_advert_at],
      ['Pierwsze wykrycie', n.first_seen_at],
      ['Pozycja', isFiniteCoordinate(n.latitude, n.longitude) ? `${n.latitude.toFixed(4)}, ${n.longitude.toFixed(4)}` : '—'],
      ['RSSI', n.rssi ?? '—'],
      ['SNR', n.snr ?? '—'],
    ];
    return `<div><strong>${escapeText(n.name || nodeKey(n).slice(0,10))}</strong><dl style="display:grid;grid-template-columns:auto 1fr;gap:4px 12px;margin:8px 0 0;font-size:.78rem">${rows.map(([k,v]) => `<dt style="color:var(--muted)">${k}</dt><dd style="margin:0">${escapeText(v ?? '—')}</dd>`).join('')}</dl></div>`;
  }
  $('mcBtnCompare')?.addEventListener('click', () => {
    const favs = [...state.favs];
    const ns = nodesArr();
    const byPk = new Map(ns.map((n) => [nodeKey(n), n]));
    const picks = (state.compareIds.length ? state.compareIds : (selectedSourceId ? [selectedSourceId, favs[0]].filter(Boolean) : favs.slice(0,2)));
    const a = byPk.get(picks[0]); const b = byPk.get(picks[1]);
    const options = ns.slice(0, 200).map((n) => `<option value="${escapeText(nodeKey(n))}">${escapeText(n.name || nodeKey(n).slice(0,10))}</option>`).join('');
    const html = `
      <h3>Porównaj dwa node</h3>
      <div style="display:flex;gap:8px;margin-bottom:10px">
        <select id="mcCmpA" style="flex:1;padding:6px"><option value="">— A —</option>${options}</select>
        <select id="mcCmpB" style="flex:1;padding:6px"><option value="">— B —</option>${options}</select>
      </div>
      <div class="mc-compare-grid" id="mcCmpGrid">
        <div>${nodeSummaryHtml(a)}</div>
        <div>${nodeSummaryHtml(b)}</div>
      </div>
    `;
    const back = showModal(html);
    const selA = back.querySelector('#mcCmpA'); const selB = back.querySelector('#mcCmpB');
    if (picks[0]) selA.value = picks[0]; if (picks[1]) selB.value = picks[1];
    const upd = () => {
      state.compareIds = [selA.value, selB.value];
      back.querySelector('#mcCmpGrid').innerHTML = `<div>${nodeSummaryHtml(byPk.get(selA.value))}</div><div>${nodeSummaryHtml(byPk.get(selB.value))}</div>`;
    };
    selA.addEventListener('change', upd); selB.addEventListener('change', upd);
  });

  // ------- 16. link-quality matrix -------
  $('mcBtnMatrix')?.addEventListener('click', () => {
    const ns = nodesArr();
    const links = (latestManagement && latestManagement.map_links) || [];
    const idx = new Map(); const order = [];
    for (const n of ns.slice(0, 24)) { idx.set(nodeKey(n), n); order.push(nodeKey(n)); }
    const grid = new Map();
    for (const l of links) {
      const a = l.source_id || l.source || l.from;
      const b = l.target_id || l.target || l.to;
      const s = (l.snr ?? l.rssi);
      if (!a || !b) continue;
      grid.set(a + '|' + b, s);
    }
    const colorFor = (v) => {
      if (v == null) return 'transparent';
      const x = Math.max(-25, Math.min(25, Number(v)));
      const hue = (x + 25) * 4; // -25 → 0 (red), +25 → 200 (blue)
      return `hsl(${hue}, 70%, 55%)`;
    };
    let rows = '<tr><th></th>' + order.map((k) => `<th title="${escapeText(idx.get(k).name || k)}">${escapeText((idx.get(k).name || k).slice(0,4))}</th>`).join('') + '</tr>';
    for (const a of order) {
      rows += `<tr><th title="${escapeText(idx.get(a).name || a)}">${escapeText((idx.get(a).name || a).slice(0,8))}</th>`;
      for (const b of order) {
        if (a === b) { rows += '<td style="background:var(--section)">·</td>'; continue; }
        const v = grid.get(a + '|' + b);
        rows += `<td style="background:${colorFor(v)};color:#fff" title="${escapeText(a)} → ${escapeText(b)}: ${v ?? 'b/d'}">${v != null ? Math.round(v) : ''}</td>`;
      }
      rows += '</tr>';
    }
    showModal(`<h3>Macierz jakości linków (SNR/RSSI)</h3><p style="font-size:.75rem;color:var(--muted)">Pierwsze 24 węzły. Kolor: czerwony = słaby, zielony/niebieski = mocny.</p><div style="overflow:auto;max-height:64vh"><table class="mc-matrix-table">${rows}</table></div>`);
  });

  // ------- 17. compass + sparkline + advert-timeline injection -------
  function bearingDeg(lat1, lon1, lat2, lon2) {
    const φ1 = lat1 * Math.PI/180, φ2 = lat2 * Math.PI/180;
    const Δλ = (lon2 - lon1) * Math.PI/180;
    const y = Math.sin(Δλ) * Math.cos(φ2);
    const x = Math.cos(φ1)*Math.sin(φ2) - Math.sin(φ1)*Math.cos(φ2)*Math.cos(Δλ);
    return (Math.atan2(y, x) * 180/Math.PI + 360) % 360;
  }
  function sparklineSvg(samples, key) {
    if (!samples || samples.length < 2) return '';
    const vals = samples.map((s) => s[key]).filter((v) => Number.isFinite(v));
    if (vals.length < 2) return '';
    const min = Math.min(...vals), max = Math.max(...vals);
    const w = 120, h = 28, pad = 2;
    const span = Math.max(0.1, max - min);
    const xs = (i) => pad + (i / (vals.length - 1)) * (w - 2*pad);
    const ys = (v) => h - pad - ((v - min) / span) * (h - 2*pad);
    const d = vals.map((v, i) => `${i === 0 ? 'M' : 'L'}${xs(i).toFixed(1)},${ys(v).toFixed(1)}`).join('');
    const fill = `M${xs(0)},${h-pad} ${d.replace(/^M/, 'L')} L${xs(vals.length-1)},${h-pad} Z`;
    return `<svg class="mc-sparkline" viewBox="0 0 ${w} ${h}" aria-hidden="true"><path class="mc-spark-fill" d="${fill}"/><path d="${d}"/></svg>`;
  }
  function advertTimelineSvg(stamps) {
    if (!stamps || !stamps.length) return '';
    const now = Date.now(); const span = 24*3600e3;
    const w = 240, h = 18;
    const ticks = stamps.filter((t) => now - t <= span).map((t) => {
      const x = w - ((now - t) / span) * w;
      return `<line x1="${x.toFixed(1)}" y1="2" x2="${x.toFixed(1)}" y2="${h-2}" stroke="var(--blue)" stroke-width="1.2"/>`;
    }).join('');
    return `<svg width="${w}" height="${h}" aria-hidden="true" style="background:var(--section);border-radius:6px"><line x1="0" y1="${h/2}" x2="${w}" y2="${h/2}" stroke="var(--line)"/>${ticks}</svg>`;
  }
  function injectExpandedExtras() {
    const sel = selectedSourceId; if (!sel) return;
    const sourceNode = nodesArr().find((n) => nodeKey(n) === sel); if (!sourceNode) return;
    const targets = document.querySelectorAll('[data-identity-hex="' + CSS.escape(sel) + '"] .node-expanded, [data-node-id="' + CSS.escape(sel) + '"] .node-expanded');
    for (const t of targets) {
      if (t.querySelector('.mc-extras')) continue;
      const wrap = document.createElement('div');
      wrap.className = 'mc-extras';
      wrap.style.cssText = 'display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-top:10px;padding-top:10px;border-top:1px solid var(--line)';
      // compass relative to first favourite (or first other node with coords)
      let other = nodesArr().find((n) => state.favs.has(nodeKey(n)) && nodeKey(n) !== sel && isFiniteCoordinate(n.latitude, n.longitude));
      if (!other) other = nodesArr().find((n) => nodeKey(n) !== sel && isFiniteCoordinate(n.latitude, n.longitude));
      if (other && isFiniteCoordinate(sourceNode.latitude, sourceNode.longitude)) {
        const br = bearingDeg(sourceNode.latitude, sourceNode.longitude, other.latitude, other.longitude);
        const dist = haversineKm(sourceNode.latitude, sourceNode.longitude, other.latitude, other.longitude);
        wrap.insertAdjacentHTML('beforeend', `<div class="mc-compass" title="Kierunek do ${escapeText(other.name || nodeKey(other))}"><div class="mc-compass-dial"><div class="mc-compass-needle" style="transform:translate(-50%,-100%) rotate(${br.toFixed(0)}deg)"></div></div><div><div><strong>${br.toFixed(0)}°</strong></div><div style="font-size:.7rem;color:var(--muted)">${dist.toFixed(1)} km → ${escapeText((other.name || '').slice(0,10))}</div></div></div>`);
      }
      const spark = sparklineSvg(state.signalBuffer.get(sel), 'rssi');
      if (spark) wrap.insertAdjacentHTML('beforeend', `<div title="Historia RSSI">RSSI ${spark}</div>`);
      const adv = advertTimelineSvg(state.advertBuffer.get(sel));
      if (adv) wrap.insertAdjacentHTML('beforeend', `<div title="Adverty (24h)">Adverty 24h ${adv}</div>`);
      const histBtn = document.createElement('button');
      histBtn.type = 'button'; histBtn.className = 'mc-util-btn';
      histBtn.style.cssText = 'padding:4px 10px;font-size:.72rem';
      histBtn.textContent = 'Historia węzła';
      histBtn.addEventListener('click', (e) => { e.stopPropagation(); showHistoryModal(sel); });
      wrap.appendChild(histBtn);
      t.appendChild(wrap);
    }
  }

  // ------- 18. history modal -------
  function showHistoryModal(pk) {
    const n = nodesArr().find((x) => nodeKey(x) === pk);
    const buf = state.advertBuffer.get(pk) || [];
    const sig = state.signalBuffer.get(pk) || [];
    const lines = [];
    for (let i = buf.length - 1; i >= 0; i--) {
      lines.push(`${new Date(buf[i]).toLocaleString('pl-PL')}  advert`);
    }
    for (let i = sig.length - 1; i >= Math.max(0, sig.length - 20); i--) {
      const s = sig[i];
      lines.push(`${new Date(s.t).toLocaleString('pl-PL')}  RSSI=${s.rssi ?? '—'} SNR=${s.snr ?? '—'}`);
    }
    const text = lines.length ? lines.join('\n') : '(Brak zarejestrowanych zdarzeń w tej sesji.)';
    showModal(`<h3>Historia: ${escapeText(n ? (n.name || pk.slice(0,10)) : pk)}</h3><pre>${escapeText(text)}</pre><p style="font-size:.72rem;color:var(--muted)">Bufor klient-side z bieżącej sesji.</p>`);
  }

  // ------- 19. ring-buffer ingest -------
  function ingestBuffers() {
    const now = Date.now();
    for (const n of nodesArr()) {
      const pk = nodeKey(n); if (!pk) continue;
      const rssi = Number.isFinite(+n.rssi) ? +n.rssi : null;
      const snr = Number.isFinite(+n.snr) ? +n.snr : null;
      if (rssi != null || snr != null) {
        const buf = state.signalBuffer.get(pk) || [];
        const last = buf[buf.length - 1];
        if (!last || (last.rssi !== rssi || last.snr !== snr)) {
          buf.push({ t: now, rssi, snr });
          if (buf.length > 120) buf.shift();
          state.signalBuffer.set(pk, buf);
        }
      }
      if (n.last_advert_at) {
        const t = Date.parse(n.last_advert_at);
        if (Number.isFinite(t)) {
          const buf = state.advertBuffer.get(pk) || [];
          if (!buf.length || buf[buf.length - 1] !== t) {
            buf.push(t);
            while (buf.length && now - buf[0] > 24*3600e3) buf.shift();
            state.advertBuffer.set(pk, buf);
          }
        }
      }
    }
  }

  // ------- master apply hook -------
  // Filters now cut the data at the source (relevantNodes), so a filter change
  // has to rebuild the view; hiding rows after the fact is no longer enough.
  function rerenderFiltered() {
    if (latestState) render(latestState);
    else applyAll();
  }
  function applyAll() {
    try { ingestBuffers(); } catch (e) { console.warn('ingestBuffers', e); }
    try { renderChips(); } catch (e) { console.warn('renderChips', e); }
    try { injectRowActions(); } catch (e) { console.warn('injectRowActions', e); }
    try { hideFilteredRows(); } catch (e) { console.warn('hideFilteredRows', e); }
    try { applyMapFilters(); } catch (e) { console.warn('applyMapFilters', e); }
    try { injectExpandedExtras(); } catch (e) { console.warn('injectExpandedExtras', e); }
    try { refreshHeatLayer(); } catch (e) { console.warn('refreshHeatLayer', e); }
    try { maybeNotifyWatchlist(); } catch (e) { console.warn('notify', e); }
    syncBlackoutBtn();
    syncUrl();
  }

  // Patch render() so we re-apply after every render
  const _origRender = render;
  let _didInitialFilteredFit = false;
  window.render = function patchedRender(s) {
    const out = _origRender(s);
    try { applyAll(); } catch (e) { console.warn('applyAll', e); }
    if (!_didInitialFilteredFit && latestState && (latestState.nodes || []).length) {
      _didInitialFilteredFit = true;
      setTimeout(() => { try { fitMapToFiltered(); } catch {} }, 0);
    }
    return out;
  };
  // Note: existing internal callers reference `render` directly (closure),
  // so we additionally observe DOM mutations on the node sections to cover them.
  const sectionsEl = document.getElementById('node-sections');
  if (sectionsEl) {
    const mo = new MutationObserver(() => {
      // debounce
      if (mo._tid) clearTimeout(mo._tid);
      mo._tid = setTimeout(() => { try { applyAll(); } catch {} }, 30);
    });
    mo.observe(sectionsEl, { childList: true, subtree: true });
  }

  // Track refresh-start timestamps for countdown
  const _origRefresh = refresh;
  window.refresh = async function patchedRefresh(force) {
    bigFeatureBatch.__lastRefreshAt = Date.now();
    try {
      const r = await _origRefresh(force);
      errorBackoff = 0;
      return r;
    } catch (e) {
      errorBackoff = Math.min(120000, (errorBackoff || 5000) * 2);
      throw e;
    }
  };

  // ------- INIT -------
  readUrlState();
  renderChips();
  // initial pass once state arrives
  const waitForState = setInterval(() => {
    if (latestState) { clearInterval(waitForState); applyAll(); }
  }, 200);

  // Escape key closes modals
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      const back = document.querySelector('.mc-modal-backdrop');
      if (!back) return;
      if (typeof back.close === 'function') back.close();
      else back.remove();
    }
  });

  // expose for debugging
  window.MC_FEATURES = state;
})();
