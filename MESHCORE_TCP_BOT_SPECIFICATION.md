# MeshCore TCP Bot: Szczegolowa Specyfikacja Projektu

## 1. Cel dokumentu

Ten dokument ma byc glowna, techniczna specyfikacja projektu `meshcore-tcp-bot`.
Jego celem jest:

- opisanie zalozen systemu bez zgadywania i bez skrotow myslowych,
- rozdzielenie tego, co jest potwierdzone przez kod lub upstream, od tego, co jest tylko hipoteza albo celem rozwojowym,
- opisanie architektury runtime, komunikacji radiowej i TCP, warstwy HTTP/UI, danych trwalych i zaleznosci,
- stworzenie podstawy do dalszej przebudowy projektu w sposob kontrolowany i odtwarzalny.

Ten dokument opisuje projekt taki, jaki byl budowany i badany w obecnym repozytorium, a nie idealizowana wersje docelowa.

## 2. Zakres systemu

Projekt jest botem MeshCore pracujacym nad surowym mostem TCP udostepnianym przez repeater z firmware XIAO WiFi RS232Bridge.

System nie komunikuje sie z radiem przez BLE, nie korzysta z oficjalnego klienta desktopowego i nie steruje MeshCore przez zewnetrzne `meshcore-cli` jako glowna sciezke radiowa.
Glownym celem jest samodzielne:

- odbieranie ramek MeshCore po TCP,
- dekodowanie publicznych komunikatow kanalowych,
- budowanie i wysylanie poprawnych ramek MeshCore z poziomu Python,
- prowadzenie stanu znanych wezlow i repeaterow,
- opcjonalne logowanie do repeaterow i odpytywanie ich o informacje zarzadcze,
- wystawienie widoku WWW i API dla stanu runtime.

W praktyce projekt laczy cztery role:

- klient transportu TCP dla RS232Bridge,
- parser/producenct ramek MeshCore,
- runtime bota z logika komend i zarzadzania,
- prosty serwer HTTP z interfejsem operatorskim.

## 3. Zalozenia podstawowe

### 3.1. Zalozenia transportowe

- Repeater wystawia surowy port TCP `5002` z ramkami RS232Bridge.
- Opcjonalnie repeater wystawia czysty port konsolowy `5001`.
- Opcjonalnie repeater wystawia port lustrzany konsoli `5003`.
- Projekt zaklada, ze TCP jest tylko tunelem do ramek MeshCore; sukces zapisu do TCP nie oznacza sukcesu radiowego.

### 3.2. Zalozenia protokolowe

- Surowy payload po zdjeciu RS232Bridge jest dokladnie formatem `Packet::writeTo()` z MeshCore.
- Publiczne kanaly hashtagowe uzywaja klucza wyprowadzanego z `sha256("#nazwa-kanalu")[:16]`.
- Prywatne datagramy i sesje zarzadcze wymagaja stalej tozsamosci bota.
- Routing po radiu moze byc flood lub direct.
- Deduplikacja odbioru musi uwzgledniac, ze ten sam pakiet moze dotrzec wieloma kopiami.

### 3.3. Zalozenia aplikacyjne

- Bot ma odpowiadac na komendy kanalowe bez fabrykowania danych RF.
- Wszystkie dane pokazywane w UI maja miec okreslone pochodzenie: advert, console, management request/response albo konfiguracja runtime.
- SQLite jest trwala warstwa stanu roboczego projektu.
- TOML sluzy jako zrodlo bootstrapowe i konfiguracja startowa.
- UI admina moze nadpisywac wybrane ustawienia runtime zapisane w SQLite.

### 3.4. Zalozenia operacyjne

- Projekt jest uruchamiany lokalnie lub w Docker Compose.
- Wdrozenie zaklada jeden lub wiecej endpointow repeaterowych.
- Projekt ma dzialac w trybie dlugiego procesu stalego, nie jako jednorazowy skrypt.

## 4. Co system robi dzisiaj

Na podstawie aktualnego kodu i README system obejmuje nastepujace obszary funkcjonalne:

- odbior surowych ramek z portu TCP repeatera,
- dekodowanie publicznych komunikatow `GRP_TXT`,
- obsluge komend bota na kanalach publicznych,
- tworzenie i wysylanie odpowiedzi kanalowych jako nowych pakietow MeshCore,
- utrzymywanie listy znanych repeaterow na podstawie `ADVERT`,
- utrzymywanie wlasnej tozsamosci kryptograficznej bota,
- obsluge prywatnych datagramow i management request/response,
- rejestr targetow management i proby logowania guest/admin,
- zapisywanie stanu do SQLite,
- udostepnianie HTTP API oraz strony glównej i panelu `/admin`.

## 5. Granice systemu

To, co nalezy do systemu:

- Pythonowy runtime w katalogu `meshcore_tcp_bot/`,
- konfiguracja `config/config.toml`,
- SQLite `./data/meshcore-bot.db`,
- identity file `./data/bot-identity.json`,
- kontener Docker uruchamiajacy bota i serwer HTTP.

To, co jest poza systemem, ale system od tego zalezy:

- firmware MeshCore w repeaterach,
- wrapper RS232Bridge w XIAO WiFi,
- dostepnosc i stabilnosc portow 5001/5002/5003,
- stan eteru i propagacja radiowa,
- poprawne dzialanie kryptografii po stronie repeatera.

To, czego system nie kontroluje:

- czy zapisany pakiet TCP zostal faktycznie nadany przez radio,
- czy odbiorca RF odebral odpowiedz,
- czy target management odpowie na `REQ` lub login,
- czy lokalna komenda `neighbors` w konsoli faktycznie reprezentuje oczekiwany wezel.

## 6. Architektura logiczna

Projekt mozna rozlozyc na nastepujace warstwy:

### 6.1. Warstwa transportu ramek

Plik odpowiedzialny glownie: `meshcore_tcp_bot/protocol.py`

Odpowiedzialnosc:

- opakowanie payloadu MeshCore do ramki RS232Bridge,
- walidacja `magic`, dlugosci i `fletcher16`,
- dekodowanie strumienia TCP w sposob inkrementalny,
- resynchronizacja po uszkodzonych danych.

Najwazniejsze elementy:

- `MAGIC = C0 3E`,
- dlugosc payloadu w big-endian,
- checksum Fletcher-16 liczony tylko po payloadzie MeshCore,
- koncowy delimiter `LF` lub `CR/LF`,
- `RS232BridgeDecoder.feed()` jako glowny parser strumienia.

### 6.2. Warstwa pakietow MeshCore

Plik odpowiedzialny glownie: `meshcore_tcp_bot/packets.py`

Odpowiedzialnosc:

- stala definicja typow tras i payloadow,
- dekodowanie pakietow przychodzacych,
- budowanie pakietow wychodzacych,
- logika kryptografii dla kanalow publicznych i datagramow prywatnych,
- budowanie loginow i management requestow.

Najwazniejsze stale:

- route: `TRANSPORT_FLOOD`, `FLOOD`, `DIRECT`, `TRANSPORT_DIRECT`,
- payload: `REQ`, `RESPONSE`, `TXT_MSG`, `ADVERT`, `GRP_TXT`, `GRP_DATA`, `ANON_REQ`, `PATH`, `TRACE`,
- request types: `GET_STATUS`, `KEEP_ALIVE`, `GET_TELEMETRY_DATA`, `GET_ACCESS_LIST`, `GET_NEIGHBORS`, `GET_OWNER_INFO`.

Najwazniejsze zalozenia kryptograficzne:

- publiczne kanaly uzywaja sekretu 16-bajtowego,
- `PUBLIC_GROUP_PSK` jest stale zdefiniowany dla kanalu publicznego,
- hashtag channel PSK jest wyprowadzany deterministycznie,
- szyfrowanie grupowe jest realizowane przez AES-128 ECB z zerowym paddingiem,
- integralnosc grupowa jest oparta o skrocony MAC o dlugosci 2 bajtow,
- prywatne datagramy korzystaja z tozsamosci bota i wspolnego sekretu ECDH.

### 6.3. Warstwa runtime bota

Plik odpowiedzialny glownie: `meshcore_tcp_bot/service.py`

Odpowiedzialnosc:

- utrzymywanie sesji do endpointow,
- odbior i dispatch przychodzacych pakietow,
- obsluga komend uzytkownika,
- wysylka odpowiedzi kanalowych i prywatnych,
- utrzymanie stanu nodes/messages/management,
- koordynacja pollingow i time-outow,
- przygotowanie snapshotu stanu dla HTTP API.

To jest centralny plik projektu i faktyczny orchestrator calego systemu.

### 6.4. Warstwa persistence

Plik odpowiedzialny glownie: `meshcore_tcp_bot/database.py`

Odpowiedzialnosc:

- utworzenie schematu SQLite,
- zapis nodes i historii advertow,
- zapis targetow management,
- zapis snapshotow neighbors/owner/ACL,
- zapis ustawien runtime w `app_settings`.

### 6.5. Warstwa HTTP i UI

Plik odpowiedzialny glownie: `meshcore_tcp_bot/web.py`

Odpowiedzialnosc:

- wystawienie `/healthz`, `/api/state`, `/`, `/admin`,
- render strony glownej z mapa Leaflet i bocznym panelem,
- render panelu admina,
- obsluga formularzy aktualizujacych runtime settings i konfiguracje.

## 7. Komunikacja: model szczegolowy

### 7.1. TCP 5002: glowna sciezka radiowa

Port `5002` jest glownym transportem dla ruchu MeshCore.

Sciezka odbioru:

1. otwarcie polaczenia TCP do repeatera,
2. czytanie strumienia bajtow,
3. skladanie ramek RS232Bridge,
4. walidacja checksumy,
5. analiza payloadu MeshCore,
6. dispatch do jednej z galezi:
   - `ADVERT`,
   - prywatny datagram,
   - `TRACE`,
   - `GRP_TXT`,
   - odpowiedz/request management.

Sciezka wysylki:

1. zbudowanie payloadu MeshCore,
2. opakowanie przez `encode_frame()`,
3. zapis do `raw_writer`,
4. `drain()` TCP,
5. opcjonalny log informacyjny.

Wazne ograniczenie:

- sukces `write + drain` potwierdza tylko dostarczenie danych do gniazda TCP, a nie skuteczna emisje radiowa ani odbior po drugiej stronie.

### 7.2. CLI 5001: czysta konsola tekstowa

Port `5001` jest tekstowym interfejsem konsolowym repeatera.

Zastosowania:

- komenda `neighbors`,
- komenda `ver`,
- komenda `get name`,
- komenda `get owner.info`.

Model pracy:

- bot otwiera osobne polaczenie tekstowe,
- po wyslaniu komendy czyta odpowiedz do chwili bezczynnosci,
- tekst jest normalizowany przez `normalize_console_reply()`.

Ryzyka:

- brak silnego modelu odpowiedz-req jak w binarnym `REQ/RESPONSE`,
- brak gwarancji, ze CLI zwraca dane dla konkretnego zdalnego targetu,
- odpowiedz moze byc pusta, skrotowa albo zalezna od aktualnego kontekstu repeatera.

### 7.3. Console mirror 5003

Port `5003` sluzy glownie do:

- wzbogacenia telemetrycznego o `SNR` i `RSSI`,
- odczytu niektorych odpowiedzi pomocniczych, np. ACL w formie konsolowej.

Model pracy:

- bot moze czytac linie mirroru,
- parser rozpoznaje linie `RX, len=... SNR=... RSSI=...`,
- telemetria jest dopasowywana do ostatnio odebranych pakietow po route/payload type/payload len.

To wzbogacenie jest probabilistyczne i zalezne od zgodnosci czasowej, a nie twardo zwiazanego ID pakietu.

## 8. Typy komunikacji obslugiwane przez system

### 8.1. Publiczne wiadomosci kanalowe

Typ payloadu: `GRP_TXT`

Cel:

- odbior polecen od uzytkownikow na publicznych lub hashtagowych kanalach,
- wysylanie odpowiedzi bota na ten sam kanal.

Elementy logiki:

- dekodowanie sekretu kanalu po nazwie i/lub `psk`,
- walidacja MAC i odszyfrowanie tresci,
- rozdzielenie `sender` i `content`,
- sprawdzenie, czy kanal nalezy do `listen_channels`,
- utworzenie obiektu `MeshMessage`,
- przekazanie do `_handle_command()`.

### 8.2. Prywatne datagramy

Typ payloadu: glownie `TXT_MSG`, ale tez `REQ`, `RESPONSE`, `PATH`, `ANON_REQ`.

Cel:

- prywatne wiadomosci node-to-node,
- logowanie do repeatera,
- management requests i odpowiedzi.

Warunki konieczne:

- bot musi miec stale `MeshcoreIdentity`,
- nadawca lub target musi byc rozpoznawalny jako kontakt z public key,
- musi istniec poprawny sekret wspoldzielony.

### 8.3. Management traffic

Sciezki zarzadcze wystepuja w dwoch formach:

- binarny management przez `ANON_REQ`, `REQ`, `RESPONSE`, `PATH`,
- pomocniczy tekstowy odczyt przez konsolowe `5001/5003`.

Binarny management jest architektonicznie bardziej poprawny, ale trudniejszy operacyjnie.
Konsola jest prostsza, ale mniej formalna i moze miec niejednoznaczna semantyke.

## 9. Komendy bota

Na podstawie aktualnego README i runtime:

- `!ping`: odpowiedz `pong`,
- `!help`: lista znanych komend,
- `!test`: odpowiedz diagnostyczna zawierajaca nadawce, liczbe hopow i opcjonalnie RF/distance,
- `!trace`: odpowiedz z rekonstrukcja drogi pakietu,
- `!neighbors`: odpowiedz streszczajaca wiedze o sasiedztwie.

Wazne:

- odpowiedzi sa generowane szablonami,
- szablony moga byc edytowane runtime przez panel admina,
- odpowiedzi kanalowe i prywatne nie musza miec identycznej formy,
- w poprzednich iteracjach wystepowaly proby skracania odpowiedzi kanalowych z powodu problemow z dostarczaniem.

## 10. Tozsamosc bota

Bot ma wlasny plik tozsamosci, domyslnie `./data/bot-identity.json`.

Znaczenie tej tozsamosci:

- sluzy do prywatnych wiadomosci,
- sluzy do loginow guest/admin do repeaterow,
- sluzy do budowania requestow management,
- definiuje hash-prefix bota widoczny w protokole.

Bez stalej tozsamosci nie ma stabilnej komunikacji prywatnej i nie ma sensownej sesji management.

## 11. Model danych runtime

Najwazniejsze struktury:

- `EndpointSession`: stan polaczenia z repeaterem,
- `MeshMessage`: pojedyncza wiadomosc widoczna dla aplikacji,
- `NodeRecord`: znany repeater/room server wykryty z advertow,
- `ManagementTargetState`: stan targetu management,
- `RuntimeSnapshot`: agregat stanu zwracany przez API.

Najwazniejsze pola komunikatu `MeshMessage`:

- `endpoint_name`,
- `channel_name`,
- `channel_psk`,
- `sender`,
- `sender_identity_hex`,
- `content`,
- `packet_type`,
- `route_name`,
- `path_hashes`,
- `path_len`,
- `received_at`,
- `channel_hash`,
- `snr`,
- `rssi`,
- `distance_km`,
- `raw_payload_hex`.

## 12. Model danych trwalych: SQLite

Plik odpowiedzialny: `meshcore_tcp_bot/database.py`

### 12.1. Tabele glówne

- `nodes`: aktualny stan znanych repeaterow i room serverow,
- `advert_history`: historia advertow,
- `management_targets`: zapis targetow management,
- `neighbor_snapshots`: naglowki pobran sasiedztwa,
- `neighbor_edges`: konkretne wpisy sasiedztwa,
- `owner_snapshots`: snapshoty owner info,
- `acl_snapshots`: snapshoty ACL,
- `acl_entries`: wpisy ACL,
- `app_settings`: runtime settings panelu admina.

### 12.2. Znaczenie tabel

`nodes`:

- sluzy jako glowny katalog znanych repeaterow,
- jest zasilane przez `ADVERT`,
- przechowuje wspolrzedne i ostatni znany endpoint.

`advert_history`:

- pozwala analizowac kolejne obserwacje tego samego wezla,
- jest zrodlem danych historycznych, nie tylko aktualnego stanu.

`management_targets`:

- pozwala rozdzielic to, co widac na radiu, od tego, co ma byc aktywnie odpytywane przez management,
- target moze byc wskazany przez `identity_hex`, prefix albo nazwe.

`neighbor_snapshots` + `neighbor_edges`:

- przechowuja wynik pobran sasiedztwa,
- posiadaja `requester_role`, czyli z jakiej sciezki pochodzil pomiar,
- nie sa ograniczone do jednego zrodla prawdy; to projektowo wygodne, ale moze mieszac semantyke.

`app_settings`:

- to runtime layer nad konfiguracja,
- po seedzie z panelu admina SQLite staje sie zrodlem aktywnych ustawien dla wybranych obszarow.

## 13. UI: strona glowna

Plik odpowiedzialny: `meshcore_tcp_bot/web.py`

Strona glowna `/` to pojedynczy HTML z osadzonym CSS i JavaScript.

### 13.1. Charakter UI

- interfejs jest mapowy,
- glownym elementem jest mapa Leaflet,
- po prawej stronie znajduje sie overlay sidebar,
- na dole po lewej jest legenda mapy,
- wyglad opiera sie o jasne tlo, polprzezroczyste panele i serifowe fonty.

### 13.2. Główne obszary UI

#### a) Mapa

Mapa pokazuje:

- pozycje znanych repeaterow i room serverow,
- lacza miedzy wezlem zrodlowym a jego znanymi sasiadami,
- etykiety sygnalu na liniach,
- stan wybranego wezla.

#### b) Sidebar

Sidebar sklada sie z:

- paska podsumowania,
- sekcji listy wezlow,
- widoku szczegolow wybranego wezla,
- tabeli bezposrednich sasiadow,
- wykresu historii sygnalu dla wybranego linku.

#### c) Karty summary

Summary grid pokazuje skrocone metryki stanu, m.in. liczbe wezlow, wiadomosci, aktywnosc, stan diagnostyczny lub podobne agregaty pochodzace z `api/state`.

#### d) Widok wybranego wezla

Po wybraniu wezla UI pokazuje:

- role,
- czas ostatniego advertu,
- liste bezposrednich sasiadow,
- sygnal i dystans,
- wykres historii sygnalu dla wybranego sasiada.

### 13.3. Zasilanie UI

Strona glowna korzysta z `/api/state` jako glownego zrodla danych.

Z `api/state` pobierane sa m.in.:

- `nodes`,
- `messages`,
- `diagnostics`,
- `management.map_links`,
- `management.latest_neighbors`,
- `management.signal_history`.

### 13.4. Ograniczenia UI

- UI nie jest niezaleznym zrodlem prawdy; pokazuje to, co zbudowal runtime,
- jesli runtime miesza znaczenie danych z `console` i `management`, UI tez to pokaże,
- mapa nie dowodzi skutecznosci radiowej, pokazuje jedynie stan modelu aplikacji.

## 14. UI: panel admina

Plik odpowiedzialny: `meshcore_tcp_bot/web.py`

### 14.1. Dostep

- endpoint `/admin`,
- logowanie haslem z `MESHCORE_ADMIN_PASSWORD`,
- sesja przez `SessionMiddleware`,
- dodatkowy sekret sesji przez `MESHCORE_ADMIN_SESSION_SECRET`.

### 14.2. Funkcje panelu

Panel umozliwia:

- edycje ustawien ogolnych bota,
- wlaczanie i wylaczanie komend,
- edycje szablonow odpowiedzi komend,
- dodawanie i usuwanie kanalow,
- dodawanie i usuwanie endpointow,
- dodawanie i usuwanie targetow management,
- podglad identity bota i regeneracje klucza.

### 14.3. Znaczenie panelu dla architektury

Panel admina nie jest tylko frontendem.
Jest tez runtime writerem, bo zapisuje dane do `app_settings` oraz targetow/endpointow/channels, przez co zmienia aktywna konfiguracje procesu.

To oznacza, ze projekt ma dwa poziomy konfiguracji:

- bootstrap z TOML,
- runtime persistence z SQLite.

## 15. Konfiguracja

Glowne zrodlo bootstrapowe: `config/config.toml` lub `config/config.example.toml`

### 15.1. Sekcje konfiguracyjne

- `[bot]`: nazwa, prefix odpowiedzi, prefix komend, lista kanalow, historia wiadomosci,
- `[identity]`: sciezka do pliku identity,
- `[web]`: host i port HTTP,
- `[logging]`: poziom logowania,
- `[storage]`: sciezka do SQLite,
- `[management]`: timeouty, polling, credentials shared, liczba rekordow neighbors,
- `[[channels]]`: definicja kanalow,
- `[[endpoints]]`: definicja repeaterow TCP,
- `[[management_nodes]]`: definicja targetow management.

### 15.2. Znaczenie endpointu

Endpoint opisuje fizyczne wejscie do sieci MeshCore, a nie koniecznie zdalny target management.

Przyklad:

- `raw_host/raw_port` to polaczenie z wrapperem RS232Bridge,
- `console_host/console_port` to tekstowe CLI,
- `console_mirror_host/console_mirror_port` to mirror z telemetria,
- `latitude/longitude` to wspolrzedne repeatera, z ktorego bot wchodzi do sieci.

### 15.3. Znaczenie management targetu

Management target to logiczny cel zarzadzania, czyli repeater lub room server, do ktorego bot chce sie zalogowac i wysylac zapytania.

Target moze miec:

- nazwe,
- endpoint zrodlowy, przez ktory ma byc osiagniety,
- hash prefix,
- identity hex,
- guest password,
- admin password,
- preferowany role,
- notatki.

## 16. Management: model szczegolowy

### 16.1. Fazy pracy management

1. wykrycie targetu z konfiguracji lub auto-discovery,
2. proba ustalenia jego identity,
3. ewentualna proba loginu guest/admin,
4. request `status`,
5. request `neighbors`,
6. request `owner`,
7. request `ACL` jesli uprawnienia i konfiguracja na to pozwalaja,
8. utrzymywanie stanu sesji i retry policy.

### 16.2. Dwie sciezki management

#### Sciezka A: binarny management MeshCore

To jest formalnie poprawna sciezka zgodna z protokolem.

Obejmuje:

- `ANON_REQ` dla loginu,
- `REQ/RESPONSE` dla zapytan,
- `PATH` dla direct-route learning,
- prywatne datagramy szyfrowane sekretem wspoldzielonym.

#### Sciezka B: konsola tekstowa

To jest sciezka pomocnicza, pragmatyczna i mniej formalna.

Obejmuje:

- `neighbors` po CLI,
- `ver`, `get name`, `get owner.info`,
- `get acl` po mirrorze w niektorych iteracjach.

Ta sciezka jest prostsza do uruchomienia, ale semantycznie slabsza.

### 16.3. Kluczowe ryzyko management

Najwiekszy problem projektowy polega na tym, ze dane management pochodzace z wielu sciezek moga wygladac podobnie w SQLite i UI, mimo ze nie maja tej samej wartosci dowodowej.

Przyklad:

- sukces `neighbors` z `requester_role = console` nie ma takiej samej semantyki jak sukces `neighbors` z `requester_role = admin` po prawdziwym `REQ/RESPONSE` do konkretnego targetu.

## 17. Ograniczenia i obecne napiecia architektoniczne

### 17.1. Brak RF ACK

Projekt nie ma radiowego potwierdzenia dostarczenia odpowiedzi.

Widac tylko:

- odbior komendy,
- zbudowanie odpowiedzi,
- zapis odpowiedzi do TCP.

Nie widac twardo:

- czy radio nadało pakiet,
- czy target odebral pakiet,
- czy odpowiedz wrocila inną drogą.

### 17.2. Mieszanie semantyk danych

SQLite i UI lacza dane pochodzace z:

- advertow,
- local console,
- remote management,
- heurystyk odleglosci,
- runtime settings.

Bez wyraznego rozdzielenia pochodzenia bardzo latwo o mylna interpretacje.

### 17.3. Dual source of truth

Po uruchomieniu panelu admina aktywna konfiguracja czesciowo przestaje byc TOML-em, a zaczyna byc SQLite.

To komplikuje debugging, bo:

- plik konfiguracyjny moze mowic jedno,
- runtime moze uzywac czegos innego,
- restart procesu nie musi oznaczac powrotu do TOML.

### 17.4. Centralnosc `service.py`

Za duza czesc logiki projektu skupia sie w jednym pliku.

Skutki:

- wysoka zlozonosc mentalna,
- duze ryzyko regresji po zmianach,
- trudniejsze rozdzielenie odpowiedzialnosci,
- trudniejsze testowanie izolowane.

## 18. Zrodla i poziom zaufania

### 18.1. Zrodla pierwotne w tym repo

Najwyzszy poziom zaufania dla aktualnego zachowania projektu maja:

- `meshcore_tcp_bot/service.py`,
- `meshcore_tcp_bot/packets.py`,
- `meshcore_tcp_bot/protocol.py`,
- `meshcore_tcp_bot/database.py`,
- `meshcore_tcp_bot/web.py`,
- `config/config.toml`,
- `README.md`,
- `COMPANION_MODE_ANALYSIS.md`.

### 18.2. Zrodla pierwotne poza repo projektu

Na podstawie dotychczasowych analiz projekt opieral sie takze na zrodlach z workspace:

- `meshcore-xiao-wifi-serial2tcp/mesh_client.py`,
- firmware MeshCore w katalogach `meshcore-firmware` i/lub clone upstream,
- pliki firmware odpowiedzialne za `Mesh.cpp`, `Utils.cpp`, `BaseChatMesh.cpp`,
- przyklady companion i repeater implementation z upstream MeshCore.

Te zrodla sa kluczowe dla zrozumienia:

- struktury pakietow,
- szyfrowania grupowego i prywatnego,
- loginu repeatera,
- neighbors request/response,
- znaczenia `PATH`, `REQ`, `RESPONSE`, `ANON_REQ`.

### 18.3. Zrodla wtórne

Zrodla pomocnicze, ale nizsze rangi:

- wnioski z logow runtime,
- obserwacje z `api/state`,
- dokumenty notatkowe powstale podczas debugowania,
- rekonstrukcje zachowania na podstawie eksperymentow.

### 18.4. Zasada interpretacji

W razie konfliktu miedzy zrodlami nalezy przyjmowac kolejnosc:

1. upstream firmware i rzeczywisty kod aktualnego repo,
2. surowe logi i zrzuty runtime,
3. README i dokumenty opisowe,
4. hipotezy z debugowania.

## 19. Co powinno byc uznawane za potwierdzone, a co nie

### Potwierdzone

- projekt uzywa RS232Bridge framing `C0 3E + len + payload + fletcher16 + newline`,
- glowna komunikacja radiowa idzie przez TCP `5002`,
- istnieje HTTP API `/api/state`,
- istnieje panel `/admin`,
- projekt uzywa SQLite jako warstwy persistence,
- projekt ma wlasna tozsamosc bota,
- projekt probuje obslugiwac publiczne kanaly, DM i management.

### Niepotwierdzone lub zalezne od warunkow

- skutecznosc radiowa wysylanych odpowiedzi,
- jednoznacznosc danych `neighbors` z lokalnej konsoli,
- stabilnosc management requestow do wszystkich targetow,
- pelna zgodnosc kazdej galezi prywatnej komunikacji z upstream w kazdym przypadku brzegowym.

## 20. Wnioski projektowe na przyszlosc

Jesli projekt ma byc budowany dalej w sposob bezpieczny, kolejne iteracje powinny opierac sie na ponizszych zasadach:

- najpierw specyfikacja, potem implementacja,
- kazda zmiana runtime musi miec jawny cel i kryterium sukcesu,
- rozdzielenie danych pochodzacych z `console` i z binarnego management,
- wydzielenie `service.py` na mniejsze moduly odpowiedzialnosci,
- zachowanie rozdzialu pomiedzy warstwa radiowa, persistence i UI,
- praca tylko w repo z historia git,
- dokumentowanie rzeczywistych zalozen semantycznych danych wyswietlanych w UI.

## 21. Minimalna definicja sukcesu dla nowej implementacji

Nowa, uporzadkowana implementacja tego projektu powinna spelnic co najmniej nastepujace kryteria:

- poprawny odbior i wysylka `GRP_TXT` po TCP,
- twarde rozdzielenie sukcesu TCP od sukcesu RF,
- jawne oznaczanie pochodzenia danych w persistence i UI,
- przewidywalny model konfiguracji: albo TOML, albo runtime DB z czytelnymi zasadami priorytetu,
- testowalna implementacja management,
- osobna dokumentacja zachowania UI i API,
- pelna historia zmian w git od pierwszego dnia pracy.

## 22. Podsumowanie jednozdaniowe

`meshcore-tcp-bot` jest jednoczesnie klientem surowego protokolu MeshCore po TCP, eksperymentalnym companion node, runtime'em bota, baza danych stanu sieci i webowym panelem operatorskim, a glownym problemem projektu nie jest brak funkcji, tylko zbyt slabe rozdzielenie warstw, zrodel prawdy i znaczenia danych.