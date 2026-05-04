# AI Issues and Failure Modes

Ten plik dokumentuje moje bledy z tej pracy nad `meshcore-tcp-bot`.
Celem nie jest obrona decyzji, tylko zapisanie konkretnych problemow, na ktore mam zwracac uwage w przyszlosci.

## 1. Zmienianie runtime bez wystarczajacego bezpiecznika

Blad:

- wprowadzalem zmiany bez pelnej mozliwosci cofniecia ich do potwierdzonego stanu,
- dotykalem aktywnego runtime mimo tego, ze repo nie mialo historii git,
- zbyt latwo przechodzilem z analizy do modyfikacji dzialajacego systemu.

Zasada na przyszlosc:

- jesli repo nie ma `git`, najpierw trzeba zalozyc, ze kazda zmiana jest ryzykowna,
- przy problemach produkcyjnych najpierw read-only analiza, potem plan cofania, dopiero potem edycja,
- nie wolno wchodzic w eksperymentalne zmiany runtime bez jasnego punktu rollbacku.

## 2. Zbyt szeroka ingerencja przy niepewnej diagnozie

Blad:

- w pewnym momencie zbyt szybko uznalem, ze prywatne wiadomosci sa glownym zrodlem regresji,
- w odpowiedzi odcialem cala sciezke DM i pozmienialem semantyke neighbors,
- to byla zbyt duza ingerencja wobec poziomu pewnosci diagnozy.

Zasada na przyszlosc:

- przy problemie transportowym nie wolno usuwać calej galezi funkcjonalnej tylko dlatego, ze koreluje czasowo z regresja,
- najpierw trzeba oddzielic hipotezy od dowodow,
- jesli fix zmienia semantyke systemu, trzeba to traktowac jak zmiane architektoniczna, nie szybki patch.

## 3. Mieszanie naprawy z reinterpretacja danych

Blad:

- zamiast tylko zdiagnozowac, ze `console` i `management` maja inna wartosc dowodowa, zmienilem runtime i zapytania UI,
- to ingerowalo nie tylko w debugowanie, ale tez w to, co system pokazuje jako prawde.

Zasada na przyszlosc:

- kiedy semantyka danych jest niejasna, najpierw trzeba to udokumentowac,
- nie wolno naprawiac mylacego UI przez ukrywanie danych, jesli nie ma zgody na taka zmiane,
- najpierw analiza i opis zrodla danych, potem decyzja, czy filtrowac, oznaczac, czy przebudowac model.

## 4. Zbyt szybkie przejscie od debugowania do przebudowy

Blad:

- po frustracji uzytkownika przeszedlem z debugowania konkretnego toru do prob „powrotu do prostszego wariantu”,
- to bylo bardziej przebudowanie zachowania niz minimalny krok diagnostyczny.

Zasada na przyszlosc:

- w kryzysie trzeba zmniejszac zakres zmian, nie zwiekszac,
- najpierw trzeba potwierdzic minimalny reproducer i minimalny fix,
- nie wolno zamieniac jednej hipotezy w duzy refactor pod presja czasu.

## 5. Za slabe oddzielenie: "co wiem" od "co podejrzewam"

Blad:

- czasami traktowalem mocna hipoteze jak praktycznie potwierdzony root cause,
- w efekcie proponowane lub wdrazane zmiany byly zbyt agresywne.

Zasada na przyszlosc:

- kazdy istotny wniosek powinien byc jawnie oznaczony jako:
  - potwierdzony kodem,
  - potwierdzony logiem,
  - potwierdzony testem,
  - albo tylko hipoteza,
- jesli nie ma twardego dowodu, zmiana musi byc bardziej zachowawcza.

## 6. Niedoszacowanie znaczenia braku historii git

Blad:

- za pozno potraktowalem brak `.git` jako glowny problem operacyjny,
- przez to wykonane zmiany byly duzo bardziej ryzykowne niz w normalnym repo.

Zasada na przyszlosc:

- brak historii version control musi byc traktowany jako czerwony alarm,
- przy pierwszej powaznej interwencji trzeba jasno powiedziec: bez gita tylko read-only albo minimalne, odwracalne zmiany,
- nie wolno zakladac, ze „potem sie cofnie”, jesli nie ma z czego.

## 7. Za slaba dyscyplina przy pracy na systemie aktywnym

Blad:

- pracowalem na aktywnym kontenerze i aktywnej bazie, a nie na izolowanym wariancie roboczym,
- przez to zmiany mogly bezposrednio wplywac na biezacy stan uslugi.

Zasada na przyszlosc:

- przy problemach runtime najpierw trzeba ustalic, czy praca ma byc:
  - tylko analityczna,
  - na kopii,
  - czy na aktywnej instancji,
- jesli aktywna instancja jest jedynym srodowiskiem, kazdy patch musi byc jeszcze bardziej minimalny.

## 8. Niepotrzebne tworzenie nowego repo przy innym oczekiwaniu uzytkownika

Blad:

- zaczalem ruch w kierunku nowego repo, kiedy uzytkownik chcial miec efekt tu, w obecnym projekcie,
- to bylo odejscie od bezposredniego polecenia.

Zasada na przyszlosc:

- gdy uzytkownik chce plik w konkretnym repo, rezultat ma trafic tam, chyba ze padla jasna zgoda na inne miejsce,
- nie wolno "porzadkowac procesu" kosztem bezposredniego wymagania uzytkownika.

## 9. Zbyt malo jawnego ostrzezenia przed zmianami semantycznymi

Blad:

- nie zawsze wystarczajaco jasno komunikowalem, ze planowana zmiana zmieni semantyke systemu, a nie tylko naprawi blad,
- szczegolnie dotyczylo to DM i filtrowania neighbors.

Zasada na przyszlosc:

- przed kazda zmiana, ktora zmienia znaczenie danych, routing lub zrodlo prawdy, trzeba to nazwac wprost,
- jesli zmiana nie jest czystym bugfixem, trzeba to traktowac jak decyzje projektowa.

## 10. Za slaba hierarchia priorytetow

Blad:

- momentami mieszalem kilka problemow naraz: radio replies, DM, management, neighbors UI, admin runtime,
- to zwiekszalo chaos i utrudnialo rozliczenie efektu pojedynczej zmiany.

Zasada na przyszlosc:

- jeden krok powinien rozwiazywac jeden problem,
- jesli problem nie jest jednowymiarowy, trzeba najpierw rozpisac warstwy i testowac je osobno,
- nie wolno wrzucac napraw transportu, management i prezentacji danych do jednej sekwencji zmian.

## 11. Bledy praktyczne przy pracy interaktywnej

Blad:

- pojawily sie tez zwykle bledy operacyjne, np. nietrafiony kierunek pracy i niepotrzebne rozwijanie watkow pobocznych,
- w terminalu byly tez nieudane jednorazowe polecenia diagnostyczne, ktore nie dawaly wartosci proporcjonalnej do ryzyka zamieszania.

Zasada na przyszlosc:

- diagnostyka musi byc krotka, jednoznaczna i reprodukowalna,
- jesli polecenie nie daje twardego nowego sygnalu, nie nalezy go eskalowac w kolejny patch,
- najpierw trzeba ograniczac przestrzen problemu, a nie mnozyc boczne eksperymenty.

## 12. Meta-zasada nadrzedna

Najwazniejsza lekcja z tej sesji:

- przy systemie bez gita, z aktywnym runtime i z niepewnym root cause, moim domyslnym trybem powinno byc:
  - analiza,
  - dokumentacja,
  - minimalny reproducer,
  - minimalny patch,
  - jawny rollback plan,
  - i dopiero wtedy wdrozenie.

Nie odwrotnie.

## 13. Twarde bezpieczniki przed rozpierdolem

To sa zasady, ktore maja dzialac jak bezwzgledne bezpieczniki, nie jako sugestie.

### 13.1. Stop-rules: kiedy nie wolno nic edytowac

Nie wolno edytowac runtime ani konfiguracji, jesli jednoczesnie zachodzi ktorykolwiek z warunkow:

- repo nie ma historii git i nie ma innego pewnego rollbacku,
- system dziala na jedynej aktywnej instancji bez kopii roboczej,
- root cause nie jest nawet przyblizony do jednej warstwy,
- zmiana dotyka kilku warstw naraz: radio, runtime, SQLite, UI,
- nie ma przygotowanego planu cofniecia krok po kroku,
- nie ma jasnego testu, ktory odroznia stan przed i po zmianie,
- uzytkownik oczekuje diagnozy, a nie zmiany zachowania.

W takiej sytuacji dozwolone sa tylko:

- odczyt plikow,
- analiza logow,
- zapytania do API,
- zapytania read-only do SQLite,
- dokumentacja i plan dzialania.

### 13.2. Zasada jednej warstwy

Jedna zmiana moze dotykac tylko jednej z ponizszych warstw naraz:

- transport TCP / RS232Bridge,
- parser i builder pakietow MeshCore,
- logika runtime bota,
- management session state,
- persistence SQLite,
- UI i prezentacja,
- konfiguracja bootstrapowa.

Jesli zmiana wymaga dotkniecia wiecej niz jednej warstwy, trzeba ja rozbic na osobne etapy.

## 14. Obowiazkowa checklista przed jakakolwiek zmiana

Przed pierwszym patchem trzeba jawnie sprawdzic i zapisac:

- czy repo ma git,
- czy istnieje branch, commit lub tag, do ktorego mozna wrocic,
- czy istnieje kopia bazy SQLite,
- czy istnieje kopia pliku identity,
- czy runtime korzysta z TOML, czy juz z `app_settings` w SQLite,
- czy zmiana dotyczy aktywnej instancji,
- jaki jest minimalny test potwierdzajacy problem,
- jaki jest minimalny sygnal sukcesu po zmianie,
- jaki jest dokladny rollback plan.

Jesli choc jeden z punktow nie jest znany, najpierw trzeba uzupelnic te informacje.

## 15. Obowiazkowy backup przed patchem

Przed kazda zmiana aktywnego systemu trzeba wykonac co najmniej logiczny backup:

- kopia `config/config.toml`,
- kopia `data/meshcore-bot.db`,
- kopia `data/bot-identity.json`,
- kopia plikow, ktore beda edytowane.

Jesli nie ma git, backup plikow zrodlowych jest obowiazkowy, nie opcjonalny.

Minimalny bezpieczny zestaw backupu:

- katalog `config/`,
- katalog `meshcore_tcp_bot/`,
- katalog `data/` bez nadpisywania oryginalu.

## 16. Zasady pracy z SQLite

SQLite w tym projekcie nie jest cachem pomocniczym. To jest aktywne zrodlo prawdy dla czesci runtime.

Dlatego:

- nie wolno zakladac, ze restart procesu przywraca konfiguracje z TOML,
- nie wolno zmieniac `app_settings` bez sprawdzenia aktualnego stanu,
- nie wolno nadpisywac runtime flag na stale jako elementu eksperymentu,
- trzeba odroznic dane historyczne od danych aktywnie konsumowanych przez runtime.

Przed zmiana w SQLite trzeba odpowiedziec sobie na pytania:

- czy ta wartosc jest bootstrapowa czy aktywna runtime,
- czy zmiana przezyje restart,
- czy UI bedzie ja pozniej pokazywal jako aktualna,
- czy to jest migracja, naprawa, czy tymczasowy eksperyment.

## 17. Zasady pracy z aktywnym runtime

Jesli usluga jest uruchomiona i podlaczona do repeatera:

- nie wolno robic zmian semantycznych bez zgody,
- nie wolno jednoczesnie zmieniac runtime i bazy,
- nie wolno robic "na szybko" zmian, ktore wplywaja na routing odpowiedzi,
- nie wolno traktowac `docker compose up -d --build` jako bezpiecznego restartu, jesli nie ma rollbacku.

Przy aktywnym runtime najpierw trzeba preferowac:

- instrumentacje read-only,
- dodatkowe logowanie tylko jesli nie zmienia zachowania,
- pomiary,
- testy jednostkowe poza glowna instancja,
- repliki lub kopie, jesli sa dostepne.

## 18. Zasady pracy z komunikacja radiowa

Warstwa radiowa jest najlatwiejsza do zepsucia i najtrudniejsza do udowodnienia po fakcie.

Dlatego:

- nie wolno utozsamiac sukcesu TCP z sukcesem RF,
- nie wolno zmieniac formatu odpowiedzi i harmonogramu wysylki w jednym kroku bez testu,
- nie wolno jednoczesnie zmieniac channel hash, PSK, routing i szablon odpowiedzi,
- nie wolno traktowac pojedynczego sukcesu lub pojedynczej porazki jako dowodu ogolnej poprawnosci.

Kazdy eksperyment radiowy musi miec stale elementy:

- dokladnie jedna zmieniana zmienna,
- zapisane wejscie testowe,
- zapisany oczekiwany efekt,
- zapisany wynik faktyczny,
- mozliwosc powrotu do poprzedniego stanu.

## 19. Zasady pracy z management i neighbors

Najbardziej zdradliwy obszar tego projektu to mieszanie danych management i console.

Dlatego:

- nie wolno nazywac danych `neighbors` z `console` tym samym, co `neighbors` z prawdziwego `REQ/RESPONSE`, jesli nie sa semantycznie rownowazne,
- nie wolno filtrowac lub przepisywac tych danych bez zgody i bez opisu skutku,
- nie wolno poprawiac UI przez ciche ukrywanie rekordow,
- trzeba jawnie oznaczac pochodzenie danych w modelu i w prezentacji.

Jesli znaczenie danych jest watpliwe, najpierw trzeba:

- opisac zrodlo danych,
- opisac poziom zaufania,
- dopiero potem decydowac o wykorzystaniu ich w UI lub komendach.

## 20. Zasady przed dotknieciem `service.py`

`service.py` jest plikiem wysokiego ryzyka.

Przed edycja tego pliku trzeba zawsze zapisac:

- jaka funkcja lub metoda ma zostac zmieniona,
- jaki konkretny objaw ma byc naprawiony,
- jakie skutki uboczne sa mozliwe,
- jak sprawdzic, czy zmiana nie dotknela innej galezi,
- jak przywrocic poprzednia wersje.

Jesli zmiana w `service.py` nie miesci sie w jednym jasnym celu, to prawdopodobnie powinna byc rozbita albo odlozona.

## 21. Zasady przed dotknieciem `database.py`

`database.py` zmienia nie tylko to, co jest zapisywane, ale tez to, co pozniej system uwaza za stan prawdziwy.

Dlatego:

- nie wolno zmieniac zapytan query-layer tylko po to, zeby UI wygladal lepiej,
- nie wolno zmieniac zapytan summary bez zrozumienia, jakie ekrany i komendy z nich korzystaja,
- nie wolno wprowadzac filtrow semantycznych bez ich jawnego opisania.

## 22. Zasady przed dotknieciem `web.py`

`web.py` ma byc warstwa prezentacji i wejscia operatorskiego, a nie miejscem maskowania problemow runtime.

Dlatego:

- UI nie moze klamac przez przemilczenie,
- jesli dane sa niepewne, UI powinno to oznaczac, a nie udawac precyzje,
- nie wolno zmieniac nazw i etykiet danych tak, by wygladaly bardziej wiarygodnie niz sa,
- panel admina nie moze byc traktowany jako wygodny sposob na "tymczasowe" zmiany semantyczne.

## 23. Zasady komunikacji z uzytkownikiem przed ryzykowna zmiana

Przed kazda ryzykowna zmiana trzeba wprost napisac:

- co zamierzam zmienic,
- dlaczego uwazam to za potrzebne,
- czego nie jestem pewien,
- jaki jest mozliwy koszt uboczny,
- jak cofne zmiane, jesli efekt bedzie zly.

Jesli nie da sie tego uczciwie opisac w 3-5 punktach, to znaczy, ze zmiana jest za slabo zrozumiana.

## 24. Minimalny protokol bezpiecznej zmiany

Kazda zmiana wysokiego ryzyka powinna isc wedlug tego schematu:

1. Odczyt stanu obecnego.
2. Zapis backupu.
3. Zapis hipotezy i testu sukcesu.
4. Jedna mala zmiana.
5. Walidacja tylko tej jednej zmiany.
6. Decyzja: zostaje albo rollback.

Nie wolno robic kilku zmian pod rzad, jesli poprzednia nie zostala potwierdzona albo cofnieta.

## 25. Minimalny protokol rollbacku

Rollback nie moze byc mysla typu "jak cos to cofne".

Rollback musi zawierac:

- liste plikow do przywrocenia,
- liste danych runtime do przywrocenia,
- informacje, czy trzeba restartowac proces,
- informacje, jak sprawdzic, ze system wrocil do poprzedniego stanu,
- informacje, czy rollback nie zostawia skutkow ubocznych w SQLite.

## 26. Czerwone flagi, po ktorych trzeba przerwac prace i wrócic do analizy

Trzeba natychmiast przerwac edycje, jesli pojawia sie ktorykolwiek z sygnalow:

- uzytkownik zaczyna sygnalizowac, ze wczesniej dzialalo lepiej,
- nie da sie jednoznacznie powiedziec, czy psujemy transport, dane czy UI,
- kolejna zmiana ma "moze pomoze" jako glowna motywacje,
- zmiana wymaga naraz edycji kodu i danych runtime,
- nie da sie juz prosto opisac roznicy miedzy stanem przed i po,
- problem zaczyna sie rozlewac na nowe obszary funkcjonalne.

Wtedy nalezy:

- zatrzymac patche,
- zrobic inwentaryzacje zmian,
- spisac stan systemu,
- ustalic nowy plan od najnizszej warstwy.

## 27. Ostateczna zasada ochronna

Jesli istnieje ryzyko, ze kolejna zmiana bardziej utrudni odzyskanie stabilnego stanu niz pomoze w diagnozie, nie wolno jej wykonywac.

W takiej sytuacji poprawna akcja to:

- dokumentacja,
- inwentaryzacja,
- backup,
- i dopiero pozniej decyzja o kolejnym ruchu.

## 28. Nie tworzyc dodatkowych dokumentow `.md` poza README bez wyraznej potrzeby

Blad:

- w nowym repo startowym dodalem dodatkowe pliki `.md` organizacyjne, mimo ze repo mialo byc zwarte, kodowe i GitHub-ready bez dokumentacyjnego balastu,
- ta decyzja rozwadniala strukture repo i tworzyla pliki, ktore nie byly potrzebne do uruchomienia ani rozwoju pierwszego kroku implementacji.

Zasada na przyszlosc:

- domyslnie jedynym dokumentem markdown w repo ma byc `README.md`, chyba ze uzytkownik wyraznie poprosi o dodatkowy dokument,
- zasady procesu, decyzje techniczne i plan etapow nalezy w pierwszej kolejnosci wyrazac przez kod, commity i zwięzly README,
- nie wolno rozpychac nowego repo plikami opisowymi, jesli nie wnosza bezposredniej wartosci wykonawczej.