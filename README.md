# Gem Hunter

![screenshot of the CLI](https://github.com/wojciechsacewicz/Gem-Hunter/blob/main/screenshot.png "Gem Hunter Main Menu")

https://github.com/wojciechsacewicz/Gem-Hunter/blob/main/screenshot.png
Version: 1.0 (that will never get updated)

Projekt zaliczeniowy na przedmiot „Nierelacyjne bazy danych” (UG, Informatyka i Ekonometria, semestr 3).

## Opis

Gem Hunter to pipeline do zbierania i oceny ofert pracy:

- Importuje URL-e z plików sitemap.
- Filtruje kolejkę prostymi regułami.
- Pobiera i parsuje szczegóły ofert (HTTP).
- Refiltruje na podstawie zebranych danych.
- Skoruje oferty z użyciem Gemini (płatne API).
- Eksportuje listy wyników do GENERATED_FILES.
- Generuje raporty i wykresy.

## Wymagania

- Python 3.10+
- MongoDB (lokalnie lub zdalnie)
- Pakiety z requirements.txt

## Szybki start

1. Umieść sitemap-y w assets/sitemaps i nazwij je:
   - justjoinit-sitemaps.xml
   - rocketjobs-sitemaps.xml
2. Umieść CV w assets/cv/moje_cv.pdf (lub ustaw CV_PATH w .env).
3. Skopiuj .env.example do .env i uzupełnij wartości.
4. Zainstaluj zależności: pip install -r requirements.txt
5. Uruchom CLI: python main.py
6. Postępuj według kolejności menu (poniżej).

## Pobieranie świeżych sitemap

Jeśli chcesz pobrać świeże sitemap-y:

1. Otwórz: https://public.justjoin.com/rocketjobs/sitemaps/active-jobs/part0.xml
2. Zapisz jako: rocketjobs-sitemaps.xml
3. Otwórz: https://public.justjoin.com/justjoin/sitemaps/active-jobs/part0.xml
4. Zapisz jako: justjoinit-sitemaps.xml
5. Podmień pliki w assets/sitemaps na pobrane wersje.

## Setup (krok po kroku)

1. Skopiuj .env.example do .env i uzupełnij wartości.
2. Upewnij się, że MongoDB działa (lokalnie lub zdalnie).
3. Umieść CV w assets/cv/moje_cv.pdf (lub ustaw CV_PATH w .env).
4. Umieść sitemap-y w assets/sitemaps zgodnie z nazwami w sekcji powyżej.
5. Zainstaluj zależności: pip install -r requirements.txt

## Uruchomienie

- CLI: python main.py
- Harvester z podglądem: python run_harvester.py --max 50 --refresh 5 --verbose

## Konfiguracja (.env)

Wartości konfiguracyjne wczytywane są z .env przez src/config.py.

Najważniejsze zmienne:

- MONGO_URI (domyślnie mongodb://localhost:27017)
- DB_NAME (domyślnie GemHunterDB)
- GEMINI_API_KEY (wymagane dla skoringu)
- CV_PATH (domyślnie assets/cv/moje_cv.pdf)

Uwaga: Skoring działa tylko, gdy podasz klucz GEMINI_API_KEY. To płatne API.

## Notatki

- Skala AI: 1–10 (Gem Finder Showcase pokazuje też 5-gwiazdkowy widok).
- Reguły filtrowania są w src/pre_filter.py.
- Tryb dev pokazuje logi (uruchomienie z flagą --verbose tam, gdzie dostępna).

W repo jest dodany .env.example i .gitignore z odpowiednimi wpisami.

## Requirements

Zobacz requirements.txt.
