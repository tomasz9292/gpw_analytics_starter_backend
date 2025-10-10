# Instrukcja integracji frontendu z backtestem portfela

Poniższy przewodnik opisuje, jak frontend może poprawnie wywoływać endpoint `POST /backtest/portfolio`, który uruchamia backtest portfela na bazie kursów **zamknięcia** spółek.

## Przegląd endpointów

| Endpoint | Metoda | Zastosowanie |
| --- | --- | --- |
| `/backtest/portfolio` | `POST` | Właściwy backtest na podstawie payloadu JSON (manual lub auto). |
| `/backtest/portfolio` | `GET` | Wariant pomocniczy (query string), użyteczny do szybkich testów i debugowania. |
| `/backtest/portfolio/tooling` | `GET` | Metadane do budowy formularzy – podpowiada dostępne opcje, zakresy i opisy pól. |
| `/symbols` | `GET` | Lista dostępnych tickerów (np. do autocomplete). |

## Model requestu

Payload jest walidowany przez model [`BacktestPortfolioRequest`](../api/main.py). Wspólne parametry dla obu trybów:

| Pole | Typ | Wymagane | Opis |
| --- | --- | --- | --- |
| `start` | `YYYY-MM-DD` | Nie (default `2015-01-01`) | Data, od której pobierane są kursy **zamknięcia**. |
| `rebalance` | `"none" \| "monthly" \| "quarterly" \| "yearly"` | Nie (default `"monthly"`) | Określa częstotliwość rebalancingu. |
| `manual` | obiekt | Tak (jeśli brak `auto`) | Konfiguracja portfela manualnego. |
| `auto` | obiekt | Tak (jeśli brak `manual`) | Konfiguracja selekcji automatycznej. |

### Tryb `manual`

```json
{
  "manual": {
    "symbols": ["CDR.WA", "PKN.WA"],
    "weights": [0.6, 0.4]
  }
}
```

* `symbols` – lista GPW tickerów (wielkość liter i sufiks `.WA` są normalizowane do symbolu surowego). Minimum jeden wpis.
* `weights` – opcjonalne, lista wag odpowiadająca kolejności symboli. Jeżeli brak, portfel jest równoważony.

### Tryb `auto`

```json
{
  "auto": {
    "top_n": 5,
    "weighting": "equal",
    "components": [
      {"lookback_days": 252, "metric": "total_return", "weight": 5}
    ],
    "filters": {
      "include": ["CDR.WA", "PKN.WA"],
      "exclude": ["JSW.WA"],
      "prefixes": ["A", "B"]
    }
  }
}
```

* `top_n` – ile najlepszych spółek (po score) wej­dzie do portfela (`1-100`).
* `weighting` – `"equal"` (wszystkie spółki z równą wagą) lub `"score"` (waga proporcjonalna do wyniku rankingu).
* `components` – lista elementów oceny. Aktualnie dostępna metryka to `total_return`, która bazuje na stopie zwrotu z kursów **zamknięcia**.
  * `lookback_days` – ile dni wstecz szukamy bazowej ceny (zakres `1-3650`).
  * `weight` – waga komponentu w sumarycznym score (`1-10`).
* `filters` – opcjonalne ograniczenia wszechświata spółek.
  * `include` – tylko wskazane tickery (priorytet nad prefiksami).
  * `exclude` – pomijane tickery (po normalizacji).
  * `prefixes` – dopuszczone prefiksy symboli (np. `"A"` dla wszystkich zaczynających się na A).

## Przykłady wywołań

### POST – portfel manualny

```http
POST /backtest/portfolio
Content-Type: application/json

{
  "start": "2020-01-01",
  "rebalance": "quarterly",
  "manual": {
    "symbols": ["CDR.WA", "PKN.WA"],
    "weights": [0.5, 0.5]
  }
}
```

### POST – selekcja automatyczna

```http
POST /backtest/portfolio
Content-Type: application/json

{
  "start": "2018-01-01",
  "rebalance": "monthly",
  "auto": {
    "top_n": 3,
    "weighting": "score",
    "components": [
      {"lookback_days": 63, "metric": "total_return", "weight": 6},
      {"lookback_days": 252, "metric": "total_return", "weight": 4}
    ]
  }
}
```

### GET – szybkie testy

* Manual: `/backtest/portfolio?mode=manual&symbols=CDR.WA&symbols=PKN.WA&start=2022-01-01`
* Auto: `/backtest/portfolio?mode=auto&top_n=3&components=252:total_return:5`

Parametr `components` można podawać jako JSON w query stringu, np. `%7B%22lookback_days%22%3A126%2C%22metric%22%3A%22total_return%22%2C%22weight%22%3A5%7D`.

## Struktura odpowiedzi

Endpoint zwraca obiekt [`PortfolioResp`](../api/main.py) zawierający:

* `equity` – listę punktów `{ "date": "YYYY-MM-DD", "value": <float> }`, gdzie wartość jest zbudowana z kursów zamknięcia i wag portfela.
* `stats` – agregaty (`cagr`, `max_drawdown`, `volatility`, `sharpe`, `last_value`). Wartość `last_value` odpowiada ostatniemu punktowi equity.

## Wskazówki implementacyjne dla frontendu

1. **Pobieranie metadanych** – odczytaj `/backtest/portfolio/tooling`, aby zasilić kontrolki formularza domyślnymi zakresami i opisami.
2. **Walidacja symboli** – użyj `/symbols?q=...` do autosugestii. Backend i tak normalizuje wejścia (`CDR.WA` → `CDPROJEKT`).
3. **Budowa payloadu** – w zależności od trybu, front wysyła `manual` albo `auto`. Wysyłanie obu naraz zakończy się błędem 422.
4. **Obsługa błędów** – backend zwraca kody 400/404/422 z komunikatami (np. brak danych historycznych). Pokazuj je użytkownikowi.
5. **Prezentacja wyników** – wykres equity można budować bezpośrednio na tablicy `equity`. Statystyki pokazuj obok, wszystkie są wyliczone z kursów zamknięcia.

Dzięki powyższym krokom frontend może w spójny sposób uruchamiać backtest portfela oparty na kursach zamknięcia i prezentować jego rezultaty.
