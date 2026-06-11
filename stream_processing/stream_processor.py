import json
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


# ─────────────────────────────────────────────────────────────────────────────
# Konfiguracja Kafki i topiców
# ─────────────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

TOPIC_WEJSCIOWY = "transactions"
TOPIC_PRZETWORZONE = "processed-transactions"
TOPIC_ALERTY = "fraud-alerts"
TOPIC_STATYSTYKI = "transaction-window-stats"

TOPICI_WYJSCIOWE = [TOPIC_PRZETWORZONE, TOPIC_ALERTY, TOPIC_STATYSTYKI]

OKNO_STATYSTYK_SEKUNDY = 60
CO_ILE_WYSYLAC_STATYSTYKI = 10

POLSKIE_MIASTA = {"Warsaw", "Krakow", "Gdansk"}
ZAGRANICZNE_MIASTA = {"Berlin", "London", "Lagos"}


# ─────────────────────────────────────────────────────────────────────────────
# Funkcje pomocnicze
# ─────────────────────────────────────────────────────────────────────────────

def zamien_date_na_datetime(timestamp):
    """Zamienia timestamp z wiadomości na obiekt datetime w UTC."""
    timestamp = timestamp.replace("Z", "+00:00")
    data = datetime.fromisoformat(timestamp)

    if data.tzinfo is None:
        data = data.replace(tzinfo=timezone.utc)

    return data.astimezone(timezone.utc)


def utworz_topici_jesli_nie_istnieja():
    """Tworzy topici wyjściowe, jeśli jeszcze nie istnieją."""
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="stream-processor-admin",
    )

    istniejace_topici = set(admin.list_topics())
    topici_do_utworzenia = []

    for nazwa_topicu in TOPICI_WYJSCIOWE:
        if nazwa_topicu not in istniejace_topici:
            topici_do_utworzenia.append(
                NewTopic(
                    name=nazwa_topicu,
                    num_partitions=6,
                    replication_factor=1,
                )
            )

    if topici_do_utworzenia:
        try:
            admin.create_topics(topici_do_utworzenia)
            print("Utworzono topici:", [topic.name for topic in topici_do_utworzenia])
        except TopicAlreadyExistsError:
            pass

    admin.close()


def policz_poziom_ryzyka(wynik):
    """Zamienia punktowy wynik ryzyka na poziom opisowy."""
    if wynik >= 75:
        return "critical"
    if wynik >= 50:
        return "high"
    if wynik >= 25:
        return "medium"
    return "low"


# ─────────────────────────────────────────────────────────────────────────────
# Logika biznesowa: wykrywanie podejrzanych transakcji
# ─────────────────────────────────────────────────────────────────────────────

def policz_ryzyko(transakcja, historia_uzytkownikow):
    """
    Liczy ryzyko transakcji na podstawie prostych reguł biznesowych.

    Zwraca:
    - risk_score, czyli wynik od 0 do 100,
    - risk_flags, czyli listę powodów, dla których transakcja wygląda podejrzanie.
    """
    user_id = transakcja["user_id"]
    kwota = float(transakcja["amount"])
    kategoria = transakcja["merchant_category"]
    miasto = transakcja["city"]
    czas_transakcji = zamien_date_na_datetime(transakcja["timestamp"])

    historia = historia_uzytkownikow[user_id]
    poprzednie_transakcje = list(historia["transakcje"])

    wynik = 0
    powody = []

    # Reguła 1: bardzo mała płatność online może oznaczać testowanie karty.
    if kategoria == "online_shop" and kwota <= 5:
        wynik += 35
        powody.append("very_low_online_transaction")

    # Reguła 2: duża transakcja w elektronice lub paliwie jest bardziej ryzykowna.
    if kwota >= 3000 and kategoria in {"electronics", "fuel"}:
        wynik += 45
        powody.append("high_value_sensitive_category")

    # Reguła 3: wypłata z bankomatu za granicą na wysoką kwotę.
    if miasto in ZAGRANICZNE_MIASTA and kategoria == "atm" and kwota >= 1000:
        wynik += 50
        powody.append("foreign_atm_withdrawal")

    # Reguła 4: wiele transakcji jednego użytkownika w krótkim czasie.
    if len(poprzednie_transakcje) >= 5:
        wynik += 20
        powody.append("high_user_velocity_5min")

    # Reguła 5: kilka bardzo małych płatności online pod rząd.
    male_platnosci_online = 0
    for poprzednia in poprzednie_transakcje:
        if poprzednia["merchant_category"] == "online_shop" and float(poprzednia["amount"]) <= 5:
            male_platnosci_online += 1

    if kategoria == "online_shop" and kwota <= 5 and male_platnosci_online >= 2:
        wynik += 25
        powody.append("repeated_card_testing_pattern")

    # Reguła 6: szybka zmiana lokalizacji z Polski na zagranicę.
    ostatnie_miasto = historia["ostatnie_miasto"]
    ostatni_czas = historia["ostatni_czas"]

    if ostatnie_miasto in POLSKIE_MIASTA and miasto in ZAGRANICZNE_MIASTA and ostatni_czas:
        roznica_minut = (czas_transakcji - ostatni_czas).total_seconds() / 60
        if 0 <= roznica_minut <= 30:
            wynik += 30
            powody.append("impossible_travel_pattern")

    # To pole pochodzi z generatora danych. Nie traktujemy go jako reguły,
    # tylko zostawiamy jako informację do późniejszej oceny jakości.
    if transakcja.get("is_fraud") is True:
        powody.append("simulated_ground_truth_fraud")

    return min(wynik, 100), powody


def usun_stare_dane(historia_uzytkownikow, okno_globalne, aktualny_czas):
    """Czyści stare dane z pamięci, żeby program nie trzymał wszystkiego bez końca."""
    granica_okna = aktualny_czas.timestamp() - OKNO_STATYSTYK_SEKUNDY

    while okno_globalne and okno_globalne[0]["event_ts"].timestamp() < granica_okna:
        okno_globalne.popleft()

    granica_historii_uzytkownika = aktualny_czas.timestamp() - 5 * 60

    for user_id in list(historia_uzytkownikow.keys()):
        historia = historia_uzytkownikow[user_id]

        while historia["transakcje"] and historia["transakcje"][0]["event_ts"].timestamp() < granica_historii_uzytkownika:
            historia["transakcje"].popleft()

        if not historia["transakcje"] and historia["ostatni_czas"]:
            if historia["ostatni_czas"].timestamp() < granica_historii_uzytkownika:
                del historia_uzytkownikow[user_id]


def przetworz_transakcje(transakcja, historia_uzytkownikow, okno_globalne):
    """Dodaje do transakcji wynik ryzyka i aktualizuje krótką historię użytkownika."""
    czas_transakcji = zamien_date_na_datetime(transakcja["timestamp"])
    usun_stare_dane(historia_uzytkownikow, okno_globalne, czas_transakcji)

    wynik_ryzyka, powody = policz_ryzyko(transakcja, historia_uzytkownikow)
    poziom_ryzyka = policz_poziom_ryzyka(wynik_ryzyka)

    przetworzona = dict(transakcja)
    przetworzona["processed_at"] = datetime.now(timezone.utc).isoformat()
    przetworzona["risk_score"] = wynik_ryzyka
    przetworzona["risk_level"] = poziom_ryzyka
    przetworzona["risk_flags"] = powody
    przetworzona["requires_manual_review"] = poziom_ryzyka in {"high", "critical"}
    przetworzona["processor_version"] = "2.0-simple-pl"

    # Aktualizacja historii użytkownika.
    user_id = transakcja["user_id"]
    historia = historia_uzytkownikow[user_id]

    transakcja_do_historii = dict(transakcja)
    transakcja_do_historii["event_ts"] = czas_transakcji
    transakcja_do_historii["risk_score"] = wynik_ryzyka
    transakcja_do_historii["risk_level"] = poziom_ryzyka

    historia["transakcje"].append(transakcja_do_historii)
    historia["ostatnie_miasto"] = transakcja["city"]
    historia["ostatni_czas"] = czas_transakcji

    okno_globalne.append(transakcja_do_historii)

    return przetworzona


# ─────────────────────────────────────────────────────────────────────────────
# Statystyki okienne
# ─────────────────────────────────────────────────────────────────────────────

def zbuduj_statystyki_okienne(okno_globalne):
    """Buduje statystyki z ostatnich 60 sekund."""
    transakcje = list(okno_globalne)
    liczba_transakcji = len(transakcje)
    suma_kwot = round(sum(float(t["amount"]) for t in transakcje), 2)

    liczba_alertow = 0
    liczba_fraudow_symulowanych = 0
    po_miastach = defaultdict(int)
    po_kategoriach = defaultdict(int)

    for transakcja in transakcje:
        if transakcja["risk_level"] in {"high", "critical"}:
            liczba_alertow += 1

        if transakcja.get("is_fraud") is True:
            liczba_fraudow_symulowanych += 1

        po_miastach[transakcja["city"]] += 1
        po_kategoriach[transakcja["merchant_category"]] += 1

    if liczba_transakcji > 0:
        srednia_kwota = round(suma_kwot / liczba_transakcji, 2)
        udzial_alertow = round(liczba_alertow / liczba_transakcji, 4)
    else:
        srednia_kwota = 0
        udzial_alertow = 0

    return {
        "window_seconds": OKNO_STATYSTYK_SEKUNDY,
        "window_end": datetime.now(timezone.utc).isoformat(),
        "transaction_count": liczba_transakcji,
        "total_amount": suma_kwot,
        "avg_amount": srednia_kwota,
        "alerts_count": liczba_alertow,
        "simulated_frauds_count": liczba_fraudow_symulowanych,
        "alert_rate": udzial_alertow,
        "by_city": dict(sorted(po_miastach.items())),
        "by_category": dict(sorted(po_kategoriach.items())),
    }


def wyslij_statystyki_jesli_czas(producer, okno_globalne, ostatnia_wysylka):
    """Wysyła statystyki okienne co kilka sekund."""
    teraz = time.time()

    if teraz - ostatnia_wysylka < CO_ILE_WYSYLAC_STATYSTYKI:
        return ostatnia_wysylka

    statystyki = zbuduj_statystyki_okienne(okno_globalne)

    producer.send(
        TOPIC_STATYSTYKI,
        key="global",
        value=statystyki,
    )
    producer.flush()

    print("Wysłano statystyki okienne:", statystyki)
    return teraz


# ─────────────────────────────────────────────────────────────────────────────
# Główna pętla programu
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("Uruchamianie stream processora...")
    print("Kafka:", KAFKA_BOOTSTRAP_SERVERS)

    utworz_topici_jesli_nie_istnieja()

    consumer = KafkaConsumer(
        TOPIC_WEJSCIOWY,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="fraud-stream-processor",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        key_deserializer=lambda x: x.decode("utf-8") if x else None,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda x: x.encode("utf-8") if x else None,
    )

    # Historia użytkowników z ostatnich kilku minut.
    historia_uzytkownikow = defaultdict(lambda: {
        "transakcje": deque(),
        "ostatnie_miasto": None,
        "ostatni_czas": None,
    })

    # Globalne okno transakcji z ostatniej minuty.
    okno_globalne = deque()
    ostatnia_wysylka_statystyk = time.time()

    print("Processor działa.")
    print(f"Czytam z topicu: {TOPIC_WEJSCIOWY}")
    print(f"Zapisuję do topiców: {TOPICI_WYJSCIOWE}")

    while True:
        paczka_wiadomosci = consumer.poll(timeout_ms=1000, max_records=100)

        if not paczka_wiadomosci:
            ostatnia_wysylka_statystyk = wyslij_statystyki_jesli_czas(
                producer,
                okno_globalne,
                ostatnia_wysylka_statystyk,
            )
            continue

        for _partycja, wiadomosci in paczka_wiadomosci.items():
            for wiadomosc in wiadomosci:
                transakcja = wiadomosc.value

                try:
                    przetworzona = przetworz_transakcje(
                        transakcja,
                        historia_uzytkownikow,
                        okno_globalne,
                    )

                    user_id = przetworzona["user_id"]

                    # 1. Zapis każdej przetworzonej transakcji do Kafki.
                    producer.send(
                        TOPIC_PRZETWORZONE,
                        key=user_id,
                        value=przetworzona,
                    )

                    # 2. Jeśli transakcja jest ryzykowna, tworzymy alert.
                    if przetworzona["requires_manual_review"]:
                        alert = {
                            "alert_id": "ALERT-" + przetworzona["transaction_id"],
                            "created_at": datetime.now(timezone.utc).isoformat(),
                            "transaction_id": przetworzona["transaction_id"],
                            "user_id": przetworzona["user_id"],
                            "amount": przetworzona["amount"],
                            "city": przetworzona["city"],
                            "merchant_category": przetworzona["merchant_category"],
                            "risk_score": przetworzona["risk_score"],
                            "risk_level": przetworzona["risk_level"],
                            "risk_flags": przetworzona["risk_flags"],
                            "recommended_action": "manual_review",
                        }

                        if przetworzona["risk_level"] == "critical":
                            alert["recommended_action"] = "block_and_manual_review"

                        producer.send(
                            TOPIC_ALERTY,
                            key=user_id,
                            value=alert,
                        )

                        print("ALERT:", alert)
                    else:
                        print(
                            "OK:",
                            przetworzona["transaction_id"],
                            "user=", przetworzona["user_id"],
                            "risk=", przetworzona["risk_score"],
                        )

                except Exception as blad:
                    print("Błąd przetwarzania wiadomości:", blad)
                    print("Wiadomość:", transakcja)

        producer.flush()
        ostatnia_wysylka_statystyk = wyslij_statystyki_jesli_czas(
            producer,
            okno_globalne,
            ostatnia_wysylka_statystyk,
        )


if __name__ == "__main__":
    main()
