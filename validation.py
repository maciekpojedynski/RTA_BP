st.markdown("### 💰 Analiza Opłacalności Biznesowej (ROI)")
col_money1, col_money2, col_money3 = st.columns(3)

# Przykładowe wyliczenia oparte na Twoich danych (zakładając, że masz kolumnę is_fraud i amount)
if not df_alerts.empty:
    # 1. Uratowana kasa (Słuszne alerty)
    # Dla uproszczenia: sumujemy kwoty alertów krytycznych, które system zablokował
    uratowana_kasa = df_alerts[df_alerts['risk_level'] == 'critical']['amount'].astype(float).sum()
    
    # 2. Koszt operacyjny manual review (np. 5 zł za sprawdzenie jednego alertu "high")
    liczba_manual_reviews = len(df_alerts[df_alerts['risk_level'] == 'high'])
    koszt_operacyjny = liczba_manual_reviews * 5.0
    
    # 3. Czysty zysk wdrożenia systemu (Uratowane minus koszty operacyjne)
    roi_netto = uratowana_kasa - koszt_operacyjny
else:
    uratowana_kasa, koszt_operacyjny, roi_netto = 0, 0, 0

with col_money1:
    st.metric(label="Uratowany Kapitał (Zablokowane Fraudby)", value=f"{uratowana_kasa:,.2f} PLN", delta="Wzrost ochrony")

with col_money2:
    st.metric(label="Koszt Operacyjny Weryfikacji", value=f"{koszt_operacyjny:,.2f} PLN", delta="- Koszt pracy", delta_color="inverse")

with col_money3:
    st.metric(label="Oszczędności Netto (ROI)", value=f"{roi_netto:,.2f} PLN", delta="Czysty zysk systemu", delta_color="off")