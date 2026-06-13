import streamlit as st
import pandas as pd
import json
import os
import datetime

st.set_page_config(
    page_title="Real-Time Fraud Detection Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.sidebar.header("🔌 Status Połączenia")

DB_HOST = os.environ.get("DB_HOST")
DB_USER = os.environ.get("DB_USER")
DB_NAME = os.environ.get("DB_NAME")
DB_PORT = os.environ.get("DB_PORT", "5432")

if DB_HOST:
    st.sidebar.success(f"Połączono z Docker DB: {DB_HOST}")
    MODE = "PRODUCTION_DB"
else:
    st.sidebar.info("💡 Tryb deweloperski: Wykryto brak kontenera bazy. Uruchomiono tryb offline (JSON Seed).")
    MODE = "LOCAL_SEED"

@st.cache_data(ttl=5)
def fetch_data():
    if MODE == "PRODUCTION_DB":
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=os.environ.get("DB_PASSWORD"),
                port=DB_PORT
            )
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            cur.execute("SELECT * FROM processed_transactions ORDER BY timestamp DESC LIMIT 500;")
            tx_data = cur.fetchall()
            
            cur.execute("SELECT * FROM fraud_alerts ORDER BY created_at DESC;")
            alerts_data = cur.fetchall()
            
            cur.execute("SELECT * FROM transaction_window_stats ORDER BY window_end DESC LIMIT 100;")
            stats_data = cur.fetchall()
            
            cur.close()
            conn.close()
            
            return pd.DataFrame(tx_data), pd.DataFrame(alerts_data), pd.DataFrame(stats_data)
            
        except Exception as e:
            st.sidebar.error(f"Błąd połączenia z bazą: {e}. Fallback do JSON.")
            return load_from_json()
    else:
        return load_from_json()

def load_from_json():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base_dir, 'infrastructure', 'database', 'init', 'seed-data.json')
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        df_tx = pd.DataFrame(data.get('processed_transactions', []))
        df_alerts = pd.DataFrame(data.get('fraud_alerts', []))
        df_stats = pd.DataFrame(data.get('transaction_window_stats', []))
        return df_tx, df_alerts, df_stats
    except Exception as e:
        st.error(f"Krytyczny błąd odczytu pliku seed: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

df_tx, df_alerts, df_stats = fetch_data()

st.title("🛡️ System Wykrywania Oszustw Finansowych (RTA)")
st.markdown("Monitorowanie strumieni danych transakcyjnych i alertów modeli ML w czasie rzeczywistym.")

st.markdown("### 📈 Kluczowe Wskaźniki Efektywności")
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    total_tx = len(df_tx) if not df_tx.empty else 0
    st.metric(label="Przetworzone Transakcje", value=total_tx)

with kpi2:
    total_frauds = len(df_alerts) if not df_alerts.empty else 0
    st.metric(label="Zablokowane Oszustwa", value=total_frauds, delta=f"{total_frauds} alertów", delta_color="inverse")

with kpi3:
    if not df_tx.empty and 'amount' in df_tx.columns:
        avg_amount = round(df_tx['amount'].astype(float).mean(), 2)
        st.metric(label="Średnia Wartość Operacji", value=f"{avg_amount} PLN")
    else:
        st.metric(label="Średnia Wartość Operacji", value="0.00 PLN")

with kpi4:
    st.metric(label="Opóźnienie Strumienia", value="< 150ms", delta="Stabilne")

st.markdown("---")

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Analiza Wolumenu i Ruchu", 
    "🚨 Centrum Zarządzania Alertami", 
    "🧮 Agregacje Okienkowe",
    "💰 Analiza ROI i Biznesu"
])

with tab1:
    st.subheader("Analiza wolumenu operacji finansowych")
    
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.markdown("##### Kwoty transakcji w czasie")
        if not df_tx.empty and 'amount' in df_tx.columns:
            chart_df = df_tx.copy()
            time_col = 'timestamp' if 'timestamp' in chart_df.columns else ('created_at' if 'created_at' in chart_df.columns else None)
            if time_col:
                chart_df = chart_df.sort_values(time_col)
                st.line_chart(data=chart_df, x=time_col, y='amount')
            else:
                st.line_chart(chart_df['amount'])
        else:
            st.info("Brak wystarczających danych do wyrysowania osi czasu kwot.")
            
    with col_right:
        st.markdown("##### Podgląc strumienia wejściowego (Ostatnie rekordy)")
        if not df_tx.empty:
            st.dataframe(df_tx.head(15), width="stretch")
        else:
            st.warning("Oczekiwanie na pojawienie się danych transakcyjnych w systemie...")

with tab2:
    st.subheader("🚨 Wykryte próby nadużyć i anomalie")
    
    if not df_alerts.empty:
        st.error(f"System zarejestrował {len(df_alerts)} incydentów wymagających weryfikacji.")
    
        st.markdown("##### Rejestr zgłoszeń modeli ML i reguł bezpieczeństwa")
        st.dataframe(
            df_alerts, 
            width="stretch",
            column_config={
                "risk_score": st.column_config.ProgressColumn(
                    "Poziom Ryzyka",
                    help="Wynik dopasowania anomalii przez algorytm ML + Reguły",
                    format="%.2f",
                    min_value=0.0,
                    max_value=100.0,
                )
            }
        )
    else:
        st.success("✅ Brak aktywnych alertów. Wszystkie transakcje spełniają kryteria bezpieczeństwa.")

with tab3:
    st.subheader("🧮 Agregacje danych w oknach czasowych")
    st.markdown("Dane dostarczane przez warstwę Stream Processing (Osoba 3) opisujące zachowanie systemu w ujęciu interwałowym.")
    
    if not df_stats.empty:
        st.dataframe(df_stats, width="stretch")
        
        count_col = 'transaction_count' if 'transaction_count' in df_stats.columns else ('tx_count' if 'tx_count' in df_stats.columns else None)
        if count_col:
            st.markdown("##### Liczba transakcji w poszczególnych oknach systemowych")
            st.bar_chart(data=df_stats, y=count_col)
    else:
        st.info("Brak przetworzonych agregacji okienkowych w bazie danych. System oczekuje na start silnika streamingu.")

with tab4:
    st.subheader("💰 Wpływ wdrożenia systemu na wynik finansowy (ROI)")
    st.markdown("Analiza korzyści majątkowych wynikających z automatycznego blokowania oszustw kartowych oraz kosztów utrzymania operacyjnego zespołu review.")

    if not df_alerts.empty:
        # Zapewnienie poprawności typów danych
        df_alerts['amount'] = df_alerts['amount'].astype(float)
        
        # Obliczenia ogólnych KPI finansowych
        total_saved_potential = df_alerts['amount'].sum()
        liczba_high_alerts = len(df_alerts[df_alerts['risk_level'] == 'high'])
        koszt_operacyjny = liczba_high_alerts * 5.0
        oszczednosci_netto_total = total_saved_potential - koszt_operacyjny

        # Wyświetlenie kart finansowych
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric(label="💵 Łączny Uratowany Kapitał", value=f"{total_saved_potential:,.2f} PLN")
        with m2:
            st.metric(label="📉 Koszty Operacyjne (Manual Review)", value=f"{koszt_operacyjny:,.2f} PLN", delta_color="inverse")
        with m3:
            st.metric(label="🚀 Czyste Oszczędności Systemu (Net ROI)", value=f"{oszczednosci_netto_total:,.2f} PLN")

        st.markdown("---")
        st.markdown("##### 📈 Kumulacja oszczędności finansowych w czasie")
        
        time_alert_col = 'created_at' if 'created_at' in df_alerts.columns else ('timestamp' if 'timestamp' in df_alerts.columns else None)
        
        if time_alert_col:
            df_chart_money = df_alerts.copy()
            df_chart_money = df_chart_money.sort_values(time_alert_col)
            
            # Nowa poprawiona logika finansowa:
            # Każdy zarejestrowany alert to uratowana kwota transakcji.
            # Jeśli to 'critical' -> czysty zysk (pełna automatyzacja).
            # Jeśli to 'high' -> ratujemy kwotę, ale ponosimy koszt 5 PLN za pracę analityka.
            df_chart_money['financial_impact'] = df_chart_money.apply(
                lambda r: r['amount'] if r['risk_level'] == 'critical' else (r['amount'] - 5.0 if r['risk_level'] == 'high' else 0.0),
                axis=1
            )
            
            # Suma skumulowana
            df_chart_money['Zaoszczędzone Pieniądze (Suma skumulowana)'] = df_chart_money['financial_impact'].cumsum()
            
            # Wykres liniowy kierujący się do góry
            st.line_chart(data=df_chart_money, x=time_alert_col, y='Zaoszczędzone Pieniądze (Suma skumulowana)')
        else:
            st.info("Brak znacznika czasu w alertach, aby wygenerować wykres skumulowany.")
    else:
        st.info("Brak zarejestrowanych alertów w systemie. Wartość uratowanego kapitału wynosi obecnie 0.00 PLN.")