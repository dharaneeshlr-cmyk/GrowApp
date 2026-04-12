import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import os

# =========================
# CONFIG
# =========================
DATA_FILE = "trades.csv"

st.set_page_config(page_title="Zerodha Pro Trade Dashboard", layout="wide")

# =========================
# INIT DATA
# =========================
if not os.path.exists(DATA_FILE):
    df_init = pd.DataFrame(columns=[
        "Date","Symbol","Segment","Strategy","Type",
        "Qty","Buy","Sell","Charges","PnL",
        "Expiry","Strike","Option Type"
    ])
    df_init.to_csv(DATA_FILE, index=False)

# Load data
df = pd.read_csv(DATA_FILE)

# =========================
# SIDEBAR INPUT
# =========================
st.sidebar.title("➕ Add / Import Trade")

# Manual Entry
with st.sidebar.expander("Manual Entry"):
    with st.form("trade_form"):
        date = st.date_input("Date", datetime.today())
        symbol = st.text_input("Symbol")
        segment = st.selectbox("Segment", ["Equity","Futures","Options"])
        strategy = st.selectbox("Strategy", ["Scalping","Intraday","Swing","Option Buying","Option Selling"])
        trade_type = st.selectbox("Type", ["Long","Short"])
        qty = st.number_input("Quantity", min_value=1)
        buy = st.number_input("Buy Price", min_value=0.0)
        sell = st.number_input("Sell Price", min_value=0.0)
        charges = st.number_input("Charges", min_value=0.0)

        expiry = st.text_input("Expiry (Optional)")
        strike = st.text_input("Strike (Optional)")
        opt_type = st.selectbox("Option Type", ["","CE","PE"])

        submit = st.form_submit_button("Add Trade")

        if submit:
            gross = (sell - buy)*qty if trade_type=="Long" else (buy - sell)*qty
            pnl = gross - charges

            new_row = pd.DataFrame([[date,symbol,segment,strategy,trade_type,
                                     qty,buy,sell,charges,pnl,
                                     expiry,strike,opt_type]],
                                   columns=df.columns)
            df = pd.concat([df,new_row], ignore_index=True)
            df.to_csv(DATA_FILE,index=False)
            st.success("Trade added")

# Zerodha CSV Upload
with st.sidebar.expander("📥 Import Zerodha CSV"):
    uploaded_file = st.file_uploader("Upload Zerodha Trade Book CSV", type=["csv"])

    if uploaded_file:
        zdf = pd.read_csv(uploaded_file)

        # Basic mapping (adjust if needed)
        try:
            zdf = zdf.rename(columns={
                "tradingsymbol":"Symbol",
                "quantity":"Qty",
                "price":"Buy"
            })

            zdf["Sell"] = zdf["Buy"]
            zdf["Charges"] = 0
            zdf["PnL"] = 0
            zdf["Segment"] = "Equity"
            zdf["Strategy"] = "Imported"
            zdf["Type"] = "Long"
            zdf["Date"] = datetime.today()

            final = zdf[df.columns.intersection(zdf.columns)]
            df = pd.concat([df, final], ignore_index=True)
            df.to_csv(DATA_FILE, index=False)

            st.success("Imported successfully")
        except Exception as e:
            st.error(f"Error: {e}")

# =========================
# DASHBOARD
# =========================
st.title("📊 Zerodha Pro Analytics Dashboard")

if not df.empty:
    df['Date'] = pd.to_datetime(df['Date'])

    df = df.sort_values('Date')
    df['Cumulative'] = df['PnL'].cumsum()

    wins = df[df['PnL'] > 0]
    losses = df[df['PnL'] <= 0]

    total_pnl = df['PnL'].sum()
    win_rate = len(wins)/len(df)*100 if len(df)>0 else 0

    avg_win = wins['PnL'].mean() if not wins.empty else 0
    avg_loss = losses['PnL'].mean() if not losses.empty else 0

    expectancy = (win_rate/100)*avg_win + (1-win_rate/100)*avg_loss

    df['Peak'] = df['Cumulative'].cummax()
    df['DD'] = df['Cumulative'] - df['Peak']
    max_dd = df['DD'].min()

    sharpe = df['PnL'].mean()/df['PnL'].std() if df['PnL'].std()!=0 else 0

    # Metrics
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("PnL", f"₹{total_pnl:,.0f}")
    c2.metric("Win %", f"{win_rate:.1f}%")
    c3.metric("Expectancy", f"₹{expectancy:,.0f}")
    c4.metric("Sharpe", f"{sharpe:.2f}")
    c5.metric("Max DD", f"₹{max_dd:,.0f}")

    # Charts
    st.subheader("📈 Equity Curve")
    st.line_chart(df.set_index('Date')['Cumulative'])

    st.subheader("📊 Monthly Heatmap")
    df['Month'] = df['Date'].dt.to_period('M')
    heat = df.groupby('Month')['PnL'].sum()
    st.bar_chart(heat)

    st.subheader("📊 Strategy Performance")
    st.bar_chart(df.groupby('Strategy')['PnL'].sum())

    st.subheader("📊 Options Analysis")
    opt_df = df[df['Segment']=="Options"]
    if not opt_df.empty:
        st.bar_chart(opt_df.groupby('Option Type')['PnL'].sum())

    # Table
    st.subheader("📋 Trade Log")
    st.dataframe(df, use_container_width=True)

else:
    st.info("No trades yet")

# Download
st.download_button("Download CSV", df.to_csv(index=False), "trade_log.csv")
