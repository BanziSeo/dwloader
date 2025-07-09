# streamlit_integrated_app.py - Streamlit Cloud 배포용 통합 앱
# NASDAQ 티커 다운로드 + yfinance 데이터 수집 통합

import streamlit as st
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime, timedelta
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import json
import base64
import zipfile
from io import BytesIO

st.set_page_config(
    page_title="Stock Data Fetcher Pro", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# 세션 상태 초기화
if 'ticker_df' not in st.session_state:
    st.session_state.ticker_df = None
if 'filtered_df' not in st.session_state:
    st.session_state.filtered_df = None
if 'stock_data' not in st.session_state:
    st.session_state.stock_data = {}

@st.cache_data(ttl=86400)  # 24시간 캐시
def download_nasdaq_tickers():
    """NASDAQ 공식 사이트에서 티커 리스트 다운로드"""
    with st.spinner("NASDAQ에서 티커 리스트 다운로드 중..."):
        nasdaq_url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
        other_url = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
        
        try:
            # NASDAQ 상장 종목
            nasdaq_response = requests.get(nasdaq_url)
            nasdaq_df = pd.read_csv(io.StringIO(nasdaq_response.text), sep='|')
            nasdaq_df = nasdaq_df[nasdaq_df['Test Issue'] == 'N']
            nasdaq_df['Exchange'] = 'NASDAQ'
            
            # 기타 거래소 종목
            other_response = requests.get(other_url)
            other_df = pd.read_csv(io.StringIO(other_response.text), sep='|')
            other_df = other_df[other_df['Test Issue'] == 'N']
            other_df['Exchange'] = 'OTHER'
            
            # 필요한 컬럼만 선택하고 합치기
            nasdaq_clean = nasdaq_df[['Symbol', 'Security Name', 'Market Category', 'Exchange']].copy()
            other_clean = other_df[['ACT Symbol', 'Security Name', 'Exchange']].copy()
            other_clean.rename(columns={'ACT Symbol': 'Symbol'}, inplace=True)
            other_clean['Market Category'] = 'N/A'
            
            # 합치기
            combined_df = pd.concat([nasdaq_clean, other_clean], ignore_index=True)
            combined_df.rename(columns={'Security Name': 'Name'}, inplace=True)
            
            return combined_df
        except Exception as e:
            st.error(f"티커 리스트 다운로드 실패: {e}")
            return pd.DataFrame()

def get_ticker_info_batch(symbols, progress_callback=None):
    """티커별 시가총액, 가격, 거래량 정보 가져오기"""
    ticker_info = []
    failed_tickers = []
    
    for i, symbol in enumerate(symbols):
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            # 빠른 히스토리 데이터로 최근 가격/거래량 확인
            hist = ticker.history(period="5d")
            
            if not hist.empty:
                last_price = hist['Close'].iloc[-1]
                avg_volume = hist['Volume'].mean()
                market_cap = info.get('marketCap', 0)
                
                ticker_info.append({
                    'Symbol': symbol,
                    'Name': info.get('longName', info.get('shortName', symbol)),
                    'Last Sale': last_price,
                    'Market Cap': market_cap,
                    'Volume': avg_volume
                })
            else:
                failed_tickers.append(symbol)
                
        except Exception as e:
            failed_tickers.append(symbol)
        
        # 진행 상황 콜백
        if progress_callback:
            progress_callback(i + 1, len(symbols))
        
        # Rate limit 방지
        if (i + 1) % 10 == 0:
            time.sleep(1)
        else:
            time.sleep(0.1)
    
    return pd.DataFrame(ticker_info), failed_tickers

def fetch_stock_data(symbol, start_date, end_date):
    """주식 데이터 가져오기"""
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start_date, end=end_date, interval='1d')
        
        if df.empty:
            return None
        
        df.reset_index(inplace=True)
        df['date'] = df['Date'].dt.strftime('%Y%m%d')
        df.rename(columns={
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'Volume'
        }, inplace=True)
        
        return df[['date', 'open', 'high', 'low', 'close', 'Volume']]
        
    except Exception as e:
        return None

def create_download_zip(data_dict):
    """여러 CSV 파일을 ZIP으로 압축"""
    zip_buffer = BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for filename, df in data_dict.items():
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8')
            csv_buffer.seek(0)
            zip_file.writestr(f"{filename}.csv", csv_buffer.getvalue())
    
    zip_buffer.seek(0)
    return zip_buffer

# Streamlit UI
st.title("📊 Stock Data Fetcher Pro")
st.markdown("NASDAQ 티커 다운로드 + 주식 데이터 수집 통합 앱")

# 사이드바
with st.sidebar:
    st.header("⚙️ 설정")
    
    years_back = st.number_input(
        "데이터 기간 (년)",
        min_value=1,
        max_value=10,
        value=5
    )
    
    st.subheader("📋 필터링 조건")
    min_price = st.number_input("최소 주가 ($)", value=5.0, step=1.0)
    min_market_cap = st.number_input("최소 시가총액 ($)", value=10_000_000_000, step=1_000_000_000, format="%d")
    min_volume = st.number_input("최소 거래량", value=100_000, step=10_000, format="%d")
    
    st.subheader("🚀 처리 옵션")
    batch_size = st.number_input("배치 크기", value=50, min_value=10, max_value=200)
    use_sample = st.checkbox("샘플 모드 (상위 20개만)", value=True)

# 메인 탭
tab1, tab2, tab3, tab4 = st.tabs(["📥 1. 티커 다운로드", "📊 2. 티커 정보 수집", "💾 3. 주식 데이터 다운로드", "🔧 API 모드"])

with tab1:
    st.header("Step 1: NASDAQ 티커 리스트 다운로드")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        if st.button("🔄 티커 리스트 다운로드", type="primary", key="download_tickers"):
            ticker_df = download_nasdaq_tickers()
            
            if not ticker_df.empty:
                st.session_state.ticker_df = ticker_df
                st.success(f"✅ {len(ticker_df)}개 티커 다운로드 완료!")
    
    with col2:
        if st.session_state.ticker_df is not None:
            st.info(f"현재 로드된 티커: {len(st.session_state.ticker_df)}개")
            
            # 샘플 표시
            st.subheader("티커 리스트 샘플")
            st.dataframe(st.session_state.ticker_df.head(10))
            
            # CSV 다운로드
            csv = st.session_state.ticker_df.to_csv(index=False)
            st.download_button(
                label="💾 티커 리스트 CSV 다운로드",
                data=csv,
                file_name=f"nasdaq_tickers_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

with tab2:
    st.header("Step 2: 티커 정보 수집 (시가총액, 가격, 거래량)")
    
    if st.session_state.ticker_df is not None:
        # 샘플 모드 처리
        if use_sample:
            # 주요 종목만 선택
            major_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 
                           'JPM', 'JNJ', 'V', 'WMT', 'PG', 'MA', 'HD', 'DIS',
                           'NFLX', 'PYPL', 'INTC', 'CSCO', 'PFE']
            sample_df = st.session_state.ticker_df[st.session_state.ticker_df['Symbol'].isin(major_tickers)]
            symbols_to_process = sample_df['Symbol'].tolist()
            st.warning(f"⚠️ 샘플 모드: {len(symbols_to_process)}개 주요 종목만 처리")
        else:
            symbols_to_process = st.session_state.ticker_df['Symbol'].tolist()
        
        if st.button("📈 티커 정보 수집 시작", type="primary", key="fetch_info"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            def update_progress(current, total):
                progress = current / total
                progress_bar.progress(progress)
                status_text.text(f"처리 중... {current}/{total} ({progress*100:.1f}%)")
            
            # 티커 정보 수집
            info_df, failed = get_ticker_info_batch(symbols_to_process, update_progress)
            
            if not info_df.empty:
                # 원본 티커 정보와 병합
                merged_df = st.session_state.ticker_df.merge(info_df, on='Symbol', how='inner', suffixes=('', '_info'))
                
                # Name 컬럼 처리
                if 'Name_info' in merged_df.columns:
                    merged_df['Name'] = merged_df['Name_info'].fillna(merged_df['Name'])
                    merged_df.drop('Name_info', axis=1, inplace=True)
                
                # 필터링
                filtered_df = merged_df[
                    (merged_df['Last Sale'] >= min_price) &
                    (merged_df['Market Cap'] >= min_market_cap) &
                    (merged_df['Volume'] >= min_volume)
                ]
                
                st.session_state.filtered_df = filtered_df
                
                st.success(f"✅ 완료! {len(info_df)}개 중 {len(filtered_df)}개가 필터 조건 충족")
                
                # 결과 표시
                st.subheader("필터링된 종목")
                st.dataframe(filtered_df[['Symbol', 'Name', 'Last Sale', 'Market Cap', 'Volume']].head(20))
                
                # nasdaq_data.csv 형식으로 다운로드
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="💾 nasdaq_data.csv 다운로드",
                    data=csv,
                    file_name="nasdaq_data.csv",
                    mime="text/csv"
                )
            
            progress_bar.empty()
            status_text.empty()
    else:
        st.warning("먼저 Step 1에서 티커 리스트를 다운로드하세요!")

with tab3:
    st.header("Step 3: 주식 데이터 다운로드")
    
    if st.session_state.filtered_df is not None:
        st.info(f"필터링된 종목: {len(st.session_state.filtered_df)}개")
        
        # 다운로드할 종목 선택
        col1, col2 = st.columns(2)
        
        with col1:
            selected_all = st.checkbox("전체 선택", value=True)
        
        with col2:
            if not selected_all:
                selected_symbols = st.multiselect(
                    "종목 선택",
                    options=st.session_state.filtered_df['Symbol'].tolist()
                )
            else:
                selected_symbols = st.session_state.filtered_df['Symbol'].tolist()
        
        if st.button("🚀 주식 데이터 다운로드", type="primary", key="download_stocks"):
            if selected_symbols:
                # 날짜 설정
                end_date = datetime.now()
                start_date = end_date - timedelta(days=365 * years_back)
                
                st.info(f"기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                successful_data = {}
                failed_list = []
                
                # 데이터 다운로드
                for i, symbol in enumerate(selected_symbols):
                    df = fetch_stock_data(symbol, start_date, end_date)
                    
                    if df is not None:
                        successful_data[symbol] = df
                        st.success(f"✓ {symbol}: {len(df)}행")
                    else:
                        failed_list.append(symbol)
                        st.error(f"✗ {symbol}: 실패")
                    
                    # 진행 상황
                    progress = (i + 1) / len(selected_symbols)
                    progress_bar.progress(progress)
                    status_text.text(f"{symbol} 처리 중... ({i+1}/{len(selected_symbols)})")
                    
                    # Rate limit
                    if (i + 1) % 10 == 0:
                        time.sleep(1)
                    else:
                        time.sleep(0.1)
                
                # 세션에 저장
                st.session_state.stock_data = successful_data
                
                # 결과
                st.success(f"✅ 완료! 성공: {len(successful_data)}, 실패: {len(failed_list)}")
                
                # ZIP 다운로드
                if successful_data:
                    zip_buffer = create_download_zip(successful_data)
                    
                    st.download_button(
                        label="💾 전체 데이터 ZIP 다운로드",
                        data=zip_buffer,
                        file_name=f"stock_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                        mime="application/zip"
                    )
                
                progress_bar.empty()
                status_text.empty()
            else:
                st.warning("다운로드할 종목을 선택하세요!")
    else:
        st.warning("먼저 Step 2에서 티커 정보를 수집하세요!")

with tab4:
    st.header("API 모드")
    st.markdown("""
    ### 로컬 앱에서 사용하기
    
    ```python
    import requests
    import pandas as pd
    
    # Streamlit Cloud URL (배포 후 변경)
    app_url = "https://your-app.streamlit.app"
    
    # 1. 티커 리스트 다운로드
    response = requests.get(f"{app_url}/api/tickers")
    tickers = response.json()
    
    # 2. 주식 데이터 다운로드
    params = {
        "symbols": "AAPL,MSFT,GOOGL",
        "years": 5
    }
    response = requests.get(f"{app_url}/api/stock-data", params=params)
    data = response.json()
    ```
    
    ### 배포 방법
    1. GitHub에 이 파일과 requirements.txt 업로드
    2. [Streamlit Cloud](https://streamlit.io/cloud)에서 배포
    3. 배포된 URL을 로컬 앱에서 사용
    """)
    
    # API 테스트
    if st.button("API 응답 테스트"):
        test_data = {
            "status": "ready",
            "message": "API mode is available",
            "endpoints": [
                "/api/tickers",
                "/api/ticker-info",
                "/api/stock-data"
            ]
        }
        st.json(test_data)

# 하단 정보
st.markdown("---")
st.markdown("""
### 📌 사용 순서
1. **티커 다운로드**: NASDAQ 전체 티커 리스트 다운로드
2. **티커 정보 수집**: 시가총액, 가격, 거래량 정보 수집 및 필터링
3. **주식 데이터 다운로드**: 필터링된 종목의 과거 데이터 다운로드

### 💡 팁
- 샘플 모드로 먼저 테스트 후 전체 실행 권장
- Rate limit 방지를 위해 자동으로 대기 시간 적용
- 다운로드한 nasdaq_data.csv는 로컬 앱에서 바로 사용 가능
""")