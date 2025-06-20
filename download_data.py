# download_tool.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pro Trading Simulator - 데이터 다운로드 도구 (Streamlit Cloud 버전)
ZIP 다운로드 기능 포함
"""

import streamlit as st
import yfinance as yf
import pandas as pd
from pathlib import Path
import time
import zipfile
import io
from datetime import datetime

# 설정
CACHE_DIR = Path("data_cache")
CACHE_DIR.mkdir(exist_ok=True)

st.set_page_config(
    page_title="Trading Simulator 데이터 다운로더",
    page_icon="📊",
    layout="wide"
)

def download_ticker_data(ticker: str, period: str = "8y"):
    """티커 데이터 다운로드"""
    path = CACHE_DIR / f"{ticker}.csv"
    
    # 이미 파일이 있으면 스킵
    if path.exists():
        return True, "이미 다운로드됨"
    
    # 3번 재시도
    for attempt in range(3):
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period=period, auto_adjust=False)
            
            if not df.empty:
                # 컬럼명 정규화
                df.columns = [str(c).capitalize() for c in df.columns]
                
                # CSV로 저장
                df.to_csv(path)
                
                return True, f"{len(df)}개 데이터"
            else:
                if attempt < 2:
                    time.sleep(1)
                    continue
                return False, "데이터 없음"
                
        except Exception as e:
            if attempt < 2:
                time.sleep(1)
                continue
            return False, str(e)
    
    return False, "3번 시도 실패"

def create_download_zip():
    """모든 CSV를 ZIP으로 묶기"""
    zip_buffer = io.BytesIO()
    csv_files = list(CACHE_DIR.glob("*.csv"))
    
    if not csv_files:
        return None
    
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for csv_file in csv_files:
            zipf.write(csv_file, csv_file.name)
    
    return zip_buffer.getvalue()

def main():
    st.title("📊 Trading Simulator 데이터 다운로더")
    st.markdown("### Streamlit Cloud에서 실행 중! (Rate Limit 없음)")
    st.markdown("---")
    
    # 사이드바에 정보 표시
    with st.sidebar:
        st.header("ℹ️ 정보")
        st.info("""
        **Streamlit Cloud 장점:**
        - ✅ Rate Limit 없음
        - ✅ 빠른 다운로드
        - ✅ 안정적인 연결
        """)
        
        # 현재 시간 표시
        st.caption(f"현재 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # MODELBOOK.txt 읽기
    try:
        # Streamlit Cloud에서는 uploaded file로 처리
        modelbook_file = st.file_uploader("MODELBOOK.txt 업로드", type=['txt'])
        
        if modelbook_file is not None:
            content = modelbook_file.read().decode('utf-8')
            all_tickers = [line.strip().upper() for line in content.split('\n') if line.strip()]
            st.success(f"✅ {len(all_tickers)}개 티커 로드 완료!")
        else:
            st.warning("⚠️ MODELBOOK.txt 파일을 업로드해주세요.")
            st.stop()
    except Exception as e:
        st.error(f"❌ 파일 읽기 오류: {e}")
        st.stop()
    
    # 현재 상태
    existing_files = list(CACHE_DIR.glob("*.csv"))
    existing_tickers = [f.stem for f in existing_files]
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("전체 티커", len(all_tickers))
    with col2:
        st.metric("다운로드 완료", len(existing_files))
    with col3:
        st.metric("남은 티커", len(all_tickers) - len(existing_files))
    
    # 다운로드 옵션
    st.markdown("### 📥 다운로드 옵션")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        download_mode = st.selectbox(
            "다운로드 범위",
            ["남은 것만", "테스트 (5개)", "일부 (20개)", "절반 (50개)", "전체"],
            index=0
        )
    
    with col2:
        period = st.selectbox(
            "데이터 기간",
            ["1y", "2y", "5y", "8y", "10y", "max"],
            index=3  # 기본값 8y
        )
    
    with col3:
        batch_size = st.number_input("배치 크기", min_value=1, max_value=50, value=10)
    
    with col4:
        delay = st.number_input("대기 시간(초)", min_value=0.1, max_value=5.0, value=0.5, step=0.1)
    
    # 다운로드할 티커 결정
    if download_mode == "남은 것만":
        selected_tickers = [t for t in all_tickers if t not in existing_tickers]
    elif download_mode == "테스트 (5개)":
        selected_tickers = all_tickers[:5]
    elif download_mode == "일부 (20개)":
        selected_tickers = all_tickers[:20]
    elif download_mode == "절반 (50개)":
        selected_tickers = all_tickers[:50]
    else:
        selected_tickers = all_tickers
    
    # 이미 있는 것 제외
    new_tickers = [t for t in selected_tickers if t not in existing_tickers]
    
    st.info(f"📌 {len(new_tickers)}개 티커를 다운로드합니다.")
    
    # 다운로드 버튼
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if st.button("🚀 다운로드 시작", type="primary", use_container_width=True):
            
            if len(new_tickers) == 0:
                st.warning("다운로드할 티커가 없습니다!")
            else:
                # 진행 상황 표시
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # 배치별로 처리
                total_batches = (len(new_tickers) + batch_size - 1) // batch_size
                
                success_list = []
                fail_list = []
                
                # 로그 영역
                log_container = st.container()
                
                start_time = time.time()
                
                for batch_idx in range(total_batches):
                    batch_start = batch_idx * batch_size
                    batch_end = min(batch_start + batch_size, len(new_tickers))
                    batch = new_tickers[batch_start:batch_end]
                    
                    with log_container:
                        st.markdown(f"**배치 {batch_idx + 1}/{total_batches}**")
                        
                        for i, ticker in enumerate(batch):
                            global_idx = batch_start + i
                            progress = (global_idx + 1) / len(new_tickers)
                            progress_bar.progress(progress)
                            status_text.text(f"다운로드 중: {ticker} ({global_idx + 1}/{len(new_tickers)})")
                            
                            ok, result = download_ticker_data(ticker, period)
                            
                            if ok:
                                success_list.append(ticker)
                                st.success(f"✅ {ticker}: {result}")
                            else:
                                fail_list.append((ticker, result))
                                st.error(f"❌ {ticker}: {result}")
                            
                            # 대기
                            if global_idx < len(new_tickers) - 1:
                                time.sleep(delay)
                
                # 완료
                elapsed = int(time.time() - start_time)
                status_text.text("✨ 다운로드 완료!")
                
                # 결과 요약
                st.markdown("---")
                st.success(f"""
                ### 🎉 다운로드 완료!
                - ✅ 성공: {len(success_list)}개
                - ❌ 실패: {len(fail_list)}개  
                - ⏱️ 소요 시간: {elapsed}초 ({elapsed/60:.1f}분)
                - 📊 평균 속도: {len(new_tickers)/elapsed:.1f}개/초
                """)
                
                # 실패 목록
                if fail_list:
                    with st.expander("❌ 실패한 티커 상세"):
                        for ticker, reason in fail_list:
                            st.write(f"- {ticker}: {reason}")
                
                st.balloons()
    
    with col2:
        # 데이터 초기화 버튼
        if st.button("🗑️ 데이터 초기화", help="모든 다운로드된 데이터 삭제"):
            if st.checkbox("정말로 삭제하시겠습니까?"):
                for f in CACHE_DIR.glob("*.csv"):
                    f.unlink()
                st.rerun()
    
    # ZIP 다운로드 섹션
    st.markdown("---")
    st.markdown("### 💾 데이터 다운로드")
    
    existing_files = list(CACHE_DIR.glob("*.csv"))
    if existing_files:
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"📦 {len(existing_files)}개 CSV 파일 준비됨")
            
            if st.button("📥 ZIP 파일 생성", use_container_width=True):
                with st.spinner("ZIP 파일 생성 중..."):
                    zip_data = create_download_zip()
                    
                if zip_data:
                    st.download_button(
                        label="💾 data_cache.zip 다운로드 (클릭!)",
                        data=zip_data,
                        file_name=f"stock_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip",
                        mime="application/zip",
                        use_container_width=True
                    )
        
        with col2:
            # 다운로드된 티커 목록
            with st.expander("📋 다운로드된 티커 목록"):
                downloaded = sorted([f.stem for f in existing_files])
                # 10개씩 묶어서 표시
                for i in range(0, len(downloaded), 10):
                    st.write(", ".join(downloaded[i:i+10]))
    else:
        st.warning("아직 다운로드된 데이터가 없습니다.")
    
    # 사용 방법
    with st.expander("📖 사용 방법"):
        st.markdown("""
        1. **MODELBOOK.txt 업로드**: 티커 목록이 있는 텍스트 파일
        2. **다운로드 옵션 선택**: 범위, 기간, 배치 크기 등
        3. **다운로드 시작**: 버튼 클릭으로 시작
        4. **ZIP 다운로드**: 완료 후 ZIP 파일로 받기
        
        **💡 팁:**
        - Streamlit Cloud에서는 Rate Limit이 거의 없습니다
        - 배치 크기를 크게 하면 빠르지만 오류 가능성 증가
        - 실패한 티커는 "남은 것만" 옵션으로 재시도
        """)

if __name__ == "__main__":
    main()