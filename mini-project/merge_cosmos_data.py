import pandas as pd
from google.colab import files

# === 1. 파일 업로드 ===
uploaded = files.upload()

# === 2. CSV 읽기 ===
assets = pd.read_csv("assets-of-cosmos-fully.csv")
accounts = pd.read_csv("active-accounts-count-of-cosmos-latest.csv")

print("[1] 원본 데이터 크기:")
print(" - assets:", assets.shape)
print(" - accounts:", accounts.shape)
print()

# === 3. ATOM 데이터만 필터링 ===
assets = assets[assets["displayName"].str.upper() == "ATOM"].copy()
print("[2] ATOM 데이터만 필터링 후:", assets.shape)
print(assets.head(3))
print()

# === 4. timestamp를 UNIX 밀리초(ms) 기준으로 변환 ===
assets["timestamp"] = pd.to_datetime(assets["timestamp"], unit="ms")
accounts["timestamp"] = pd.to_datetime(accounts["timestamp"], unit="ms")

print("[3] 변환된 timestamp 샘플:")
print(" - assets:", assets["timestamp"].head(3).tolist())
print(" - accounts:", accounts["timestamp"].head(3).tolist())
print()

# === 5. 날짜 범위 필터링 (2024-11-01 ~ 2025-10-31) ===
start_date = pd.to_datetime("2024-11-01")
end_date = pd.to_datetime("2025-10-31")

assets = assets[(assets["timestamp"] >= start_date) & (assets["timestamp"] <= end_date)]
accounts = accounts[(accounts["timestamp"] >= start_date) & (accounts["timestamp"] <= end_date)]

print("[4] 날짜 범위 필터링 후 크기:")
print(" - assets:", assets.shape)
print(" - accounts:", accounts.shape)
print()

print("[5] assets 날짜 범위:", assets["timestamp"].min(), "~", assets["timestamp"].max())
print("[5] accounts 날짜 범위:", accounts["timestamp"].min(), "~", accounts["timestamp"].max())
print()

# === 6. 컬럼 이름 변경 ===
assets = assets.rename(columns={"price": "atom_price", "value": "value"})
accounts = accounts.rename(columns={"activeAccountCount": "account_value"})

# === 7. 정규화 함수 ===
def normalize(series):
    if series.empty:
        return series
    return (series - series.min()) / (series.max() - series.min())

# === 8. 정규화된 컬럼 추가 ===
assets["atom_norm"] = normalize(assets["atom_price"])
assets["value_norm"] = normalize(assets["value"])
accounts["account_norm"] = normalize(accounts["account_value"])

# === 9. timestamp 기준 병합 ===
merged = pd.merge(assets, accounts, on="timestamp", how="inner")

print("[6] 병합 후 크기:", merged.shape)
print("병합 키 예시 (timestamp):", merged["timestamp"].head(5).tolist() if not merged.empty else "없음")
print()

# === 10. 날짜 포맷 정리 ===
if not merged.empty:
    merged["date"] = merged["timestamp"].dt.strftime("%Y-%m-%d")

# === 11. 필요한 컬럼만 남기기 ===
if not merged.empty:
    merged = merged[["date", "atom_price", "atom_norm", "account_value", "account_norm", "value", "value_norm"]]

# === 12. 결과 확인 ===
print("[7] 최종 병합 결과 (상위 5개):")
display(merged.head() if not merged.empty else "병합 결과가 비어 있음")

# === 13. CSV 저장 및 다운로드 ===
if not merged.empty:
    output_filename = "merged-cosmos-data.csv"
    merged.to_csv(output_filename, index=False)
    files.download(output_filename)
else:
    print("병합 결과가 비어 있어 CSV를 생성하지 않습니다.")
