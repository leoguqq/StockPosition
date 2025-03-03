import yfinance as yf
import requests
import pandas as pd
import traceback
import os


# === 配置区 ===
NOTION_VERSION = '2022-06-28'
PROXY = ""  # "http://代理IP:端口" "http://127.0.0.1:10809"

# Secrets 配置
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID = os.getenv("DATABASE_ID")

# Notion字段名称配置
LAST_PRICE_NAME = 'Last Price'
USD_PRICE_NAME = 'USD Price'
SHARES_NAME = '股数'
ASSETS_NAME = '资产$'
RATIO_NAME = '比例'
SHORT_NAME_PROP = '简称'
CURRENCY_PROP = '币种'

# 特殊记录名称
CASH_NAME = '现金'
NET_ASSET_NAME = '净资产'


# 货币映射表（处理特殊代码）
CURRENCY_MAPPER = {
    'CNY': 'USDCNY=X',
    'RMB': 'USDCNY=X',
    'HKD': 'HKDUSD=X',
    'JPY': 'JPYUSD=X',
    'EUR': 'EURUSD=X'
}


# === 工具函数 ===
def get_notion_headers():
    """生成Notion API请求头"""
    return {
        'Notion-Version': NOTION_VERSION,
        'Authorization': f'Bearer {NOTION_API_KEY}',
        'Content-Type': 'application/json'
    }


def validate_stock_code(code):
    """验证股票代码有效性"""
    valid_suffix = ('.HK', '.SS', '.SZ')
    return any(code.endswith(s) for s in valid_suffix) or (len(code) <= 5 and code.isalpha())


# === 数据获取模块 ===
def query_notion_entries():
    """获取数据库所有条目"""
    url = f'https://api.notion.com/v1/databases/{DATABASE_ID}/query'
    entries = []

    try:
        response = requests.post(
            url,
            headers=get_notion_headers(),
            json={"page_size": 100},
            timeout=15
        )
        response.raise_for_status()

        for entry in response.json().get('results', []):
            try:
                prop = entry['properties']
                name = prop['Name']['title'][0]['plain_text'].strip() if prop['Name']['title'] else ''

                entry_data = {
                    'id': entry['id'],
                    'name': name,
                    'is_stock': True,
                    'shares': prop.get(SHARES_NAME, {}).get('number', 0),
                    'current_assets': prop.get(ASSETS_NAME, {}).get('number', 0),
                    'current_ratio': prop.get(RATIO_NAME, {}).get('number', 0),
                    'usd_price': prop.get(USD_PRICE_NAME, {}).get('number', None)
                }

                # 标记特殊记录
                if name in [CASH_NAME, NET_ASSET_NAME]:
                    entry_data['is_stock'] = False

                entries.append(entry_data)
            except Exception as e:
                print(f"⚠️ 解析条目失败: {str(e)}")

        return entries

    except Exception as e:
        print(f"❌ Notion查询失败: {str(e)}")
        return []


def fetch_fx_rates(currencies):
    """获取货币汇率（带缓存和重试）"""
    currencies = list(set([c.upper() for c in currencies if c and c.upper() != 'USD']))
    if not currencies:
        return {}

    fx_pairs = []
    for c in currencies:
        if c in CURRENCY_MAPPER:
            fx_pairs.append(CURRENCY_MAPPER[c])
        else:
            fx_pairs.append(f"{c}USD=X")

    session = requests.Session()
    if PROXY:
        session.proxies = {"http": PROXY, "https": PROXY}

    rates = {}
    try:
        fx_tickers = yf.Tickers(" ".join(fx_pairs), session=session)

        for c, pair in zip(currencies, fx_pairs):
            try:
                ticker = fx_tickers.tickers[pair]
                data = ticker.history(period='1d', interval='1m').tail(1)

                if data.empty:
                    continue

                rate = data['Close'].iloc[-1]
                if 'CNY' in pair or 'RMB' in pair:
                    rates[c] = 1 / rate
                else:
                    rates[c] = rate
            except Exception as e:
                print(f"⚠️ 汇率获取失败 {pair}: {str(e)}")

        rates['USD'] = 1.0
        return rates

    except Exception as e:
        print(f"❌ 汇率获取全局错误: {str(e)}")
        return {}


def fetch_stock_data(stock_codes):
    """获取股票数据（增强容错）"""
    valid_data = {}
    if not stock_codes:
        return valid_data

    session = requests.Session()
    if PROXY:
        session.proxies = {"http": PROXY, "https": PROXY}

    try:
        tickers = yf.Tickers(" ".join(stock_codes), session=session)
        currencies = []
        valid_codes = []

        # 预筛选有效代码
        for code in stock_codes:
            try:
                ticker = tickers.tickers[code]
                info = ticker.info or {}
                currency = info.get('currency', 'USD').upper()
                currencies.append(currency)
                valid_codes.append(code)
            except Exception as e:
                print(f"⏩ 跳过无效代码 {code}: {str(e)}")

        # 获取汇率
        fx_rates = fetch_fx_rates(currencies)

        # 处理有效代码
        for code, currency in zip(valid_codes, currencies):
            try:
                ticker = tickers.tickers[code]
                info = ticker.info or {}

                # 获取价格
                price = None
                for field in ['currentPrice', 'regularMarketPrice', 'previousClose']:
                    price = info.get(field)
                    if price is not None:
                        break

                if price is None:
                    hist = ticker.history(period='1d')
                    price = hist['Close'].iloc[-1] if not hist.empty else None

                if price is None:
                    print(f"⏩ 跳过 {code}: 无价格数据")
                    continue

                # 计算美元价格
                fx_rate = fx_rates.get(currency, 1.0)
                try:
                    usd_price = price * fx_rate
                except TypeError:
                    usd_price = None

                valid_data[code] = {
                    'price': round(float(price), 4),
                    'usd_price': round(usd_price, 4) if usd_price else None,
                    'longName': str(info.get('longName', code))[:200],  # 防止超长
                    'currency': currency[:3]  # 统一为3位代码
                }

            except Exception as e:
                print(f"⚠️ {code} 数据处理异常: {str(e)}")

    except Exception as e:
        print(f"❌ 股票数据获取失败: {str(e)}")

    return valid_data


# === 计算模块 ===
def calculate_assets(entries, stock_data):
    """执行资产计算（带多重保护）"""
    try:
        # 提取关键记录
        stock_entries = [e for e in entries if e['is_stock']]
        cash_entry = next((e for e in entries if e['name'] == CASH_NAME), None)
        net_asset_entry = next((e for e in entries if e['name'] == NET_ASSET_NAME), None)

        # 验证现金记录有效性
        if not cash_entry or not isinstance(cash_entry['current_assets'], (int, float)):
            raise ValueError("现金记录无效或缺失")

        # 现金资产直接读取
        cash_assets = float(cash_entry['current_assets'])
        cash_entry['new_assets'] = cash_assets  # 直接使用现有值

        # 计算股票总资产
        total_stock_assets = 0.0
        for entry in stock_entries:
            code = entry['name']
            entry['new_assets'] = 0.0
            if code in stock_data and isinstance(stock_data[code]['usd_price'], (int, float)):
                shares = float(entry['shares']) if isinstance(entry['shares'], (int, float)) else 0.0
                usd_price = stock_data[code]['usd_price']
                entry['new_assets'] = usd_price * shares
                total_stock_assets += entry['new_assets']

        # 计算净资产总值为现金+股票
        new_net_asset_value = cash_assets + total_stock_assets
        if net_asset_entry:
            net_asset_entry['new_assets'] = new_net_asset_value

        # 计算比例（基于新净值）
        for entry in entries:
            try:
                asset = entry.get('new_assets', 0.0)
                ratio = asset / new_net_asset_value if new_net_asset_value != 0 else 0.0
                entry['new_ratio'] = round(ratio, 4)
            except Exception as e:
                print(f"⚠️ {entry['name']} 比例计算失败: {str(e)}")
                entry['new_ratio'] = 0.0

        return entries
    except Exception as e:
        print(f"❌ 资产计算失败: {str(e)}")
        traceback.print_exc()
        return entries


# === 更新模块 ===
def update_notion_properties(page_id, data):
    """更新股票相关属性"""
    try:
        # 数据校验
        if not isinstance(data['price'], (int, float)) or pd.isnull(data['price']):
            raise ValueError("无效价格")
        if not isinstance(data['usd_price'], (int, float)) or pd.isnull(data['usd_price']):
            raise ValueError("无效美元价格")

        properties = {
            LAST_PRICE_NAME: {"number": float(data['price'])},
            USD_PRICE_NAME: {"number": float(data['usd_price'])},
            SHORT_NAME_PROP: {
                "rich_text": [{
                    "type": "text",
                    "text": {"content": data['longName']}
                }]
            },
            CURRENCY_PROP: {
                "select": {"name": data['currency']}
            }
        }

        for _ in range(3):
            try:
                response = requests.patch(
                    f"https://api.notion.com/v1/pages/{page_id}",
                    headers=get_notion_headers(),
                    json={"properties": properties},
                    timeout=20
                )
                response.raise_for_status()
                return True
            except requests.exceptions.RequestException as e:
                print(f"↻ 重试更新 {page_id}: {str(e)}")
        return False

    except Exception as e:
        print(f"⏩ 跳过更新 {page_id}: {str(e)}")
        return False


def update_asset_properties(page_id, assets, ratio):
    """更新资产和比例"""
    try:
        properties = {
            ASSETS_NAME: {"number": round(float(assets), 2)},
            RATIO_NAME: {"number": round(float(ratio), 4)}
        }

        for _ in range(3):
            try:
                response = requests.patch(
                    f"https://api.notion.com/v1/pages/{page_id}",
                    headers=get_notion_headers(),
                    json={"properties": properties},
                    timeout=20
                )
                response.raise_for_status()
                return True
            except requests.exceptions.RequestException as e:
                print(f"↻ 重试更新资产 {page_id}: {str(e)}")
        return False

    except Exception as e:
        print(f"⏩ 跳过资产更新 {page_id}: {str(e)}")
        return False


# === 主程序 ===
def main():
    print("=== 开始同步 ===")

    # 获取Notion数据
    entries = query_notion_entries()
    if not entries:
        print("❌ 未获取到数据库条目")
        return

    # 提取有效股票代码
    stock_codes = [e['name'] for e in entries if e['is_stock'] and validate_stock_code(e['name'])]
    print(f"📋 待处理股票: {', '.join(stock_codes)}")

    # 获取股票数据
    stock_data = fetch_stock_data(stock_codes)

    # 执行资产计算
    entries = calculate_assets(entries, stock_data)

    # 更新Notion
    success = 0
    for entry in entries:
        try:
            # 股票记录更新（价格+资产）
            if entry['is_stock']:
                code = entry['name']
                if code not in stock_data:
                    print(f"⏩ 跳过 {code}: 无数据")
                    continue

                data = stock_data[code]
                price_ok = update_notion_properties(entry['id'], data)
                asset_ok = update_asset_properties(entry['id'], entry['new_assets'], entry['new_ratio'])
                if price_ok and asset_ok:
                    success += 1
                    print(f"🔄 更新 {entry['name']} 数据")

            # 特殊记录更新（仅资产）
            else:
                # 明确处理现金和净资产记录
                if entry['name'] in [CASH_NAME, NET_ASSET_NAME]:
                    asset_ok = update_asset_properties(
                        entry['id'],
                        entry.get('new_assets', 0),
                        entry.get('new_ratio', 0)
                    )
                    if asset_ok:
                        success += 1
                        print(f"🔄 更新 {entry['name']} 数据")

        except Exception as e:
            print(f"⚠️ 记录 {entry['name']} 更新异常: {str(e)}")

    print(f"\n✅ 同步完成: 成功更新 {success}/{len(entries)} 条记录")


if __name__ == "__main__":
    main()
