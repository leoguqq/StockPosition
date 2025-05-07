import requests
import pandas as pd
import traceback
from iFinDPy import *
import os


# === 配置区 ===
NOTION_VERSION = '2022-06-28'
PROXY = ""  # "http://代理IP:端口" "http://127.0.0.1:10809"

# Secrets 配置
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID = os.getenv("DATABASE_ID")
IFIND_USERNAME = os.getenv("IFIND_USERNAME")
IFIND_PASSWORD = os.getenv("IFIND_PASSWORD")


# Notion字段名称配置
LAST_PRICE_NAME = 'Last Price'
USD_PRICE_NAME = 'USD Price'
SHARES_NAME = '股数'
ASSETS_NAME = '资产$'
RATIO_NAME = '比例'
SHORT_NAME_PROP = '简称'
CURRENCY_NAME = '币种'



# 特殊记录名称
CASH_NAME = '现金'
NET_ASSET_NAME = '净资产'

# 代理配置（可选）
# PROXY = ""  # "http://代理IP:端口"

# 货币映射表（iFinD格式）
CURRENCY_MAPPER = {
    'CNY': 'USDCNY.FX',
    'RMB': 'USDCNY.FX',
    'HKD': 'HKDUSD.FX',
    'JPY': 'JPYUSD.FX',
    'EUR': 'EURUSD.FX'
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
    """验证股票代码有效性（新增后缀支持）"""
    valid_suffix = ('.HK', '.SZ', '.SH', '.O', '.N', '.T')
    return any(code.endswith(s) for s in valid_suffix) or (len(code) <= 5 and code.isalpha())

def determine_currency(code):
    """根据股票代码后缀确定币种"""
    if code.endswith('.HK'):
        return 'HKD'
    elif code.endswith(('.SZ', '.SH')):
        return 'CNY'
    elif code.endswith(('.O', '.N')):
        return 'USD'
    elif code.endswith('.T'):
        return 'JPY'
    else:
        return 'USD'  # 默认情况

# === 数据获取模块 ===
def query_notion_entries():
    """获取数据库所有条目（移除币种字段）"""
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
                }

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
    """获取货币汇率（使用iFinD接口）"""
    currencies = list(set([c.upper() for c in currencies if c and c.upper() != 'USD']))
    if not currencies:
        return {}

    # 构造iFinD请求代码
    fx_pairs = []
    for c in currencies:
        if c in CURRENCY_MAPPER:
            fx_pairs.append(CURRENCY_MAPPER[c])
        else:
            fx_pairs.append(f"{c}USD.FX")

    rates = {}
    try:
        # 发送批量请求
        response = THS_RQ(",".join(fx_pairs), "lastest_price")
        if response.errorcode != 0:
            print(f"❌ 汇率获取失败: {response.errmsg}")
            return {}

        # 构建汇率字典
        fx_data = dict(zip(response.thscode, response.data['lastest_price']))

        # 处理每个货币
        for c in currencies:
            pair = CURRENCY_MAPPER.get(c, f"{c}USD.FX")
            if pair in fx_data:
                rate = float(fx_data[pair])
                # 处理需要反向的汇率
                if pair.startswith('USD') and c in ['CNY', 'RMB']:
                    rates[c] = 1 / rate
                else:
                    rates[c] = rate

        rates['USD'] = 1.0
        return rates

    except Exception as e:
        print(f"❌ 汇率获取异常: {str(e)}")
        return {}

def fetch_stock_data(stock_codes):
    """获取股票数据（使用iFinD接口）"""
    valid_data = {}
    if not stock_codes:
        return valid_data

    try:
        # 批量获取最新价格
        response = THS_RQ(",".join(stock_codes), "latest")
        if response.errorcode != 0:
            print(f"❌ 股票数据获取失败: {response.errmsg}")
            return {}

        # 解析响应数据
        for idx, code in enumerate(response.thscode):
            price = float(response.data['latest'][idx])
            valid_data[code] = {
                'price': round(price, 4),
                'longName': code  # 名称字段需要其他接口获取，暂用代码代替
            }

        return valid_data

    except Exception as e:
        print(f"❌ 股票数据获取异常: {str(e)}")
        return {}

# === 计算模块 ===
def calculate_assets(entries, stock_data, fx_rates):
    """执行资产计算"""
    try:
        # 分离不同类型记录
        stock_entries = [e for e in entries if e['is_stock']]
        cash_entry = next((e for e in entries if e['name'] == CASH_NAME), None)
        net_asset_entry = next((e for e in entries if e['name'] == NET_ASSET_NAME), None)

        # 验证现金记录
        if not cash_entry or not isinstance(cash_entry['current_assets'], (int, float)):
            raise ValueError("现金记录无效或缺失")

        # 初始化资产值
        cash_assets = float(cash_entry['current_assets'])
        cash_entry['new_assets'] = cash_assets
        total_stock_assets = 0.0

        # 计算股票资产
        for entry in stock_entries:
            code = entry['name']
            entry['new_assets'] = 0.0
            if code in stock_data:
                stock_info = stock_data[code]
                shares = float(entry['shares']) if isinstance(entry['shares'], (int, float)) else 0.0
                currency = entry['currency']  # 从预处理过的条目获取币种

                # 计算美元价格
                fx_rate = fx_rates.get(currency, 1.0)
                usd_price = stock_info['price'] * fx_rate

                entry['price'] = stock_info['price']
                entry['usd_price'] = round(usd_price, 4)
                entry['new_assets'] = usd_price * shares
                entry['longName'] = stock_info['longName']

                total_stock_assets += entry['new_assets']

        # 计算净资产
        new_net_value = cash_assets + total_stock_assets
        if net_asset_entry:
            net_asset_entry['new_assets'] = new_net_value

        # 计算比例
        for entry in entries:
            try:
                asset_value = entry.get('new_assets', 0.0)
                entry['new_ratio'] = round(asset_value / new_net_value, 4) if new_net_value != 0 else 0.0
            except Exception as e:
                entry['new_ratio'] = 0.0

        return entries

    except Exception as e:
        print(f"❌ 资产计算失败: {str(e)}")
        traceback.print_exc()
        return entries

# === 更新模块 ===
def update_notion_properties(page_id, data):
    """更新股票属性（增加币种字段更新）"""
    try:
        properties = {
            LAST_PRICE_NAME: {"number": float(data['price'])},
            USD_PRICE_NAME: {"number": float(data['usd_price'])},
            CURRENCY_NAME: {
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

    # iFinD登录
    try:
        ret = THS_iFinDLogin(IFIND_USERNAME, IFIND_PASSWORD)
        if ret != 0:
            print("❌ iFinD登录失败")
            return
        print("✅ iFinD登录成功")
    except Exception as e:
        print(f"❌ iFinD登录异常: {str(e)}")
        return

    # 获取Notion数据
    entries = query_notion_entries()
    if not entries:
        print("❌ 未获取到数据库条目")
        return

    # 处理股票条目并确定币种
    stock_entries = [e for e in entries if e['is_stock']]
    for entry in stock_entries:
        code = entry['name']
        entry['currency'] = determine_currency(code)

    # 准备股票代码
    stock_codes = [e['name'] for e in stock_entries if validate_stock_code(e['name'])]
    print(f"📋 待处理股票: {', '.join(stock_codes)}")

    # 获取汇率数据
    currencies = [entry['currency'] for entry in stock_entries]
    fx_rates = fetch_fx_rates(currencies)

    # 获取股票数据
    stock_data = fetch_stock_data(stock_codes)

    # 计算资产
    entries = calculate_assets(entries, stock_data, fx_rates)

    # 更新Notion
    success = 0
    for entry in entries:
        try:
            if entry['is_stock']:
                code = entry['name']
                if code not in stock_data:
                    continue

                update_data = {
                    'price': entry.get('price', 0),
                    'usd_price': entry.get('usd_price', 0),
                    'currency': entry.get('currency', 'USD')
                }

                if update_notion_properties(entry['id'], update_data) and \
                        update_asset_properties(entry['id'], entry['new_assets'], entry['new_ratio']):
                    success += 1
                    print(f"🔄 更新 {code} 成功")

            elif entry['name'] in [CASH_NAME, NET_ASSET_NAME]:
                if update_asset_properties(entry['id'], entry['new_assets'], entry['new_ratio']):
                    success += 1
                    print(f"🔄 更新 {entry['name']} 成功")

        except Exception as e:
            print(f"⚠️ 更新异常 {entry['name']}: {str(e)}")

    print(f"\n✅ 同步完成: 成功更新 {success}/{len(entries)} 条记录")

if __name__ == "__main__":
    main()
