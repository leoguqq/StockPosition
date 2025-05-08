# -*- coding: UTF-8 -*-

import requests
from datetime import datetime
import pytz
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from typing import List, Dict
import os

# Secrets 配置
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID = os.getenv("DATABASE_ID")
NOTICE_EMAIL_TO = os.getenv("NOTICE_EMAIL_TO")
NOTICE_EMAIL_FROM = os.getenv("NOTICE_EMAIL_FROM")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
REFRESH_TOKEN=os.getenv("REFRESH_TOKEN")

# 配置信息
NOTION_VERSION = '2022-06-28'
PROXY = ""  # "http://代理IP:端口" "http://127.0.0.1:10809"

# 邮件配置
NOTICE_EMAIL_SENDER = 'GIT机器人'
SMTP_SERVER = 'smtp.qq.com'
SMTP_PORT = 465

# API endpoints
IFIND_BASE_URL = 'https://ft.10jqka.com.cn'
TOKEN_URL = f'{IFIND_BASE_URL}/api/v1/get_access_token'
REALTIME_URL = f'{IFIND_BASE_URL}/api/v1/real_time_quotation'

# Notion字段名称配置
LAST_PRICE_NAME = 'Last Price'
HIGH_LINE_NAME = 'High Line'
LOW_LINE_NAME = 'Low Line'

# 时区配置
BEIJING_TZ = pytz.timezone('Asia/Shanghai')

# 全局缓存access_token
ACCESS_TOKEN_CACHE = None


def get_notion_headers():
    """生成Notion请求头"""
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json"
    }


def get_ifind_access_token():
    """获取iFinD access_token（带缓存机制）"""
    global ACCESS_TOKEN_CACHE

    if ACCESS_TOKEN_CACHE:
        return ACCESS_TOKEN_CACHE

    headers = {
        "Content-Type": "application/json",
        "refresh_token": REFRESH_TOKEN
    }

    try:
        response = requests.post(TOKEN_URL, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get('errorcode') != 0:
            raise Exception(f"Token获取失败: {data.get('message')}")

        ACCESS_TOKEN_CACHE = data['data']['access_token']
        return ACCESS_TOKEN_CACHE

    except Exception as e:
        print(f"❌ 获取access_token失败: {str(e)}")
        return None


def query_notion_entries() -> List[Dict]:
    """查询Notion数据库条目（带分页处理）"""
    url = f'https://api.notion.com/v1/databases/{DATABASE_ID}/query'
    entries = []
    has_more = True
    next_cursor = None

    while has_more:
        payload = {"page_size": 100}
        if next_cursor:
            payload["start_cursor"] = next_cursor

        try:
            response = requests.post(
                url,
                headers=get_notion_headers(),
                json=payload,
                timeout=15
            )
            response.raise_for_status()
            data = response.json()

            for page in data.get("results", []):
                props = page.get("properties", {})
                entry = {
                    "page_id": page["id"],
                    "name": parse_title(props.get("Name")),
                    "symbol": parse_rich_text(props.get("简称")),
                    "high_line": parse_number(props.get(HIGH_LINE_NAME)),
                    "low_line": parse_number(props.get(LOW_LINE_NAME))
                }
                entries.append(entry)

            has_more = data.get("has_more", False)
            next_cursor = data.get("next_cursor")

        except requests.exceptions.RequestException as e:
            print(f"Notion API请求失败: {str(e)}")
            break

    return entries


def parse_title(prop: Dict) -> str:
    """解析标题属性（Name字段）"""
    if prop and prop.get("type") == "title":
        return "".join([t["plain_text"] for t in prop["title"]])
    return ""


def parse_rich_text(prop: Dict) -> str:
    """解析富文本属性（简称字段）"""
    if prop and prop.get("type") == "rich_text":
        return "".join([t["plain_text"] for t in prop["rich_text"]])
    return ""


def parse_number(prop: Dict) -> float:
    """解析数字属性"""
    if prop and prop.get("type") == "number":
        return prop["number"]
    return None


def fetch_stock_prices(codes: List[str]) -> Dict[str, float]:
    """通过iFinD HTTP API获取股票最新价格"""
    access_token = get_ifind_access_token()
    if not access_token:
        return {}

    headers = {
        "Content-Type": "application/json",
        "access_token": access_token
    }

    payload = {
        "codes": ",".join(codes),
        "indicators": "latest"
    }

    try:
        response = requests.post(
            REALTIME_URL,
            headers=headers,
            json=payload,
            timeout=15
        )
        response.raise_for_status()
        data = response.json()

        if data.get('errorcode') != 0:
            print(f"❌ iFinD API错误: {data.get('message')}")
            return {}

        # 修正后的数据结构解析
        price_dict = {}
        for item in data.get('tables', []):
            try:
                thscode = item.get('thscode', '')
                latest_list = item.get('table', {}).get('latest', [])

                if not thscode or not latest_list:
                    continue

                # 取最新价格（列表最后一个元素）
                latest_price = float(latest_list[-1])
                price_dict[thscode] = latest_price

            except (KeyError, IndexError, ValueError) as e:
                print(f"⚠️ 解析异常 {thscode}: {str(e)}")
                continue

        return price_dict

    except requests.exceptions.RequestException as e:
        print(f"❌ 请求iFinD API失败: {str(e)}")
        return {}
    except Exception as e:
        print(f"❌ 未知错误: {str(e)}")
        traceback.print_exc()
        return {}

def send_alert_email(action: str, record: Dict, price: float):
    """发送警报邮件（使用symbol显示）"""
    timestamp = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    direction = "向上" if action == "high" else "向下"
    threshold = record["high_line"] if action == "high" else record["low_line"]

    subject = f"{record['name']}{direction}突破"
    content = (
        f"股票 {record['name']}（{record['symbol']}）\n"
        f"北京时间：{timestamp}\n"
        f"当前价格：{round(price, 2)}\n"
        f"{direction}突破： {round(threshold, 2)}"
    )


    msg = MIMEText(content, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = formataddr([NOTICE_EMAIL_SENDER, NOTICE_EMAIL_FROM])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
    msg['To'] = formataddr(["Leo", NOTICE_EMAIL_TO])  # 括号里的对应收件人邮箱昵称、收件人邮箱账号

    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(NOTICE_EMAIL_FROM, SMTP_PASSWORD)
            server.sendmail(NOTICE_EMAIL_FROM, [NOTICE_EMAIL_TO,], msg.as_string())
            server.quit()  # 关闭连接
        return True
    except Exception as e:
        print(f"邮件发送失败: {str(e)}")
        return False


def update_notion_property(page_id: str, last_price: float = None, high: float = None, low: float = None):
    """更新Notion属性"""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    properties = {}

    if last_price is not None:
        properties[LAST_PRICE_NAME] = {"number": round(last_price, 2)}
    if high is not None:
        properties[HIGH_LINE_NAME] = {"number": round(high, 2)}
    if low is not None:
        properties[LOW_LINE_NAME] = {"number": round(low, 2)}

    if not properties:
        return True

    try:
        response = requests.patch(
            url,
            headers=get_notion_headers(),
            json={"properties": properties},
            timeout=10
        )
        response.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"更新Notion失败: {str(e)}")
        return False


def main():
    print("=== 开始股票监控任务 ===")

    # 获取Notion数据
    records = query_notion_entries()
    print(f"获取到 {len(records)} 条股票记录")

    # 构建股票代码列表（使用name字段作为股票代码）
    valid_records = [r for r in records if r["name"]]
    codes = [r["name"] for r in valid_records]

    # 获取实时价格
    prices = fetch_stock_prices(codes)
    if not prices:
        print("❌ 未能获取任何股票价格")
        return

    for record in valid_records:
        code = record['name']
        price = prices.get(code)
        if price is None:
            print(f"⚠️ 未找到 {code} 的价格数据")
            continue

        # 更新Last Price到Notion
        update_success = update_notion_property(
            page_id=record['page_id'],
            last_price=price
        )
        if not update_success:
            print(f"⚠️ 更新{LAST_PRICE_NAME}失败: {code}")
            continue

        # 检查高低线数据完整性
        if None in [record["high_line"], record["low_line"]]:
            print(f"跳过 {code}: 缺失高低线数据")
            continue

        # 判断突破条件
        if price > record["high_line"]:
            print(f"↑ {record['symbol']} 突破高位线")
            if send_alert_email("high", record, price):
                new_high = price * 1.01
                if update_notion_property(record['page_id'], high=new_high):
                    print(f"已更新 {record['symbol']} 高位线至 {new_high:.2f}")
        elif price < record["low_line"]:
            print(f"↓ {record['symbol']} 突破低位线")
            if send_alert_email("low", record, price):
                new_low = price * 0.99
                if update_notion_property(record['page_id'], low=new_low):
                    print(f"已更新 {record['symbol']} 低位线至 {new_low:.2f}")
        else:
            print(f"▬ {record['symbol']} 价格在正常区间")

    print("=== 监控任务完成 ===")


if __name__ == "__main__":
    main()
