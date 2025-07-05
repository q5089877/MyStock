
"""
股票推薦系統（重構版）
======================
* 將所有數值參數抽出成全域變數，方便日後調整
* 保留原有演算法邏輯不變
* 以中文說明與註解方便維護
* 於過濾流程中記錄每一支股票被排除的原因 (LOG)
"""

import datetime
import pandas as pd
from config import logger
from flask import current_app
from functools import partial
from linebot.models import TextSendMessage
from .strategies import technical, chip
from .utils import is_weekday, df_mask_helper
from .crawlers import get_twse_data, get_tpex_data, get_other_data, get_economic_events

# =============================================================================
# 全域參數（依需求自行調整）
# =============================================================================
# ---- 策略 1 相關 ----
STRAT1_MIN_CLOSE_PRICE = 10                 # 收盤價 > 20
STRAT1_RED_K_RATIO = 0.995                   # 收盤 / 開盤 > 1.01 (紅 K 且實體漲幅 > 1%)
STRAT1_BREAK_HIGH_RATIO = 1.00              # 今日收盤 > 昨日最高 * 1.00
STRAT1_K9_DIFF_THRESHOLD = 50               # |K9 - D9| < 22
STRAT1_J9_UPPER_LIMIT = 80                 # J9 < 100
STRAT1_TWO_DAY_GAIN_RATIO = 1.005            # 今日收盤 > 昨日收盤 * 1.02 (漲幅 2%)
STRAT1_LIMIT_UP_RATIO = 1.02                # 連續兩日漲幅 5% 以上
STRAT1_UPPER_SHADOW_THRESHOLD = 0.06        # 上影線長度 < 昨收 * 3%
STRAT1_SKYROCKET_N_DAYS = 5               # 飆股判斷區間天數
STRAT1_SKYROCKET_K_CHANGE = 0.12            # 飆股判斷 K 變動幅度
STRAT1_VOLUME_THRESHOLD = 400              # 今日成交量 > 2000
STRAT1_MEAN5_VOLUME_THRESHOLD = 800        # 5 日均量 > 1000
STRAT1_MEAN20_VOLUME_THRESHOLD = 800       # 20 日均量 > 1000

# ---- 策略 2 相關 ----
STRAT2_MIN_CLOSE_PRICE = 20
STRAT2_J9_UPPER_LIMIT = 100
STRAT2_VOLUME_THRESHOLD = 1500

# ---- 策略 3 相關 ----
STRAT3_MIN_CLOSE_PRICE = 20
STRAT3_ONE_DAY_GAIN_RATIO = 1.003
STRAT3_K9_LOWER_LIMIT = 15
STRAT3_VOLUME_THRESHOLD = 200

# =============================================================================
# 主流程
# =============================================================================


def update_and_broadcast(app, target_date=None, need_broadcast=True):
    """
    更新並推播推薦清單
    """
    with app.app_context():
        if not target_date:
            target_date = datetime.date.today()
        logger.info(f"資料日期 {str(target_date)}")
        if not is_weekday(target_date):
            logger.info("假日不進行更新與推播")
            return

        market_data_df = _update_market_data(target_date)
        if market_data_df.shape[0] == 0:
            logger.info("休市不進行更新與推播")
            return

        logger.info("開始更新推薦清單")
        watch_list_df_1 = _update_watch_list(
            market_data_df, _get_strategy_1, strategy_name="策略1"
        )
        watch_list_df_3 = _update_watch_list(
            market_data_df, _get_strategy_3, strategy_name="策略3"
        )
        watch_list_dfs = [watch_list_df_1, watch_list_df_3]
        logger.info("推薦清單更新完成")

        logger.info("開始讀取經濟事件")
        start_date = (target_date + datetime.timedelta(days=1)
                      ).strftime("%Y-%m-%d")
        end_date = (target_date + datetime.timedelta(days=3)
                    ).strftime("%Y-%m-%d")
        economic_events = get_economic_events(start_date, end_date)
        logger.info("經濟事件讀取完成")

        logger.info("開始進行好友推播")
        _broadcast_watch_list(target_date, watch_list_dfs,
                              economic_events, need_broadcast)
        logger.info("好友推播執行完成")


# =============================================================================
# 取得並整合市場資料
# =============================================================================
def _update_market_data(target_date) -> pd.DataFrame:
    """
    下載並合併台股 (TWSE/TPEX) 與其他延伸資料
    """
    twse_df = get_twse_data(target_date)
    tpex_df = get_tpex_data(target_date)
    market_data_df = pd.concat([twse_df, tpex_df])

    if market_data_df.shape[0] == 0:
        return market_data_df

    other_df = get_other_data(target_date)
    market_data_df = pd.merge(
        other_df,
        market_data_df,
        how="left",
        on=["代號", "名稱", "股票類型"],
    ).sort_index()

    # 顯示台積電資料作 sanity check
    logger.info(f"核對 [2330 台積電] {target_date} 交易資訊")
    tsmc = market_data_df.loc["2330"]
    for column, value in tsmc.items():
        if isinstance(value, list) and len(value) > 0:
            logger.info(f"{column}: {value[-1]} (history length={len(value)})")
        else:
            logger.info(f"{column}: {value}")
    return market_data_df


# =============================================================================
# 核心：取得推薦清單並記錄排除原因
# =============================================================================
def _update_watch_list(market_data_df: pd.DataFrame,
                       strategy_func,
                       strategy_name: str,
                       other_funcs=None) -> pd.DataFrame:
    """
    根據指定策略過濾股票，回傳推薦清單並記錄排除原因
    """
    logger.info(f"{strategy_name} | 原始股票數量: {market_data_df.shape[0]}")
    fundamental_mask, technical_mask, chip_mask = strategy_func(market_data_df)

    # 合併所有條件
    combined_mask = fundamental_mask + technical_mask + chip_mask
    watch_list_df = df_mask_helper(market_data_df, combined_mask)

    # 依產業別排序
    watch_list_df = watch_list_df.sort_values(by=["產業別"], ascending=False)

    # 其他自定義篩選 (如需)
    if other_funcs:
        for func in other_funcs:
            watch_list_df = watch_list_df[watch_list_df.index.to_series().apply(
                func)]

    # ---- 產生排除原因 LOG ----
    excluded = market_data_df.index.difference(watch_list_df.index)
    # 建立每個條件對應名稱 (方便閱讀)
    condition_names = [f"條件{i+1}" for i in range(len(combined_mask))]

    for stock_id in excluded:
        failed_conditions = []
        for cond_name, cond_mask in zip(condition_names, combined_mask):
            # 若該股票在該條件下為 False，代表未通過
            if not cond_mask.get(stock_id, True):
                failed_conditions.append(cond_name)
        logger.info(
            f"{strategy_name} | {stock_id} 被排除原因: {', '.join(failed_conditions)}")

    logger.info(f"{strategy_name} | 通過股票數量: {watch_list_df.shape[0]}")
    return watch_list_df


# =============================================================================
# 各策略條件
# =============================================================================
def _get_strategy_1(market_data_df) -> tuple:
    """
    策略 1：成長 + 均線多頭排列 + 飆股潛力
    """
    fundamental_mask = [
        # 營收成長至少其中一項 > 0%
        (market_data_df["(月)營收月增率(%)"] > 0) |
        (market_data_df["(月)營收年增率(%)"] > 0) |
        (market_data_df["(月)累積營收年增率(%)"] > 0),
    ]

    technical_mask = [
        # 收盤價 > STRAT1_MIN_CLOSE_PRICE
        technical.technical_indicator_constant_check_df(
            market_data_df, "收盤", "more", STRAT1_MIN_CLOSE_PRICE, days=1
        ),
        # MA1 > MA5
        technical.technical_indicator_greater_or_less_one_day_check_df(
            market_data_df, "收盤", "mean5", "more", 1, days=1
        ),
        # MA5 > MA20
        technical.technical_indicator_greater_or_less_one_day_check_df(
            market_data_df, "mean5", "mean20", "more", 1, days=1
        ),
        # MA20 > MA60
        technical.technical_indicator_greater_or_less_one_day_check_df(
            market_data_df, "mean20", "mean60", "more", 1, days=1
        ),
        # 收盤價 > STRAT1_RED_K_RATIO * 開盤價
        technical.technical_indicator_greater_or_less_one_day_check_df(
            market_data_df, "收盤", "開盤", "more", STRAT1_RED_K_RATIO, days=1
        ),
        # 今天收盤 > 昨日最高 * STRAT1_BREAK_HIGH_RATIO
        technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "收盤", "最高", "more", STRAT1_BREAK_HIGH_RATIO, days=1
        ),
        # K9 向上
        technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "k9", "k9", "more", 1, days=1
        ),
        # D9 < 90
        technical.technical_indicator_constant_check_df(
            market_data_df, "d9", "less", 90, days=1
        ),
        # |K9 - D9| < STRAT1_K9_DIFF_THRESHOLD
        technical.technical_indicator_difference_one_day_check_df(
            market_data_df, "k9", "d9", STRAT1_K9_DIFF_THRESHOLD, days=1
        ),
        # J9 < STRAT1_J9_UPPER_LIMIT
        technical.technical_indicator_constant_check_df(
            market_data_df, "j9", "less", STRAT1_J9_UPPER_LIMIT, days=1
        ),
        # 今天收盤 > 昨日收盤 * STRAT1_TWO_DAY_GAIN_RATIO
        technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "收盤", "收盤", "more", STRAT1_TWO_DAY_GAIN_RATIO, days=1
        ),
        # 不能連續兩天漲幅都超過 STRAT1_LIMIT_UP_RATIO
        ~technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "收盤", "收盤", "more", STRAT1_LIMIT_UP_RATIO, days=2
        ),
        # 上影線長度 < STRAT1_UPPER_SHADOW_THRESHOLD * 昨收
        technical.technical_indicator_difference_two_day_check_df(
            market_data_df, "最高", "收盤", "less", STRAT1_UPPER_SHADOW_THRESHOLD, "收盤", days=1
        ),
        # 滿足飆股條件
        technical.skyrocket_check_df(
            market_data_df,
            n_days=STRAT1_SKYROCKET_N_DAYS,
            k_change=STRAT1_SKYROCKET_K_CHANGE,
            consecutive_red_no_upper_shadow_days=2,
        ),
    ]

    chip_mask = [
        # 成交量 > STRAT1_VOLUME_THRESHOLD
        technical.volume_greater_check_df(
            market_data_df, shares_threshold=STRAT1_VOLUME_THRESHOLD, days=1
        ),
        # 今天成交量 > 昨天成交量
        technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "volume", "volume", "more", 1, days=1
        ),
        # 今天成交量 > 5 日均量
        technical.technical_indicator_greater_or_less_one_day_check_df(
            market_data_df, "volume", "mean_5_volume", "more", 1, days=1
        ),
        # 5 日均量 > STRAT1_MEAN5_VOLUME_THRESHOLD
        technical.technical_indicator_constant_check_df(
            market_data_df, "mean_5_volume", "more", STRAT1_MEAN5_VOLUME_THRESHOLD, days=1
        ),
        # 20 日均量 > STRAT1_MEAN20_VOLUME_THRESHOLD
        technical.technical_indicator_constant_check_df(
            market_data_df, "mean_20_volume", "more", STRAT1_MEAN20_VOLUME_THRESHOLD, days=1
        ),
        # 「今天 5 日均量」> 「昨天 5 日均量」
        technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "mean_5_volume", "mean_5_volume", "more", 1, days=1
        ),
        # 外資買超 >= 0
        chip.foreign_buy_positive_check_df(market_data_df, threshold=0),
    ]
    return fundamental_mask, technical_mask, chip_mask


def _get_strategy_2(market_data_df) -> tuple:
    """
    策略 2：動能 + 均線多頭
    （僅示範抽參數，演算法維持不變）
    """
    fundamental_mask = [
        (market_data_df["(月)營收月增率(%)"] > 0) |
        (market_data_df["(月)營收年增率(%)"] > 0) |
        (market_data_df["(月)累積營收年增率(%)"] > 0),
    ]

    technical_mask = [
        technical.technical_indicator_constant_check_df(
            market_data_df, "收盤", "more", STRAT2_MIN_CLOSE_PRICE, days=1
        ),
        technical.technical_indicator_greater_or_less_one_day_check_df(
            market_data_df, "收盤", "mean5", "more", 1, days=1
        ),
        technical.technical_indicator_greater_or_less_one_day_check_df(
            market_data_df, "收盤", "mean20", "more", 1, days=1
        ),
        technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "mean60", "mean60", "more", 1, days=1
        ),
        technical.technical_indicator_greater_or_less_one_day_check_df(
            market_data_df, "k9", "d9", "more", 1, days=1
        ),
        technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "j9", "j9", "more", 1, days=1
        ),
        technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "osc", "osc", "more", 1, days=1
        ),
        technical.technical_indicator_constant_check_df(
            market_data_df, "j9", "less", STRAT2_J9_UPPER_LIMIT, days=1
        ),
    ]

    chip_mask = [
        technical.volume_greater_check_df(
            market_data_df, shares_threshold=STRAT2_VOLUME_THRESHOLD, days=1
        ),
        technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "volume", "volume", "more", 1, days=1
        ),
        technical.technical_indicator_greater_or_less_one_day_check_df(
            market_data_df, "volume", "mean_5_volume", "more", 1, days=1
        ),
    ]
    return fundamental_mask, technical_mask, chip_mask


def _get_strategy_3(market_data_df) -> tuple:
    """
    策略 3：回檔後轉強 + 飆股條件
    """
    fundamental_mask = []

    technical_mask = [
        technical.technical_indicator_constant_check_df(
            market_data_df, "收盤", "more", STRAT3_MIN_CLOSE_PRICE, days=1
        ),
        technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "收盤", "收盤", "more", STRAT3_ONE_DAY_GAIN_RATIO, days=1
        ),
        technical.technical_indicator_greater_or_less_one_day_check_df(
            market_data_df, "收盤", "開盤", "more", 1, days=1
        ),
        technical.technical_indicator_greater_or_less_one_day_check_df(
            market_data_df, "收盤", "mean60", "more", 1, days=1
        ),
        technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "mean20", "mean20", "more", 1, days=1
        ),
        technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "mean60", "mean60", "more", 1, days=1
        ),
        # 五天內最低價曾經跌到 MA20 以下
        ~technical.technical_indicator_greater_or_less_one_day_check_df(
            market_data_df, "最低", "mean20", "more", 1, days=5
        ),
        # 昨天下跌
        ~technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "收盤", "收盤", "more", 1, days=2
        ),
        technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "k9", "k9", "more", 1, days=1
        ),
        technical.technical_indicator_constant_check_df(
            market_data_df, "k9", "more", STRAT3_K9_LOWER_LIMIT, days=1
        ),
        technical.skyrocket_check_df(
            market_data_df,
            n_days=STRAT1_SKYROCKET_N_DAYS,  # 與策略1 共用飆股參數
            k_change=STRAT1_SKYROCKET_K_CHANGE,
            consecutive_red_no_upper_shadow_days=0,
        ),
    ]

    chip_mask = [
        technical.volume_greater_check_df(
            market_data_df, shares_threshold=STRAT3_VOLUME_THRESHOLD, days=1
        ),
        technical.technical_indicator_greater_or_less_two_day_check_df(
            market_data_df, "volume", "volume", "less", 1, days=1
        ),
        chip.foreign_buy_positive_check_df(market_data_df, threshold=0),
    ]
    return fundamental_mask, technical_mask, chip_mask


# =============================================================================
# 推播訊息組裝
# =============================================================================
def _broadcast_watch_list(target_date, watch_list_dfs, economic_events, need_broadcast):
    """
    整理推播文字並以 LINE Bot 推送
    """
    final_recommendation_text = ""
    for i, watch_list_df in enumerate(watch_list_dfs):
        if watch_list_df.empty:
            final_recommendation_text += f"🔎 [策略{i+1}] 無推薦股票\n"
            logger.info(f"[策略{i+1}] 無推薦股票")
        else:
            final_recommendation_text += (
                f"🔎 [策略{i+1}] 股票有 {len(watch_list_df)} 檔\n"
                + "\n###########\n\n"
            )
            logger.info(f"[策略{i+1}] 股票有 {len(watch_list_df)} 檔")
            for stock_id, v in watch_list_df.iterrows():
                final_recommendation_text += f"{stock_id} {v['名稱']}  {v['產業別']}\n"
                logger.info(f"{stock_id} {v['名稱']}  {v['產業別']}")
        final_recommendation_text += "\n###########\n\n"

    if economic_events:
        final_recommendation_text += "📆 預計經濟事件\n###########\n\n"
        logger.info("預計經濟事件")
        for event in economic_events:
            final_recommendation_text += f"{event['date']} - {event['country']} - {event['title']}\n"
            logger.info(
                f"{event['date']} - {event['country']} - {event['title']}")
        final_recommendation_text += "\n###########\n\n"

    final_recommendation_text += f"資料來源: 台股 {str(target_date)}"
    final_recommendation_text += f"\nJohnKuo © {current_app.config['YEAR']} ({current_app.config['VERSION']})"

    if need_broadcast:
        line_bot_api = current_app.config['LINE_BOT_API']
        line_bot_api.broadcast(TextSendMessage(text=final_recommendation_text))
