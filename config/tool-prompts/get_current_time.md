返回当前**北京时间**（Asia/Shanghai，UTC+08:00），字段包括 `beijing_iso`（ISO 8601，带 +08:00）、`beijing_date`（日期）、`weekday`（星期）与 `timezone`。
兼容字段 `utc_iso` / `utc_date` 是**真实 UTC**（`utc_compat=true`），不要把它当作北京时间使用。
无参数、只读；不查询用户本地时区、天气或日程。回答"现在几点/今天几号"时一律按北京时间的返回值表述。
