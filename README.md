# automation-new-instruments

Курсовая работа. Бурков Илья

В данном проекте представлены следующие файлы:

Скрипт на Python, который запрашивает все инструменты с биржи, и далее сохраняет их в файл, при следующем запуске повторяет действие. Далее смотрит на отличия, и если поменялась информация о новых инструментах то присылает сообщение в слак. Команда запуска `python3 ./scripts/listings.py --channel "channel_name"` Так как код работает со слаком, я добавил флаг `--test`, который позволит проверяющим запустить код, без взаимодействия со Slack.

Основной код находится в папке funds_controller, и написан на с++. Код работает с СУБД ClickHouse. С помощью него можно выполнить множество команд, например взять/отдать займ, открыть/закрыть хедж, перевести средства/займы между субаккаунтами.
Код использует другие файлы, которых нет в проекте, данные файлы не относятся к проекту и не играют ключевой роли в реализации логики. 

Примеры таблиц:
```
CREATE TABLE LOANS_INFO_v2 (
    id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime64,
    subaccount LowCardinality(String),
    asset LowCardinality(String),
    amount Decimal(21, 12),
    initial_subaccount LowCardinality(String),
    type LowCardinality(String),
    loan_id String,
    status LowCardinality(String),
) ENGINE = SummingMergeTree(amount)
ORDER BY (subaccount, loan_id, asset); 

CREATE TABLE TRANSACTIONS_v1 (
    id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime64,
    from_subaccount String,
    from_wallet String,
    to_subaccount String,
    to_wallet String,
    asset String,
    amount Decimal(21, 12),
    type String,
    inner_id String,
    status String,
) ENGINE = MergeTree()
ORDER BY (timestamp);
```

Всего таблиц 6
