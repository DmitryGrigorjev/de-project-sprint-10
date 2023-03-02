DROP TABLE IF EXISTS GRIGORJEVDEYANDEXRU__STAGING.currencies CASCADE;
CREATE TABLE GRIGORJEVDEYANDEXRU__STAGING.currencies
(
    date_update datetime,
    currency_code int,
    currency_code_with int,
    currency_with_div float
)
-- сортировка и  сегментация
ORDER BY date_update
SEGMENTED BY date_update::date all nodes
PARTITION BY date_update::date;

DROP TABLE IF EXISTS GRIGORJEVDEYANDEXRU__STAGING.transactions CASCADE;
CREATE TABLE GRIGORJEVDEYANDEXRU__STAGING.transactions
(
    operation_id varchar(255),
    account_number_from int,
    account_number_to int,
    currency_code int,
	country varchar(100),
	status varchar(100),
	transaction_type varchar(100),
	amount int,
	transaction_dt datetime
)
-- сортировка и  сегментация
ORDER BY hash(transaction_dt,operation_id)
SEGMENTED BY hash(transaction_dt,operation_id) all nodes
PARTITION BY hash(transaction_dt,operation_id);

-- да, упустил момент
-- думаю пригодится проекция по датам
CREATE PROJECTION GRIGORJEVDEYANDEXRU__STAGING.transactions_proj_dt as
SELECT
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
	country,
	status,
	transaction_type,
	amount,
	transaction_dt
FROM
    GRIGORJEVDEYANDEXRU__STAGING.transactions
ORDER BY transaction_dt;

-- для инкрементальной загрузки
DROP TABLE IF EXISTS GRIGORJEVDEYANDEXRU__STAGING.transactions_max_date CASCADE;
CREATE TABLE GRIGORJEVDEYANDEXRU__STAGING.transactions_max_date
(
    max_dt date
)