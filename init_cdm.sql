CREATE SEQUENCE seq_global_metrics MINVALUE 1;

DROP TABLE IF EXISTS GRIGORJEVDEYANDEXRU.global_metrics CASCADE;
CREATE TABLE GRIGORJEVDEYANDEXRU.global_metrics
(	
	id int not null default(seq_currencies_rate.nextval) primary key,
	date_update datetime not null,
	currency_from int not null,
	amount_total float not null,
	cnt_transactions int not null,
	avg_transactions_per_account float not null,
	cnt_accounts_make_transactions int not null
)
-- сортировка и  сегментация
ORDER BY date_update
SEGMENTED BY hash(date_update) all nodes
PARTITION BY date_update::date;