CREATE SEQUENCE seq_currencies MINVALUE 1;

DROP TABLE IF EXISTS GRIGORJEVDEYANDEXRU__DWH.h_currencies CASCADE;
CREATE TABLE GRIGORJEVDEYANDEXRU__DWH.h_currencies
(
	hk_currencies int not null default(seq_currencies.nextval) primary key,
    currency_code_stg int not null,
    currency_code varchar(100) not null,
	currency_name varchar(100) not null
);

CREATE SEQUENCE seq_currencies_rate MINVALUE 1;

DROP TABLE IF EXISTS GRIGORJEVDEYANDEXRU__DWH.l_currencies_rate CASCADE;
CREATE TABLE GRIGORJEVDEYANDEXRU__DWH.l_currencies_rate
(	
	hk_currencies_rate int not null default(seq_currencies_rate.nextval) primary key,
	lk_currencies_main int not null,
    lk_currencies_with int not null,
	currencies_div float not null,
	dt datetime not null
)
-- сортировка и  сегментация
ORDER BY dt
SEGMENTED BY dt all nodes
PARTITION BY dt::date;

ALTER TABLE GRIGORJEVDEYANDEXRU__DWH.l_currencies_rate ADD CONSTRAINT l_currencies_rate_main_FK FOREIGN KEY (lk_currencies_main) REFERENCES GRIGORJEVDEYANDEXRU__DWH.h_currencies(hk_currencies);
ALTER TABLE GRIGORJEVDEYANDEXRU__DWH.l_currencies_rate ADD CONSTRAINT l_currencies_rate_with_FK FOREIGN KEY (lk_currencies_with) REFERENCES GRIGORJEVDEYANDEXRU__DWH.h_currencies(hk_currencies);

CREATE SEQUENCE seq_accounts MINVALUE 1;
CREATE SEQUENCE seq_country MINVALUE 1;
CREATE SEQUENCE seq_status MINVALUE 1;
CREATE SEQUENCE seq_transaction_type MINVALUE 1;

DROP TABLE IF EXISTS GRIGORJEVDEYANDEXRU__DWH.h_accounts CASCADE;
CREATE TABLE GRIGORJEVDEYANDEXRU__DWH.h_accounts
(	hk_account_number int not null default(seq_accounts.nextval) primary key,
	account_number_stg int not null,
	account_data varchar(255) not null
);

DROP TABLE IF EXISTS GRIGORJEVDEYANDEXRU__DWH.h_country CASCADE;
CREATE TABLE GRIGORJEVDEYANDEXRU__DWH.h_country
(	hk_country int not null default(seq_country.nextval) primary key,
	country_stg varchar(255) not null
);

DROP TABLE IF EXISTS GRIGORJEVDEYANDEXRU__DWH.h_status CASCADE;
CREATE TABLE GRIGORJEVDEYANDEXRU__DWH.h_status
(	hk_status int not null default(seq_status.nextval) primary key,
	status_stg varchar(255) not null
);

DROP TABLE IF EXISTS GRIGORJEVDEYANDEXRU__DWH.h_transaction_type CASCADE;
CREATE TABLE GRIGORJEVDEYANDEXRU__DWH.h_transaction_type
(	hk_transaction_type int not null default(seq_transaction_type.nextval) primary key,
	transaction_type_stg varchar(255) not null
);

CREATE SEQUENCE seq_transactions MINVALUE 1;

DROP TABLE IF EXISTS GRIGORJEVDEYANDEXRU__DWH.h_transactions CASCADE;
CREATE TABLE GRIGORJEVDEYANDEXRU__DWH.h_transactions
(
    hk_transactions int not null default(seq_transactions.nextval) primary key,
	operation_id varchar(255) not null,
    lk_account_from int not null,
    lk_account_to int not null,
    lk_currencies int not null,
	lk_country int not null,
	lk_status int not null,
	lk_transaction_type int not null,
	amount int not null,
	transaction_dt datetime
)
-- сортировка и  сегментация
ORDER BY transaction_dt
SEGMENTED BY hash(transaction_dt) all nodes
PARTITION BY transaction_dt::date;

ALTER TABLE GRIGORJEVDEYANDEXRU__DWH.h_transactions ADD CONSTRAINT l_account_FK FOREIGN KEY (lk_account_from) REFERENCES GRIGORJEVDEYANDEXRU__DWH.h_accounts(hk_account_number);
ALTER TABLE GRIGORJEVDEYANDEXRU__DWH.h_transactions ADD CONSTRAINT l_account_to_FK FOREIGN KEY (lk_account_to) REFERENCES GRIGORJEVDEYANDEXRU__DWH.h_accounts(hk_account_number);
ALTER TABLE GRIGORJEVDEYANDEXRU__DWH.h_transactions ADD CONSTRAINT l_country_FK FOREIGN KEY (lk_country) REFERENCES GRIGORJEVDEYANDEXRU__DWH.h_country(hk_country);
ALTER TABLE GRIGORJEVDEYANDEXRU__DWH.h_transactions ADD CONSTRAINT l_status_FK FOREIGN KEY (lk_status) REFERENCES GRIGORJEVDEYANDEXRU__DWH.h_status(hk_status);
ALTER TABLE GRIGORJEVDEYANDEXRU__DWH.h_transactions ADD CONSTRAINT l_transaction_type_FK FOREIGN KEY (lk_transaction_type) REFERENCES GRIGORJEVDEYANDEXRU__DWH.h_transaction_type(hk_transaction_type);
ALTER TABLE GRIGORJEVDEYANDEXRU__DWH.h_transactions ADD CONSTRAINT l_currency_FK FOREIGN KEY (lk_currencies) REFERENCES GRIGORJEVDEYANDEXRU__DWH.h_currencies(hk_currencies);