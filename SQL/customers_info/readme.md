# Тестовое задание по SQL

## Описание таблиц:


### tblClients – информация о клиентах.
- ClientID – идентификатор клиента,
- Поле Status может принимать следующие значения: 'Active', 'Updated by client', 'Deleted', 'Passive'

### tblTestClients – содержит клиентов, которые используются для тестирования новых продуктов, этих клиентов следует исключать из статистики
- ClientID – идентификатор клиента

### tblClientBalanceOperation – транзакции покупок клиентов

- BalanceOperationID – идентификатор покупки
- ClientID – идентификатор клиента
- Amount – сумма покупки в $
- SignOfPayment – признак того, что покупка прошла успешно или безуспешно (SignOfPayment=1 – успешная покупка)
- OperationTime - дата транзакции


## Задание:  

Вывести детализацию по клиенту:

a. ID клиента

b. Дата и сумма первой покупки

c. Дата и сумма повторной (следующей после первой) покупки

d. Дата последней покупки

e. Сумма покупок, совершенных в течение месяца после первой покупки

f. Время (кол-во дней) между первой и повторной покупкой

g. Среднее время (кол-во дней) между покупками

