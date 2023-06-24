# Service documentation

## Methods
- POST /calculate/top/films
> Вычислить ТОП фильмов на основе среднего рейтинга.
> Результат складывается в отдельную таблицу

- POST /calculate/user/recommendations
> Вычислить рекомендации для пользователя
> Результат складывается в отдельную таблицу

- GET /recommend/user
> Получить рекомендацию для пользователя


## Ответы
- Успешный ответ
```
{
    "status_code":200,
    "text":"OK",
    "data":[fimid1, ..., filmIdN] #или None, если метод не возвращает значения
}
```

- В случае ошибки
```
{
    "status_code":500,
    "text":"Текст ошибки",
    "data":None
}
```