
# Импорт адресов из ГАР БД ФИАС в elasticsearch

Позволяет получить адреса всех домов конкретного региона и сохранить их в БД elasticsearch

На входе - полная разархивированная выгрузка ГАР БД ФИАС с [официального сайта](https://fias-file.nalog.ru/Updates)

На выходе индекс в elastic, следующего содержания:
```json
{
    "_index": "fias",
    "_id": "24722081",
    "_score": 0.0947784,
    "_source": {
        "region": "автономный округ Чукотский",
        "town": "село Омолон",
        "street": "улица Клубная",
        "house": "дом 8Б",
        "extra_house": "корпус 2",
        "leftover": "сельское поселение Омолон",
        "muni": "Муниципальный район Билибинский"
    }
}
```

## Установка и запуск:

1. Перед запуском убедитесь, что проект имеет следующую структуру:

```bash
├── data
│   ├── 16
│   │   ├── AS_ADDR_OBJ_*.XML
│   │   ├── AS_HOUSES_*.XML
│   │   └── AS_MUN_HIERARCHY_*.XML
│   ├── 87
│   │   ├── AS_ADDR_OBJ_*.XML
│   │   ├── AS_HOUSES_*.XML
│   │   └── AS_MUN_HIERARCHY_*.XML
│   ├── AS_ADDHOUSE_TYPES_*.XML
│   ├── AS_ADDR_OBJ_TYPES_*.XML
│   ├── AS_HOUSE_TYPES_*.XML
│   └── AS_OBJECT_LEVELS_*.XML
├── docker-compose.yml
├── fias2es
│   ├── fias_mapping.json
│   ├── fias_parser.py
│   └── upload_elastic.py
├── main.py
├── README.md
└── requirements.txt
```

2. Запустить elastic и kibana

```docker-compose up -d```

3. Установить зависимости

```pip install -r requirements.txt```


4. Разобрать адреса региона и положить их в БД

```python main.py --region_id=87```


## Струтура проекта

- fias_mapping.json - описание индекса для elastic
- fias_parser.py - парсер ГАР БД ФИАС
- upload_elastic.py - загрузчик датафрейма в elastic


## Благодарность

Скрипты написаны на основании [поста на Хабре](https://habr.com/ru/post/595107/), а также следующих проектов:
- https://github.com/nurtdinovadf/garbdfias - Parsing GAR BD FIAS, Python Tutorial

- https://github.com/shigabeev/address-normalizer - Open Address Parser


## Планы по развитию

- автоматический загрузчик обновлений с сайта (сейчас в ручном режиме)
- api для поиска адресов в elastic