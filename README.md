# Витрины для рекомендательной системы и аналитики (Pyspark, DataLake, HDFS, parquet)

Полный пайплайн для создания витрины данных для системы рекомендаций кандидатов в друзья в социальной сети на основе местоположения и активности пользователя.

## Общее описание

Витрина для рекомендательной системы будет использоваться для предложения друзей на основе активности пользователей. Приложение будет предлагать пользователю написать человеку, если пользователь и кандидат в друзья:
 - состоят в одном канале,
 - до этого не переписывались,
 - находятся на расстоянии не более 1 км друг от друга.
Витрины для аналитики должны будут помочь изучить аудиторию соцсети, чтобы в будущем запустить монетизацию. Они должны дать возможность получить следующие показатели:
 - Город наибольший по количеству сообщений, лайков и подписок.
 - В каком городе регистрируется больше всего новых пользователей.
 - Определить домашние населенные пункты, а также в какие места и как часто путешествуют пользователи.

На основе аналитики предполагается реализовать таргетированную рекламу, учитывать местонахождение пользователя.


## 
### Структура репозитория
Внутри `src` расположены две папки:
- `/src/dags` — папка с Airflow дагами реализующими выполнение пайплайна 
- `/src/scripts` — папка со скриптами, содержащими вспомогательные инструменты и  логику реализации этапов пайплайна.

### Описание дагов
- recsys_first_city_dag — объединяет данные из таблицы событий и справочника населенных пунктов, находя ближайший к месту события пункт и сохраняет итоговый результат в HDFS-хранилище.

### Порядок запуска дагов
- сначала запускаем first_dag_sprint7_city_dag
- потом остальные два в любом порядке

### Структура HDFS хранилища
- В папке /user/iasivkov/data/geo лежит parquet-файл events, содержит информацию о событиях с координатами
(stage слой, обновляется с некоторой периодичностью) 
- В папке /user/iasivkov/snapshots/geo лежит parquet-файл geo_t, таблица с координатами городов
- В папке /user/iasivkov/data/dds с помощью first_dag_sprint7_city_dag создаётся parquet-файл events_city, содержит информацию о событиях и городе
(dds-слой, обновляется раз в день)
- В папке /user/iasivkov/data/analytics лежат parquet-файлы dm_region, dm_user, dm_rec_sys  
с витринами по регионам, пользователям и рекомендациям друзей, создаются первые два с помощью sprint7_user_dag и последний — sprint7_region_dag
(первые два обновляются раз в день: в 9:00 и 12:00 соответственно и последний раз в неделю, в понедельник в 00:00)
