# Data Marts for Recommendation Systems and Analytics (Pyspark, DataLake, HDFS, parquet)

A complete pipeline for creating a data mart for a friend recommendation system in a social network based on user location and activity.

## General Description

The data mart for the recommendation system will be used to suggest friends based on user activity. The application will suggest that the user message someone if the user and the potential friend:
- Are in the same channel,
- Have not previously messaged each other,
- Are located no more than 1 km from each other.

The analytical data marts will help study the social network audience to enable future monetization. They should provide the ability to gather the following metrics:
- The city with the highest number of messages, likes, and subscriptions,
- The city with the highest number of new user registrations,
- Identification of users' home towns, as well as where and how often they travel.

Based on this analytics, targeted advertising will be implemented, taking into account the user’s location.

## Data Lake Architecture Description
### Repository Structure
Inside the `src` directory, there are two folders:
- `/src/dags` — a folder containing Airflow DAGs that implement the pipeline execution.
- `/src/scripts` — a folder containing scripts with auxiliary tools and logic for implementing the pipeline stages.

### DAGs Description
- **recsys_events_region_dag** — merges data from the events table and the locality reference, finding the closest locality to the event location and saving the final result in the HDFS storage.
- **recsys_region_dag** — generates a data mart with statistics on localities and saves the final result in the HDFS storage.
- **recsys_user_dag** — generates a data mart with user movement statistics and a mart for recommendations.

### DAGs Execution Order
- First, run **recsys_events_region_dag**.
- Then, run the other two in any order.

### Data Mart Descriptions

**User Data Mart**:
- **user_id** — user identifier.
- **current_city** — the current city where the last message was sent from.
- **home_city** — the user’s home locality where they stayed for more than 27 days.
- **travel_count** — the number of visited cities (repeated visits are counted separately).
- **travel_array** — a list of visited cities (sorted by time of visit).

**Localities Data Mart**:
- **month** — the calculation month.
- **week** — the calculation week.
- **zone_id** — locality (city) identifier.
- **week_message** — number of messages per week.
- **week_reaction** — number of reactions per week.
- **week_subscription** — number of subscriptions per week.
- **week_user** — number of registrations per week.
- **month_message** — number of messages per month.
- **month_reaction** — number of reactions per month.
- **month_subscription** — number of subscriptions per month.
- **month_user** — number of registrations per month.

**Recommendations Data Mart**:
- **user_left** — the first user.
- **user_right** — the second user.
- **processed_dttm** — data mart calculation date.
- **zone_id** — locality (city) identifier.
- **local_time** — local time.

### HDFS Storage Structure
- In the folder `/user/iasivkov/data/geo`, there is a parquet file **events**, containing event information with coordinates (stage layer, updated periodically).
- In the folder `/user/iasivkov/snapshots/geo`, there is a parquet file **geo_t**, a table with city coordinates.
- In the folder `/user/iasivkov/data/dds`, the parquet file **events_city** is created using **recsys_events_region_dag**, containing event and city information (dds layer, updated daily).
- In the folder `/user/iasivkov/data/analytics`, there are parquet files **dm_region**, **dm_user**, and **dm_rec_sys** containing data marts for regions, users, and friend recommendations, created by **recsys_region_dag** and **recsys_user_dag** respectively. The first two are updated daily at 9:00 and 12:00, and the last one weekly, on Monday at 00:00.
