#Echo Park Labs Imagery Demo

Landsat Reader

###Bounding Box Problem
There are some bounding boxes that cross the date line. A hack is to create a maximum `west_lon` value that will force a more complicated query. Same goes for the minimum `east_lon` that forces a complex query. Here's how you can currently check what the maximum `west_lon` is:
```sql
SELECT west_lon
FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index] 
WHERE west_lon = (
SELECT MIN(west_lon)
FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index]
WHERE east_lon < 0
AND west_lon > 0 )
LIMIT 1
```

here's the query for the `east_lon`:
```sql
SELECT east_lon
FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index] 
WHERE east_lon = (
SELECT MAX(east_lon)
FROM [bigquery-public-data:cloud_storage_geo_index.landsat_index]
WHERE east_lon < 0
AND west_lon > 0 )
LIMIT 1
```