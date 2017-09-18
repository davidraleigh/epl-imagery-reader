#Echo Park Labs Imagery Demo

Landsat Reader

## Build, Run and Debug Jupyter Notebook

Based off of this documentation:
https://davidraleigh.github.io/2017/06/27/Debugging-a-Remote-Docker-Container-with-PyCharm.html

```bash
cd ~/gcp-imagery-reader/
sudo docker build -t us.gcr.io/echoparklabs/imagery-reader .
cd ~/remote-debug-docker/
sudo docker tag us.gcr.io/echoparklabs/imagery-reader test-image
sudo docker build -t debug-image .

sudo docker run -p 52022:22 -p 80:8888 -it --privileged --name=temp-python-debug debug-image
```

## Exposing a single Jupyter Notebook using kubernetes
```bash
gcloud container clusters create jupytercluster --machine-type n1-standard-4 --num-nodes 3 --zone us-west1-c --scopes https://www.googleapis.com/auth/projecthosting,storage-rw,bigquery
gcloud container clusters get-credentials jupytercluster --zone us-west1-c
kubectl create -f deploy.yaml
kubectl expose deployment gcp-imagery-deploy --type="LoadBalancer"
kubectl get service gcp-imagery-deploy
```

## Sample

```python

import matplotlib.pyplot as plt
from osgeo import gdal

from urllib.parse import urlparse
from datetime import date
from epl.imagery.reader import MetadataService, Landsat, Storage, SpacecraftID, Metadata

gdal.UseExceptions()
metadataService = MetadataService()

utah_box = (-112.66342163085938, 37.738141282210385, -111.79824829101562, 38.44821130413263)
d_start = date(2016, 7, 20)
d_end = date(2016, 7, 28)


# # bounding_box = (-115.927734375, 34.52466147177172, -78.31054687499999, 44.84029065139799)
rows = metadataService.search(SpacecraftID.LANDSAT_8, start_date=d_start, end_date=d_end, bounding_box=utah_box,
                        limit=10, sql_filters=["cloud_cover<=5"])

base_mount_path = '/imagery'
metadata = Metadata(rows[0], base_mount_path)
gsurl = urlparse(metadata.base_url)
storage = Storage(gsurl[1])

b_mounted = storage.mount_sub_folder(gsurl[2], base_mount_path)
landsat = Landsat(base_mount_path)

band_numbers = [4, 3, 2]
nda = landsat.get_ndarray(band_numbers, metadata)

%matplotlib inline
plt.figure(figsize=[16,16])
plt.imshow(nda)

```

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