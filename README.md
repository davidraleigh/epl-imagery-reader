# Echo Park Labs Imagery Demo

This is a demo for a gRPC-GIS talk I'll be doing at FOSS4G in Tanzania in 2018. This won't work right now as a few things are omitted.

## Building and Running on AWS
Environment variables `ACCESS_KEY_ID` and `SECRET_ACCESS_KEY` need to be set. Instructions for fuse on [s3fs-fuse github](https://github.com/s3fs-fuse)
```bash
docker build \
--rm=true --no-cache --pull=true \
--build-arg ACCESS_KEY_ID=$ACCESS_KEY_ID \
--build-arg SECRET_ACCESS_KEY=$SECRET_ACCESS_KEY \
-t aws-imagery-reader -f AWSDockerfile ./
```

for debugging s3fs:
```bash
s3fs landsat-pds /imagery \
-o passwd_file=/etc/passwd-s3fs -d -d -f -o f2 -o curldbg
```

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

## Development

creating proto
```bash
python -mgrpc_tools.protoc -I=./proto/ --python_out=./epl/service/imagery --grpc_python_out=./epl/service/imagery ./proto/epl_imagery_api.proto
```
