Word Count Kafka Streams App in Kubernetes

This application extends Word count application by running 2 instances of it and exposing it to be used for interactive queries.

## Prerequisites

* Minikube
* Skaffold

## Running the application

After prerequisites are installed, run the following command from the root of the project

```shell script
cd dependencies
./install.sh
```

This will install Strimzi Kafka Operator and starts a Kafka Cluster

Use Kubectl or K9s to make sure dependencies pods are up. 

```
kubectl get pods
```
The output should be something similar.
```
strimzi-cluster-operator-54565f8c56-qnbpq   1/1     Running   0          9h
word-entity-operator-7d74c6d856-gjz7s       3/3     Running   0          9h
word-kafka-0                                2/2     Running   0          9h
word-zookeeper-0
```

Now run the word count app using the following command

```shell script
cd ..
skaffold dev -f skaffold.yml
```

It should start 2 pods of the application.