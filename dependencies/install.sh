#!/bin/bash

SCRIPTPATH="$( cd "$(dirname "$0")" || exit ; pwd -P )"

helm repo add strimzi https://strimzi.io/charts/

if ! helm list -q | grep -q strimzi
then
  echo "Installing Strimzi Kafka Operator Helm chart..."
  helm install --wait strimzi strimzi/strimzi-kafka-operator --version 0.18.0
else
  echo "Strimzi Kafka Operator Helm chart already installed, skipping installation..."
fi

cd "$SCRIPTPATH" || exit 1
skaffold run

echo "Dependency installation complete."