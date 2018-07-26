# The base-deps Docker image installs main libraries needed to run Ray

FROM ubuntu:xenial
RUN apt-get update \
    && apt-get install -y \
        git \
        wget \
        cmake \
        pkg-config \
        build-essential \
        autoconf \
        curl \
        libtool \
        unzip \
        flex \
        bison \
    && apt-get clean \
    && apt-get install -y python python-dev python-pip python-virtualenv \
    && rm -rf /var/lib/apt/lists/* \
    && pip install \
        flatbuffers \
        cython==0.27.3 \
        kubernetes \
    && apt-get update \
    && apt-get install -y apt-transport-https \
    && curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
    && touch /etc/apt/sources.list.d/kubernetes.list \
    && echo "deb http://apt.kubernetes.io/ kubernetes-xenial main" | tee -a /etc/apt/sources.list.d/kubernetes.list \
    && apt-get update \
    && apt-get install -y kubectl