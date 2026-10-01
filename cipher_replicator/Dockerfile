#FROM registry.access.redhat.com/rhscl/nodejs-8-rhel7
FROM risingstack/alpine:3.7-v8.10.0-4.8.0
MAINTAINER Avanza Innovations <bilal.mahroof@avanzainnovations.com>
#RUN subscription-manager attach --auto
#RUN yum repolist
#RUN yum repolist enabled
#RUN yum -y groupinstall `Development Tools`
#RUN yum -y module install nodejs:8/development


#RUN  useradd -ms /bin/bash 1001
RUN adduser -S 1001
#WORKDIR /home/avanza
# RUN bash -c "npm --version"
#RUN node -v
#RUN npm -v
#RUN bash -c "mkdir -p /home/1001/app/logs"
#RUN bash -c "mkdir -p /home/1001/app/dist"
WORKDIR /opt/app-root
COPY package.json .
COPY . .
# RUN bash -c "npm install"
RUN npm install
USER 1001
