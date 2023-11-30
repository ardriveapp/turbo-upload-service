ARG NODE_VERSION=18.17.0

FROM node:${NODE_VERSION}-bullseye-slim
WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y git
COPY . .
RUN yarn 
CMD yarn db:migrate:latest && yarn test
