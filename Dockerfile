ARG NODE_VERSION=18.16.0
ARG NODE_VERSION_SHORT=18

FROM node:${NODE_VERSION}-bullseye-slim AS builder

# Build
WORKDIR /usr/src/app
COPY . .
RUN yarn && yarn build

FROM gcr.io/distroless/nodejs:${NODE_VERSION_SHORT}
WORKDIR /usr/src/app

# Add shell
COPY --from=busybox:1.35.0-uclibc /bin/sh /bin/sh
COPY --from=busybox:1.35.0-uclibc /bin/addgroup /bin/addgroup
COPY --from=busybox:1.35.0-uclibc /bin/adduser /bin/adduser
COPY --from=busybox:1.35.0-uclibc /bin/chown /bin/chown

# Create user
RUN addgroup -g 1000 node \
  && adduser -u 1000 -G node -s /bin/sh -D node
RUN chown -R node ./
USER node

# Copy build files
COPY --from=builder --chown=node /usr/src/app/lib ./lib
COPY --from=builder --chown=node /usr/src/app/node_modules ./node_modules
COPY --from=builder --chown=node /usr/src/app/docs ./docs

EXPOSE 3000
CMD ["./lib/index.js"]
