FROM node:20.9.0-alpine
RUN apk add g++ make py3-pip
WORKDIR /fyp_backend_cron_job
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build:prod
CMD ["npm", "run", "start"]