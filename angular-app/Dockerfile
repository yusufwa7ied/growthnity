# Use Node.js for building Angular
FROM node:20-alpine AS build

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies (allow legacy peer deps to avoid ERESOLVE in build)
RUN npm ci --legacy-peer-deps

# Copy source code
COPY . .

# Build Angular app for production
RUN npm run build

# Use Nginx to serve the built app
FROM nginx:alpine

# Copy built app from build stage
COPY --from=build /app/dist/angular-app/browser /usr/share/nginx/html

# Copy nginx configuration
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Expose port
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
