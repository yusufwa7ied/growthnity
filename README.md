# Performance Tracking System

Full-stack application with Django REST API backend and Angular frontend.

## 🚀 Quick Start with Docker

### Prerequisites
- Docker Desktop installed and running
- Git installed

### Run the entire application

```bash
# Navigate to project directory
cd my_project

# Build and start all services (database, backend, frontend)
docker-compose up --build

# Access the application:
# - Frontend: http://localhost
# - Backend API: http://localhost:8000
# - Django Admin: http://localhost:8000/admin
```

### Stop the application

```bash
# Stop all services
docker-compose down

# Stop and remove all data (including database)
docker-compose down -v
```

---

## 🛠️ Local Development (Without Docker)

### Backend Setup

```bash
cd backend

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run migrations
python manage.py migrate

# Create superuser (optional)
python manage.py createsuperuser

# Run development server
python manage.py runserver
```

### Frontend Setup

```bash
cd angular-app

# Install dependencies
npm install

# Run development server
npm start

# Access at http://localhost:4200
```

---

## 📝 Environment Variables

Copy `.env.example` to `.env` and update values:

```bash
cd backend
cp .env.example .env
# Edit .env with your values
```

### Required Variables:
- `SECRET_KEY` - Django secret key
- `DEBUG` - Set to False in production
- `DB_NAME`, `DB_USER`, `DB_PASSWORD` - Database credentials
- `ALLOWED_HOSTS` - Comma-separated list of allowed domains

---

## 🗄️ Database

### Development
- Uses SQLite by default (db.sqlite3)

### Production
- Uses PostgreSQL
- Set `USE_POSTGRES=True` in .env

---

## 📦 Project Structure

```
my_project/
├── backend/              # Django REST API
│   ├── api/             # Main API app
│   ├── backend/         # Django settings
│   ├── Dockerfile       # Backend container
│   └── requirements.txt # Python dependencies
├── angular-app/         # Angular frontend
│   ├── src/            # Source code
│   ├── Dockerfile      # Frontend container
│   └── nginx.conf      # Nginx configuration
└── docker-compose.yml   # Multi-container setup
```

---

## 🚢 Deployment

### Next Steps:
1. ✅ Project is ready for deployment
2. Push code to GitHub
3. Deploy to AWS (EC2, Elastic Beanstalk, or ECS)

### Before Deploying:
- Generate new SECRET_KEY
- Set DEBUG=False
- Configure proper ALLOWED_HOSTS
- Set up production database
- Configure HTTPS/SSL

---

## 📚 Useful Commands

### Docker Commands
```bash
# View running containers
docker ps

# View logs
docker-compose logs -f

# Rebuild specific service
docker-compose up --build backend

# Run Django commands in container
docker-compose exec backend python manage.py migrate
docker-compose exec backend python manage.py createsuperuser
```

### Django Commands
```bash
# Make migrations
python manage.py makemigrations

# Apply migrations
python manage.py migrate

# Collect static files
python manage.py collectstatic

# Create admin user
python manage.py createsuperuser
```

### Angular Commands
```bash
# Build for production
npm run build

# Run tests
npm test
```
