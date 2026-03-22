# CCA-EIA-FAST-API Folder Structure

The project follows a modular and organized folder structure, with clearly separated concerns. Here's an overview of the folder structure:


    cca-eia-fast-api/
    ├── app/                         # Main application directory
    │   ├── __init__.py              # Application initialization (app factory)
    │   ├── controllers/             # HTTP request handlers (routes)
    │   │   ├── __init__.py          # Initialization file for controllers
    │   ├── services/                # Business logic and service layer
    │   │   ├── __init__.py          # Initialization file for services
    │   ├── utils/                   # Utility functions and helper methods
    │   │   ├── __init__.py          # Initialization file for utils
    │   ├── connections/             # Database and all other connection related logic
    │   │   ├── __init__.py          # Initialization file for database handling
    │   ├── models/                  # SQLAlchemy ORM models (database tables)
    │   │   ├── __init__.py          # Initialization file for models
    │   ├── middleware/              # Middleware configuration and setup
    │   │   ├── __init__.py          # different types of middlewares
    │   ├── schemas/                 # Define schema modles using for the application
    │   │   ├── __init__.py          # Initialization file 
    ├── tests/                       # Unit and integration tests
    │   ├── __init__.py              # Initialization file for tests
    ├── requirements.txt             # Python dependencies for the application
    └── run.py                       # Entry point to run the Flask application
    └── .gitignore                   # Git ignore file to exclude files from version control

## Installation

### 1. Clone the Repository

Clone the repository to your local machine:


    git clone https://github.com/AMD-DEAE-CEME/cca-eia-fast-api.git
    cd cca-eia-fast-api

### 2. Create a Virtual Environment

Create a virtual environment to manage project dependencies:

    python3 -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`

### 3. Install Dependencies

Install the required dependencies using requirements.txt:

    pip install -r requirements.txt

## Run file

    uvicorn run:app --reload