SECRET_KEY = "local-student-lakehouse-secret-key-change-for-production"
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False
FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": False}
