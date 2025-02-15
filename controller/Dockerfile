
FROM python:3.9
WORKDIR /app
COPY namespace_migration_controller.py .
RUN pip install kubernetes
CMD ["python", "namespace_migration_controller.py"]
