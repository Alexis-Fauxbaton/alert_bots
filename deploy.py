# deploy prefect scripts
from daily import main


# run every hour
main.serve(
    name="Daily",
    cron="0 * * * *",
)