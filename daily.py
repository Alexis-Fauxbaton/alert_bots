from news import get_next_week_calendar, create_table
from eco_calendar import main as update_calendar

def main():
    create_table()
    get_next_week_calendar()
    update_calendar()
    
if __name__ == "__main__":
    main()