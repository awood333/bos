'''DailyMilkRunner'''

from milk_functions.RawMilkUpdate import RawMilkUpdate

if __name__ == "__main__":
    try:
        raw_milk_update = RawMilkUpdate()
    except HaltScriptException as e:
        print(e)