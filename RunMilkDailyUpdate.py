'''
RunMilkDailyUpdate.py
'''


from rawmilkupdate import RawMilkUpdate, HaltScriptException
from milkaggregates import MilkAggregates

def main():
    try:
        rm = RawMilkUpdate()
        # rm.write_to_csv()
        rm.halt_script()
    except HaltScriptException as e:
        print("\n" + "="*40)
        print("ERROR: Halting Script Execution")
        print("="*40)
        print(f"Reason: {e}")
        print("="*40 + "\n")
        # Proceed to the next module

    ma = MilkAggregates()
    # ma.create_write_to_csv()

if __name__ == "__main__":
    main()
    
