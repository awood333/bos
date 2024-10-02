'''finance\\milk_income.py'''

import pandas as pd

class MilkIncome:
    def __init__(self):
        self.data = self.GetData()
        self.income = self.calcMilkIncome()
        
    def GetData(self):        
        
        income = pd.read_csv('F:\\COWS\\data\\PL_data\\milk_income\\milk_income_base.csv')
        income['datex'] = pd.to_datetime(income['datex'])
        
        numcols = ['liters','gross_baht','tax','other']
        
        for col in numcols:
            income[col] = income[col].astype(float)
            
        self.data = {'income': income}
        return self.data
        
    def calcMilkIncome(self):

        income = self.data['income']

        income['net']       = income['gross_baht'] - income['tax'] - income['other']
        income['/liter']    = income['net'] / income['liters']
        income['bonus']     =  income['/liter']  - income['base']
        income['bonus value'] = income['bonus'] * income['liters']

        return self.income

