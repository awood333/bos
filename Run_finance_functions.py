'''Run_finance_functions.py'''
from container import get_dependency, container

class RunFinanceFunctions:
    def __init__(self):
        
        self.feedcost_basics = get_dependency('feedcost_basics')
        self.status_data = get_dependency('status_data')
        self.date_range = get_dependency('date_range')

        self.finance_basics = get_dependency('finance_basics')
        self.capex_basics = get_dependency('capex_basics')
        self.capex_projects = get_dependency('capex_projects')
        self.tax_docs_depreciation = get_dependency('tax_docs_depreciation')
        self.tax_docs_noncapex = get_dependency('tax_docs_noncapex')
        self.milk_income = get_dependency('milk_income')

        self.net_revenue = get_dependency('net_revenue')
        self.income_statement = get_dependency('income_statement')

if __name__ == "__main__":
    RunFinanceFunctions()