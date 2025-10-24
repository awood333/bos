'''Run_finance_functions.py'''
from container import get_dependency
from finance_functions.tax_docs.tax_docs_depreciation import TaxDocs_Depreciation
from finance_functions.tax_docs.tax_docs_non_capex import TaxDocs_NonCapex

class RunFinanceFunctions:
    def __init__(self):
        
        self.feedcost_basics    = get_dependency('feedcost_basics')
        self.status_data        = get_dependency('status_data')
        self.finance_basics     = get_dependency('finance_basics')
        self.capex_basics       = get_dependency('capex_basics')
        self.capex_projects     = get_dependency('capex_projects')
        self.milk_income        = get_dependency('milk_income')
        self.net_revenue        = get_dependency('net_revenue')
        self.income_statement   = get_dependency('income_statement')

        self.tax_docs_depreciation  = TaxDocs_Depreciation()
        self.tax_docs_noncapex      = TaxDocs_NonCapex()

if __name__ == "__main__":
    RunFinanceFunctions()