''''''
from finance_functions.finance_basics import FinanceBasics
from finance_functions.income.IncomeStatement import IncomeStatement
from finance_functions.income.MilkIncome import MilkIncome
from finance_functions.PL.NetRevenue import NetRevenue

from finance_functions.capex.CapexBasics import CapexBasics
from finance_functions.capex.CapexProjects import CapexProjects

from finance_functions.tax_docs.TaxDocs_NonCapex import TaxDocs_NonCapex
from finance_functions.tax_docs.TaxDocs_Depreciation import TaxDocs_Depreciation


class RunFinanceFunctions:
    def __init__(self):
        
        self.FinanceBasics = FinanceBasics()
        self.CapexBasics = CapexBasics()
        self.CapexProjects = CapexProjects()
        
        self.TaxDocs_Depreciation = TaxDocs_Depreciation()
        self.TaxDocs_NonCapex = TaxDocs_NonCapex()
        
        self.IncomeStatement = IncomeStatement()
        self.MilkIncome = MilkIncome()
        
        self.NetRevenue = NetRevenue()
        

if __name__ == "__main__":
    RunFinanceFunctions()
    