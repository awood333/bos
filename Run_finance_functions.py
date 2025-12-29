'''Run_finance_functions.py'''

from finance_functions.finance_basics import FinanceBasics
from finance_functions.capex.capex_projects import CapexProjects
from finance_functions.capex.capex_basics import CapexBasics

from finance_functions.income.milk_income import MilkIncome
from finance_functions.income.income_statement import IncomeStatement
from finance_functions.net_revenue.discard.net_rev_this_lactation_WB import NetRevThisLactation_WB
from finance_functions.net_revenue.discard.net_rev_this_lactation_model import NetRevThisLactation_model

from finance_functions.PL.NetRevenue import NetRevenue
from finance_functions.tax_docs.tax_docs_depreciation import TaxDocs_Depreciation
from finance_functions.tax_docs.tax_docs_non_capex import TaxDocs_NonCapex
from finance_functions.PL.cow_PL import CowPL



class RunFinanceFunctions:
    def __init__(self):
        
        
        self.finance_basics     = FinanceBasics()
        self.capex_basics       = CapexBasics()
        self.capex_projects     = CapexProjects()
        self.milk_income        = MilkIncome()
        self.income_statement   = IncomeStatement()
        self.net_revenue        = NetRevenue()
        self.nrtl_WB            = NetRevThisLactation_WB()
        self.nrtl_model         = NetRevThisLactation_model()
        self.cow_pl             = CowPL()
        
        self.tax_docs_depreciation  = TaxDocs_Depreciation()
        self.tax_docs_noncapex      = TaxDocs_NonCapex()


        # Call processing methods
        self.finance_basics.load_and_process()
        self.capex_basics.load_and_process()
        self.capex_projects.load_and_process()
        self.milk_income.load_and_process()

        self.income_statement.load_and_process()
        self.net_revenue.load_and_process()
        self.nrtl_WB.load_and_process()
        self.nrtl_model.load_and_process()
        self.cow_pl.load_and_process()
        
        # self.tax_docs_depreciation.load_and_process()   uncomment after revising script
        # self.tax_docs_noncapex.load_and_process()


if __name__ == "__main__":
    obj=RunFinanceFunctions()
    
  