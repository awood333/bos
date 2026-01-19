# pylint: disable=import-outside-toplevel,redefined-outer-name
'''Dependency Injection Container for Bos application'''
import inspect
from typing import Dict, Any, Optional, Callable, TypeVar, Type
import threading
from datetime import datetime
import networkx as nx
import matplotlib.pyplot as plt

T = TypeVar('T')

class Container:
    """Dependency Injection Container with singleton pattern and lazy loading"""
    
    _instance = None
    _lock = threading.RLock()   #Rlock ( reentrant lock) allows multiple modules to access the same thread
                                #a synchronization primitive used in multithreaded programming to prevent race conditions 
                                # and ensure thread safety when accessing shared resources
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):

        self._dependency_graph = nx.DiGraph()

        if not getattr(self, '_initialized', False):
            print(f"🚀 Container starting up at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self._singletons: Dict[str, Any] = {}
            self._factories: Dict[str, Callable] = {}
            self._transients: Dict[str, Callable] = {}
            self._creating: set = set() 
            self._creation_order = []
            self._lock = threading.RLock()
            self._initialized = True
            self._register_dependencies()
            print(f"✅ Container initialization complete at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")            


    def _register_dependencies(self):
        """Register all dependencies with their creation functions  
            A singleton is a design pattern that ensures a class has only one instance
            throughout the entire application lifecycle. Once created, the same instance is reused every time you ask for it
            When someone asks for 'date_range' use the _create_date_range method to create it
            But only create it ONCE (singleton behavior)
            Lazy Creation - The actual DateRange instance is only created when someone first calls: container.get('date_range)"""     
        # Register core data dependencies
        self.register_singleton('milk_basics', self._create_milk_basics)
        self.register_singleton('date_range', self._create_date_range)
        
        # Status
        self.register_singleton('status_data',          self._create_status_data)
        self.register_singleton('wet_dry',             self._create_wet_dry)
        self.register_singleton('model_groups',        self._create_model_groups)
        self.register_singleton('whiteboard_groups',   self._create_whiteboard_groups)        
        self.register_singleton('model_groups_tenday', self._create_model_groups)        
        
        # Insem
        self.register_singleton('insem_ultra_basics',   self._create_insem_ultra_basics)
        self.register_singleton('insem_ultra_data',     self._create_insem_ultra_data)
        self.register_singleton('check_last_stop',      self._create_check_last_stop)
        self.register_singleton('i_u_merge',            self._create_i_u_merge)
        self.register_singleton('ipiv',                 self._create_ipiv)
        self.register_singleton('next_ultra_check',     self._create_next_ultra_check)
        
        # Feed
        self.register_singleton('feedcost_basics',      self._create_feedcost_basics)
        self.register_singleton('feedcost_beans',       self._create_feedcost_beans)
        self.register_singleton('feedcost_cassava',     self._create_feedcost_cassava)
        self.register_singleton('feedcost_corn',        self._create_feedcost_corn)
        self.register_singleton('feedcost_bypass_fat',  self._create_feedcost_bypass_fat)
        self.register_singleton('feedcost_total',       self._create_feedcost_total)
        self.register_singleton('feedcost_sequences',   self._create_feedcost_sequences)

      
        # milk_functions
        self.register_singleton('milk_aggregates',      self._create_milk_aggregates)
        self.register_singleton('raw_milk_update',      self._create_raw_milk_update)

        #plot functions
        self.register_singleton('plot_net_revenue_model', self._create_plot_net_revenue_model)
        self.register_singleton('run_lactation_plot',     self._create_run_lactation_plot)
        
        #groups and tests
        self.register_singleton('whiteboard_groups',    self._create_whiteboard_groups)     
        self.register_singleton('model_groups',         self._create_model_groups)     
        self.register_singleton('compare_model_whiteboard_groups_last', self._create_compare_model_whiteboard_groups_last)     
        self.register_singleton('wet_dry_groups',       self._create_wet_dry_groups)
     
        # Lactation
        self.register_singleton('lactation_basics',     self._create_lactation_basics)
        self.register_singleton('create_lactations',    self._create_lactations)
        self.register_singleton('this_lactation',       self._create_this_lactation)
        self.register_singleton('weekly_lactations',    self._create_weekly_lactations)
        self.register_singleton('lactations',           self._create_lactations)        
        self.register_singleton('lactations_log_standard',      self._create_lactations_log_standard)
        self.register_singleton('lactation_plots', self._create_lactation_plots)                                          

        # Report Milk
        self.register_singleton('create_report_milk',      self._create_report_milk)
        self.register_singleton('create_report_milk_xlsx', self._create_report_milk_xlsx)        
        self.register_singleton('run_milk_dash_app',       self._create_run_milk_dash_app)        


        # Finance
        self.register_singleton('finance_basics',       self._create_finance_basics)
        self.register_singleton('capex_basics',         self._create_capex_basics)         
        self.register_singleton('capex_projects',       self._create_capex_projects)
        self.register_singleton('income_statement',     self._create_income_statement)        
        self.register_singleton('milk_income',          self._create_milk_income)
        self.register_singleton('cow_pl',               self._create_cow_pl)
        self.register_singleton('depreciation',         self._create_depreciation)        
        self.register_singleton('net_revenue',          self._create_net_revenue)
        # self.register_singleton('net_revenue_by_cow_by_date', self._create_net_revenue_by_cow_by_date)
        self.register_singleton('sahagon',              self._create_sahagon)

        # Report/Dashboard dependencies
        self.register_singleton('report_milk',          self._create_report_milk)
        self.register_singleton('report_milk_xlsx',     self._create_report_milk_xlsx)

    def register_singleton(self, name: str, factory: Callable[[], Any]):
        """Register a singleton dependency"""
        self._factories[name] = factory
    
    def register_transient(self, name: str, factory: Callable[[], Any]):
        """Register a transient dependency (new instance each time)"""
        self._transients[name] = factory
            
    def get(self, name: str) -> Any:
        """Get a dependency by name, ensuring it is fully initialized (including load_and_process)."""
        with self._lock:
            # Return existing singleton if already created
            if name in self._singletons:
                return self._singletons[name]

            # Check if it's a registered singleton factory
            if name in self._factories:
                # Circular dependency detection and dependency graph tracking
                if not hasattr(self, '_creating'):
                    self._creating = set()
                if not hasattr(self, '_creation_order'):
                    self._creation_order = []

                if name in self._creating:
                    print("🚨 CIRCULAR DEPENDENCY DETECTED!")
                    raise RuntimeError(f"Circular dependency: {' → '.join(self._creation_order)} → {name}")

                self._creating.add(name)
                self._creation_order.append(name)

                parent = self._creation_order[-2] if len(self._creation_order) > 1 else None
                if parent:
                    self._dependency_graph.add_edge(parent, name)

                try:
                    instance = self._factories[name]()
                    # Call load_and_process() if it exists and hasn't been called yet
                    load_proc = getattr(instance, "load_and_process", None)
                    if callable(load_proc):
                        # Use a flag to avoid double-calling
                        if not hasattr(instance, "_load_and_process_called"):
                            load_proc()
                            setattr(instance, "_load_and_process_called", True)
                    self._singletons[name] = instance
#dependency graph
                    # self.show_dependency_graph() 
                    
                    
                    return instance
                finally:
                    self._creating.remove(name)
                    self._creation_order.remove(name)
                    print(f"🔚 Finished creating: {name}")

            # Check if it's a transient
            if name in self._transients:
                print(f"Creating transient: {name}")
                instance = self._transients[name]()
                load_proc = getattr(instance, "load_and_process", None)
                if callable(load_proc):
                    if not hasattr(instance, "_load_and_process_called"):
                        load_proc()
                        setattr(instance, "_load_and_process_called", True)
                return instance

            raise ValueError(f"Dependency '{name}' not registered")
        
    def get_typed(self, dependency_type: Type[T], name: Optional[str] = None) -> T:
        """Get a typed dependency"""
        key = name or dependency_type.__name__.lower()
        return self.get(key)
    
    def reset(self):
        """Reset all singletons (useful for testing)"""
        with self._lock:
            self._singletons.clear()
    
    def list_dependencies(self) -> Dict[str, str]:
        """List all registered dependencies"""

        #The ** "spreads" the key-value pairs from one dictionary into another dictionary.
        #This merges two dictionaries—one mapping singleton names to 'singleton', the other mapping transient names to 'transient'—into a single dictionary. 
        # If a name appears in both, the last one wins.
        return {
            **{name: 'singleton' for name in self._factories},   
            **{name: 'transient' for name in self._transients}
        }
    
        # Visualization of dependencies
    def show_dependency_graph(self):
        """Visualize the dependency graph and save as image  IN CURRENT WORKING DIR"""
        nx.draw(self._dependency_graph, with_labels=True, node_color='lightblue', edge_color='gray')
        plt.savefig("dependency_graph.png", bbox_inches='tight')
        print("✅ Dependency graph saved as dependency_graph.png")
        plt.show()

    
    # Factory methods for creating dependencies 
    def _create_milk_basics(self):
        from milk_basics import MilkBasics
        return MilkBasics()

    def _create_date_range(self):
        from date_range import DateRange
        return DateRange()
    
    # Status functions
    def _create_status_data(self):
        from status_functions.status_data import status_data
        return status_data()
    
    def _create_wet_dry(self):
        from status_functions.wet_dry import WetDry
        return WetDry()
    
    # Insem functions
    def _create_insem_ultra_basics(self):
        from insem_functions.insem_ultra_basics import InsemUltraBasics
        return InsemUltraBasics()
            
    def _create_insem_ultra_data(self):
        from insem_functions.insem_ultra_data import InsemUltraData
        return InsemUltraData()
        
    def _create_check_last_stop(self):
        from insem_functions.check_laststop import CheckLastStop
        return CheckLastStop()

    def _create_i_u_merge(self):
        from insem_functions.I_U_merge import I_U_merge
        return I_U_merge()
    
    def _create_ipiv(self):
        from insem_functions.ipiv import Ipiv
        return Ipiv()
    
    def _create_next_ultra_check(self):
        from insem_functions.next_ultra_check import NextUltraCheck
        return NextUltraCheck()
    



    # Feed functions 
    def _create_feedcost_basics(self):
        from feed_functions.feedcost_basics import Feedcost_basics
        return Feedcost_basics()
    
    def _create_feedcost_beans(self):
        from feed_functions.feedcost_beans import Feedcost_beans
        return Feedcost_beans()
    
    def _create_feedcost_cassava(self):
        from feed_functions.feedcost_cassava import Feedcost_cassava
        return Feedcost_cassava()
    
    def _create_feedcost_corn(self):
        from feed_functions.feedcost_corn import Feedcost_corn
        return Feedcost_corn()
    
    def _create_feedcost_bypass_fat(self):
        from feed_functions.feedcost_bypass_fat import Feedcost_bypass_fat
        return Feedcost_bypass_fat()

    def _create_feedcost_total(self):
        from feed_functions.feedcost_total import Feedcost_total
        return Feedcost_total()
    
    def _create_feedcost_sequences(self):
        from feed_functions.feedcost_sequences import FeedCostSequences
        return FeedCostSequences()
    


    # Milk functions 
    def _create_milk_aggregates(self):
        from milk_functions.milk_aggregates import MilkAggregates
        return MilkAggregates()
    
    def _create_raw_milk_update(self):
        from milk_functions.raw_milk_update import RawMilkUpdate
        return RawMilkUpdate()
    
    # plot functions
    def _create_plot_net_revenue_model(self):
        from plot_functions.plot_net_revenue_model import PlotNetRevenueModel
        return PlotNetRevenueModel()
    
    def _create_run_lactation_plot(self):
        from plot_functions.run_lactation_plot import RunLactationPlot
        return RunLactationPlot()



    # Groups and tests  
    def _create_model_groups(self):
        from groups_and_tests.model_groups import ModelGroups
        return ModelGroups()
        
    def _create_whiteboard_groups(self):
        from groups_and_tests.whiteboard_groups import WhiteboardGroups
        return WhiteboardGroups()
    
    def _create_compare_model_whiteboard_groups_last(self):
        from groups_and_tests.compare_model_whiteboard_groups_last import CompareModelWhiteboardGroups_Last
        return CompareModelWhiteboardGroups_Last()

    def _create_wet_dry_groups(self):
        from groups_and_tests.wet_dry_groups import WetDryGroups
        return WetDryGroups()


    # Lactation functions 
    def _create_lactation_basics(self):
        from milk_functions.lactation.lactation_basics import LactationBasics
        return LactationBasics()
    
    def _create_this_lactation(self):
        from milk_functions.lactation.this_lactation import ThisLactation
        return ThisLactation()
    
    def _create_lactations(self):
        from milk_functions.lactation.lactations import Lactations
        return Lactations()    
    
    def _create_weekly_lactations(self):
        from milk_functions.lactation.weekly_lactations import WeeklyLactations
        return WeeklyLactations()
    
    def _create_lactations(self):
        from milk_functions.lactation.lactations import Lactations
        return Lactations()
        
    
    # Lactation measurements
    def _create_lactations_log_standard(self):
        from groups_and_tests.lactation_measurements.lactations_log_standard import LactationsLogStandard
        return LactationsLogStandard()
    
    def _create_lactation_plots(self):
        from groups_and_tests.lactation_measurements.lactation_plots import LactationPlots
        return LactationPlots()        
    


    #Report_Milk
    def _create_report_milk(self):
        from milk_functions.report_milk.report_milk import ReportMilk
        return ReportMilk()

    def _create_report_milk_xlsx(self):
        from milk_functions.report_milk.report_milk_xlsx import ReportMilkXlsx
        return ReportMilkXlsx()          

    def _create_run_milk_dash_app(self):
        from milk_functions.report_milk.milk_dash_app import run_milk_dash_app
        return run_milk_dash_app  # <-- Do NOT call the function here! <--- returns the function, not the result of calling it
            # lazy loading - the function will be called in the app module

    # Finance functions 
    def _create_finance_basics(self):
        from finance_functions.finance_basics import FinanceBasics
        return FinanceBasics()
    
    def _create_capex_basics(self):
        from finance_functions.capex.capex_basics import CapexBasics
        return CapexBasics()  
     
    def _create_capex_projects(self):
        from finance_functions.capex.capex_projects import CapexProjects
        return CapexProjects()

    def _create_income_statement(self):
        from finance_functions.income.income_statement import IncomeStatement
        return IncomeStatement()       

    def _create_milk_income(self):
        from finance_functions.income.milk_income import MilkIncome
        return MilkIncome()
        
    def _create_depreciation(self):
        from finance_functions.PL.depreciation import DepreciationCalc
        return DepreciationCalc()
    
    def _create_net_revenue(self):
        from finance_functions.PL.NetRevenue import NetRevenue
        return NetRevenue()
    
    def _create_cow_pl(self):
        from finance_functions.PL.cow_PL import CowPL
        return CowPL()
        
    # def _create_net_revenue_by_cow_by_date(self):
    #     from finance_functions.net_revenue.net_revenue_by_cow_by_date import NetRevenueByCowByDate
    #     return NetRevenueByCowByDate()
    
    def _create_sahagon(self):
        from finance_functions.income.sahagon import sahagon
        return sahagon()    



# Global container instance
container = Container()

# Convenience functions
def get_dependency(name: str) -> Any:
    """Get a dependency from the global container"""
    return container.get(name)

def get_typed_dependency(dependency_type: Type[T], name: Optional[str] = None) -> T:
    """Get a typed dependency from the global container"""
    return container.get_typed(dependency_type, name)

def reset_container():
    """Reset the global container (useful for testing)"""
    container.reset()

    
#Yes, your if __name__ == "__main__": guard is correct.
#It ensures that the code inside only runs when you execute container.py directly, 
# not when it is imported as a module.
if __name__ == "__main__":
    pass
