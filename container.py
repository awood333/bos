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
        
        # Status
        self.register_singleton('status_data',          self._create_status_data)
        self.register_singleton('status_data2',         self._create_status_data2)
        self.register_singleton('wet_dry',              self._create_wet_dry)
        self.register_singleton('status_groups',        self._create_status_groups)
        self.register_singleton('status_groups_tenday', self._create_status_groups)        
        
        # Insem
        self.register_singleton('insem_ultra_basics',   self._create_insem_ultra_basics)
        self.register_singleton('insem_ultra_data',     self._create_insem_ultra_data)
        self.register_singleton('check_last_stop',      self._create_check_last_stop)
        self.register_singleton('i_u_merge',            self._create_i_u_merge)
        self.register_singleton('ipiv',                 self._create_ipiv)
        
        # Feed
        self.register_singleton('feedcost_basics',      self._create_feedcost_basics)
        self.register_singleton('feedcost_beans',       self._create_feedcost_beans)
        self.register_singleton('feedcost_cassava',     self._create_feedcost_cassava)
        self.register_singleton('feedcost_corn',        self._create_feedcost_corn)
        self.register_singleton('feedcost_bypass_fat',  self._create_feedcost_bypass_fat)
        self.register_singleton('feedcost_total',       self._create_feedcost_total)
        
        # Milk
        self.register_singleton('milk_aggregates',      self._create_milk_aggregates)
        self.register_singleton('milking_groups_tenday',self._create_milking_groups_tenday)
        self.register_singleton('sahagon',              self._create_sahagon)
        
        # Lactation
        self.register_singleton('lactation_basics',     self._create_lactation_basics)
        self.register_singleton('_create_lactations',   self._create_lactations)
        self.register_singleton('this_lactation',       self._create_this_lactation)
        self.register_singleton('weekly_lactations',    self._create_weekly_lactations)

        # Report Milk
        self.register_singleton('_create_report_milk',      self._create_report_milk)
        self.register_singleton('_create_report_milk_xlsx', self._create_report_milk_xlsx)        
        self.register_singleton('run_milk_dash_app',        self._create_run_milk_dash_app)        


        # Finance
        self.register_singleton('milk_income',          self._create_milk_income)
        self.register_singleton('net_revenue',          self._create_net_revenue)
        self.register_singleton('cow_pl',               self._create_cow_pl)
        
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
        """Get a dependency by name"""
        with self._lock:
            print(f"[CONTAINER] Lock acquired for: {name}")
            print(f"[CONTAINER] Currently creating: {self._creating}")
            print(f"[CONTAINER] Singletons: {list(self._singletons.keys())}")

            # Check if it's a singleton that's already created
            if name in self._singletons:
                print(f"[CONTAINER] Returning existing singleton for: {name}")
                return self._singletons[name]
            
            # Check if it's a registered singleton factory
            if name in self._factories:
                # Get complete call stack for debugging
                stack_info = []
                for i, frame in enumerate(inspect.stack()[1:8]):  # Get 7 levels deep
                    stack_info.append(f"   {i}: {frame.filename}:{frame.lineno} in {frame.function}")
                
                print(f"🔍 Creating singleton: {name}")
                print("📍 Complete call stack:")
                for line in stack_info:
                    print(line)
                
                # Initialize tracking attributes if they don't exist
                if not hasattr(self, '_creating'):
                    self._creating = set()
                if not hasattr(self, '_creation_order'):
                    self._creation_order = []
                
                # Check for circular dependencies with detailed chain tracking
                if name in self._creating:
                    print("🚨 CIRCULAR DEPENDENCY DETECTED!")
                    print(f"   Trying to create: {name}")
                    print(f"   Currently creating: {list(self._creating)}")
                    print(f"   Creation order: {' → '.join(self._creation_order)}")
                    raise RuntimeError(f"Circular dependency: {' → '.join(self._creation_order)} → {name}")
                
                self._creating.add(name)
                self._creation_order.append(name)

                # Track dependency graph edge
                parent = self._creation_order[-2] if len(self._creation_order) > 1 else None
                if parent:
                    self._dependency_graph.add_edge(parent, name)                
                
                try:
                    print(f"🏗️  Starting creation of: {name}")
                    instance = self._factories[name]()
                    self._singletons[name] = instance
                    print(f"✅ Successfully created: {name}")
                    return instance
                finally:
                    self._creating.remove(name)
                    self._creation_order.remove(name)
                    print(f"🔚 Finished creating: {name}")
        
        # Check if it's a transient
        if name in self._transients:
            print(f"Creating transient: {name}")
            return self._transients[name]()
        
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

    
    # Factory methods for creating dependencies - NO PARAMETERS!
    
    def _create_status_data(self):
        from status_functions.statusData import StatusData
        return StatusData()
    
    def _create_status_data2(self):
        from status_functions.statusData2 import StatusData2
        return StatusData2()
    
    def _create_wet_dry(self):
        from status_functions.wet_dry import WetDry
        return WetDry()
    
    def _create_status_groups(self):
        from status_functions.status_groups import statusGroups
        return statusGroups()
    
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

    # Milk functions 
    def _create_milk_aggregates(self):
        from milk_functions.milk_aggregates import MilkAggregates
        return MilkAggregates()
    
    def _create_milking_groups_tenday(self):
        from milk_functions.milking_groups_tenday import MilkingGroups_tenday
        return MilkingGroups_tenday()
    
    def _create_sahagon(self):
        from milk_functions.sahagon import sahagon
        return sahagon()

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
    def _create_milk_income(self):
        from finance_functions.income.MilkIncome import MilkIncome
        return MilkIncome()
    
    def _create_net_revenue(self):
        from finance_functions.PL.NetRevenue import NetRevenue
        return NetRevenue()
    
    def _create_cow_pl(self):
        from finance_functions.PL.cow_PL import CowPL
        return CowPL()

    


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


if __name__ == "__main__":
    # Example usage
    print("Available dependencies:")
    for name, dep_type in container.list_dependencies().items():
        print(f"  {name}: {dep_type}")
    
# Try creating all dependencies
    container.get('status_data')
    container.get('status_data2')
    container.get('wet_dry')
    container.get('status_groups')
    container.get('insem_ultra_basics')
    container.get('insem_ultra_data')
    container.get('check_last_stop')
    container.get('i_u_merge')
    container.get('ipiv')
    container.get('feedcost_basics')
    # container.get('feedcost_beans')
    # container.get('feedcost_cassava')
    # container.get('feedcost_corn')
    # container.get('feedcost_bypass_fat')
    container.get('feedcost_total')
    container.get('this_lactation')
    container.get('milk_aggregates')
    # container.get('milking_groups_tenday')

    container.get('lactation_basics')
    container.get('milk_income')
    container.get('net_revenue')
    # container.get('cow_pl')
    container.get('report_milk')
    # container.get('report_milk_xlsx')


    # ref show_dependency_graph above.   after running your script, open dependency_graph.png to view the graph.
    container.show_dependency_graph()    