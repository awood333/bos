'''Persistent Container Service'''
import socket
import pickle
import threading
import time
import importlib
import sys
import os
from container import Container


# Server Side (ContainerService):

# Starts a socket server on localhost:9999.
# Instantiates a single Container object and pre-loads some dependencies.
# Listens for client requests (get or reset).
# When a client requests a dependency, it returns the already-instantiated singleton from the container (if available), or creates it if not.
# If the client requests a reset, it clears all singletons (forces re-instantiation on next request).

class ContainerService:
    def __init__(self, host='localhost', port=9999, poll_interval=2):
        self.host = host
        self.port = port
        self.container = Container()
        self.server_socket = None
        self.running = False
        self.poll_interval = poll_interval
        self._file_mtimes = {}  # Track last modification times
        self._dep_to_module = {}  # Map dependency name to module file

        self._register_file_monitoring()    

    def _register_file_monitoring(self):
        # Map dependency names to their module files
        for name, factory in self.container._factories.items():
            mod_name = factory.__module__
            try:
                mod = sys.modules.get(mod_name) or importlib.import_module(mod_name)
                mod_file = getattr(mod, '__file__', None)
                if mod_file:
                    self._dep_to_module[name] = mod_file
                    self._file_mtimes[mod_file] = os.path.getmtime(mod_file)
            except Exception:
                continue

        
    def start_server(self):
        """Start the container service server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.running = True
            print(f"Container service started on {self.host}:{self.port}")
            
            # Pre-load common dependencies
            self._preload_dependencies()

            # Start file monitoring thread  
            threading.Thread(target=self._monitor_files, daemon=True).start()

            
            while self.running:
                try:
                    client_socket, addr = self.server_socket.accept()
                    threading.Thread(
                        target=self._handle_client, 
                        args=(client_socket,)
                    ).start()
                except OSError:
                    break
                    
        except Exception as e:
            print(f"Server error: {e}")
        finally:
            self._cleanup()


    def _monitor_files(self):
            """Monitor dependency module files for changes and reload if needed"""
            while self.running:
                for dep, mod_file in self._dep_to_module.items():
                    try:
                        new_mtime = os.path.getmtime(mod_file)
                        old_mtime = self._file_mtimes.get(mod_file)
                        if old_mtime is not None and new_mtime > old_mtime:
                            print(f"🟢 FILE UPDATED: {mod_file}")
                            print(f"🔄 Detected change in {mod_file}. Reloading module and re-instantiating '{dep}'...")
                            # Reload the module
                            mod_name = self.container._factories[dep].__module__
                            mod = sys.modules.get(mod_name)
                            if mod:
                                importlib.reload(mod)
                            # Re-instantiate the singleton
                            if dep in self.container._singletons:
                                del self.container._singletons[dep]
                            self.container.get(dep)
                            self._file_mtimes[mod_file] = new_mtime
                    except Exception as e:
                        print(f"File monitoring error for {mod_file}: {e}")
                time.sleep(self.poll_interval)



    
    def _preload_dependencies(self):
        """Pre-load frequently used dependencies"""
        preload_list = [
             'status_data', 
            'feedcost_basics', 'insem_ultra_basics'
        ]
        
        print("Pre-loading dependencies...")
        for dep_name in preload_list:
            try:
                self.container.get(dep_name)
                print(f"  ✓ {dep_name}")
            except Exception as e:
                print(f"  ✗ {dep_name}: {e}")
        print("Pre-loading complete!")
    
    def _handle_client(self, client_socket):
        print("[SERVER] _handle_client called")
        try:
            data = client_socket.recv(4096)
            request = pickle.loads(data)
                        
            if request['action'] == 'get':
                print(f"[SERVER] About to get dependency: {request['name']}")
                dependency = self.container.get(request['name'])
                print(f"[SERVER] Got dependency: {request['name']}")
                response = {'success': True, 'data': dependency}
                
            elif request['action'] == 'reset':
                self.container.reset()
                response = {'success': True, 'data': None}
            else:
                response = {'success': False, 'error': 'Unknown action'}
            
            client_socket.send(pickle.dumps(response))
            
        except Exception as e:
            error_response = {'success': False, 'error': str(e)}
            client_socket.send(pickle.dumps(error_response))
        finally:
            client_socket.close()
    
    def stop_server(self):
        """Stop the container service"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
    
    def _cleanup(self):
        """Cleanup resources"""
        if self.server_socket:
            self.server_socket.close()
        print("Container service stopped")




# Client Side (ContainerClient):
# Connects to the server and requests a dependency by name.
# Receives the dependency object (pickled) from the server.
# If the server is unavailable, falls back to local instantiation.

class ContainerClient:
    def __init__(self, host='localhost', port=9999):
        self.host = host
        self.port = port
    

    def get_dependency(self, name: str):
        """Get a dependency from the service"""
        try:
            print(f"[CLIENT] Connecting to {self.host}:{self.port} for '{name}'")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
            
            request = {'action': 'get', 'name': name}
            print(f"[CLIENT] Sending request: {request}")
            sock.send(pickle.dumps(request))

            # --- Receive all data until the socket is closed ---
            chunks = []
            while True:
                print("[CLIENT] Waiting to receive chunk...")
                chunk = sock.recv(65536)  # 64KB per chunk
                print(f"[CLIENT] Received chunk of size: {len(chunk)}")
                if not chunk:
                    print("[CLIENT] No more data, breaking receive loop.")
                    break
                chunks.append(chunk)
            response_data = b''.join(chunks)
            print(f"[CLIENT] Total response size: {len(response_data)}")
            response = pickle.loads(response_data)
            print(f"[CLIENT] Unpickled response: {type(response)} keys: {list(response.keys())}")

            sock.close()
            
            if response['success']:
                print("[CLIENT] Dependency received successfully.")
                return response['data']
            else:
                print(f"[CLIENT] Error in response: {response['error']}")
                raise Exception(response['error'])
                
        except Exception as e:
            print(f"[CLIENT] Failed to get dependency '{name}': {e}")
            from container import get_dependency
            return get_dependency(name)

if __name__ == "__main__":

    
    if len(sys.argv) > 1 and sys.argv[1] == 'start':
        service = ContainerService()
        try:
            service.start_server()
        except KeyboardInterrupt:
            service.stop_server()
    else:
        print("Usage: python container_service.py start")