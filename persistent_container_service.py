'''Persistent Container Service'''
import socket
import pickle
import threading
import time
from container import Container

class ContainerService:
    def __init__(self, host='localhost', port=9999):
        self.host = host
        self.port = port
        self.container = Container()
        self.server_socket = None
        self.running = False
        
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
    
    def _preload_dependencies(self):
        """Pre-load frequently used dependencies"""
        preload_list = [
            'date_range', 'milk_basics', 'status_data', 
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
        """Handle client requests for dependencies"""
        try:
            data = client_socket.recv(4096)
            request = pickle.loads(data)
            
            if request['action'] == 'get':
                dependency = self.container.get(request['name'])
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

# Client class for accessing the service
class ContainerClient:
    def __init__(self, host='localhost', port=9999):
        self.host = host
        self.port = port
    
    def get_dependency(self, name: str):
        """Get a dependency from the service"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
            
            request = {'action': 'get', 'name': name}
            sock.send(pickle.dumps(request))
            
            response_data = sock.recv(8192)  # Increased buffer size
            response = pickle.loads(response_data)
            
            sock.close()
            
            if response['success']:
                return response['data']
            else:
                raise Exception(response['error'])
                
        except Exception as e:
            print(f"Failed to get dependency '{name}': {e}")
            # Fallback to local container
            from container import get_dependency, container
            return get_dependency(name)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'start':
        service = ContainerService()
        try:
            service.start_server()
        except KeyboardInterrupt:
            service.stop_server()
    else:
        print("Usage: python container_service.py start")