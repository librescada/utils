import asyncio
import asyncua
from asyncua import ua, Node
from asyncua import Client as asyncClient
from asyncua.sync import Client as syncClient
from asyncua import Server as asyncServer
from asyncua.sync import SyncNode
from asyncua.crypto.security_policies import SecurityPolicyBasic256Sha256

import datetime
import logging
from collections import deque
from pprint import pprint

logger = logging.getLogger(__name__)


class uaclient_librescada(asyncClient):
    """

    """
    
    @classmethod
    async def create(cls, ua_parameters, local=False, secure=False, docker=False): # __init__ alternative for async classes
        """
        Create a new client instance
        """
        
        if docker:
            url = ua_parameters['url_docker']
        else:
            if local:
                url = ua_parameters['url_local']
            else:
                url = ua_parameters['url']
            
            
        self = cls(url=url)
        # self = super().__init__(url=url)
        self.uri = ua_parameters['uri']
        self.logger = logging.getLogger(__name__).parent
            
        if secure:
            await self.set_security(
                SecurityPolicyBasic256Sha256,
                certificate=ua_parameters(['client_certificate']),
                private_key=ua_parameters(['client_private_key']),
                server_certificate=ua_parameters(['server_certificate'])
            )
        else:
            self.set_user('admin')
        
        self.server_structure = None
        self.default_retry_time = 10
        self.default_max_retries = 100
        
        return self
    
    # async def connect():
    #     await super.conn
    
    async def get_server_structure(self):
        """ 
        Function that returns the structure of the server in a dictionary.
        - Duplicates IDs are allowed as long as not within the same tree level
        - Three levels of hierarchy are supported (user_object - folder - variable)
        - Structure expected for the OPC server: 
        
            root
                - Types
                - Views
                - Objects
                    - Aliases
                    - Server
                    - user_object_1
                    - user_object_N
                        - user_variable_1
                        - user_variable_N
                        - user_folder_1
                            - user_variable_N+1
                            - user_variable_N+M
                        - user_folder_N
                            - user_variable_N+M+1
                            - user_variable_N+M+K
                        
            Example:
                - Types
                - Views
                - Objects
                    - Aliases
                    - Server
                    - measurements
                        - TT-DES-001  (float)
                        - TT-DES-002  (float)
                        - HW1TT21     (float)
                        - PT-DES-001  (float)
                        - PT-DES-002  (float)
                        - FT-DES-001  (float)
                        - FT-AQU-101a (float)
                        
                    - inputs
                        - FT-AQU-101a (float)
                        - ZT-AQU-101a (float)
                        - FF-AQU-101a (float)
                        
                    - controllers
                        - hot_water_temp_controller
                            - online: (boolean)
                            - active: (boolean)
                            - Kp: (float)
                            - Ki: (float)
                            - input_id: ZT-AQU-101a (string)
                            - output_id: HW1TT21 (string)
                        - hot_water_flow_controller
                            - online: (boolean)
                            - active: (boolean)
                            - Kp: (float)
                            - Ki: (float)
                            - input_id:  FF-AQU-101a (string)
                            - output_id: FT-AQU-101a (string)
        """
        
        object_nodes = await self.nodes.objects.get_children()

        objs = {}
        
        for obj in object_nodes:
            name = await obj.read_browse_name()
            if name.Name != 'Server' and name.Name != 'Aliases':
                objs[name.Name] = {'name':name.Name, 'node':obj}
            
        for obj in objs:     # Look in each object or the one specfied (measurements, inputs, controllers, etc)
            obj_children = await objs[obj]['node'].get_children()
            objs[obj]['children'] = {}
            for obj_child in obj_children:
                obj_child_name = await obj_child.read_browse_name()
                objs[obj]['children'][obj_child_name.Name] = {'name':obj_child_name.Name, 'node':obj_child}
                
                obj_child_children = await obj_child.get_children()
                if len(obj_child_children)>0: # Folder
                    objs[obj]['children'][obj_child_name.Name]['children'] = {}
                    for obj_child_child in obj_child_children:
                        obj_child_child_name = await obj_child_child.read_browse_name()
                        objs[obj]['children'][obj_child_name.Name]['children'][obj_child_child_name.Name] = {'name':obj_child_child_name.Name, 'node':obj_child_child}                
                    
        self.server_structure = objs
        
        return objs
    
    async def reconnect(self, retry_time=None, max_retries=None):
        if not retry_time:
            retry_time = self.default_retry_time
        
        if not max_retries:
            max_retries = self.default_max_retries
            
        connected = False; retries = 0
        while not connected and retries<max_retries:
            self.logger.info(f'Waiting {retry_time} seconds before retrying')
            await asyncio.sleep(retry_time)
        
            try:
                retries += 1
                await self.connect()    
                await self.check_connection()
                connected = True
            except Exception as e: 
                self.logger.error(f'Failed reconnection attempt: {e}')
    
    async def check_object_in_server(self, object_name:str):
        """ Function that checks if an object exists in the server """
        
        objects_node = self.nodes.objects
        
        object_nodes = await objects_node.get_children()
        for obj_node in object_nodes:
            name  = await obj_node.read_browse_name()
            if name.Name == object_name:
                return True, obj_node
        
        # Object not found
        return False, []
    
    async def setup_objects(self, object_name:str, type=None, delete_if_exists=True, include_online=False)-> asyncua.Node:
        """
        Habría que cambiarle el nombre -> get_objects?, también habría que sustituirla para que
        lo único que haga sea comprobar si un objeto existe, borrarlo si se
        especifica, y  crearlo. 
        
        Function that creates objects in the OPC UA server if they don't exist

        Args:
            type: (str, requiered): Type of the object to create.

        Returns:
            (asyncua.Node): Nodes of the created/retrieved objects depending on the type
        """
        
        async def setup_object(object_name:str, include_online:bool, delete_if_exists:bool) -> asyncua.Node:
            # Retrieve or create object
            found, obj = await self.check_object_in_server(object_name)
            if found:
                self.logger.info(f'Object {object_name} already exists in server')                    
            
            if found and delete_if_exists:
                await obj.delete()
                self.logger.info(f'Object {object_name} deleted')
            
            if not found or delete_if_exists:
                obj = await self.nodes.objects.add_object(idx, object_name)
                self.logger.info(f'Object {object_name} added to server')
                
            if include_online:
                await obj.add_variable(obj.nodeid.Identifier, 'online', False)
                
            return obj

        idx = await self.get_namespace_index(self.uri)
        if not self.server_structure:
            self.server_structure = await self.get_server_structure()


        # Individual objects mantained for backwards compatibility, 
        # when a new object needs to be created, just specify a name and 
        # whether it should include the online variable or delete it if it exists
        
        if type=='gateway':
            # Retrieve or create measurements object
            object_name = 'measurements'
            meas_obj = await setup_object(object_name, include_online=False, delete_if_exists=False)

            # Retrieve or create inputs object
            object_name = 'inputs'
            inputs_obj = await setup_object(object_name, include_online=False, delete_if_exists=False)

            # Retrieve or create gateways object
            object_name = 'gateways'
            gateways_obj = await setup_object(object_name, include_online=True, delete_if_exists=True)

            return meas_obj, inputs_obj, gateways_obj
        
        elif type=='controller':
            # Check for measurements and inputs objects, if they do not exist raise error
            found, inputs_obj = await self.check_object_in_server('inputs')
            if not found: raise RuntimeError('Object inputs not found in server')
            found, _ = await self.check_object_in_server('measurements')
            if not found: raise RuntimeError('Object measurements not found in server')
            
            # Retrieve or create controllers object
            object_name = 'controllers'
            controllers_obj = await setup_object(object_name, include_online=True, delete_if_exists=True)

            return controllers_obj, inputs_obj
        
        elif type=='data_logging':
            object_name = 'data_logging'
            obj = await setup_object(object_name, include_online=True, delete_if_exists=True)
                
            return obj
        
        # elif type=='test':
        #     # Retrieve or create measurements object
        #     found, meas_obj = await self.check_object_in_server('measurements')
        #     if found:
        #         self.logger.info('Object measurements already exists in server')
        #     else:
        #         self.logger.info('Object measurements not found in server, added')
        #         meas_obj = await self.nodes.objects.add_object(idx, 'measurements')

        #     # Retrieve or create inputs object
        #     found, inputs_obj = await self.check_object_in_server('inputs')
        #     if found:
        #         self.logger.info('Object inputs already exists in server')
        #     else:
        #         self.logger.info('Object inputs not found in server, added')
        #         inputs_obj = await self.nodes.objects.add_object(idx, 'inputs')

        #     return meas_obj, inputs_obj
        
        elif type=='signal_generator':
            object_name = 'signal_generator'
            obj = await setup_object(object_name, include_online=True, delete_if_exists=True)
                
            return obj
        
        elif type=='finite_state_machines':
            object_name = 'finite_state_machines'
            obj = await setup_object(object_name, include_online=True, delete_if_exists=True)
                
            return obj
        
        else: 
            obj = await setup_object(object_name, include_online=include_online, delete_if_exists=delete_if_exists)
            
            return obj
            # raise ValueError(f'Type {type} not recognized')

    async def setup_object(self, object_config:dict, include_online=True, delete_if_exists=True):
        """ 
        
            Configure object in opc server, if it doesn't exist,
            it will create the object and its variables, and return 
            an updated dictionary including the nodes of the variables.
            If it exists, it will delete and create it again. 
            
            Inputs:
                ua_client: uaclient_librescada object
                object_config: Dictionary with object configuration:
                    object_config = {
                        "name": "object_name",
                        "children": 
                            [
                                'var1': {'type': 'float', 'value (optional)': 0.0},
                                ...
                                'varN': {'type': 'string', 'value (optional)': 'initial_value'},
                                
                                'folder1': {'type: 'folder', 'children':{
                                    'var1': {'type': 'float', 'value (optional)': 0.0},
                                    ...
                                    'varN': {'type': 'string', 'value (optional)': 'initial_value'},
                                    
                                    'subfolder1': {'type: 'folder', 'children':{...}},
                                    ...,
                                    'subfolderN': {'type: 'folder', 'children':{...}}
                                }},
                                ...,
                                'folderN': {'type: 'folder', 'children':{...}}
                            ]
                    }
            
            Outputs:
                object config: Updated object config with nodes of variables
                online_node: Node of online variable
                
            Expected final server structure:
            
                Types
                ...
                Objects:
                    · measurements
                    · inputs
                    · virtual_signals
                    · signals_extended
                    ...
                    · new_object
                        - online: Boolean
                        - var1: Type, Initial value
                        - var2: Type, Initial value
                        - ...
                        - varN: Type, Initial value
                        - folder1:
                            - var1: Type, Initial value
                            - varN: Type, Initial value
                            - subfolder1:
                                · var1: Type, Initial value
                                · ...
                                · varN: Type, Initial value
                            - ...
                            - subfolderN
                        - ...
                        - folderN
            
        """
        
        def create_value_of_type(type_str, value=None):
            try:
                type_ = globals()['__builtins__'][type_str]
            
            except KeyError:
                logger.error(f'Error creating empty value for type {type_str}, supported types are: {globals()["__builtins__"].keys()}')
                
                raise
            
            if not value:
                return type_()
            else:
                return type_(value)
            
        async def setup_folder(var:dict, parent_node, var_name, delete_if_exists=False):
            """Recursively create folders and variables in opc server
            
            Args:
                var (dict): Configuration of children of parent folder
                parent_node (asyncua.Node): Parent node
                var_name (str): Name of parent folder

            Returns:
                var (dict): Updated var dictionary with nodes of variables
            """
            if existing_obj and not delete_if_exists:
                try:
                    folder_node = [child for child in parent_node.get_children() if child.get_browse_name().Name == var_name][0]
                except IndexError:
                    raise ValueError(f'Folder {var_name} not found in {object_name}')
            else:
                folder_node = await parent_node.add_folder(parent_node.nodeid.Identifier, var_name)
            
            for child_key in var['children']:
                child = var['children'][child_key]
                # child_key = child.key()
                
                if child['type'] == 'folder':
                    var = await setup_folder(child, folder_node, child_key)
                    self.logger.info(f'Folder {child_key} added to object {object_name}')
                else:
                    existing_var = False
                    if existing_obj and not delete_if_exists:
                        child_node = [child for child in folder_node.get_children() if child.get_browse_name().Name == child_key]
                        if child_node[0]:
                            child['node'] = child_node[0]
                            existing_var = True
                            self.logger.info(f'Variable {child_key} retrieved from object {object_name}')
                    
                    if not existing_var:
                        if not 'type' in child:
                            raise ValueError(f'Variable {child_key} in object {object_name} has no type defined (float, int, str, etc.)')
                        
                        value = child.get('value', None)
                        value = create_value_of_type(child['type'], value)
                        
                        child['node'] = await folder_node.add_variable(folder_node.nodeid.Identifier, child_key, value)
                        self.logger.info(f'Variable {child_key} added to object {object_name}')
                                
            return var
                    
        object_name = object_config['name']
        
        # Create object in opc server
        existing_obj, _ = await self.check_object_in_server(object_name)
        
        object_node = await self.setup_objects(object_name=object_name, delete_if_exists=delete_if_exists)
        object_config['node'] = object_node
        
        # Retrieve online node
        if include_online:
            online_node = await self.find_nodes(var_list=['online'], object=object_name)
            online_node = online_node[0]
        else:
            online_node = None
        
        for child_key in object_config['children']:
            child = object_config['children'][child_key]
            # child_key = child.key()
            if child['type'] == 'folder':
                child = await setup_folder(child, object_node, child_key)
            else:
                existing_var = False
                if existing_obj and not delete_if_exists:
                    child_node = await self.find_nodes(var_list=[child_key], object=object_name)
                    if child_node[0]:
                        child['node'] = child_node[0]
                        existing_var = True
                        self.logger.info(f'Variable {child_key} retrieved from object {object_name}')
                
                if not existing_var:
                    value = child.get('value', None)
                    value = create_value_of_type(child['type'], value)

                    child['node'] = await object_node.add_variable(object_node.nodeid.Identifier, child_key, value)
                    self.logger.info(f'Variable {child_key} added to object {object_name}')
                                             
        return object_config, online_node
            
    async def get_signals(self, signal_ids):
        # objects = ['measurements', 'inputs', 'virtual_signals']
        
        nodes = await self.find_nodes(signal_ids)
        if any(nodes) == []:
            signal_not_found = [signal_ids[i] for i in range(len(signal_ids)) if nodes[i]==[]]
            raise KeyError(f'No signals found with id: {signal_not_found}')
        
        return nodes
    
    async def find_nodes(self, var_list:list, object='', folder='', log=True):
        """ Function that looks for a node in all or specific objects
            in a OPC server. Async version 
            
            opc_client: OPC client object
            var_list: list of variables to find
            node_structure: node structure of the server, if not provided, it will be retrieved
            object: name of the object to look in, if empty, all objects are searched
            folder: name of the folder to look in, if empty, all folders are searched
            
            AVISO. Esta función es terrible y está programada por un mono. En algún momento
                   hay que darle una pensada o estudiar la librería si hay mejores formas de
                   hacerlo
                
        """
        if not self.server_structure:
            self.server_structure = await self.get_server_structure()
            
        if folder and not object:
            raise ValueError('Folder specified but no object, if folder specified, parent object is requiered')
         
        if object:
            if object not in self.server_structure:
                # First try updating the server structure
                self.server_structure = await self.get_server_structure()
                if object not in self.server_structure:
                    raise RuntimeError(f'Object {object} not found in server')
            
            if folder:
                if folder not in self.server_structure[object]['children']:
                    raise RuntimeError(f'Folder {folder} not found in object {object}')
                else: objects = [self.server_structure[object]]
            elif var_list[0] == object:
                return [self.server_structure[object]]
            else: # Con esto, si hay nodos con el mismo nombre en varios objetos se coge el primero, si se especifica un objeto es porque se quiere que se busque sólo en ese objeto
                # objects = self.server_structure
                
                # Alternativa
                objects = {object: self.server_structure[object]}
        else:
            objects = self.server_structure
            if log: self.logger.info(f'Object {object} specified, looking only in that object')
            
        # Make sure it is a list
        # if not isinstance(objects, list): objects = [objects]
        # Find the nodes in the list of objs
        var_nodes = []
        for var_name in var_list:
            nodeNotFound = True

            for obj in objects:
                if folder:
                    if 'children' in obj['children'][folder]:
                        obj_children = obj['children'][folder]['children'] 
                    else: break
                else:
                    obj_children = objects[obj]['children']
                    
                if var_name in obj_children:
                    var_nodes.append(obj_children[var_name]['node'])
                    nodeNotFound = False
                    break
                else:
                    for obj_children_child in obj_children:
                        if 'children' in obj_children[obj_children_child]: # Folder
                            if var_name in obj_children[obj_children_child]['children']:
                                var_nodes.append(obj_children[obj_children_child]['children'][var_name]['node'])
                                nodeNotFound = False
                                break

                    # Node not found in this object
        
            if nodeNotFound:
                var_nodes.append([])
                self.logger.error(f'Node for variable {var_name} could not be found on server')

        return var_nodes
            
    async def read_values(self, nodes:list, datavalue=False):
        """
            Read the value of multiple nodes in one ua call with the option 
            to include additional information.
        """
        
        if datavalue:
            return [(await self.get_node(node).read_value()).Value for node in nodes]            
        else:
            values = []
            for node in nodes:
                try:
                    value = await self.get_node(node).read_value()
                except Exception:
                    value = None
                values.append(value)
            
            return values
        
    async def write_values(self, nodes:list, values:list):
        """
            Write values to multiple nodes in one ua call
        """
        
        return [await self.get_node(node).write_value(dv) for node, dv in zip(nodes, values)]
    
    async def write_float_value(self, var, value):
        dv = ua.DataValue(
            Value=ua.Variant(value, ua.VariantType.Double),
            SourceTimestamp=datetime.datetime.utcnow(),
            ServerTimestamp=datetime.datetime.utcnow()
        )
        
        try:
            await self.get_node(var).write_value(dv)
        except ua.uaerrors.BadTypeMismatch:
            dv = ua.DataValue(
                Value=ua.Variant(value, ua.VariantType.Float),
                SourceTimestamp=datetime.datetime.utcnow(),
                ServerTimestamp=datetime.datetime.utcnow()
            )
            await self.get_node(var).write_value(dv)    
    
async def get_control_loop(opc_client, controller_name, node_structure=None):
    controller_node = findNodes_sync(opc_client=opc_client, var_list=[controller_name], 
                                     object='controllers', node_structure=node_structure)
    if not controller_node[0]:
        raise RuntimeError(f'Controller {controller_name} not found in OPC server')
    
    for var_node in controller_node[0].get_children():
        var_name = var_node.read_browse_name().Name
        controller_vars = {}
        controller_vars[var_name]['node']  = var_node.__str__()
        
    return controller_vars

class async_extendedClient(asyncClient):
    async def read_values(self, nodes, datavalue=False):
        """
        Read the value of multiple nodes in one ua call with the option 
        to include additional information.
        """
        results = [];
        if isinstance(nodes, Node):
            results.append(await nodes.read_attribute(ua.AttributeIds.Value))
        else:
            for node in nodes:
                if isinstance(node, Node):
                    results.append(await node.read_attribute(ua.AttributeIds.Value))
                else:
                    results.append(None)
            
        if datavalue: # Return a list of datavalue objects
            return results
        else:         # Return a list of values
            # print([result.Value.Value for result in results])
            return [result.Value.Value if result is not None else None for result in results]

    async def read_values2(self, nodes, datavalue=False):
        """
        Read the value of multiple nodes in one ua call with the option 
        to include additional information.
        """
        
        if datavalue:
            return [(await self.get_node(node).read_value()).Value for node in nodes]            
        else:
            values = []
            for node in nodes:
                try:
                    value = await self.get_node(node).read_value()
                except Exception:
                    value = None
                values.append(value)
            
            return values
            # return [(await self.get_node(node).read_value()) for node in nodes]
    
    async def write_values(self, nodes, values):
        """
        Write values to multiple nodes in one ua call
        """
        
        return [await self.get_node(node).write_value(dv) for node, dv in zip(nodes, values)]
                
class extendedClient(syncClient):
    def read_values(self, nodes, datavalue=False):
        """
        Read the value of multiple nodes in one ua call with the option 
        to include additional information.
        """
        
        results = [];
        # One node object
        if isinstance(nodes, SyncNode):
            results.append(nodes.read_attribute(ua.AttributeIds.Value))
        # One node string
        elif isinstance(nodes, str):
            results.append(self.get_node(nodes).read_attribute(ua.AttributeIds.Value))
        else:
            # Multiple node objects or strings
            for node in nodes:
                if isinstance(node, SyncNode):
                    results.append(node.read_attribute(ua.AttributeIds.Value))
                elif isinstance(node, str):
                    if node:
                        results.append(self.get_node(node).read_attribute(ua.AttributeIds.Value))
                    else:
                        results.append(None)
                else:
                    results.append(None)
            
        if datavalue: # Return a list of datavalue objects
            return results
        else:         # Return a list of values
            # print([result.Value.Value for result in results])
            return [result.Value.Value if result is not None else None for result in results]

class async_extendedServer(asyncServer):
    # def value_to_datavalue(val, varianttype=None):
    #     """
    #     convert anyting to a DataValue using varianttype
    #     """
    #     if isinstance(val, ua.DataValue):
    #         return val
    #     if isinstance(val, ua.Variant):
    #         return ua.DataValue(val, SourceTimestamp=datetime.datetime.utcnow())
    #     return ua.DataValue(ua.Variant(val, varianttype), SourceTimestamp=datetime.datetime.utcnow())

    async def write_values(self, nodes, values):
            """
            Write values to multiple nodes in one ua call
            """
            # nodeids = [node.nodeid for node in nodes]
            
            # if isinstance(values[0], ua.DataValue):
            #     dvs = values
            # else:
            #     dvs = [self.value_to_datavalue(val) for val in values]
            for node, dv in zip(nodes, values):
                node = self.get_node(node)
                name = await node.read_browse_name()
                print(f'{name}: {dv}')
                result = await node.write_value(dv)
                print(result)
            results = [await node.write_value(dv) for node, dv in zip(nodes, values)]
            # result = await self.write_attribute_value(nodeids[0], dvs[0], ua.AttributeIds.Value)
            # print(result)
            # pprint(dvs[1])
            # [await self.write_attribute_value(nodeid, dv, ua.AttributeIds.Value) for nodeid, dv in zip(nodeids, dvs)]
            # for result in results:
            # print(results[0].check())

async def write_float_opc(var, value):
    dv = ua.DataValue(
        Value=ua.Variant(value, ua.VariantType.Double),
        SourceTimestamp=datetime.datetime.utcnow(),
        ServerTimestamp=datetime.datetime.utcnow()
    )
    
    try:
       await var.write_value(dv)
    except ua.uaerrors.BadTypeMismatch:
        dv = ua.DataValue(
            Value=ua.Variant(value, ua.VariantType.Float),
            SourceTimestamp=datetime.datetime.utcnow(),
            ServerTimestamp=datetime.datetime.utcnow()
        )
        await var.write_value(dv)

def readValuesUA(client, group, initial_read=False, log=True):
    """Function that reads a group of tags from an OPC UA server

    Args:
        client ([type]): asyncua client object
        group ([type]): Group dict list
        inital_read ([boolean]): First time this function is called, it will 
        output the read values through the logger

    Returns:
        [type]: [description]
    """
    if group['opcTag_list'][0]:
        try:
            values = client.read_values(group['opcTag_list'], datavalue=False)
            # print(group['opcTag_list'])
            for idx in range(len(group["measurements"].keys())):
                # pprint(values[idx].Value)
                group["measurements"][group["varId_list"][idx]]["values"].append(values[idx])
                group["measurements"][group["varId_list"][idx]]["time"].append(datetime.datetime.now(tz=datetime.timezone.utc))
                # group["measurements"][group["varId_list"][idx]]["values"].append(values[idx].Value.Value)
                # group["measurements"][group["varId_list"][idx]]["time"].append(values[idx].SourceTimestamp)
                # if initial_read: logger.info(f'Tag {group["name"]} - {group["sensorId_list"][idx]}: {values[idx]}')
                
            if log: logger.info(f'Values read for group {group["name"]}')
        except Exception as e:
            if log: logger.error(f'Error in read for group {group["name"]}: {e}')
            raise e

    else: 
        if log: logger.warning(f'No values read for group {group["name"]}, opcTag_list field is empty')
    
    return group

async def async_readValuesUA(client, group, initial_read=False, consisting_server_time=False):
    """Function that reads a group of tags from an OPC UA server

    Args:
        client ([type]): asyncua client object
        group ([type]): Group dict list
        inital_read ([boolean]): First time this function is called, it will 
        output the read values through the logger

    Returns:
        [type]: [description]
    """

    # try:
    values = await client.read_values(group['opcTag_list'], datavalue=True)
    for idx in range(len(group["measurements"].keys())):
        if values[idx] is not None:
            # pprint(f'Leído valor: {values[idx].Value.Value} con tiempo {values[idx].SourceTimestamp}')
            group["measurements"][group["varId_list"][idx]]["values"].append(values[idx].Value.Value)
            group["measurements"][group["varId_list"][idx]]["source_time"].append(values[idx].SourceTimestamp)
            group["measurements"][group["varId_list"][idx]]["server_time"].append(values[idx].ServerTimestamp)
        else:
            group["measurements"][group["varId_list"][idx]]["values"].append(nan)
            group["measurements"][group["varId_list"][idx]]["source_time"].append(datetime.datetime.now(tz=datetime.timezone.utc))
            group["measurements"][group["varId_list"][idx]]["server_time"].append(datetime.datetime.now(tz=datetime.timezone.utc))
            
    if consisting_server_time:
        group["time"].append(datetime.datetime.now(tz=datetime.timezone.utc))
        # if initial_read: logger.info(f'Tag {group["name"]} - {group["sensorId_list"][idx]}: {values[idx]}')
            
    # except Exception as e:
    #     logger.error(f'Error en lectura del grupo {group["name"]}: {e}')

    return group

# class SubscriptionHandler:
#     """
#     The SubscriptionHandler is used to handle the data that is received for the subscription.
#     """
#     def datachange_notification(self, node: Node, val, data):
#         """
#         Callback for asyncua Subscription.
#         This method will be called when the Client received a data change message from the Server.
#         """
#         logger.info('datachange_notification %r %s', node, val)
        

async def setup_objects(server, idx, type='gateway'):
    """Function that creates objects in the OPC UA server if they don't exist

    Args:
        client (asyncua.Client, requiered): asyncua client object
        idx (type, requiered): Namespace of the server
        type: (str, optional): Type of the object to create. Defaults to 'gateway'.

    Returns:
        (asyncia.Node): Nodes of the created/retrieved objects depending on the type
    """
    async def setup_object(object_name):
        # Retrieve or create object
        found, obj = await check_object_in_server(server, object_name)
        if found:
            logger.info(f'Object {object_name} already exists in server')
        else:
            logger.info(f'Object {object_name} not found in server, added')
            obj = await server.nodes.objects.add_object(idx, object_name)
            
        return obj

    if type=='gateway':
        # Retrieve or create measurements object
        object_name = 'measurements'
        meas_obj = await setup_object(object_name)

        # Retrieve or create inputs object
        object_name = 'inputs'
        inputs_obj = await setup_object(object_name)

        # Retrieve or create gateways object
        object_name = 'gateways'
        gateways_obj = await setup_object(object_name)

        return meas_obj, inputs_obj, gateways_obj
    
    elif type=='controller':
        # Check for measurements and inputs objects, if they do not exist raise error
        found, inputs_obj = await check_object_in_server(server, 'inputs')
        if not found: raise RuntimeError('Object inputs not found in server')
        found, _ = await check_object_in_server(server, 'measurements')
        if not found: raise RuntimeError('Object measurements not found in server')
        
        # Retrieve or create controllers object
        object_name = 'controllers'
        controllers_obj = await setup_object(object_name)

        return controllers_obj, inputs_obj
    
    elif type=='data_logging':
        object_name = 'data_logging'
        obj = await setup_object(object_name)
            
        return obj
    
    
    elif type=='test':
        # Retrieve or create measurements object
        found, meas_obj = await check_object_in_server(server, 'measurements')
        if found:
            logger.info('Object measurements already exists in server')
        else:
            logger.info('Object measurements not found in server, added')
            meas_obj = await server.nodes.objects.add_object(idx, 'measurements')

        # Retrieve or create inputs object
        found, inputs_obj = await check_object_in_server(server, 'inputs')
        if found:
            logger.info('Object inputs already exists in server')
        else:
            logger.info('Object inputs not found in server, added')
            inputs_obj = await server.nodes.objects.add_object(idx, 'inputs')

        return meas_obj, inputs_obj
    
    elif type=='signal_generator':
        object_name = 'signal_generator'
        obj = await setup_object(object_name)
            
        return obj
    
    elif type=='finite_state_machines':
        object_name = 'finite_state_machines'
        obj = await setup_object(object_name)
            
        return obj
    
    else: raise ValueError(f'Type {type} not recognized')

def readValuesDA(client, group, initial_read=False):
    """Function that reads a group of tags from an OPC DA server

    Args:
        client (_type_): _description_
        group (_type_): _description_
    """
        
    if initial_read:
        try:
            # print(f'Tag {uniqueGroupValue}: {group["opcTag_list"]}')
            # Add values and time fields to each variable
            [group["measurements"][varName].update({'values':deque(maxlen=maxLen), 'time':deque(maxlen=maxLen)}) for varName in group["measurements"].keys()]

            varIdx = 0
            # for name, value, quality, time in client.iread(group["opcTag_list"], timeout=10, group=group["name"]):
            for name, value, quality, time in client.iread(group["opcTag_list"], timeout=10, group=group["name"]):
                # print(f'Tag {group["name"]} - {group["sensorId_list"][varIdx]}: {value}')
                if value is not None: group["measurements"][group["varId_list"][varIdx]]["values"].append(value)
                if time is not None:  group["measurements"][group["varId_list"][varIdx]]["time"].append(time)
                
                varIdx += 1
        except Exception:
            logger.error(f'Error en lectura inicial del grupo {group["name"]}')
    else:
        varIdx = 0
        try:
            client.info()
        except Exception as e:
            # initial_read = True
            logger.warning(f'OPC Server error, reconnecting: {e}')
            client.connect(config['servidor']['server'], config['servidor']['host'])
            for name, value, quality, time in client.iread(group["opcTag_list"], timeout=10, group=group["name"]):
                # print(f'Tag {group["name"]} - {group["sensorId_list"][varIdx]}: {value}')
                if value is not None: group["measurements"][group["varId_list"][varIdx]]["values"].append(value)
                if time is not None:  group["measurements"][group["varId_list"][varIdx]]["time"].append(time)
                
                varIdx += 1
                
        else:
           
            for name, value, quality, time in client.iread(group=group["name"], timeout=10):
                print(f'Tag {group["name"]} - {group["sensorId_list"][varIdx]}: {value}')
                if value is not None: group["measurements"][group["varId_list"][varIdx]]["values"].append(value)
                if time is not None:  group["measurements"][group["varId_list"][varIdx]]["time"].append(time)
                
                varIdx += 1
        # except Exception:
        #         logger.error(f'Error en lectura del grupo {group["name"]}')
                                
    return group

def findNodes(opc_client, var_list, return_node_structure=False, node_structure=[], find_objects=False):
    """ Function that looks for a node in all the objects
        of the server. Sync version"""
    
    if node_structure:
        objs = node_structure
    else:
        objects_node = opc_client.nodes.objects
        
        object_nodes = objects_node.get_children()
        objs_idx = len(object_nodes)-1

        objs = [{} for _ in range(len(object_nodes))]
        
        while objs_idx>=0:
            # logger.info(f'Looking in folder {object_nodes[objs_idx].read_browse_name()}')
            nodes = object_nodes[objs_idx].get_children()
            for node in nodes:
                name = node.read_browse_name()
                objs[objs_idx].update({name.Name:node})
            
            objs_idx = objs_idx-1
    
    var_nodes = []
    for var_name in var_list:
        nodeNotFound = True
        for obj_node in objs:
            if var_name in obj_node.keys():
                var_nodes.append(obj_node[var_name])
                nodeNotFound = False
                logger.info(f'Node found for {var_name}: {obj_node[var_name]}')

                
        if nodeNotFound:
            var_nodes.append('')
            logger.error(f'Node for variable {var_name} could not be found on server')

    if return_node_structure: return objs
    else: return var_nodes

def findNode(opc_client, varToFind):
    """ Function that looks for a node in all the objects
        of the server. Replaced by findNodes """
    
    nodeNotFound = True
    objects_node = opc_client.nodes.objects
    
    object_nodes = objects_node.get_children()
    objs_idx = len(object_nodes)-1

    while nodeNotFound and objs_idx>=0:
        # logger.info(f'Looking in folder {object_nodes[objs_idx].read_browse_name()}')
        nodes = object_nodes[objs_idx].get_children()
        for node in nodes:
            name = node.read_browse_name()
            if name.Name == varToFind:
                varNode = node
                nodeNotFound = False
        
        objs_idx = objs_idx-1
                
    if nodeNotFound:
        logger.error(f'Node for variable {varToFind} could not be found on server')
        return None
    else:
        logger.info(f'Node found for {varToFind}: {varNode}')
        return varNode
    
def findNodes_sync(opc_client, var_list, object='', folder='', node_structure=[], log=True):
    """ Function that looks for a node in all or specific objects
        in a OPC server. Async version 
        
        opc_client: OPC client object
        var_list: list of variables to find
        node_structure: node structure of the server, if not provided, it will be retrieved
        object: name of the object to look in, if empty, all objects are searched
        folder: name of the folder to look in, if empty, all folders are searched
            
    """
    
    if folder and not object:
        raise ValueError('Folder specified but no object, if folder specified, parent object is requiered')
                    
    if node_structure:
        server_structure = node_structure
    else:
        server_structure = get_server_structure_sync(opc_client, log=False)
         
         
    if object:
        if log: logger.info(f'Object {object} specified, looking only in that object')
        if object not in server_structure:
            raise RuntimeError(f'Object {object} not found in server')
        else:
            if folder:
                if folder not in server_structure[object]['children']:
                    raise RuntimeError(f'Folder {folder} not found in object {object}')
            elif var_list[0] == object:
                return [server_structure[object]]
            else: # Con esto, si hay nodos con el mismo nombre en varios objetos se coge el primero, si se especifica un objeto es porque se quiere que se busque sólo en ese objeto
                # objects = server_structure
                
                # Alternativa
                objects = {object: server_structure[object]}
    else:
        objects = server_structure
         
    # Make sure it is a list
    # if not isinstance(objects, list): objects = [objects]
    # Find the nodes in the list of objs
    var_nodes = []
    for var_name in var_list:
        nodeNotFound = True

        if folder:
            obj_children = objects['children'][folder]['children']
            if var_name in obj_children:
                var_nodes.append(obj_children[var_name]['node'])
                nodeNotFound = False
        else:
            for obj in objects:
                obj_children = objects[obj]['children']
                    
                if var_name in obj_children:
                    var_nodes.append(obj_children[var_name]['node'])
                    nodeNotFound = False
                    break
                else:
                    for obj_children_child in obj_children:
                        if 'children' in obj_children[obj_children_child]: # Folder
                            if var_name in obj_children[obj_children_child]['children']:
                                var_nodes.append(obj_children[obj_children_child]['children'][var_name]['node'])
                                nodeNotFound = False
                                break

                    # Node not found in this object
        
        if nodeNotFound:
            var_nodes.append([])
            logger.error(f'Node for variable {var_name} could not be found on server')

    return var_nodes

async def check_object_in_server(opc_client, object_name):
    """ Function that checks if an object exists in the server """
    
    objects_node = opc_client.nodes.objects
    
    object_nodes = await objects_node.get_children()
    for obj_node in object_nodes:
        name  = await obj_node.read_browse_name()
        if name.Name == object_name:
            return True, obj_node
    
    # Object not found
    return False, []

async def check_folder_in_server(opc_client, object_name, folder_name):
    """ Function that checks if a folder exists in an object of the server """
    
    objects_node = opc_client.nodes.objects
        
    found=False
    for obj_node in await objects_node.get_children():
        if obj_node.read_browse_name().Name == object_name:
            found=True
            obj_node = obj_node
            break
    
    # Object not found
    if not found: raise RuntimeError(f'Object {object_name} not found in server')

    for fld_node in await obj_node.get_children():
        if fld_node.read_browse_name().Name == folder_name:
            return True, fld_node
    
    return False, []

def get_server_structure_sync(opc_client, log=False):
        
    """ Function that returns the structure of the server in a dictionary.
    - Duplicates IDs are allowed as long as not within the same tree level
    - Three levels of hierarchy are supported (user_object - folder - variable)
    - Structure expected for the OPC server: 
    
        root
            - Types
            - Views
            - Objects
                - Aliases
                - Server
                - user_object_1
                - user_object_N
                    - user_variable_1
                    - user_variable_N
                    - user_folder_1
                        - user_variable_N+1
                        - user_variable_N+M
                    - user_folder_N
                        - user_variable_N+M+1
                        - user_variable_N+M+K
                    
        Example:
            - Types
            - Views
            - Objects
                - Aliases
                - Server
                - measurements
                    - TT-DES-001  (float)
                    - TT-DES-002  (float)
                    - HW1TT21     (float)
                    - PT-DES-001  (float)
                    - PT-DES-002  (float)
                    - FT-DES-001  (float)
                    - FT-AQU-101a (float)
                    
                - inputs
                    - FT-AQU-101a (float)
                    - ZT-AQU-101a (float)
                    - FF-AQU-101a (float)
                    
                - controllers
                    - hot_water_temp_controller
                        - online: (boolean)
                        - active: (boolean)
                        - Kp: (float)
                        - Ki: (float)
                        - input_id: ZT-AQU-101a (string)
                        - output_id: HW1TT21 (string)
                    - hot_water_flow_controller
                        - online: (boolean)
                        - active: (boolean)
                        - Kp: (float)
                        - Ki: (float)
                        - input_id:  FF-AQU-101a (string)
                        - output_id: FT-AQU-101a (string)
    """
    
    object_nodes = opc_client.nodes.objects.get_children()

    objs = {}
    
    for obj in object_nodes:
        name = obj.read_browse_name()
        if name.Name != 'Server' and name.Name != 'Aliases':
            objs[name.Name] = {'name':name.Name, 'node':obj}
        
    for obj in objs:     # Look in each object or the one specfied (measurements, inputs, controllers, etc)
        obj_children = objs[obj]['node'].get_children()
        objs[obj]['children'] = {}
        for obj_child in obj_children:
            obj_child_name = obj_child.read_browse_name()
            objs[obj]['children'][obj_child_name.Name] = {'name':obj_child_name.Name, 'node':obj_child}
            
            obj_child_children = obj_child.get_children()
            if len(obj_child_children)>0: # Folder
                objs[obj]['children'][obj_child_name.Name]['children'] = {}
                for obj_child_child in obj_child_children:
                    obj_child_child_name = obj_child_child.read_browse_name()
                    objs[obj]['children'][obj_child_name.Name]['children'][obj_child_child_name.Name] = {'name':obj_child_child_name.Name, 'node':obj_child_child}                
                
    # if log: pprint(objs)
        
    return objs
    
async def get_server_structure(opc_client, log=False):
        
    """ Function that returns the structure of the server in a dictionary.
    - Duplicates IDs are allowed as long as not within the same tree level
    - Three levels of hierarchy are supported (user_object - folder - variable)
    - Structure expected for the OPC server: 
    
        root
            - Types
            - Views
            - Objects
                - Aliases
                - Server
                - user_object_1
                - user_object_N
                    - user_variable_1
                    - user_variable_N
                    - user_folder_1
                        - user_variable_N+1
                        - user_variable_N+M
                    - user_folder_N
                        - user_variable_N+M+1
                        - user_variable_N+M+K
                    
        Example:
            - Types
            - Views
            - Objects
                - Aliases
                - Server
                - measurements
                    - TT-DES-001  (float)
                    - TT-DES-002  (float)
                    - HW1TT21     (float)
                    - PT-DES-001  (float)
                    - PT-DES-002  (float)
                    - FT-DES-001  (float)
                    - FT-AQU-101a (float)
                    
                - inputs
                    - FT-AQU-101a (float)
                    - ZT-AQU-101a (float)
                    - FF-AQU-101a (float)
                    
                - controllers
                    - hot_water_temp_controller
                        - online: (boolean)
                        - active: (boolean)
                        - Kp: (float)
                        - Ki: (float)
                        - input_id: ZT-AQU-101a (string)
                        - output_id: HW1TT21 (string)
                    - hot_water_flow_controller
                        - online: (boolean)
                        - active: (boolean)
                        - Kp: (float)
                        - Ki: (float)
                        - input_id:  FF-AQU-101a (string)
                        - output_id: FT-AQU-101a (string)
    """
    
    object_nodes = await opc_client.nodes.objects.get_children()

    objs = {}
    
    for obj in object_nodes:
        name = await obj.read_browse_name()
        if name.Name != 'Server' and name.Name != 'Aliases':
            objs[name.Name] = {'name':name.Name, 'node':obj}
        
    for obj in objs:     # Look in each object or the one specfied (measurements, inputs, controllers, etc)
        obj_children = await objs[obj]['node'].get_children()
        objs[obj]['children'] = {}
        for obj_child in obj_children:
            obj_child_name = await obj_child.read_browse_name()
            objs[obj]['children'][obj_child_name.Name] = {'name':obj_child_name.Name, 'node':obj_child}
            
            obj_child_children = await obj_child.get_children()
            if len(obj_child_children)>0: # Folder
                objs[obj]['children'][obj_child_name.Name]['children'] = {}
                for obj_child_child in obj_child_children:
                    obj_child_child_name = await obj_child_child.read_browse_name()
                    objs[obj]['children'][obj_child_name.Name]['children'][obj_child_child_name.Name] = {'name':obj_child_child_name.Name, 'node':obj_child_child}                
                
    if log: pprint(objs)
        
    return objs
        
async def async_findNodes(opc_client, var_list, object='', folder='', node_structure=[], log=True):
    """ Function that looks for a node in all or specific objects
        in a OPC server. Async version 
        
        opc_client: OPC client object
        var_list: list of variables to find
        node_structure: node structure of the server, if not provided, it will be retrieved
        object: name of the object to look in, if empty, all objects are searched
        folder: name of the folder to look in, if empty, all folders are searched
            
    """
    
    if folder and not object:
        raise ValueError('Folder specified but no object, if folder specified, parent object is requiered')
                    
    if node_structure:
        server_structure = node_structure
    else:
        server_structure = await get_server_structure(opc_client, log=False)
         
        # if object:
        #     if object not in objs.keys():
        #     raise RuntimeError(f'Object {object} not found in server')
        # else:
        #     objects = objs[object]['node']
        #     if log: logger.info(f'Object {object} specified, looking only in that object')
         
        #     if folder:
        #         if folder not in objs[obj]['children'].keys():
        #         raise RuntimeError(f'Folder {folder} not found in object {object}')
        #     else:
        #         obj_children = objs[obj]['children'][folder]['node']
        #         if log: logger.info(f'Folder {folder} specified, looking only in that folder')
        # else:
        #     obj_children = objs[obj]['children']
         
         
    if object:
        if object not in server_structure:
            raise RuntimeError(f'Object {object} not found in server')
        else:
            if folder:
                if folder not in server_structure[object]['children']:
                    raise RuntimeError(f'Folder {folder} not found in object {object}')
                else: objects = [server_structure[object]]
            elif var_list[0] == object:
                return [server_structure[object]]
            else: # Con esto, si hay nodos con el mismo nombre en varios objetos se coge el primero, si se especifica un objeto es porque se quiere que se busque sólo en ese objeto
                # objects = server_structure
                
                # Alternativa
                objects = {object: server_structure[object]}
    else:
        objects = server_structure
        if log: logger.info(f'Object {object} specified, looking only in that object')
         
    # Make sure it is a list
    # if not isinstance(objects, list): objects = [objects]
    # Find the nodes in the list of objs
    var_nodes = []
    for var_name in var_list:
        nodeNotFound = True

        for obj in objects:
            if folder:
                if 'children' in obj['children'][folder]:
                    obj_children = obj['children'][folder]['children'] 
                else: break
            else:
                obj_children = objects[obj]['children']
                
            if var_name in obj_children:
                var_nodes.append(obj_children[var_name]['node'])
                nodeNotFound = False
                break
            else:
                for obj_children_child in obj_children:
                    if 'children' in obj_children[obj_children_child]: # Folder
                        if var_name in obj_children[obj_children_child]['children']:
                            var_nodes.append(obj_children[obj_children_child]['children'][var_name]['node'])
                            nodeNotFound = False
                            break

                # Node not found in this object
       
        if nodeNotFound:
            var_nodes.append([])
            logger.error(f'Node for variable {var_name} could not be found on server')

    return var_nodes

async def async_findNode(opc_client, varToFind):
    """ Function that looks for a node in all the objects
        of the server """
    
    nodeNotFound = True
    objects_node = opc_client.nodes.objects
    
    object_nodes = await objects_node.get_children()
    objs_idx = len(object_nodes)-1

    while nodeNotFound and objs_idx>=0:
        # logger.info(f'Looking in folder {object_nodes[objs_idx].read_browse_name()}')
        nodes = await object_nodes[objs_idx].get_children()
        for node in nodes:
            name = await node.read_browse_name()
            if name.Name == varToFind:
                varNode = node
                nodeNotFound = False
        
        objs_idx = objs_idx-1
                
    if nodeNotFound:
        logger.error(f'Node for variable {varToFind} could not be found on server')
        return None
    else:
        logger.info(f'Node found for {varToFind}: {varNode}')
        return varNode

def opcda_server_configuration(config, groups):    
    
    client = OpenOPC.client()
    print(client.servers(config['servidor']['host']))
    client.connect(config['servidor']['server'], config['servidor']['host'])

    print(client.info())
    
    # Create tag list for each group
    for grpIdx in range(len(groups)):
        tags = ['*.' + tag for tag in groups[grpIdx]["sensorId_list"]]
        tags = client.list(tags, recursive=True, flat=True)
        groups[grpIdx]["opcTag_list"] = tags
    
    # Create groups in opcda server and perform initial read
        groups[grpIdx] = readValuesDA(client=client, group=groups[grpIdx], initial_read=True)

    return client, groups

def opcua_server_configuration(config, groups=[], log=True, initial_attempt=True, role='user', secure=True, local=False, docker=False):

    # Connection to OPC server
    # async with Client(url=url) as opc_client:
    if docker:
        url = config["ua_parameters"]["url_docker"]
    elif local:
        url=config["ua_parameters"]["url_local"] 
    else:
        url=config["ua_parameters"]["url"] 
    
    opc_client = extendedClient(url)
    
    if secure:
        opc_client.set_security(
            SecurityPolicyBasic256Sha256,
            certificate=config['ua_parameters'][f'client_certificate_{role}'],
            private_key=config['ua_parameters'][f'client_private_key_{role}'],
            server_certificate=config['ua_parameters']['server_certificate']
        )
    else:
        if role=='admin': opc_client.set_user('admin')
    
    try:
        opc_client.connect()
        logger.info(f'Connected to OPC UA server: {url} with role {role}')
    except OSError as e:
        if log: logger.error(f'Connection attemp to OPC UA server failed: {e}')
        raise e
    
    # Setup OPC server
    # idx = opc_client.get_namespace_index('Servidor de prueba')
    opc_client.load_data_type_definitions()
    node_structure = get_server_structure_sync(opc_client, log=log)
    
    if role=='user':
        maxLen = config["monitoring"]["maxLen"]
        
        # Create tag list for each group
        # node_structure = findNodes(opc_client=opc_client, var_list=[], return_node_structure=True)
        for grpIdx in range(len(groups)):
            # tags = ['*.' + tag for tag in groups[grpIdx]["sensorId_list"]]
            # tags = opc_client.list(tags, recursive=True, flat=True)
            # groups[grpIdx]["opcTag_list"] = tags
            if groups[grpIdx]['name'] == 'inputs':
                object = 'inputs'    
            else:
                object = 'measurements'
                
            nodes = findNodes_sync(opc_client=opc_client, var_list=groups[grpIdx]['sensorId_list'], 
                                   object=object, node_structure=node_structure)
            # nodes = findNodes(opc_client=opc_client, var_list=groups[grpIdx]['sensorId_list'], node_structure=node_structure)
            groups[grpIdx]["opcTag_list"] = [node.__str__() if node else [] for node in nodes ] # Store string of node
            
            # Check if any node was found
            if not any( [False if item=='[]' else True for item in groups[grpIdx]['opcTag_list']] ):
                groups[grpIdx]['opcTag_list'] = [[] for _ in range(len(groups[grpIdx]['sensorId_list']))]
                logger.error(f'No nodes found for {groups[grpIdx]["name"]}')
            
            if groups[grpIdx]['sensorId_list'].__len__() != groups[grpIdx]["opcTag_list"].__len__(): 
                raise Exception('Found tags in opc server with duplicated names, make sure they are unique before continuing')
            
            # For each variable in the group
            # groups[grpIdx]["opcTag_list"] = []
            for var_idx in range(len(groups[grpIdx]["opcTag_list"])):
                var_name = groups[grpIdx]["varId_list"][var_idx]
                
                # # Get node
                # varNode = await async_findNode(opc_client=opc_client, varToFind=groups[grpIdx]["measurements"][varName]["sensor_id"])
                # groups[grpIdx]["measurements"][varName]["node"] = varNode
                # groups[grpIdx]["opcTag_list"].append(varNode)
                groups[grpIdx]["measurements"][var_name]["node"] = groups[grpIdx]["opcTag_list"][var_idx]
                
                if initial_attempt:
                    # Add values and time fields
                    groups[grpIdx]["measurements"][var_name].update({'values':deque(maxlen=maxLen), 
                                                                'time':deque(maxlen=maxLen)})
            if initial_attempt:
                # Perform initial read
                groups[grpIdx] = readValuesUA(opc_client, group=groups[grpIdx], initial_read=True)
                
            # Create new groups in dict format
            read_groups = {}
            for grp in groups:
                grp_name = grp['name']
                read_groups[grp_name] = grp
        
        return opc_client, groups, read_groups
        
    elif role=='admin':
        """Loop dictionary structure:
            (inherited from config - control)
            id
            title
            description
            system_diagram
            inputs
            input_id
            output_id
            setpoint_id
            type
            controller_diagram
            input 
                (generated here) node, value 
                (inherited from config - inputs) var_id, input_id, type, unit, description, subsystem 
            output
                (generated here) node, value
                (inherited from config - measurements) var_id, sensor_id, type, unit, description, group
            setpoint
                (generated here) node, value
                (inherited from config - inputs) var_id, input_id, type, unit, description, subsystem

        Raises:
            RuntimeError: If node for control loop not found in opc server

        Returns:
            opc_client: OPC client instance with admin rights 
            groups: List of dictionaries with categorized measurements
            loops: List of dictionaries with control loops
        """
        
        # if not secure: opc_client.set_user('admin')

        # opc_client = []
        # groups = []
        # # Create tag list for each input        
        groups = dict(); groups['opcTag_list'] = []

        for input in config['inputs']:
            input = config['inputs'][input]
            # print(input['input_id'])
            nodes = findNodes_sync(opc_client=opc_client, var_list=[input['input_id']], 
                                   object='inputs', node_structure=node_structure)
            # nodes = findNodes(opc_client=opc_client, var_list=[input['input_id']], node_structure=node_structure)
            groups[input['var_id']] = input
            groups[input['var_id']]['node'] = nodes[0].__str__() if nodes[0] else []
            if initial_attempt: # Perform initial read
                groups[input['var_id']]['value'] = nodes[0].read_value() if nodes[0] else []
                
            groups['opcTag_list'].append( nodes[0].__str__() if nodes[0] else [])
            # groups[input['var_id']]['node'] = nodes[0].__str__()
        #     groups[grpIdx]["opcTag_list"] = [node.__str__() for node in nodes] # Store string of node
    
        
        loops = {}
        for loop_id in config['control']:
            try:
                loop = config['control'][loop_id]
                
                # Control variables
                for var, field in zip(['input_id', 'output_id', 'setpoint_id'], ['input', 'output', 'setpoint']):
                    
                    
                    # loop[field]['node'] = groups[loop[var]]['node']
                    
                    if field == 'output': 
                        id = config["measurements"][loop[var]]['sensor_id']
                        loop[field] = config['measurements'][loop[var]]
                    else: 
                        id = config["inputs"][loop[var]]['input_id']
                        loop[field] = config['inputs'][loop[var]]
                    
                    nodes = findNodes_sync(opc_client=opc_client, var_list=[id], node_structure=node_structure)          
                    # nodes = findNodes(opc_client=opc_client, var_list=[loop[var]], node_structure=node_structure)
                    loop[field]['node'] = nodes[0].__str__() if nodes[0] else []
                    
                    # Get initial values
                    if initial_attempt:
                        loop[field]['value'] = nodes[0].read_value() if nodes[0] else []
                    
                # State variables
                # loop['online'] = {}; loop['active'] = {}
                # nodes = findNodes_sync(opc_client=opc_client, var_list=['online', 'active'], 
                #                        object='controllers', folder=loop['id'], node_structure=node_structure)
                # loop['online']['node'] = nodes[0].__str__() if nodes[0] else []
                # loop['active']['node'] = nodes[1].__str__() if nodes[1] else []
                
                # State variables and controller parameters
                controller_node = findNodes_sync(opc_client=opc_client, var_list=[loop['id']], 
                                                 object='controllers', node_structure=node_structure)
                if not controller_node[0]:
                    raise RuntimeError(f'Controller {loop["id"]} not found in OPC server')
                
                for var_node in controller_node[0].get_children():
                    var_name = var_node.read_browse_name().Name
                    loop[var_name] = {}
                    loop[var_name]['node']  = var_node.__str__()
                    if initial_attempt:
                        loop[var_name]['value'] = var_node.read_value()
                
                check_var = 'online'
                if check_var not in loop:
                    logger.error(f'Variable {check_var} not found in controller configuration {loop["id"]}')
                
                check_var = 'active'
                if check_var not in loop:
                    logger.error(f'Variable {check_var} not found in controller configuration {loop["id"]}') 
                       
                # State variables
                # loop['online'] = {}; loop['active'] = {}
                # nodes = findNodes_sync(opc_client=opc_client, var_list=['online', 'active'], 
                #                        object='controllers', folder=loop['id'], node_structure=node_structure)
                # loop['online']['node'] = nodes[0].__str__() if nodes[0] else []
                # loop['active']['node'] = nodes[1].__str__() if nodes[1] else []
                
                # Get initial values
                # if initial_attempt:
                #     loop['online']['value'] = opc_client.read_value(nodes[0]) if nodes[0] else []
                #     loop['active']['value'] = opc_client.read_value(nodes[1]) if nodes[1] else []
                
                loop['available'] = True
                
                loops[loop_id] = loop
            

            except Exception as e:
                logger.error(f'Error in loop {loop_id} in control: {e}')
                loop['available'] = False
                loops[loop_id] = loop
            
        return opc_client, groups, loops
            
    else: raise ValueError(f'Invalid role: {role}')
           
async def async_opcua_server_configuration(config, groups, consisting_server_time=False):
    maxLen = config["monitoring"]["maxLen"]
    
    # Connection to OPC server
    # async with Client(url=url) as opc_client:
    opc_client = async_extendedClient( url=config["servidor"]["url_ua"] )
    await opc_client.connect()
    logger.info(f'Connected to OPC UA server: {config["servidor"]["url_ua"]}')

    # Setup OPC server
    # idx = opc_client.get_namespace_index('Servidor de prueba')
    await opc_client.load_data_type_definitions()
    
    # Create tag list for each group
    node_structure = await async_findNodes(opc_client=opc_client, var_list=[], return_node_structure=True)
    for grpIdx in range(len(groups)):
        # tags = ['*.' + tag for tag in groups[grpIdx]["sensorId_list"]]
        # tags = opc_client.list(tags, recursive=True, flat=True)
        # groups[grpIdx]["opcTag_list"] = tags
        groups[grpIdx]["opcTag_list"] = await async_findNodes(opc_client=opc_client, var_list=groups[grpIdx]['sensorId_list'], node_structure=node_structure)
        
        # For each variable in the group
        # groups[grpIdx]["opcTag_list"] = []
        for var_idx in range(len(groups[grpIdx]["opcTag_list"])):
            var_name = groups[grpIdx]["varId_list"][var_idx]
            
            # # Get node
            # varNode = await async_findNode(opc_client=opc_client, varToFind=groups[grpIdx]["measurements"][varName]["sensor_id"])
            # groups[grpIdx]["measurements"][varName]["node"] = varNode
            # groups[grpIdx]["opcTag_list"].append(varNode)
            groups[grpIdx]["measurements"][var_name]["node"] = groups[grpIdx]["opcTag_list"][var_idx]
            
            
            # Add values and time fields
            groups[grpIdx]["measurements"][var_name].update({'values':deque(maxlen=maxLen), 
                                                       'source_time':deque(maxlen=maxLen),
                                                       'server_time':deque(maxlen=maxLen)
                                                       })
            
        if consisting_server_time: groups[grpIdx]["time"] = deque(maxlen=maxLen)
        
        # Perform initial read
        groups[grpIdx] = await async_readValuesUA(opc_client, group=groups[grpIdx], initial_read=True)
        
    return opc_client, groups
    
    
