'''
Async MQTT Client library, intended to be used as a Mixin
8/4/2022 V 1.0.0 N Waterton - Initial Release
26/5/2022 V 1.0.1 N Waterton - Bug fixes
9/8/2025 V 3.0.0 N Waterton - updated to use aiomqtt, added signals, fixed bugs, tidy up and make into package
'''
import re
from ast import literal_eval
import logging
from signal import SIGINT, SIGTERM
import asyncio

import aiomqtt

__version__ = "3.0.0"

class Will:
    
    def __init__(self, topic=None, payload=None, qos=0, retain=False, properties=None):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retain=retain
        self.properties = properties

class MQTTMixin():
    '''
    Async MQTT Mixin client intended to be used as a mixin
    all methods not starting with '_' can be sent as commands to MQTT topic
    feedback is published to pubtopic plus name (if given)
    '''
    __version__ = __version__
    invalid_commands = ['start', 'stop', 'mqtt_stop', 'subscribe', 'unsubscribe', '']
    
    def __init__(self, *args, **kwargs):
        self._log = kwargs.pop("log", None) or logging.getLogger('Main.'+__class__.__name__)
        self._debug = self._log.getEffectiveLevel() <= logging.DEBUG if self._log else False
        self._log.info(f'{__class__.__name__} library v{__class__.__version__}')
        self._broker = kwargs.pop("ip", None)
        self._port = kwargs.pop("port", 1883)
        self._user = kwargs.pop("user", None)
        self._password = kwargs.pop("mqtt_password", None)
        self._pubtopic = kwargs.pop("pubtopic", 'default')
        topic = kwargs.pop("topic", '/default/#')
        self._topic = topic if not topic.endswith('/#') else topic[:-2]
        self._name = kwargs.pop("name", None)
        self._json_out = kwargs.pop("json_out", False)
        self._polling_config = kwargs.pop("poll", None)
        self._will = kwargs.pop("will", None) or Will(topic=self._get_pubtopic('status'), payload="Offline", qos=0, retain=False)
        self._polling = []
        self._poll = None
        self._mqttc = None
        self._method_dict = {}  #filled in later
        self._delimiter = r'\='
        self._topic_override = None
        self._exit = False
        self._history = {}
        self._tasks = set()
        self._publish_tasks = set()
        #set up exit signals
        self.add_signals()
        self._super_init(*args, **kwargs)
        
        self._loop = asyncio.get_running_loop()

        if self._broker is not None:
            self._add_task(self._connect_client())
            self._add_task(self._built_method_dict())
            
    def add_signals(self):
        '''
        setup signals to exit program
        '''
        try:    #might not work on windows
            self._loop.add_signal_handler(SIGINT, self.mqtt_stop)
            self._loop.add_signal_handler(SIGTERM, self.mqtt_stop)
        except Exception:
            self._log.warning('signal error')
                
    def _super_init(self, *args, **kwargs):
        '''
        init super classes if they are not object
        '''
        classes_in_order = self.__class__.__mro__
        i = classes_in_order.index(super().__thisclass__)
        if not classes_in_order[i+1].__init__ == object.__init__:
            super().__init__(*args, **kwargs)
        
    async def _built_method_dict(self):
        '''
        builds the callable methods dict
        need a retry delay, to allow the super class to initialize first
        '''
        await asyncio.sleep(5)  #wait for super class to initialize
        #updated from
        #self._method_dict = {func:getattr(self, func) for func in dir(self) if callable(getattr(self, func)) and not func.startswith("_")}
        retry = 5
        for x in range(retry):
            error = False
            for func in dir(self):
                if not func.startswith('_') and func not in self._method_dict.keys() and func not in self.invalid_commands:
                    try:
                        _func = getattr(self, func)
                        if callable(_func):
                            self._method_dict[func] = _func
                            self._log.debug('Added callable function: {}'.format(func))
                    except Exception as e:
                        self._log.debug('Initialization error: {}, retry: {}'.format(func, x))
                        if x < retry-1:
                            error = True
                            await asyncio.sleep(5)
                            break
                        self._log.debug('No More retries, continuing')
            if not error:
                break
                
        self._log.debug('Callable functions initialized')             
        self._setup_polling()
        
    def _setup_polling(self):            
        if self._polling_config:
            self._log.debug('Setting up Polling')
            self._polling = [p for p in self._polling_config[1:] if p in self._method_dict.keys()]
            if self._polling:
                self._poll = self._polling_config[0] if self._polling_config[0] > 0 else None
        if self._poll:
            self._add_task(self._poll_status())
        else:
            self._log.debug('Polling is disabled')
                
    def _add_task(self, callback):
        '''
        add callback task to self._tasks and run as a background task
        '''
        try:
            #task = asyncio.create_task(callback)
            task = self._loop.create_task(callback)
            self._tasks.add(task)
            task.add_done_callback(self._tasks.remove)
            return task
        except Exception as e:
            self._log.warning('task error: {}'.format(e))
        return None
        
    def _add_publish_task(self, publish):
        '''
        add publish task to self._publish_tasks and run as a background task
        '''
        try:
            #task = asyncio.create_task(self._async_publish(topic, message))
            task = self._loop.create_task(publish)
            self._publish_tasks.add(task)
            task.add_done_callback(self._publish_tasks.remove)
            return task
        except Exception as e:
            self._log.exception(e)
            self._log.warning('publish task error: {}'.format(e))
        return None
                
    async def _connect_client(self):
        '''
        main loop, will automatically reconnect to MQTT broker after 5 second timeout on disconnect
        processes messages in a seperate task
        '''
        while not self._exit:
            try:
                # connect to broker
                self._log.info('Connecting to MQTT broker: {}'.format(self._broker))
                async with aiomqtt.Client(self._broker,
                                          port=self._port,
                                          username=self._user,
                                          password=self._password,
                                          will=self._will,
                                          logger=self._log) as self._mqttc:
                    await self._on_connect()
                    async for message in self._mqttc.messages:
                        self._add_task(self._process_mqtt_message(message))

            except aiomqtt.MqttError:
                self._log.warning(f"Connection lost; Reconnecting in 5 seconds ...")
            except asyncio.CancelledError:
                self._log.info('cancelled')
                break
            except Exception as e:
                self._log.exception("Connection error: {}".format(e))
            await asyncio.sleep(5)
        self._log.info('exited')
        
    async def subscribe(self, topic, qos=0):
        '''
        utiltity to subscribe to an MQTT topic
        '''
        if self._MQTT_connected:
            topic = topic.replace('//','/')
            self._log.info('subscribing to: {}'.format(topic))
            await self._mqttc.subscribe(topic, qos)
            
    async def unsubscribe(self, topic):
        '''
        utiltity to unsubscribe from an MQTT topic
        '''
        if self._MQTT_connected:
            topic = topic.replace('//','/')
            self._log.info('unsubscribing from: {}'.format(topic))
            await self._mqttc.unsubscribe(topic)
        
    @property
    def _MQTT_connected(self):
        return bool(self._mqttc._client.is_connected() if self._mqttc else False)
        
    async def _waitForMQTT(self, timeout=60):
        '''
        Utility to wait for MQTT connection, with optional timeout
        returns false if not broker defined
        '''
        if self._broker:
            count = 0
            while not self._MQTT_connected and (count:= count+1) < timeout:
                await asyncio.sleep(1)
        return self._MQTT_connected
        
    async def _on_connect(self):
        '''
        on connection to the broker, subscribes to
        self._topic/all/# and 
        self._topic/self._name/# if name is defined in self._name or
        self._topic/# if self._name not defined
        '''
        self._log.info('MQTT broker connected')
        await self.subscribe('{}/all/#'.format(self._topic))
        if self._name:
            await self.subscribe('{}/{}/#'.format(self._topic, self._name))
        else:
            await self.subscribe('{}/#'.format(self._topic))
        self._history = {}
        await self._async_publish('status', 'Online')
        
    async def _process_mqtt_message(self, msg):
        '''
        processes a received message from self._topic subscription
        '''
        #self._log.info(msg.topic + ": " + str(msg.payload))
        try:
            command, args = self._get_command(msg)
            await self._publish_command(command, args)
            
        except asyncio.CancelledError:
            self._log.info('cancelled')
        except Exception as e:
            self._log.exception(e)
        
    def _get_pubtopic(self, topic=None):
        '''
        returns the topic to publish to depending on if self._name is defined or not
        '''
        pubtopic = self._pubtopic
        if self._name:
            pubtopic = '{}/{}'.format(pubtopic,self._name)
        if topic:
            pubtopic = '{}/{}'.format(pubtopic, topic)
        return pubtopic
        
    def _publish(self, topic=None, message=None):
        '''
        runs self._publish as a task
        '''
        self._add_publish_task(self._async_publish(topic, message))
            
    async def _async_publish(self, topic=None, message=None):
        '''
        publishes message to topic
        '''
        if topic is None and message is None:
            self._log.debug(f'Not pubishing: {topic}: {message}')
            return
        try:
            if self._MQTT_connected:
                pubtopic = self._get_pubtopic(topic)
                self._log.info("publishing item: {}: {}".format(pubtopic, message))
                await self._mqttc.publish(pubtopic, str(message))
                self._log.debug('message item: {}: {} published'.format(pubtopic, message))
            else:
                self._log.warning(f'MQTT not connected - not publishing {topic}: {message}')
        except Exception as e:
            self._log.exception(e)
            
    async def _poll_status(self):
        '''
        publishes commands in self._polling every self._poll seconds
        '''
        try:
            while not self._exit:
                await asyncio.sleep(self._poll)
                self._log.info('Polling...')
                for cmd in self._polling:
                    if cmd in self._method_dict.keys():
                        result = await self._method_dict[cmd]()
                        if self._json_out or not isinstance(result, dict):
                            await self._async_publish(cmd, result)
                        else:
                            self._decode_topics(result)
                    else:
                        self._log.warning('Polling command: {cmd} not found')
        except asyncio.CancelledError:
            pass
        self._log.info('Poll loop exited')
               
    def _decode_topics(self, state, prefix=None, override=False):
        '''
        decode json data dict, and _publish as individual topics to
        brokerFeedback/topic the keys are concatenated with _ to make one unique
        topic name strings are expressly converted to strings to avoid unicode
        representations
        '''
        for k, v in state.items():
            if isinstance(v, dict):
                if prefix is None:
                    self._decode_topics(v, k, override=override)
                else:
                    self._decode_topics(v, '{}_{}'.format(prefix, k), override=override)
            else:
                if isinstance(v, list):
                    newlist = []
                    for i in v:
                        if isinstance(i, dict):
                            for ki, vi in i.items():
                                newlist.append((str(ki), vi))
                        else:
                            newlist.append(str(i))
                    v = newlist
                if prefix is not None:
                    k = '{}_{}'.format(prefix, k)
                 
                if override or self._has_changed(k, v):
                    self._publish(k, str(v))
                
    def _has_changed(self, k, v):
        '''
        checks to see if value has changed, returns True/False
        '''
        v = str(v)
        previous = self._history.get(k)
        if previous != v:
            self._history[k] = v
            return True
        return False
                
    def _get_command(self, msg):
        '''
        extract command and args from MQTT msg
        '''
        command = args = None
        topic_command = str(msg.topic).split('/')[-1]
        msg_command = msg.payload.decode('UTF-8')
            
        if topic_command in self._method_dict.keys():
            command = topic_command
            #parse arg
            args = re.split(self._delimiter, msg_command)
            try:
                args = [literal_eval(v) if re.match(r'\[|\{|\(|True|False|\d',v) else v for v in args]
            except Exception as e:
                if self._debug:
                    self._log.warning('error parsing args: {}'.format(e))
                
            args = self._filter_list(args)
             
        elif msg_command in self._method_dict.keys():
            command = msg_command
            
        else:
            #undefined
            cmd = topic_command if topic_command not in [self._name, 'all'] else msg_command
            if cmd not in self.invalid_commands:
                self._log.warning('Received invalid command: {}'.format(cmd))
            
        return command, args
             
    async def _publish_command(self, command, args=None):
        '''
        executes async command with args and publishes the results
        '''
        try:
            value = await self._execute_command(command, args)
        except Exception as e:
            self._log.error(e)
            value = None
            
        if self._topic_override: #override the topic to publish to
            command = self._topic_override
            self._topic_override = None
            
        if self._json_out or not isinstance(value, dict):
            await self._async_publish(command, value)
        else:
            self._decode_topics(value, override=True)
        
    async def _execute_command(self, command, args):
        '''
        execute the command (if any) with args (if any)
        return value received (if any)
        '''
        value = None
        if command:
            if command in self.invalid_commands:
                self._log.warning("can't run {} from MQTT".format(command))
                return None
            try:
                self._log.info('Received command: {}'.format(command))
                self._log.info('args: {}'.format(args))
                
                if args:
                    value = await self._method_dict[command](*args)
                else:
                    value = await self._method_dict[command]()
            except Exception as e:
                self._log.warning('Command error {} {}: {}'.format(command, args, e))
            self._log.debug('return value: {}'.format(value))
        return value
        
    def _filter_list(self, fl):
        '''
        utility function to strip '' out of lists, and trim leading/trailing spaces
        returns filtered list
        '''
        return list(filter(lambda x: (x !=''), [x.strip() if isinstance(x, str) else x for x in fl]))
        
    def mqtt_stop(self):
        asyncio.create_task(self._mqtt_stop())
        
    async def _mqtt_stop(self):
        '''
        Shutdown MQTT connection and stop all tasks
        '''
        self._log.info('received SIGINT/SIGTERM, shutting down')
        self._publish('status', 'Shutdown')
        if self._MQTT_connected:
            while len(self._publish_tasks) != 0:
                self.log.debug('waiting for {} publish tasks to complete'.format(len(self._publish_tasks)))
                await asyncio.sleep(1)
            self._mqttc._client.disconnect()
            self._mqttc = None
        self._exit = True
        [task.cancel() for task in self._tasks if not task.done()]
        self._tasks = set()
        self._log.info('{} stopped'.format(self._name or 'MQTT'))
