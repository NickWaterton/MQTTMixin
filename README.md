Async Python MQTT Client library, intended to be used as a Mixin

# Install

```
pip install https://github.com/NickWaterton/MQTTMixin.git
```
**NOTE:** `pip` may be `pip3` on your system.  

# Usage

Add the class as a mixin to a class that you are creating, to include MQTT functionality:

```python
from MQTTMixin import MQTTMixin

class MyClass(MQTTMixin):

    def __init__(self, myargs, mykwvariables=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        ... my intialization stuff

    async def mynum(self, num):
         print(num)
         return num+1
```
The class should be intialized with the following variables:
```python
r = MyClass( myargs,
             mykwvariables   = kwvar,
             ip              = broker,
             port            = port,
             user            = user,
             mqtt_password   = password,
             pubtopic        = feedback,
             topic           = topic,
             name            = "name",
             poll            = (poll_interval, poll_methods),
             json_out        = False,
             log             = None
              )
```
Where the variables are:
ip              = MQTT broker ip as a string eg '192.168.100.16', default is None
port            = MQTT broker port eg 1883 (this is the default)
user            = optional MQTT user, default is None
mqtt_password   = optional MQTT password, default is None
pubtopic        = topic to publish feedback to default is 'default'
topic           = topic to receive commands on, default is '/default/#'
name            = "name" a useful name for your class, defsult is None, if given it is added to the topics for publish eg '/default/name/ 
poll            = (poll_interval, poll_methods) a tuple with an integer interval in seconds, and a method of your class to call
json_out        = publish topics as json or not, default is False
log             = optional logging object, default is None.

# Pupose
This mixin class allows you to send mqtt messages to the subscribed topic (topic) which are interpreted as async commands with arguments, the default delimiter is '='. eg
```
mosuquitto_pub -t default/name/mynum -m 2
or
mosuquitto_pub -t default/name -m mynum=2
```
args can be passed with `,` delimiting them. The delimiter can be changed by changing `self._delimiter` (default is `r'\='`)
The ouput of the command is published to pubtopic.
any method of your class can be called if it does not start with an `_`.
to exclude other methods you don't want to be able to be called, extend the list `self.invalid_commands`.
All methods should be async methods.
