# test.py
from fluent import sender
from fluent import event
sender.setup('fluentd.test', host='localhost', port=24224)
event.Event('follow', {
  'from': 'userA',
  'to':   'userB'
})

#from fluent import sender

logger = sender.FluentSender('app', host='192.168.7.246', port=24224)
logger.emit('test', {'message': 'Hello, Fluentd!'})
