from cpx_utilities.framework import event_manager
from cpx_utilities.framework.event_manager import EventManager
import logging
from .db_connection import collection
import datetime

PUBLISHER_NAME = "message_publisher"

class App():
    def __init__(self):
        self._logger = logging.getLogger()

    def execute(self,message):
        task = message["task"]
        service = message["service"]
        self._logger.info("message recieved, task: "+ task + "")
        collection.insert_one({"task":task,"service":service,"last_time_processed":datetime.datetime.now()})
        self._logger.info("message written to db")

if __name__=='__main__':
    routing_keys = ["message_publisher_test1"]
    app = App()
    event_manager = EventManager(app, PUBLISHER_NAME, routing_keys)
    event_manager.start()