from cpx_utilities.framework.schedule_manager import ScheduleManager
from .db_connection import collection
import logging

SERVICE_NAME = "todo_publisher"

class App():
    def __init__(self):
        self._logger = logging.getLogger()

    def execute(self,args):
        routing_key = "message_publisher_test1"
        data = collection.find({})
        for d in data:
            message = {
                    "service":"message_publisher_test1",
                    "task":d["task"] 
                }
            self._logger.info("publishing message with routing key "+routing_key)
            ScheduleManager.get_instance().publish_message(message,routing_key)

if __name__=='__main__':
    app = App()
    schedule_manager = ScheduleManager(app, SERVICE_NAME, 10)
    schedule_manager.start()