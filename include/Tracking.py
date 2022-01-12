from datetime import datetime

# force a change :-)

class Tracking:
    def __init__(self, name):
        self.name = name
        self.parameters = {'name': [], 'value': []}
        self.end_time = 0
        self.duration = 'Not set'
        self.success = False
        self.error_message = "Not set"
        self.start_time = datetime.now()

    def record_parameter(self, name, value):
        self.parameters['name'].append(name)
        self.parameters['value'].append(value)


    def failed(self, message=""):
        self.end_time = datetime.now()
        self.duration = self.end_time - self.start_time
        if (message != ""):
            self.error_message = message
        self.success = False

    def succeeded(self):
        self.end_time = datetime.now()
        self.duration = self.end_time - self.start_time
        self.success = True

    def set_error_message(self, msg):
        self.error_message = msg

    def get_summary(self):
        summary = "Tracking: Name:       {}\n".format(self.name)
        summary += "          Success:    {}\n".format(self.success)
        if (not self.success):
            summary += "          Message:    {}\n".format(self.error_message)
        n = len(self.parameters['name'])
        if (n > 0):
            summary += "          Parameters: Name \tValue\n"
            for i in range(0, n):
                summary += "                      {}\t\t{}\n".format(self.parameters['name'][i],
                                                                     self.parameters['value'][i])

        summary += "          Start:      {}\n".format(self.start_time)
        summary += "          End:        {}\n".format(self.end_time)
        summary += "          Duration:   {}\n".format(self.duration)

        return summary