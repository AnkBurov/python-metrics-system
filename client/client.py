import abc
import time
import socket
from collections import OrderedDict


class Command:

    @abc.abstractmethod
    def to_command_str(self):
        pass


class PutCommand(Command):

    def __init__(self, metric, value, timestamp) -> None:
        self.command = "put"
        self.metric = metric
        self.value = value
        self.timestamp = int(timestamp)

    def to_command_str(self):
        return f"{self.command} {self.metric} {self.value} {self.timestamp}\n"


class GetCommandResponse:

    def __init__(self, *args):
        self.metric = args[0]
        self.value = float(args[1])
        self.timestamp = int(float(args[2]))


class GetCommand(Command):

    def __init__(self, metric) -> None:
        self.command = "get"
        self.metric = metric

    def to_command_str(self):
        return f"{self.command} {self.metric}\n"

    def parse_response(self, metrics_response):
        received_metrics = {}

        splitted = metrics_response.split("\n")
        if len(splitted) <= 4:
            return received_metrics
        # use only meaningful elements
        raw_metrics = splitted[1:-2]

        # group by metrics name
        for raw_metrics in raw_metrics:
            metric_model = GetCommandResponse(*raw_metrics.split(" "))

            if metric_model.metric not in received_metrics:
                received_metrics[metric_model.metric] = []
            received_metrics[metric_model.metric].append((metric_model.timestamp, metric_model.value))

        # sort values
        for metrics_history in received_metrics.values():
            metrics_history.sort(key=lambda x: x[0])

        return received_metrics


class ClientError(Exception):
    pass


class Client:

    def __init__(self, host: str, port: int, timeout=None) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout or 0

    def put(self, metric, value, timestamp=time.time()):
        timestamp = int(timestamp)

        command = PutCommand(metric, value, timestamp)
        self.__send(command, "ok\n\n")

    def get(self, metric):
        command = GetCommand(metric)

        response = self.__send(command, "ok\n")

        return command.parse_response(response)

    def __send(self, command: Command, expected_answer):

        with socket.create_connection((self.host, self.port), timeout=self.timeout) as sock:
            try:
                print(f"Sending: {command.to_command_str()}")
                sock.sendall(command.to_command_str().encode("utf8"))
                print("Sent")

                # handle response
                response = b""
                while not response.endswith(b"\n\n"):
                    response += sock.recv(1024)

                decoded_response = response.decode("utf8")
                if not decoded_response.startswith(expected_answer):
                    raise ClientError(f"Failed to receive ok anwer, the actual answer is {response}")
                print(f"Received: {decoded_response}")
                return decoded_response
            except Exception as e:
                raise ClientError("Unknown error", e)
