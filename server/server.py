import abc
import asyncio

metrics_storage = {}


class Command:

    @staticmethod
    def supports(raw_metrics_string):
        pass

    @classmethod
    def parse(cls, raw_metrics_string):
        pass

    @abc.abstractmethod
    def execute(self):
        pass


class PutCommand(Command):

    def __init__(self, command, metric, value, timestamp) -> None:
        self.command = command
        self.metric = metric
        self.value = value
        self.timestamp = int(timestamp)

    @staticmethod
    def supports(raw_metrics_string: str):
        return raw_metrics_string.startswith("put")

    @classmethod
    def parse(cls, raw_metrics_string):
        raw_metric = raw_metrics_string.replace("\n", "").split(" ")
        return cls(*raw_metric)

    def execute(self):
        corr = _put_metric_to_storage()
        next(corr)
        corr.send(self)
        return "ok\n\n"


class GetCommand(Command):

    def __init__(self, command, metric):
        self.command = command
        self.metric = metric

    @staticmethod
    def supports(raw_metrics_string):
        return raw_metrics_string.startswith("get")

    @classmethod
    def parse(cls, raw_metrics_string):
        raw_metric = raw_metrics_string.replace("\n", "").split(" ")
        return cls(*raw_metric)

    def execute(self):
        corr = _get_metrics_from_storage()
        next(corr)
        metrics = corr.send(self)
        return "ok\n" + metrics + "\n"


def _put_metric_to_storage():
    while True:
        command = yield

        if command.metric not in metrics_storage:
            metrics_storage[command.metric] = []
        metric_history = metrics_storage[command.metric]
        metric_value = (command.value, command.timestamp)
        if metric_value not in metric_history:
            metric_history.append(metric_value)
            metric_history.sort(key=lambda x: x[1])


def _get_metrics_from_storage():
    while True:
        command = yield

        metrics = ""
        for key, value in metrics_storage.items():
            if key == command.metric or command.metric == "*":
                metrics_history_set = value
                for metric_history in metrics_history_set:
                    metrics = metrics + f"{key} {metric_history[0]} {metric_history[1]}\n"
        yield metrics


def process_data(request: str):
    available_commands = [PutCommand, GetCommand]
    supported_commands = list(filter(lambda comm: comm.supports(request), available_commands))
    if len(supported_commands) == 0:
        return "error\nwrong command\n\n"
    command_class = supported_commands[0]
    command = command_class.parse(request)
    return command.execute()


class ClientServerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        resp = process_data(data.decode())
        self.transport.write(resp.encode())


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(
        ClientServerProtocol,
        host, port
    )

    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()
