import time
import json
import logging
from enum import Enum

import requests
import pandas


LOGGER = logging.getLogger(__name__)

DEFAULT_URL = 'http://localhost:8998'

SERIALISE_DATAFRAME_TEMPLATE = """
for _livy_client_serialised_row in {}.toJSON().collect():
    print(_livy_client_serialised_row)
"""


def extract_serialised_dataframe(text):
    rows = []
    for line in text.split('\n'):
        rows.append(json.loads(line))
    return pandas.DataFrame(rows)


class Livy:
    
    def __init__(self, url=DEFAULT_URL, echo=True, check=True):
        self.manager = SessionManager(url)
        self.session = None
        self.echo = echo
        self.check = check
        
    def __enter__(self):
        self.start()
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        
    def start(self):
        self.session = self.manager.create_session()
        
    def close(self):
        self.session.kill()

    def run(self, code):
        output = self._execute(code)
        if self.echo and output.text:
            print(output.text)
        if self.check:
            output.raise_for_status()
        return output
        
    def read(self, dataframe_name):
        code = SERIALISE_DATAFRAME_TEMPLATE.format(dataframe_name)
        output = self._execute(code)
        output.raise_for_status()
        return extract_serialised_dataframe(output.text)

    def _execute(self, code):
        self._wait_for_session()
        LOGGER.info('Beginning code statement execution')
        statement = self.session.run_statement(code)
        statement.wait()
        LOGGER.info(
            'Completed code statement execution with status '
            f'{statement.output.status}'
        )
        return statement.output
        
    def _wait_for_session(self):
        if not self.session.ready():
            LOGGER.info('Waiting for session to start')
            while not self.session.ready():
                time.sleep(1.0)
                self.session.refresh()
            LOGGER.info('Session ready')


class JsonClient:
    
    def __init__(self, url):
        self.url = url
        
    def get(self, endpoint=''):
        response = requests.get(self._endpoint(endpoint))
        response.raise_for_status()
        return response.json()
    
    def post(self, endpoint, data=None):
        response = requests.post(self._endpoint(endpoint), json=data)
        response.raise_for_status()
        return response.json()
        
    def delete(self, endpoint=''):
        response = requests.delete(self._endpoint(endpoint))
        response.raise_for_status()
        return response.json()
        
    def _endpoint(self, endpoint):
        return self.url.rstrip('/') + endpoint
        
        
class SessionManager:
    
    def __init__(self, url):
        self.url = url
        self._client = JsonClient(url)
        
    def list_sessions(self):
        response = self._client.get('/sessions')
        return [
            Session.from_json(self.url, data)
            for data in response['sessions']
        ]

    def create_session(self, session_type='pyspark'):
        data = {'kind': session_type}
        response = self._client.post('/sessions', data)
        return Session.from_json(self.url, response)


class SessionState(Enum):
    NOT_STARTED = 'not_started'
    STARTING = 'starting'
    IDLE = 'idle'
    BUSY = 'busy'
    SHUTTING_DOWN = 'shutting_down'
    ERROR = 'error'
    DEAD = 'dead'
    SUCCESS = 'success'


class Session:
    
    def __init__(self, url, id_, state):
        self.url = url
        self.id_ = id_
        self.state = state
        self._client = JsonClient(f'{url}/sessions/{id_}')
        
    @classmethod
    def from_json(cls, url, data):
        return cls(url, data['id'], SessionState(data['state']))
    
    def __repr__(self):
        name = self.__class__.__name__
        return (
            f'{name}(url={self.url!r}, id_={self.id_}, '
            f'state={self.state!r})'
        )
    
    def run_statement(self, code):
        response = self._client.post('/statements', data={'code': code})
        return Statement.from_json(self.url, self.id_, response)
    
    def get_statements(self):
        response = self._client.get('/statements')
        return [
            Statement.from_json(self.url, self.id_, data)
            for data in response['statements']
        ]
        
    def ready(self):
        non_ready_states = {SessionState.NOT_STARTED, SessionState.STARTING}
        return self.state not in non_ready_states
        
    def refresh(self):
        response = self._client.get('/state')
        self.state = SessionState(response['state'])
    
    def kill(self):
        self._client.delete()


class StatementState(Enum):
    WAITING = 'waiting'
    RUNNING = 'running'
    AVAILABLE = 'available'
    ERROR = 'error'
    CANCELLING = 'cancelling'
    CANCELLED = 'cancelled'
    

class Statement:
    
    def __init__(self, url, session_id, id_, state, output):
        self.url = url
        self.session_id = session_id
        self.id_ = id_
        self.state = state
        self.output = output
        
        self._client = JsonClient(
            f'{url}/sessions/{session_id}/statements/{id_}'
        )
        
    @classmethod
    def from_json(cls, url, session_id, data):
        return cls(
            url, session_id,
            data['id'], StatementState(data['state']), data['output']
        )
    
    def __repr__(self):
        name = self.__class__.__name__
        return (
            f'{name}('
            f'url={self.url!r}, session_id={self.session_id}, id_={self.id_}, '
            f'state={self.state!r}, output={self.output!r})'
        )
    
    def refresh(self):
        response = self._client.get()
        
        if response['id'] != self.id_:
            raise RuntimeError('mismatched ids')
            
        self.state = StatementState(response['state'])
        
        if response['output'] is None:
            self.output = None
        else:
            self.output = Output.from_json(response['output'])
        
    def wait(self, interval=1.0):
        while self.state in {StatementState.WAITING, StatementState.RUNNING}:
            time.sleep(interval)
            self.refresh()
            
    
    
class SparkRuntimeError(Exception):
    
    def __init__(self, ename, evalue, traceback):
        self.ename = ename
        self.evalue = evalue
        self.traceback = traceback
        
    def __repr__(self):
        name = self.__class__.__name__
        components = []
        if self.ename is not None:
            components.append(f'ename={self.ename!r}')
        if self.evalue is not None:
            components.append(f'evalue={self.evalue!r}')
        return f'{name}({", ".join(components)})'
        
            
class OutputStatus(Enum):
    OK = 'ok'
    ERROR = 'error'


class Output:
    
    def __init__(self, status, text=None, ename=None, evalue=None,
                 traceback=None):
        self.status = status
        self.text = text
        self.ename = ename
        self.evalue = evalue
        self.traceback = traceback
        
    @classmethod
    def from_json(cls, data):
        return cls(
            OutputStatus(data['status']),
            data.get('data', {}).get('text/plain'),
            data.get('ename'),
            data.get('evalue'),
            data.get('traceback')
        )
        
    def __repr__(self):
        name = self.__class__.__name__
        components = [f'status={self.status!r}']
        if self.text is not None:
            components.append(f'text={self.text!r}')
        if self.ename is not None:
            components.append(f'ename={self.ename!r}')
        if self.traceback is not None:
            components.append(f'traceback={self.traceback!r}')
        return f'{name}({", ".join(components)})'
        
    def raise_for_status(self):
        if self.status == OutputStatus.ERROR:
            raise SparkRuntimeError(self.ename, self.evalue, self.traceback)