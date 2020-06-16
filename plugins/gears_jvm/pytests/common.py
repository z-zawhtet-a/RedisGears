from RLTest import Env
import json
import signal
import time


class TimeLimit(object):
    """
    A context manager that fires a TimeExpired exception if it does not
    return within the specified amount of time.
    """

    def __init__(self, timeout):
        self.timeout = timeout

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handler)
        signal.setitimer(signal.ITIMER_REAL, self.timeout, 0)

    def __exit__(self, exc_type, exc_value, traceback):
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

    def handler(self, signum, frame):
        raise Exception('timeout')

class Colors(object):
    @staticmethod
    def Cyan(data):
        return '\033[36m' + data + '\033[0m'

    @staticmethod
    def Yellow(data):
        return '\033[33m' + data + '\033[0m'

    @staticmethod
    def Bold(data):
        return '\033[1m' + data + '\033[0m'

    @staticmethod
    def Bred(data):
        return '\033[31;1m' + data + '\033[0m'

    @staticmethod
    def Gray(data):
        return '\033[30;1m' + data + '\033[0m'

    @staticmethod
    def Lgray(data):
        return '\033[30;47m' + data + '\033[0m'

    @staticmethod
    def Blue(data):
        return '\033[34m' + data + '\033[0m'

    @staticmethod
    def Green(data):
        return '\033[32m' + data + '\033[0m'

BASE_JAR_FILE = '../gears_tests/bin/gears_tests.jar'

def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster':
        env.broadcast('rg.refreshcluster')
        conn = env.envRunner.getClusterConnection()
    else:
        conn = env.getConnection()
    return conn

def putKeys(data):
    def func(conn, **kargs):
        for k, v in data.items():
            if isinstance(v, str):
                conn.execute_command('set', k, v)
            elif isinstance(v, list):
                conn.execute_command('lpush', k, *v)
            elif isinstance(v, dict):
                for key, val in v.items():
                    conn.execute_command('hset', k, key, val)
    return func

def verifyRegistrationIntegrity(env):
    try:
        with TimeLimit(1):
            while True:
                script = '''
GB('ShardsIDReader').map(lambda x: len(execute('RG.DUMPREGISTRATIONS'))).collect().distinct().count().run()
'''
                res = env.cmd('RG.PYEXECUTE', script)
                if int(res[0][0]) == 1:
                    break
                time.sleep(0.1)
    except Exception as e:
        print(Colors.Bred(str(e)))
        env.assertTrue(False, message='Registrations Integrity failed')

def dropRegistrationsAndExecutions(env):
    executions = env.cmd('RG.DUMPEXECUTIONS')
    for e in executions:
        env.cmd('RG.DROPEXECUTION', e[1])

    registrations = env.cmd('RG.DUMPREGISTRATIONS')
    for r in registrations:
        env.expect('RG.UNREGISTER', r[1]).equal('OK')

    try:
        with TimeLimit(1):
            while True:
                script1 = '''
GB('ShardsIDReader').map(lambda x: len(execute('RG.DUMPREGISTRATIONS'))).filter(lambda x: x > 0).run()
'''
                script2 = '''
GB('ShardsIDReader').map(lambda x: len(execute('RG.DUMPEXECUTIONS'))).filter(lambda x: x > 0).run()
'''
                res1 = env.cmd('RG.PYEXECUTE', script1)
                res2 = env.cmd('RG.PYEXECUTE', script2)

                if len(res1[0]) == 0 and len(res2[0]):
                    break
                time.sleep(0.1)
    except Exception as e:
        print(Colors.Bred(str(e)))
        env.assertTrue(False, message='Registrations/Executions dropping failed')


def jvmTestDecorator(preExecute=None, postExecution=None, envArgs={}):
    def jvmTest(testFunc):
        def jvmTestFunc():
            testName = 'gears_tests.%s' % testFunc.func_name 
            print(Colors.Cyan('\tRunning: %s' % testName))
            env = Env(testName = testName, **envArgs)
            conn = getConnectionByEnv(env)
            executionError = None
            res = [[],[]]
            if preExecute is not None:
                kargs = {
                    'conn': conn,
                    'env': env
                }
                preExecute(**kargs)
            with open(BASE_JAR_FILE, 'rb') as f:
                data = f.read()
                try:
                    res = env.cmd('RG.JEXECUTE', testName, data)
                    verifyRegistrationIntegrity(env)
                except Exception as e:
                    executionError = str(e)
            if res == 'OK':
                results = 'OK'    
            else:
                results = [json.loads(r) for r in res[0]]
            errs = res[1]
            kargs = {
                'env': env,
                'results': results,
                'errs': errs,
                'executionError': executionError,
                'conn': conn
            }
            testFunc(**kargs)

            if postExecution is not None:
                kargs = {
                    'env': env,
                    'conn': conn
                }
                postExecution(**kargs)

            dropRegistrationsAndExecutions(env)
        return jvmTestFunc
    return jvmTest
