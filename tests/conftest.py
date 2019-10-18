# content of conftest.py


def pytest_generate_tests(metafunc):
    if 'nworkers' in metafunc.fixturenames:
        nworkers = [2]
        metafunc.parametrize("nworkers", nworkers)
