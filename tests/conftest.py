import pytest
import docker
import ibis
import time

# Constants
GIZMOSQL_PORT = 31338


# Function to wait for a specific log message indicating the container is ready
def wait_for_container_log(container, timeout=30, poll_interval=1, ready_message="GizmoSQL server - started"):
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Get the logs from the container
        logs = container.logs().decode('utf-8')

        # Check if the ready message is in the logs
        if ready_message in logs:
            return True

        # Wait for the next poll
        time.sleep(poll_interval)

    raise TimeoutError(f"Container did not show '{ready_message}' in logs within {timeout} seconds.")


@pytest.fixture(scope="session")
def gizmosql_server():
    client = docker.from_env()
    container = client.containers.run(
        image="gizmodata/gizmosql:latest",
        name="ibis-gizmosql-test",
        detach=True,
        remove=True,
        tty=True,
        init=True,
        ports={f"{GIZMOSQL_PORT}/tcp": GIZMOSQL_PORT},
        environment={"GIZMOSQL_USERNAME": "gizmosql_username",
                     "GIZMOSQL_PASSWORD": "gizmosql_password",
                     "TLS_ENABLED": "1",
                     "PRINT_QUERIES": "1"
                     },
        stdout=True,
        stderr=True
    )

    # Wait for the container to be ready
    wait_for_container_log(container)

    yield container

    container.stop()


@pytest.fixture(scope="session")
def con(gizmosql_server):
    con = ibis.gizmosql.connect(host="localhost",
                                user="gizmosql_username",
                                password="gizmosql_password",
                                port=GIZMOSQL_PORT,
                                use_encryption=True,
                                disable_certificate_verification=True
                                )
    return con
